#include "proxy.h"
#include "parity_log_store.h"
#include "stripe_update.h"
#include "unilrc_encoder.h"
#include <iostream>

namespace ECProject
{
  namespace
  {
    datanode_proto::datanodeService::Stub *datanode_stub_for(
        std::map<std::string, std::unique_ptr<datanode_proto::datanodeService::Stub>> &ptrs, const char *ip, int port)
    {
      const std::string key = std::string(ip) + ":" + std::to_string(port);
      auto it = ptrs.find(key);
      if (it == ptrs.end())
        return nullptr;
      return it->second.get();
    }
  } // namespace

  bool ProxyImpl::execute_stripe_partial_update(const proxy_proto::CordDataUpdatePlacement &placement, const char *buf,
                                                 size_t payload_size)
  {
    const int slice_num = placement.blockkeys_size();
    if (slice_num <= 0 || static_cast<size_t>(payload_size) != placement.update_payload_size())
      return false;

    std::vector<size_t> sizes;
    sizes.reserve(static_cast<size_t>(slice_num));
    for (int i = 0; i < slice_num; ++i)
      sizes.push_back(static_cast<size_t>(placement.sizes(i)));

    std::vector<char *> slices = m_toolbox->splitCharPointer(buf, payload_size, sizes);
    const int parities_per_slice = placement.r() + 1;
    if (placement.parity_block_ids_size() != slice_num * parities_per_slice)
    {
      std::cout << "[StripeUpdate] parity metadata size mismatch\n";
      return false;
    }

    for (int j = 0; j < slice_num; ++j)
    {
      const int bid = placement.blockids(j);
      const int off = static_cast<int>(placement.offsets(j));
      const int len = static_cast<int>(placement.sizes(j));
      const char *new_data = slices[static_cast<size_t>(j)];
      std::vector<int> need_d0_indices;
      for (int pi = 0; pi < parities_per_slice; ++pi)
      {
        const int idx = j * parities_per_slice + pi;
        const int pblk = placement.parity_block_ids(idx);
        const std::string &pkey = placement.parity_block_keys(idx);
        const char *pip = placement.parity_datanode_ips(idx).c_str();
        const int pport = placement.parity_datanode_ports(idx);
        auto *stub = datanode_stub_for(m_datanode_ptrs, pip, pport);
        if (stub == nullptr)
        {
          std::cout << "[StripeUpdate] no datanode stub for parity " << pblk << std::endl;
          return false;
        }
        grpc::ClientContext ctx;
        datanode_proto::ParityLogAppendInfo req;
        datanode_proto::ParityLogAppendReply rep;
        req.set_stripe_id(placement.stripe_id());
        req.set_parity_block_id(pblk);
        req.set_parity_block_key(pkey);
        req.set_data_block_id(bid);
        req.set_range_offset(off);
        req.set_range_length(len);
        req.set_k(placement.k());
        req.set_r(placement.r());
        req.set_z(placement.z());
        req.set_block_size(placement.block_size());
        req.set_code_type(placement.code_type());
        req.set_new_data(new_data, static_cast<size_t>(len));
        grpc::Status st = stub->handleParityLogAppend(&ctx, req, &rep);
        if (!st.ok() || !rep.ok())
        {
          std::cout << "[StripeUpdate] parity log append failed blk=" << pblk << std::endl;
          return false;
        }
        if (rep.need_d0())
          need_d0_indices.push_back(idx);
      }

      if (!need_d0_indices.empty())
      {
        std::vector<char> d0(static_cast<size_t>(len));
        if (!CordRangeReadFromDatanode(placement.blockkeys(j), bid, off, d0.data(), static_cast<size_t>(len),
                                       placement.datanodeip(j).c_str(), placement.datanodeport(j)))
        {
          std::cout << "[StripeUpdate] read D0 failed data_blk=" << bid << std::endl;
          return false;
        }
        for (int idx : need_d0_indices)
        {
          const int pblk = placement.parity_block_ids(idx);
          auto *stub = datanode_stub_for(m_datanode_ptrs, placement.parity_datanode_ips(idx).c_str(),
                                         placement.parity_datanode_ports(idx));
          if (stub == nullptr)
            return false;
          grpc::ClientContext ctx;
          datanode_proto::ParityLogStoreD0Info req;
          datanode_proto::RequestResult rep;
          req.set_stripe_id(placement.stripe_id());
          req.set_parity_block_id(pblk);
          req.set_data_block_id(bid);
          req.set_range_offset(off);
          req.set_range_length(len);
          req.set_d0(d0.data(), static_cast<size_t>(len));
          grpc::Status st = stub->handleParityLogStoreD0(&ctx, req, &rep);
          if (!st.ok() || !rep.message())
          {
            std::cout << "[StripeUpdate] store D0 failed parity_blk=" << pblk << std::endl;
            return false;
          }
        }
      }

      if (!CordRangeWriteToDatanode(placement.blockkeys(j), bid, off, new_data, static_cast<size_t>(len),
                                    placement.datanodeip(j).c_str(), placement.datanodeport(j)))
      {
        std::cout << "[StripeUpdate] data write failed blk=" << bid << std::endl;
        return false;
      }
    }
    return true;
  }

  bool ProxyImpl::execute_stripe_full_update(const proxy_proto::CordDataUpdatePlacement &placement, const char *buf,
                                             size_t payload_size)
  {
    const int k = placement.k();
    const int r = placement.r();
    const int z = placement.z();
    const int block_size = placement.block_size();
    const int n = k + r + z;
    if (k <= 0 || block_size <= 0 || static_cast<size_t>(payload_size) != static_cast<size_t>(k) * static_cast<size_t>(block_size))
      return false;
    if (placement.all_block_ids_size() != n || placement.all_block_keys_size() != n ||
        placement.all_datanode_ips_size() != n || placement.all_datanode_ports_size() != n)
    {
      std::cout << "[StripeUpdate] full stripe block metadata incomplete\n";
      return false;
    }

    std::vector<char *> data_ptrs(static_cast<size_t>(k));
    for (int i = 0; i < k; ++i)
      data_ptrs[static_cast<size_t>(i)] = const_cast<char *>(buf + static_cast<size_t>(i) * static_cast<size_t>(block_size));

    std::vector<std::vector<char>> parity_bufs(static_cast<size_t>(r + z));
    std::vector<unsigned char *> parity_ptrs(static_cast<size_t>(r + z));
    for (int i = 0; i < r + z; ++i)
    {
      parity_bufs[static_cast<size_t>(i)].assign(static_cast<size_t>(block_size), 0);
      parity_ptrs[static_cast<size_t>(i)] = reinterpret_cast<unsigned char *>(parity_bufs[static_cast<size_t>(i)].data());
    }

    std::vector<unsigned char *> up_data(static_cast<size_t>(k));
    for (int i = 0; i < k; ++i)
      up_data[static_cast<size_t>(i)] = reinterpret_cast<unsigned char *>(data_ptrs[static_cast<size_t>(i)]);

    if (placement.code_type() == "UniLRC")
      encode_unilrc(k, r, z, up_data.data(), parity_ptrs.data(), block_size);
    else
      encode_azure_lrc(k, r, z, up_data.data(), parity_ptrs.data(), block_size);

    for (int i = 0; i < n; ++i)
    {
      const char *payload = nullptr;
      if (i < k)
        payload = data_ptrs[static_cast<size_t>(i)];
      else
        payload = parity_bufs[static_cast<size_t>(i - k)].data();
      if (!CordRangeWriteToDatanode(placement.all_block_keys(i), placement.all_block_ids(i), 0, payload,
                                    static_cast<size_t>(block_size), placement.all_datanode_ips(i).c_str(),
                                    placement.all_datanode_ports(i)))
      {
        std::cout << "[StripeUpdate] full write failed blk=" << placement.all_block_ids(i) << std::endl;
        return false;
      }
    }

    for (int p = k; p < n; ++p)
    {
      auto *stub = datanode_stub_for(m_datanode_ptrs, placement.all_datanode_ips(p).c_str(),
                                     placement.all_datanode_ports(p));
      if (stub == nullptr)
        continue;
      grpc::ClientContext ctx;
      datanode_proto::ParityLogClearStripeInfo req;
      datanode_proto::RequestResult rep;
      req.set_stripe_id(placement.stripe_id());
      req.set_parity_begin(k);
      req.set_parity_end(n);
      stub->handleParityLogClearStripe(&ctx, req, &rep);
    }
    return true;
  }
} // namespace ECProject
