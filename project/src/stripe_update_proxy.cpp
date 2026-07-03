#include "proxy.h"
#include "parity_log_store.h"
#include "stripe_update.h"
#include "unilrc_encoder.h"
#include <atomic>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

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

    bool stripe_parity_is_remote(const proxy_proto::CordDataUpdatePlacement &placement, int parity_idx)
    {
      if (placement.parity_cluster_ids_size() <= parity_idx)
        return false;
      return placement.parity_cluster_ids(parity_idx) != placement.cluster_id();
    }

    bool stripe_parity_peer_ready(const proxy_proto::CordDataUpdatePlacement &placement, int parity_idx,
                                  std::string *out_ip, int *out_port)
    {
      if (placement.parity_proxy_ips_size() <= parity_idx || placement.parity_proxy_ports_size() <= parity_idx)
        return false;
      *out_ip = placement.parity_proxy_ips(parity_idx);
      *out_port = placement.parity_proxy_ports(parity_idx);
      return !out_ip->empty() && *out_port > 0;
    }

    bool stripe_send_parity_append(ProxyImpl *proxy, const proxy_proto::CordDataUpdatePlacement &placement, int parity_idx,
                                   int data_block_id, int range_off, int range_len, const char *new_data, bool *need_d0)
    {
      const int pblk = placement.parity_block_ids(parity_idx);
      const std::string &pkey = placement.parity_block_keys(parity_idx);
      const char *pip = placement.parity_datanode_ips(parity_idx).c_str();
      const int pport = placement.parity_datanode_ports(parity_idx);

      datanode_proto::ParityLogAppendInfo meta;
      meta.set_stripe_id(placement.stripe_id());
      meta.set_parity_block_id(pblk);
      meta.set_parity_block_key(pkey);
      meta.set_data_block_id(data_block_id);
      meta.set_range_offset(range_off);
      meta.set_range_length(range_len);
      meta.set_k(placement.k());
      meta.set_r(placement.r());
      meta.set_z(placement.z());
      meta.set_block_size(placement.block_size());
      meta.set_code_type(placement.code_type());

      if (stripe_parity_is_remote(placement, parity_idx))
      {
        std::string peer_ip;
        int peer_port = 0;
        if (!stripe_parity_peer_ready(placement, parity_idx, &peer_ip, &peer_port))
        {
          std::cout << "[StripeUpdate] parity append missing peer meta blk=" << pblk << std::endl;
          return false;
        }
        proxy_proto::StripeParityLogAppendXfer xfer;
        xfer.set_stripe_id(placement.stripe_id());
        xfer.set_parity_block_id(pblk);
        xfer.set_parity_block_key(pkey);
        xfer.set_data_block_id(data_block_id);
        xfer.set_range_offset(range_off);
        xfer.set_range_length(range_len);
        xfer.set_k(placement.k());
        xfer.set_r(placement.r());
        xfer.set_z(placement.z());
        xfer.set_block_size(placement.block_size());
        xfer.set_code_type(placement.code_type());
        xfer.set_parity_datanode_ip(pip);
        xfer.set_parity_datanode_port(pport);
        const std::string meta_bytes = xfer.SerializeAsString();
        if (proxy->send_stripe_parity_append_peer(peer_ip, peer_port, meta_bytes, new_data,
                                                  static_cast<size_t>(range_len), need_d0))
          return true;
        std::cout << "[StripeUpdate] parity append peer xfer failed blk=" << pblk << " -> proxy " << peer_ip << ":"
                  << peer_port << " fallback direct dn=" << pip << ":" << pport << std::endl;
      }

      if (!proxy->has_datanode_stub(pip, pport))
      {
        std::cout << "[StripeUpdate] parity append no datanode stub blk=" << pblk << " dn=" << pip << ":" << pport
                  << std::endl;
        return false;
      }
      return proxy->ParityLogAppendToDatanode(meta, new_data, static_cast<size_t>(range_len), pip, pport, need_d0);
    }

    bool stripe_send_parity_store_d0(ProxyImpl *proxy, const proxy_proto::CordDataUpdatePlacement &placement,
                                     int parity_idx, int data_block_id, int range_off, int range_len, const char *d0)
    {
      const int pblk = placement.parity_block_ids(parity_idx);
      const char *pip = placement.parity_datanode_ips(parity_idx).c_str();
      const int pport = placement.parity_datanode_ports(parity_idx);

      datanode_proto::ParityLogStoreD0Info meta;
      meta.set_stripe_id(placement.stripe_id());
      meta.set_parity_block_id(pblk);
      meta.set_data_block_id(data_block_id);
      meta.set_range_offset(range_off);
      meta.set_range_length(range_len);

      if (stripe_parity_is_remote(placement, parity_idx))
      {
        std::string peer_ip;
        int peer_port = 0;
        if (!stripe_parity_peer_ready(placement, parity_idx, &peer_ip, &peer_port))
        {
          std::cout << "[StripeUpdate] store D0 missing peer meta parity_blk=" << pblk << std::endl;
          return false;
        }
        proxy_proto::StripeParityLogStoreD0Xfer xfer;
        xfer.set_stripe_id(placement.stripe_id());
        xfer.set_parity_block_id(pblk);
        xfer.set_data_block_id(data_block_id);
        xfer.set_range_offset(range_off);
        xfer.set_range_length(range_len);
        xfer.set_parity_datanode_ip(pip);
        xfer.set_parity_datanode_port(pport);
        const std::string meta_bytes = xfer.SerializeAsString();
        if (proxy->send_stripe_parity_d0_peer(peer_ip, peer_port, meta_bytes, d0, static_cast<size_t>(range_len)))
          return true;
        std::cout << "[StripeUpdate] store D0 peer xfer failed parity_blk=" << pblk << " -> proxy " << peer_ip << ":"
                  << peer_port << " fallback direct dn=" << pip << ":" << pport << std::endl;
      }

      if (!proxy->has_datanode_stub(pip, pport))
      {
        std::cout << "[StripeUpdate] store D0 no datanode stub parity_blk=" << pblk << " dn=" << pip << ":" << pport
                  << std::endl;
        return false;
      }
      return proxy->ParityLogStoreD0ToDatanode(meta, d0, static_cast<size_t>(range_len), pip, pport);
    }

    bool stripe_write_block(ProxyImpl *proxy, const proxy_proto::CordDataUpdatePlacement &placement, int block_idx,
                            const char *payload, size_t len)
    {
      const char *dip = placement.all_datanode_ips(block_idx).c_str();
      const int dport = placement.all_datanode_ports(block_idx);

      const bool remote_block = placement.all_block_cluster_ids_size() > block_idx &&
                                placement.all_block_cluster_ids(block_idx) != placement.cluster_id();

      if (!remote_block)
      {
        return proxy->CordRangeWriteToDatanode(placement.all_block_keys(block_idx), placement.all_block_ids(block_idx),
                                               0, payload, len, dip, dport);
      }

      if (placement.all_proxy_ips_size() <= block_idx || placement.all_proxy_ports_size() <= block_idx)
        return false;

      proxy_proto::StripeBlockWriteXfer xfer;
      xfer.set_block_key(placement.all_block_keys(block_idx));
      xfer.set_block_id(placement.all_block_ids(block_idx));
      xfer.set_range_offset(0);
      xfer.set_range_length(static_cast<int32_t>(len));
      xfer.set_datanode_ip(dip);
      xfer.set_datanode_port(dport);

      const std::string meta_bytes = xfer.SerializeAsString();
      const std::string &peer_ip = placement.all_proxy_ips(block_idx);
      const int peer_port = placement.all_proxy_ports(block_idx);
      if (proxy->send_stripe_block_write_peer(peer_ip, peer_port, meta_bytes, payload, len))
        return true;
      std::cout << "[StripeUpdate] full block peer xfer failed blk=" << placement.all_block_ids(block_idx)
                << " -> proxy " << peer_ip << ":" << peer_port << " fallback direct dn=" << dip << ":" << dport
                << std::endl;
      return proxy->CordRangeWriteToDatanode(placement.all_block_keys(block_idx), placement.all_block_ids(block_idx), 0,
                                             payload, len, dip, dport);
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

    const auto range_mu = cord_cluster_range_mu(placement.cluster_id());

    auto process_one_slice = [&](int j) -> bool {
      const int bid = placement.blockids(j);
      const int off = static_cast<int>(placement.offsets(j));
      const int len = static_cast<int>(placement.sizes(j));
      const char *new_data = slices[static_cast<size_t>(j)];

      std::vector<int> need_d0_indices;
      std::mutex need_d0_mu;
      std::atomic<bool> failed{false};

      // 推测性 D0 读与 parity append 并行：append 走 parity datanode，D0 读走 data datanode，互不冲突。
      std::vector<char> d0(static_cast<size_t>(len));
      std::atomic<bool> d0_read_ok{false};
      std::thread d0_read_thread([&]() {
        std::lock_guard<std::mutex> range_lk(*range_mu);
        const bool ok = CordRangeReadFromDatanode(placement.blockkeys(j), bid, off, d0.data(), static_cast<size_t>(len),
                                                  placement.datanodeip(j).c_str(), placement.datanodeport(j));
        d0_read_ok.store(ok);
      });

      std::vector<std::thread> append_threads;
      append_threads.reserve(static_cast<size_t>(parities_per_slice));
      for (int pi = 0; pi < parities_per_slice; ++pi)
      {
        append_threads.emplace_back([&, pi]() {
          if (failed.load())
            return;
          const int idx = j * parities_per_slice + pi;
          bool need_d0 = false;
          if (!stripe_send_parity_append(this, placement, idx, bid, off, len, new_data, &need_d0))
          {
            failed.store(true);
            std::cout << "[StripeUpdate] parity log append failed blk=" << placement.parity_block_ids(idx)
                      << " cluster=" << placement.cluster_id() << std::endl;
            return;
          }
          if (need_d0)
          {
            std::lock_guard<std::mutex> lk(need_d0_mu);
            need_d0_indices.push_back(idx);
          }
        });
      }

      for (auto &th : append_threads)
        th.join();
      d0_read_thread.join();

      if (failed.load())
        return false;

      if (!need_d0_indices.empty())
      {
        if (!d0_read_ok.load())
        {
          std::cout << "[StripeUpdate] read D0 failed data_blk=" << bid << std::endl;
          return false;
        }

        std::vector<std::thread> store_threads;
        store_threads.reserve(need_d0_indices.size());
        for (int idx : need_d0_indices)
        {
          store_threads.emplace_back([&, idx]() {
            if (failed.load())
              return;
            if (!stripe_send_parity_store_d0(this, placement, idx, bid, off, len, d0.data()))
            {
              failed.store(true);
              std::cout << "[StripeUpdate] store D0 failed parity_blk=" << placement.parity_block_ids(idx)
                        << " cluster=" << placement.cluster_id() << std::endl;
            }
          });
        }
        for (auto &th : store_threads)
          th.join();
        if (failed.load())
          return false;
      }

      {
        std::lock_guard<std::mutex> range_lk(*range_mu);
        if (!CordRangeWriteToDatanode(placement.blockkeys(j), bid, off, new_data, static_cast<size_t>(len),
                                      placement.datanodeip(j).c_str(), placement.datanodeport(j)))
        {
          std::cout << "[StripeUpdate] data write failed blk=" << bid << std::endl;
          return false;
        }
      }
      return true;
    };

    if (slice_num > 20)
    {
      std::cout << "[StripeUpdate] partial begin cluster=" << placement.cluster_id() << " stripe="
                << placement.stripe_id() << " slices=" << slice_num << std::endl;
    }

    for (int j = 0; j < slice_num; ++j)
    {
      if (!process_one_slice(j))
        return false;
      if (slice_num > 20 && ((j + 1) % 10 == 0 || j + 1 == slice_num))
      {
        std::cout << "[StripeUpdate] progress cluster=" << placement.cluster_id() << " stripe="
                  << placement.stripe_id() << " slice " << (j + 1) << "/" << slice_num << std::endl;
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

    std::cout << "[StripeUpdate] full stripe: primary cluster=" << placement.cluster_id()
              << " distributing " << n << " blocks (local + cross-rack proxy)\n";

    std::atomic<bool> failed{false};
    std::vector<std::thread> write_threads;
    write_threads.reserve(static_cast<size_t>(n));

    for (int i = 0; i < n; ++i)
    {
      write_threads.emplace_back([&, i]() {
        if (failed.load())
          return;
        const char *payload = nullptr;
        if (i < k)
          payload = data_ptrs[static_cast<size_t>(i)];
        else
          payload = parity_bufs[static_cast<size_t>(i - k)].data();
        if (!stripe_write_block(this, placement, i, payload, static_cast<size_t>(block_size)))
        {
          failed.store(true);
          std::cout << "[StripeUpdate] full write failed blk=" << placement.all_block_ids(i) << std::endl;
        }
      });
    }

    for (auto &th : write_threads)
      th.join();

    if (failed.load())
      return false;

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
