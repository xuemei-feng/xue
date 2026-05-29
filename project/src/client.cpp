#include "client.h"
#include "coordinator.grpc.pb.h"

#include <asio.hpp>
#include <thread>
#include <assert.h>
#include <chrono>
#include <sstream>
#include <cstring>
#include <map>
#include <mutex>
#include "unilrc_encoder.h"
namespace ECProject
{
  namespace
  {
    std::string proxy_endpoint_key(const std::string &proxy_ip, int proxy_port)
    {
      return proxy_ip + ":" + std::to_string(proxy_port);
    }

    std::mutex &mutex_for_proxy_endpoint(const std::string &proxy_ip, int proxy_port)
    {
      static std::mutex map_mutex;
      static std::map<std::string, std::unique_ptr<std::mutex>> endpoint_mutexes;
      const std::string key = proxy_endpoint_key(proxy_ip, proxy_port);
      std::lock_guard<std::mutex> lk(map_mutex);
      std::unique_ptr<std::mutex> &slot = endpoint_mutexes[key];
      if (!slot)
      {
        slot = std::make_unique<std::mutex>();
      }
      return *slot;
    }
    bool is_azure_like_code(const std::string &code_type)
    {
      return code_type == "AzureLRC" || code_type == "XueLRC";
    }

    double chron_elapsed_s(const std::chrono::high_resolution_clock::time_point &t0,
                           const std::chrono::high_resolution_clock::time_point &t1)
    {
      return std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();
    }

    bool append_key_uses_cluster_ingress(const std::string &key)
    {
      return key.find('c') != std::string::npos && key.find('#') != std::string::npos;
    }

    bool reply_uses_cluster_ingress(const coordinator_proto::ReplyProxyIPsPorts *reply)
    {
      if (reply == nullptr || reply->append_keys_size() <= 0)
      {
        return false;
      }
      return append_key_uses_cluster_ingress(reply->append_keys(0));
    }

    bool parse_append_key_block_ids(const std::string &key, std::vector<int> *block_ids)
    {
      return ToolBox::getInstance()->parse_append_key_tcp_block_ids(key, block_ids);
    }

    bool parse_append_key_cluster_id(const std::string &key, int *cluster_id)
    {
      if (cluster_id == nullptr)
      {
        return false;
      }
      const size_t hash_pos = key.find('#');
      if (hash_pos == std::string::npos)
      {
        return false;
      }
      const size_t cpos = key.rfind('c', hash_pos);
      if (cpos == std::string::npos)
      {
        return false;
      }
      *cluster_id = std::stoi(key.substr(cpos + 1, hash_pos - cpos - 1));
      return true;
    }

    void add_sparse_slice(std::map<int, std::vector<std::pair<int, int>>> &dst, int block_id,
                          int len, int off)
    {
      if (len <= 0)
      {
        return;
      }
      auto &vec = dst[block_id];
      vec.push_back(std::make_pair(len, off));
      std::sort(vec.begin(), vec.end(),
                [](const auto &a, const auto &b) { return a.second < b.second; });
      std::vector<std::pair<int, int>> merged;
      for (const auto &s : vec)
      {
        const int cur_l = s.second;
        const int cur_r = s.second + s.first - 1;
        if (merged.empty())
        {
          merged.push_back(s);
          continue;
        }
        int prev_l = merged.back().second;
        int prev_r = merged.back().second + merged.back().first - 1;
        if (cur_l <= prev_r + 1)
        {
          const int new_r = std::max(prev_r, cur_r);
          merged.back().second = prev_l;
          merged.back().first = new_r - prev_l + 1;
        }
        else
        {
          merged.push_back(s);
        }
      }
      vec.swap(merged);
    }

    std::vector<std::pair<int, int>> pad_xue_logical_ranges_to_unit_size(
        const std::vector<std::pair<int, int>> &logical_ranges, int block_size, int unit_size)
    {
      std::vector<std::pair<int, int>> padded;
      auto push_merged = [&](int logical_start, int logical_end) {
        if (logical_end <= logical_start)
        {
          return;
        }
        if (!padded.empty() && padded.back().second == logical_start)
        {
          padded.back().second = logical_end;
        }
        else
        {
          padded.emplace_back(logical_start, logical_end);
        }
      };
      for (const auto &r : logical_ranges)
      {
        int pos = r.first;
        const int end = r.second;
        while (pos < end)
        {
          const int block_id = pos / block_size;
          const int block_offset = pos % block_size;
          const int take = std::min(block_size - block_offset, end - pos);
          const int block_end_incl = block_offset + take - 1;
          const int u1 = block_end_incl / unit_size;
          const int padded_block_end = std::min(block_size, (u1 + 1) * unit_size);
          push_merged(pos, block_id * block_size + padded_block_end);
          pos += take;
        }
      }
      return padded;
    }

    void zero_fill_xue_unit_padding_gaps(char *stripe_buf,
                                         const std::vector<std::pair<int, int>> &logical_ranges,
                                         int block_size, int unit_size)
    {
      for (const auto &r : logical_ranges)
      {
        int pos = r.first;
        const int end = r.second;
        while (pos < end)
        {
          const int block_offset = pos % block_size;
          const int take = std::min(block_size - block_offset, end - pos);
          const int block_end_incl = block_offset + take - 1;
          const int u1 = block_end_incl / unit_size;
          const int padded_block_end = std::min(block_size, (u1 + 1) * unit_size);
          const int gap_start = block_offset + take;
          if (padded_block_end > gap_start)
          {
            std::memset(stripe_buf + static_cast<size_t>(pos + take), 0,
                        static_cast<size_t>(padded_block_end - gap_start));
          }
          pos += take;
        }
      }
    }

    std::map<int, std::vector<std::pair<int, int>>> build_xue_block_to_slices_from_ranges(
        const std::vector<std::pair<int, int>> &logical_ranges, int block_size)
    {
      std::map<int, std::vector<std::pair<int, int>>> block_to_slices;
      for (const auto &r : logical_ranges)
      {
        int pos = r.first;
        const int end = r.second;
        while (pos < end)
        {
          const int block_id = pos / block_size;
          const int block_offset = pos % block_size;
          const int take = std::min(block_size - block_offset, end - pos);
          add_sparse_slice(block_to_slices, block_id, take, block_offset);
          pos += take;
        }
      }
      return block_to_slices;
    }

    void extend_xue_parity_slices_in_block_map(
        std::map<int, std::vector<std::pair<int, int>>> *block_to_slices, int k, int n,
        int block_size, int unit_size)
    {
      if (block_to_slices == nullptr)
      {
        return;
      }
      std::map<int, std::vector<std::pair<int, int>>> data_only;
      for (const auto &kv : *block_to_slices)
      {
        if (kv.first >= 0 && kv.first < k)
        {
          data_only[kv.first] = kv.second;
        }
      }
      for (const auto &kv : data_only)
      {
        for (const auto &slice : kv.second)
        {
          const int block_off = slice.second;
          const int block_end = block_off + slice.first - 1;
          const int u0 = block_off / unit_size;
          const int u1 = block_end / unit_size;
          const int parity_off = u0 * unit_size;
          const int parity_end = std::min(block_size - 1, (u1 + 1) * unit_size - 1);
          const int parity_len = parity_end - parity_off + 1;
          for (int i = k; i < n; i++)
          {
            add_sparse_slice(*block_to_slices, i, parity_len, parity_off);
          }
        }
      }
    }

    bool pack_xue_tcp_payload_for_append_key(
        const char *stripe_buf,
        const std::map<int, std::vector<std::pair<int, int>>> &block_to_slices,
        const std::string &append_key, int block_size, std::vector<char> *out)
    {
      if (out == nullptr)
      {
        return false;
      }
      std::vector<int> tcp_block_ids;
      if (!ToolBox::getInstance()->parse_append_key_tcp_block_ids(append_key, &tcp_block_ids))
      {
        return false;
      }
      out->clear();
      std::map<int, size_t> slice_cursor;
      for (int bid : tcp_block_ids)
      {
        auto it = block_to_slices.find(bid);
        if (it == block_to_slices.end())
        {
          return false;
        }
        size_t &idx = slice_cursor[bid];
        if (idx >= it->second.size())
        {
          return false;
        }
        const auto &slice = it->second[idx];
        const char *src =
            stripe_buf + static_cast<size_t>(bid) * static_cast<size_t>(block_size) +
            static_cast<size_t>(slice.second);
        out->insert(out->end(), src, src + static_cast<size_t>(slice.first));
        ++idx;
      }
      return true;
    }

    void log_layout_client_send(int proxy_cluster, const std::string &append_key, int cluster_slice_size)
    {
      const size_t us = append_key.find('_');
      if (us == std::string::npos)
      {
        return;
      }
      const int stripe_id = std::stoi(append_key.substr(0, us));
      std::vector<int> tcp_block_ids;
      std::vector<int> meta_block_ids;
      if (!parse_append_key_block_ids(append_key, &tcp_block_ids))
      {
        return;
      }
      ToolBox::getInstance()->parse_append_key_meta_block_ids(append_key, &meta_block_ids);
      std::ostringstream oss;
      oss << "[XFERT] layout client->proxy_cluster=" << proxy_cluster << " stripe_id=" << stripe_id
          << " tcp_blocks=";
      for (size_t i = 0; i < tcp_block_ids.size(); ++i)
      {
        if (i > 0)
        {
          oss << ",";
        }
        oss << tcp_block_ids[i];
      }
      oss << " bytes=" << cluster_slice_size << " slices=" << tcp_block_ids.size();
      if (!meta_block_ids.empty())
      {
        oss << " meta_blocks=";
        for (size_t i = 0; i < meta_block_ids.size(); ++i)
        {
          if (i > 0)
          {
            oss << ",";
          }
          oss << meta_block_ids[i];
        }
      }
      // std::cout << oss.str() << std::endl;
    }

    void count_blocks_by_role(const std::vector<int> &block_ids, int k, int r, int z,
                              const std::string &code_type, int *data_n, int *global_n,
                              int *local_n)
    {
      if (data_n)
      {
        *data_n = 0;
      }
      if (global_n)
      {
        *global_n = 0;
      }
      if (local_n)
      {
        *local_n = 0;
      }
      for (int bid : block_ids)
      {
        if (code_type == "XueLRC")
        {
          if (bid < k)
          {
            if (data_n)
            {
              ++(*data_n);
            }
          }
          else if (bid < k + r)
          {
            if (local_n)
            {
              ++(*local_n);
            }
          }
          else if (global_n)
          {
            ++(*global_n);
          }
        }
        else if (is_azure_like_code(code_type))
        {
          if (bid < k)
          {
            if (data_n)
            {
              ++(*data_n);
            }
          }
          else if (bid < k + r)
          {
            if (global_n)
            {
              ++(*global_n);
            }
          }
          else if (local_n)
          {
            ++(*local_n);
          }
        }
      }
      (void)z;
    }

    void repack_logical_buffer_for_cluster_plans(
        char *dst, const char *src_logical, const coordinator_proto::ReplyProxyIPsPorts *reply,
        int block_size)
    {
      size_t off = 0;
      for (int i = 0; i < reply->append_keys_size(); ++i)
      {
        std::vector<int> block_ids;
        if (!parse_append_key_block_ids(reply->append_keys(i), &block_ids))
        {
          continue;
        }
        for (int bid : block_ids)
        {
          std::memcpy(dst + off, src_logical + static_cast<size_t>(bid) * block_size,
                      static_cast<size_t>(block_size));
          off += static_cast<size_t>(block_size);
        }
      }
    }

    void split_for_set_data_and_parity_by_cluster_plans(
        const coordinator_proto::ReplyProxyIPsPorts *reply, const std::vector<char *> &cluster_slice_data,
        int k, int r, int z, const std::string &code_type, int block_size,
        std::vector<char *> &data_ptr_array, std::vector<char *> &global_parity_ptr_array,
        std::vector<char *> &local_parity_ptr_array)
    {
      std::map<int, char *> data_by_id;
      std::map<int, char *> global_by_id;
      std::map<int, char *> local_by_id;
      for (int i = 0; i < reply->append_keys_size() && i < static_cast<int>(cluster_slice_data.size());
           ++i)
      {
        std::vector<int> block_ids;
        if (!parse_append_key_block_ids(reply->append_keys(i), &block_ids))
        {
          continue;
        }
        int data_n = 0;
        int global_n = 0;
        int local_n = 0;
        count_blocks_by_role(block_ids, k, r, z, code_type, &data_n, &global_n, &local_n);
        std::vector<size_t> node_slice_sizes(static_cast<size_t>(data_n + global_n + local_n),
                                               static_cast<size_t>(block_size));
        std::vector<char *> node_slices = ToolBox::getInstance()->splitCharPointer(
            cluster_slice_data[i], reply->cluster_slice_sizes(i), node_slice_sizes);
        int slice_idx = 0;
        for (int bid : block_ids)
        {
          if (slice_idx >= static_cast<int>(node_slices.size()))
          {
            break;
          }
          if (code_type == "XueLRC")
          {
            if (bid < k)
            {
              data_by_id[bid] = node_slices[slice_idx++];
            }
            else if (bid < k + r)
            {
              local_by_id[bid] = node_slices[slice_idx++];
            }
            else
            {
              global_by_id[bid] = node_slices[slice_idx++];
            }
          }
          else if (is_azure_like_code(code_type))
          {
            if (bid < k)
            {
              data_by_id[bid] = node_slices[slice_idx++];
            }
            else if (bid < k + r)
            {
              global_by_id[bid] = node_slices[slice_idx++];
            }
            else
            {
              local_by_id[bid] = node_slices[slice_idx++];
            }
          }
        }
      }
      for (int bid = 0; bid < k; ++bid)
      {
        auto it = data_by_id.find(bid);
        data_ptr_array.push_back(it != data_by_id.end() ? it->second : nullptr);
      }
      const int global_begin = (code_type == "XueLRC") ? (k + r) : k;
      const int global_end = (code_type == "XueLRC") ? (k + r + z) : (k + r);
      for (int bid = global_begin; bid < global_end; ++bid)
      {
        auto it = global_by_id.find(bid);
        global_parity_ptr_array.push_back(it != global_by_id.end() ? it->second : nullptr);
      }
      const int local_begin = (code_type == "XueLRC") ? k : (k + r);
      const int local_end = (code_type == "XueLRC") ? (k + r) : (k + r + z);
      for (int bid = local_begin; bid < local_end; ++bid)
      {
        auto it = local_by_id.find(bid);
        local_parity_ptr_array.push_back(it != local_by_id.end() ? it->second : nullptr);
      }
    }
  } // namespace

  std::string Client::sayHelloToCoordinatorByGrpc(std::string hello)
  {
    coordinator_proto::RequestToCoordinator request;
    request.set_name(hello);
    coordinator_proto::ReplyFromCoordinator reply;
    grpc::ClientContext context;
    grpc::Status status = m_coordinator_ptr->sayHelloToCoordinator(&context, request, &reply);
    if (status.ok())
    {
      return reply.message();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }
  // grpc, set the parameters stored in the variable of m_encode_parameters in coordinator
  bool Client::SetParameterByGrpc(ECSchema input_ecschema)
  {
    int k = input_ecschema.k_datablock;
    int l = input_ecschema.l_localparityblock;
    int g_m = input_ecschema.g_m_globalparityblock;
    int b = input_ecschema.b_datapergroup;
    EncodeType encodetype = input_ecschema.encodetype;
    int m = b % (g_m + 1);
    if (b != k / l || (m != 0 && encodetype == Azure_LRC && g_m % m != 0))
    {
      std::cout << "Set parameters failed! Illegal parameters!" << std::endl;
      exit(0);
    }
    coordinator_proto::Parameter parameter;
    parameter.set_partial_decoding((int)input_ecschema.partial_decoding);
    parameter.set_encodetype(encodetype);
    parameter.set_s_stripe_placementtype(input_ecschema.s_stripe_placementtype);
    parameter.set_m_stripe_placementtype(input_ecschema.m_stripe_placementtype);
    parameter.set_k_datablock(k);
    parameter.set_l_localparityblock(l);
    parameter.set_g_m_globalparityblock(g_m);
    parameter.set_b_datapergroup(b);
    parameter.set_x_stripepermergegroup(input_ecschema.x_stripepermergegroup);
    grpc::ClientContext context;
    coordinator_proto::RepIfSetParaSuccess reply;
    grpc::Status status = m_coordinator_ptr->setParameter(&context, parameter, &reply);
    if (status.ok())
    {
      return reply.ifsetparameter();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      return false;
    }
  }
  /*
    Function: set
    1. send the set request including the information of key and valuesize to the coordinator
    2. get the address of proxy
    3. send the value to the proxy by socket
  */
  bool Client::set(std::string key, std::string value)
  {
    grpc::ClientContext get_proxy_ip_port;
    coordinator_proto::RequestProxyIPPort request;
    coordinator_proto::ReplyProxyIPPort reply;
    request.set_key(key);
    request.set_valuesizebytes(value.size());
    grpc::Status status = m_coordinator_ptr->uploadOriginKeyValue(&get_proxy_ip_port, request, &reply);
    if (!status.ok())
    {
      std::cout << "[SET] upload data failed!" << std::endl;
      return false;
    }
    else
    {
      std::string proxy_ip = reply.proxyip();
      int proxy_port = reply.proxyport();
      std::cout << "[SET] Send " << key << " to proxy_address:" << proxy_ip << ":" << proxy_port << std::endl;
      // read to send the value
      asio::io_context io_context;
      asio::error_code error;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints =
          resolver.resolve(proxy_ip, std::to_string(proxy_port));
      asio::ip::tcp::socket sock_data(io_context);
      asio::connect(sock_data, endpoints);

      // std::cout << "[SET] key_size:" << key.size() << ", value_size:" << value.size();
      // std::cout << ", proxy_address:" << proxy_ip << ":" << proxy_port << std::endl;
      asio::write(sock_data, asio::buffer(key, key.size()), error);
      asio::write(sock_data, asio::buffer(value, value.size()), error);
      asio::error_code ignore_ec;
      sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      sock_data.close(ignore_ec);

      // check if metadata is saved successfully
      grpc::ClientContext check_commit;
      coordinator_proto::AskIfSuccess request;
      request.set_key(key);
      OpperateType opp = SET;
      request.set_opp(opp);
      coordinator_proto::RepIfSuccess reply;
      grpc::Status status;
      status = m_coordinator_ptr->checkCommitAbort(&check_commit, request, &reply);
      if (status.ok())
      {
        if (reply.ifcommit())
        {
          return true;
        }
        else
        {
          std::cout << "[SET] " << key << " not commit!!!!!" << std::endl;
        }
      }
      else
      {
        std::cout << "[SET] " << key << " Fail to check!!!!!" << std::endl;
      }
    }
    return false;
  }

  int Client::get_append_slice_plans(std::string append_mode, const int curr_logical_offset, const int append_size, std::vector<std::vector<int>> *node_slice_sizes_per_cluster, std::vector<int> *modified_data_block_nums_per_cluster, std::vector<int> *data_ptr_size_array, int &parity_slice_size, int &parity_slice_offset)
  {
    assert(node_slice_sizes_per_cluster->size() == m_sys_config->z);
    assert(modified_data_block_nums_per_cluster->size() == m_sys_config->z);
    assert(append_mode == "UNILRC_MODE" || append_mode == "CACHED_MODE");

    int unit_size = m_sys_config->UnitSize;
    int num_unit_stripes = (curr_logical_offset + append_size - 1) / (unit_size * m_sys_config->k) - curr_logical_offset / (unit_size * m_sys_config->k) + 1;
    int curr_block_id = (curr_logical_offset / unit_size) % m_sys_config->k;
    int num_units = (curr_logical_offset + append_size - 1) / unit_size - curr_logical_offset / unit_size + 1;
    int start_data_block_id = curr_block_id;

    parity_slice_size = num_unit_stripes * unit_size;
    parity_slice_offset = curr_logical_offset / (unit_size * m_sys_config->k) * unit_size;
    if (append_mode == "UNILRC_MODE")
    {
      if (num_units == 1)
      {
        parity_slice_size = append_size;
        parity_slice_offset += curr_logical_offset % unit_size;
      }
      if (num_unit_stripes > 1 && (curr_logical_offset + append_size - 1) % (unit_size * m_sys_config->k) < unit_size - 1)
      {
        parity_slice_size = (num_unit_stripes - 1) * unit_size + (curr_logical_offset + append_size - 1) % (unit_size * m_sys_config->k) + 1;
      }
    }

    std::map<int, int> block_to_slice_sizes;
    int tmp_size = append_size;
    int tmp_offset = curr_logical_offset;

    while (tmp_size > 0)
    {
      int sub_slice_size = unit_size;
      // first slice
      if (tmp_size == append_size && curr_logical_offset % unit_size != 0)
      {
        sub_slice_size = std::min(unit_size - curr_logical_offset % unit_size, append_size);
      }
      else
      {
        sub_slice_size = std::min(unit_size, tmp_size);
      }
      if (block_to_slice_sizes.find(curr_block_id) == block_to_slice_sizes.end())
      {
        block_to_slice_sizes[curr_block_id] = sub_slice_size;
      }
      else
      {
        block_to_slice_sizes[curr_block_id] += sub_slice_size;
      }
      curr_block_id = (curr_block_id + 1) % m_sys_config->k;
      tmp_size -= sub_slice_size;
      tmp_offset += sub_slice_size;
    }

    for (int i = m_sys_config->k; i < m_sys_config->n; i++)
    {
      block_to_slice_sizes[i] = parity_slice_size;
    }

    for (int i = 0; i < m_sys_config->z; i++)
    {
      for (int j = i * m_sys_config->k / m_sys_config->z;
           j < (i + 1) * m_sys_config->k / m_sys_config->z; j++)
      {
        if (block_to_slice_sizes.find(j) != block_to_slice_sizes.end())
        {
          node_slice_sizes_per_cluster->at(i).push_back(block_to_slice_sizes[j]);
          modified_data_block_nums_per_cluster->at(i)++;
          data_ptr_size_array->push_back(block_to_slice_sizes[j]);
        }
      }

      for (int j = m_sys_config->k + i * m_sys_config->r / m_sys_config->z;
           j < m_sys_config->k + (i + 1) * m_sys_config->r / m_sys_config->z; j++)
      {
        node_slice_sizes_per_cluster->at(i).push_back(block_to_slice_sizes[j]);
      }

      for (int j = m_sys_config->k + m_sys_config->r + i * m_sys_config->z / m_sys_config->z;
           j < m_sys_config->k + m_sys_config->r + (i + 1) * m_sys_config->z / m_sys_config->z; j++)
      {
        node_slice_sizes_per_cluster->at(i).push_back(block_to_slice_sizes[j]);
      }
    }

    return start_data_block_id;
  }

  void Client::split_for_append_data_and_parity(const coordinator_proto::ReplyProxyIPsPorts *reply_proxy_ips_ports, const std::vector<char *> &cluster_slice_data, const std::vector<std::vector<int>> &node_slice_sizes_per_cluster, const std::vector<int> &modified_data_block_nums_per_cluster, std::vector<char *> &data_ptr_array, std::vector<char *> &global_parity_ptr_array, std::vector<char *> &local_parity_ptr_array)
  {
    for (int i = 0; i < cluster_slice_data.size(); i++)
    {
      std::vector<size_t> node_slice_sizes(node_slice_sizes_per_cluster[i].begin(), node_slice_sizes_per_cluster[i].end());
      std::vector<char *> node_slices = m_toolbox->splitCharPointer(cluster_slice_data[i], static_cast<size_t>(reply_proxy_ips_ports->cluster_slice_sizes(i)), node_slice_sizes);
      if (modified_data_block_nums_per_cluster[i] > 0)
      {
        data_ptr_array.insert(data_ptr_array.end(), node_slices.begin(), node_slices.begin() + modified_data_block_nums_per_cluster[i]);
      }
      global_parity_ptr_array.insert(global_parity_ptr_array.end(), node_slices.begin() + modified_data_block_nums_per_cluster[i], node_slices.begin() + modified_data_block_nums_per_cluster[i] + (m_sys_config->r / m_sys_config->z));
      local_parity_ptr_array.insert(local_parity_ptr_array.end(), node_slices.begin() + modified_data_block_nums_per_cluster[i] + (m_sys_config->r / m_sys_config->z), node_slices.end());
    }
  }

  /*
  bool Client::append(int append_size)
  {
    int tmp_append_size = append_size;
    // align to aligned size
    tmp_append_size = (tmp_append_size + m_sys_config->AlignedSize - 1) / m_sys_config->AlignedSize * m_sys_config->AlignedSize;

    while (tmp_append_size > 0)
    {
      int sub_append_size = std::min(static_cast<unsigned int>(tmp_append_size), m_sys_config->BlockSize * m_sys_config->k - m_append_logical_offset);

      bool if_append_success = false;
      if (m_sys_config->AppendMode == "UNILRC_MODE" || m_sys_config->AppendMode == "CACHED_MODE")
      {
        if_append_success = sub_append(sub_append_size);
      }
      else if (m_sys_config->AppendMode == "REP_MODE")
      {
        if_append_success = sub_append_in_rep_mode(sub_append_size);
      }

      if (!if_append_success)
      {
        std::cout << "[APPEND148] Sub append failed with sub append size " << sub_append_size << " with mode " << m_sys_config->AppendMode << "!" << std::endl;
        return false;
      }
      tmp_append_size -= sub_append_size;
    }

    return true;
  }
  */
 
  /*bool Client::sub_append_in_rep_mode(int append_size)
  {
    grpc::ClientContext get_proxy_ip_port;
    coordinator_proto::RequestProxyIPPort request;
    coordinator_proto::ReplyProxyIPsPorts reply;
    request.set_key(m_clientID);
    request.set_valuesizebytes(append_size);
    request.set_append_mode(m_sys_config->AppendMode);
    grpc::Status status = m_coordinator_ptr->uploadAppendValue(&get_proxy_ip_port, request, &reply);

    if (!status.ok())
    {
      std::cout << "[APPEND216] upload data failed!" << std::endl;
      return false;
    }
    else
    {
      std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
      std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);
      launch_append_to_proxies_serial_per_endpoint(reply, m_pre_allocated_buffer, if_commit_arr.get());

      bool all_true = std::all_of(if_commit_arr.get(), if_commit_arr.get() + reply.append_keys_size(), [](bool val)
                                  { return val == true; });

      if (all_true)
      {
        // std::cout << "[APPEND244] Client " << m_clientID << " append " << append_size << " bytes successfully!" << std::endl;
        m_append_logical_offset = (m_append_logical_offset + append_size) % (m_sys_config->BlockSize * m_sys_config->k);
        return true;
      }
      else
      {
        return false;
      }
    }

    return true;
  }*/

  void Client::launch_append_to_proxies_serial_per_endpoint(
      const coordinator_proto::ReplyProxyIPsPorts &reply, const char *send_buf, bool *if_commit_arr)
  {
    std::vector<char *> cluster_slice_data = m_toolbox->splitCharPointer(send_buf, &reply);
    std::map<std::string, std::vector<int>> indices_by_endpoint;
    for (int i = 0; i < reply.append_keys_size(); ++i)
    {
      indices_by_endpoint[proxy_endpoint_key(reply.proxyips(i), reply.proxyports(i))].push_back(i);
    }
    std::vector<std::thread> threads;
    threads.reserve(indices_by_endpoint.size());
    for (const auto &kv : indices_by_endpoint)
    {
      threads.emplace_back([this, &reply, cluster_slice_data, if_commit_arr, indices = kv.second]() {
        for (int i : indices)
        {
          int proxy_cluster = -1;
          parse_append_key_cluster_id(reply.append_keys(i), &proxy_cluster);
          log_layout_client_send(proxy_cluster, reply.append_keys(i), reply.cluster_slice_sizes(i));
          async_append_to_proxies(cluster_slice_data[i], reply.append_keys(i),
                                  reply.cluster_slice_sizes(i), reply.proxyips(i),
                                  reply.proxyports(i), i, if_commit_arr, true);
        }
      });
    }
    for (auto &thread : threads)
    {
      thread.join();
    }
  }

  void Client::launch_append_subset_serial_per_endpoint(
      const coordinator_proto::ReplyProxyIPsPorts &reply, const char *send_buf, bool *if_commit_arr,
      const std::vector<int> &indices, bool poll_commit_after_send, XueClientTimingSummary *timing)
  {
    std::vector<char *> cluster_slice_data = m_toolbox->splitCharPointer(send_buf, &reply);
    std::map<std::string, std::vector<int>> indices_by_endpoint;
    for (int i : indices)
    {
      if (i < 0 || i >= reply.append_keys_size())
      {
        continue;
      }
      indices_by_endpoint[proxy_endpoint_key(reply.proxyips(i), reply.proxyports(i))].push_back(i);
    }
    const auto wave_t0 = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;
    threads.reserve(indices_by_endpoint.size());
    for (const auto &kv : indices_by_endpoint)
    {
      threads.emplace_back([this, &reply, cluster_slice_data, if_commit_arr, poll_commit_after_send,
                            timing, endpoint_indices = kv.second]() {
        for (int i : endpoint_indices)
        {
          XueAppendNetworkTiming step_timing;
          async_append_to_proxies(cluster_slice_data[i], reply.append_keys(i),
                                  reply.cluster_slice_sizes(i), reply.proxyips(i),
                                  reply.proxyports(i), i, if_commit_arr, poll_commit_after_send,
                                  timing != nullptr ? &step_timing : nullptr);
          if (timing != nullptr)
          {
            timing->sum_tcp_to_proxy_s += step_timing.tcp_resolve_connect_write_shutdown_s;
            timing->sum_coordinator_checkCommitAbort_s +=
                step_timing.coordinator_checkCommitAbort_s;
            // std::cout << "[XUE][Timing] step append_key=" << reply.append_keys(i)
            //           << " prepare_parse_pack_encode_s=0"
            //           << " tcp_to_proxy_s=" << step_timing.tcp_resolve_connect_write_shutdown_s
            //           << " coordinator_checkCommitAbort_s="
            //           << step_timing.coordinator_checkCommitAbort_s << std::endl;
          }
        }
      });
    }
    for (auto &thread : threads)
    {
      thread.join();
    }
    if (timing != nullptr && !indices_by_endpoint.empty())
    {
      timing->ingress_parallel_wave_tcp_wall_s +=
          chron_elapsed_s(wave_t0, std::chrono::high_resolution_clock::now());
    }
  }

  bool Client::poll_append_commit(const std::string &append_key)
  {
    grpc::ClientContext check_commit;
    coordinator_proto::AskIfSuccess request;
    request.set_key(append_key);
    request.set_opp(APPEND);
    coordinator_proto::RepIfSuccess reply;
    check_commit.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
    const grpc::Status status = m_coordinator_ptr->checkCommitAbort(&check_commit, request, &reply);
    return status.ok() && reply.ifcommit();
  }

  bool Client::wait_xue_all_commits_ready(int stripe_id, XueClientTimingSummary *timing)
  {
    const auto commit_t0 = std::chrono::high_resolution_clock::now();
    grpc::ClientContext wait_ctx;
    coordinator_proto::XueStripeScheduleId wait_req;
    coordinator_proto::ReplyFromCoordinator wait_rep;
    wait_req.set_stripe_id(stripe_id);
    wait_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
    const grpc::Status wait_st =
        m_coordinator_ptr->waitXueAllCommitsReady(&wait_ctx, wait_req, &wait_rep);
    if (timing != nullptr)
    {
      timing->sum_coordinator_checkCommitAbort_s +=
          chron_elapsed_s(commit_t0, std::chrono::high_resolution_clock::now());
    }
    if (!wait_st.ok())
    {
      // std::cout << "[XUE_UPDATE] waitXueAllCommitsReady failed: " << wait_st.error_message()
      //           << std::endl;
      return false;
    }
    return true;
  }

  void Client::wait_append_keys_commit_parallel(const std::vector<std::string> &keys,
                                                const std::map<std::string, int> &key_to_index,
                                                bool *if_commit_arr, XueClientTimingSummary *timing)
  {
    if (keys.empty())
    {
      return;
    }
    const auto commit_t0 = std::chrono::high_resolution_clock::now();
    while (true)
    {
      bool pending = false;
      for (const auto &key : keys)
      {
        const auto kit = key_to_index.find(key);
        if (kit == key_to_index.end())
        {
          continue;
        }
        const int idx = kit->second;
        if (if_commit_arr[idx])
        {
          continue;
        }
        pending = true;
        if (poll_append_commit(key))
        {
          if_commit_arr[idx] = true;
        }
      }
      if (!pending)
      {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (timing != nullptr)
    {
      timing->sum_coordinator_checkCommitAbort_s +=
          chron_elapsed_s(commit_t0, std::chrono::high_resolution_clock::now());
    }
  }

  bool Client::xue_update_strict_schedule(const coordinator_proto::ReplyProxyIPsPorts &reply,
                                          const char *send_buf, bool *if_commit_arr,
                                          XueClientTimingSummary *timing)
  {
    const int stripe_id = reply.xue_schedule_stripe_id();
    std::map<std::string, int> key_to_index;
    std::vector<int> all_indices;
    for (int i = 0; i < reply.append_keys_size(); ++i)
    {
      key_to_index[reply.append_keys(i)] = i;
      all_indices.push_back(i);
    }
    if (all_indices.empty())
    {
      return false;
    }

    // std::cout << "[XUE_UPDATE] strict_schedule stripe=" << stripe_id
    //           << " ingress_keys=" << all_indices.size()
    //           << " transfer_steps=" << reply.xue_transfer_steps_size()
    //           << " parallel_groups=" << reply.xue_schedule_num_groups() << std::endl;

    launch_append_subset_serial_per_endpoint(reply, send_buf, if_commit_arr, all_indices, false,
                                             timing);

    const auto wait_t0 = std::chrono::high_resolution_clock::now();
    grpc::ClientContext wait_ctx;
    coordinator_proto::XueStripeScheduleId wait_req;
    coordinator_proto::ReplyFromCoordinator wait_rep;
    wait_req.set_stripe_id(stripe_id);
    wait_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
    const grpc::Status wait_st =
        m_coordinator_ptr->waitXueAllIngressReady(&wait_ctx, wait_req, &wait_rep);
    if (!wait_st.ok())
    {
      // std::cout << "[XUE_UPDATE] waitXueAllIngressReady failed: " << wait_st.error_message()
      //           << std::endl;
      return false;
    }
    if (timing != nullptr)
    {
      timing->wait_all_ingress_ready_s +=
          chron_elapsed_s(wait_t0, std::chrono::high_resolution_clock::now());
    }
    // std::cout << "[XUE_UPDATE] all_ingress_ready stripe=" << stripe_id << std::endl;

    if (!wait_xue_all_commits_ready(stripe_id, timing))
    {
      return false;
    }
    // std::cout << "[XUE_UPDATE] all_commits_ready stripe=" << stripe_id << std::endl;
    for (int i = 0; i < reply.append_keys_size(); ++i)
    {
      if_commit_arr[i] = true;
    }
    return true;
  }

  bool Client::xue_update_follow_schedule(const coordinator_proto::ReplyProxyIPsPorts &reply,
                                          const char *send_buf, bool *if_commit_arr,
                                          XueClientTimingSummary *timing)
  {
    const int stripe_id = reply.xue_schedule_stripe_id();
    std::map<std::string, int> key_to_index;
    for (int i = 0; i < reply.append_keys_size(); ++i)
    {
      key_to_index[reply.append_keys(i)] = i;
    }
    std::map<int, std::vector<int>> ingress_indices_by_wave;
    int max_wave = -1;
    for (const auto &cw : reply.xue_client_waves())
    {
      max_wave = std::max(max_wave, cw.wave_index());
      for (const auto &key : cw.client_ingress_append_keys())
      {
        const auto it = key_to_index.find(key);
        if (it != key_to_index.end())
        {
          ingress_indices_by_wave[cw.wave_index()].push_back(it->second);
        }
      }
    }
    std::map<int, std::vector<std::string>> commit_keys_by_wave;
    for (const auto &cw : reply.xue_commit_waves())
    {
      max_wave = std::max(max_wave, cw.wait_commit_wave());
      commit_keys_by_wave[cw.wait_commit_wave()].push_back(cw.append_key());
    }
    if (max_wave < 0)
    {
      return false;
    }

    // std::cout << "[XUE_UPDATE] follow_schedule stripe=" << stripe_id << " waves=" << (max_wave + 1)
    //           << " client_ingress_waves=" << reply.xue_client_waves_size()
    //           << " commit_waves=" << reply.xue_commit_waves_size() << std::endl;

    for (int w = 0; w <= max_wave; ++w)
    {
      const auto ingress_it = ingress_indices_by_wave.find(w);
      if (ingress_it != ingress_indices_by_wave.end() && !ingress_it->second.empty())
      {
        launch_append_subset_serial_per_endpoint(reply, send_buf, if_commit_arr, ingress_it->second,
                                                 false, timing);
      }

      const auto release_t0 = std::chrono::high_resolution_clock::now();
      grpc::ClientContext release_ctx;
      coordinator_proto::XueScheduleWaveRelease release_req;
      coordinator_proto::ReplyFromCoordinator release_rep;
      release_req.set_stripe_id(stripe_id);
      release_req.set_released_wave(w);
      release_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
      const grpc::Status release_st =
          m_coordinator_ptr->releaseXueScheduleWave(&release_ctx, release_req, &release_rep);
      if (!release_st.ok())
      {
        // std::cout << "[XUE_UPDATE] releaseXueScheduleWave failed wave=" << w << " "
        //           << release_st.error_message() << std::endl;
        return false;
      }
      if (timing != nullptr)
      {
        timing->sum_release_schedule_wave_s +=
            chron_elapsed_s(release_t0, std::chrono::high_resolution_clock::now());
      }
      // std::cout << "[XUE_UPDATE] released_schedule_wave=" << w << " stripe=" << stripe_id
      //           << std::endl;

      const auto commit_it = commit_keys_by_wave.find(w);
      if (commit_it != commit_keys_by_wave.end() && !commit_it->second.empty())
      {
        wait_append_keys_commit_parallel(commit_it->second, key_to_index, if_commit_arr, timing);
      }
    }
    return std::all_of(if_commit_arr, if_commit_arr + reply.append_keys_size(),
                       [](bool v) { return v; });
  }

  void Client::async_append_to_proxies(char *cluster_slice_data, std::string append_key, int cluster_slice_size, std::string proxy_ip, int proxy_port, int index, bool *if_commit_arr, bool poll_commit_after_send, XueAppendNetworkTiming *out_timing)
  {
    std::lock_guard<std::mutex> endpoint_lk(mutex_for_proxy_endpoint(proxy_ip, proxy_port));
    if (append_key_uses_cluster_ingress(append_key))
    {
      int proxy_cluster = -1;
      if (parse_append_key_cluster_id(append_key, &proxy_cluster))
      {
        log_layout_client_send(proxy_cluster, append_key, cluster_slice_size);
      }
    }
    const auto tcp_t0 = std::chrono::high_resolution_clock::now();
    asio::io_context io_context;
    asio::error_code error;
    asio::ip::tcp::resolver resolver(io_context);
    asio::ip::tcp::resolver::results_type endpoints =
        resolver.resolve(proxy_ip, std::to_string(proxy_port));
    asio::ip::tcp::socket sock_data(io_context);
    asio::connect(sock_data, endpoints);

    asio::write(sock_data, asio::buffer(cluster_slice_data, cluster_slice_size), error);
    asio::error_code ignore_ec;
    sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    sock_data.close(ignore_ec);
    const auto tcp_t1 = std::chrono::high_resolution_clock::now();
    if (out_timing != nullptr)
    {
      out_timing->tcp_resolve_connect_write_shutdown_s = chron_elapsed_s(tcp_t0, tcp_t1);
    }

    if (!poll_commit_after_send)
    {
      return;
    }

    const auto commit_t0 = std::chrono::high_resolution_clock::now();
    if (poll_append_commit(append_key))
    {
      if_commit_arr[index] = true;
    }
    else
    {
      std::cout << "[APPEND205] " << append_key << " not commit!!!!!" << " cluster_slice_size: "
                << cluster_slice_size << " proxy_ip: " << proxy_ip << " proxy_port: " << proxy_port
                << std::endl;
    }
    if (out_timing != nullptr)
    {
      out_timing->coordinator_checkCommitAbort_s = chron_elapsed_s(commit_t0, std::chrono::high_resolution_clock::now());
    }
  }

  void Client::get_cached_parity_slices(std::vector<char *> &global_parity_ptr_array, std::vector<char *> &local_parity_ptr_array, const int parity_slice_size, const int parity_slice_offset)
  {
    assert(global_parity_ptr_array.size() == m_sys_config->r);
    assert(local_parity_ptr_array.size() == m_sys_config->z);
    for (int i = 0; i < global_parity_ptr_array.size(); i++)
    {
      memcpy(global_parity_ptr_array[i], m_cached_buffer[i] + parity_slice_offset, parity_slice_size);
    }
    for (int i = 0; i < local_parity_ptr_array.size(); i++)
    {
      memcpy(local_parity_ptr_array[i], m_cached_buffer[i + m_sys_config->r] + parity_slice_offset, parity_slice_size);
    }
  }

  void Client::cache_latest_parity_slices(std::vector<char *> &global_parity_ptr_array, std::vector<char *> &local_parity_ptr_array, const int parity_slice_size, const int parity_slice_offset)
  {
    for (int i = 0; i < global_parity_ptr_array.size(); i++)
    {
      memcpy(m_cached_buffer[i] + parity_slice_offset, global_parity_ptr_array[i], parity_slice_size);
    }
    for (int i = 0; i < local_parity_ptr_array.size(); i++)
    {
      memcpy(m_cached_buffer[i + m_sys_config->r] + parity_slice_offset, local_parity_ptr_array[i], parity_slice_size);
    }
  }

  std::vector<int> Client::get_data_block_num_per_group(int k, int r, int z, std::string code_type)
  {
    std::vector<int> data_block_num_per_group;
    if (is_azure_like_code(code_type))
    {
      for (int i = 0; i < z; i++)
      {
        data_block_num_per_group.push_back((k / z));
      }
      data_block_num_per_group.push_back(0);
    }
    else if (code_type == "OptimalLRC")
    {
      int group_size = r + 1;
      int local_group_size = (k / z);
      int group_num_of_one_local_group = local_group_size / group_size + 1;
      int group_num = z * group_num_of_one_local_group + 1;
      for (int i = 0; i < group_num - 1; i++)
      {
        if ((i + 1) % group_num_of_one_local_group)
        {
          data_block_num_per_group.push_back(group_size);
        }
        else
        {
          data_block_num_per_group.push_back(local_group_size % group_size);
        }
      }
      data_block_num_per_group.push_back(0);
    }
    else if (code_type == "UniformLRC")
    {
      /*int group_size = r + 1;
      int local_group_size = int((k + r) / z);
      int larger_local_group_num = int((k + r) % z);

      int group_num_of_one_local_group = local_group_size / group_size + (bool)(local_group_size % group_size);
      for (int i = 0; i < z - 1; i++)
      {
        if (i + larger_local_group_num == z)
        {
          local_group_size++;
          group_num_of_one_local_group = local_group_size / group_size + (bool)(local_group_size % group_size);
        }
        for (int j = 0; j < group_num_of_one_local_group; j++)
        {
          if (j == group_num_of_one_local_group - 1)
          {
            data_block_num_per_group.push_back(local_group_size % group_size);
          }
          else
          {
            data_block_num_per_group.push_back(group_size);
          }
        }
      }
      data_block_num_per_group.push_back(local_group_size - r);
      for(int i = 0; i < group_num_of_one_local_group -1; i++)
      {
        data_block_num_per_group.push_back(0);
      }*/
      for(int i = 0; i < z -1; i++){
        data_block_num_per_group.push_back((k+r) / z);
      }
      data_block_num_per_group.push_back(0);
    }
    else if (code_type == "UniLRC")
    {
      int local_data_num = k / z;
      for (int i = 0; i < z; i++)
      {
        data_block_num_per_group.push_back(local_data_num);
      }
    }
    return data_block_num_per_group;
  }

  std::vector<int> Client::get_global_parity_block_num_per_group(int k, int r, int z, std::string code_type)
  {
    std::vector<int> global_pairty_block_num_per_group;
    if (is_azure_like_code(code_type))
    {
      for (int i = 0; i < z; i++)
      {
        global_pairty_block_num_per_group.push_back(0);
      }
      global_pairty_block_num_per_group.push_back(r);
    }
    else if (code_type == "OptimalLRC")
    {
      int group_size = r + 1;
      int local_group_size = (k / z);
      int group_num_of_one_local_group = local_group_size / group_size + 1;
      int group_num = z * group_num_of_one_local_group + 1;
      for (int i = 0; i < group_num - 1; i++)
      {
        global_pairty_block_num_per_group.push_back(0);
      }
      global_pairty_block_num_per_group.push_back(r);
    }
    else if (code_type == "UniformLRC")
    {
      /*int group_size = r + 1;
      int local_group_size = int((k + r) / z);
      int larger_local_group_num = int((k + r) % z);
      int group_num_of_one_local_group = local_group_size / group_size + (bool)(local_group_size % group_size);
      for (int i = 0; i < z - 1; i++)
      {
        if (i + larger_local_group_num == z)
        {
          local_group_size++;
          group_num_of_one_local_group = local_group_size / group_size + (bool)(local_group_size % group_size);
        }
        for (int j = 0; j < group_num_of_one_local_group; j++)
        {
          global_pairty_block_num_per_group.push_back(0);
        }
      }
      global_pairty_block_num_per_group.push_back(r);*/
      for(int i = 0; i < z - 1; i++){
        global_pairty_block_num_per_group.push_back(0);
      }
      global_pairty_block_num_per_group.push_back(r);
    }
    else if (code_type == "UniLRC")
    {
      int local_global_parity_num = r / z;
      for (int i = 0; i < z; i++)
      {
        global_pairty_block_num_per_group.push_back(local_global_parity_num);
      }
    }
    return global_pairty_block_num_per_group;
  }

  std::vector<int> Client::get_local_parity_block_num_per_group(int k, int r, int z, std::string code_type)
  {
    std::vector<int> local_parity_block_num_per_group;
    if (is_azure_like_code(code_type))
    {
      for (int i = 0; i < z; i++)
      {
        local_parity_block_num_per_group.push_back(1);
      }
      local_parity_block_num_per_group.push_back(0);
    }
    else if (code_type == "OptimalLRC")
    {
      int group_size = r + 1;
      int local_group_size = (k / z);
      int group_num_of_one_local_group = local_group_size / group_size + 1;
      int group_num = z * group_num_of_one_local_group + 1;
      for (int i = 0; i < group_num - 1; i++)
      {
        if ((i + 1) % group_num_of_one_local_group)
        {
          local_parity_block_num_per_group.push_back(0);
        }
        else
        {
          local_parity_block_num_per_group.push_back(1);
        }
      }
      local_parity_block_num_per_group.push_back(0);
    }
    else if (code_type == "UniformLRC")
    {
      /*int group_size = r + 1;
      int local_group_size = int((k + r) / z);
      int larger_local_group_num = int((k + r) % z);
      int group_num_of_one_local_group = local_group_size / group_size + (bool)(local_group_size % group_size);
      for (int i = 0; i < z; i++)
      {
        if (i + larger_local_group_num == z)
        {
          local_group_size++;
          group_num_of_one_local_group = local_group_size / group_size + (bool)(local_group_size % group_size);
        }
        for (int j = 0; j < group_num_of_one_local_group; j++)
        {
          if (j == group_num_of_one_local_group - 1)
          {
            local_parity_block_num_per_group.push_back(1);
          }
          else
          {
            local_parity_block_num_per_group.push_back(0);
          }
        }
      }*/
      for (int i = 0; i < z; i++)
      {
        local_parity_block_num_per_group.push_back(1);
      }
    }
    else if (code_type == "UniLRC")
    {
      for (int i = 0; i < z; i++)
      {
        local_parity_block_num_per_group.push_back(1);
      }
    }
    return local_parity_block_num_per_group;
  }

  void Client::split_for_set_data_and_parity(const coordinator_proto::ReplyProxyIPsPorts *reply_proxy_ips_ports, const std::vector<char *> &cluster_slice_data, const std::vector<int> &data_block_num_per_group, const std::vector<int> &global_parity_block_num_per_group, const std::vector<int> &local_parity_block_num_per_group, std::vector<char *> &data_ptr_array, std::vector<char *> &global_parity_ptr_array, std::vector<char *> &local_parity_ptr_array)
  {
    for (int i = 0; i < cluster_slice_data.size(); i++)
    {
      std::vector<size_t> node_slice_sizes(data_block_num_per_group[i] + global_parity_block_num_per_group[i] + local_parity_block_num_per_group[i], m_sys_config->BlockSize);
      std::vector<char *> node_slices = m_toolbox->splitCharPointer(cluster_slice_data[i], reply_proxy_ips_ports->cluster_slice_sizes(i), node_slice_sizes);
      data_ptr_array.insert(data_ptr_array.end(), node_slices.begin(), node_slices.begin() + data_block_num_per_group[i]);
      global_parity_ptr_array.insert(global_parity_ptr_array.end(), node_slices.begin() + data_block_num_per_group[i], node_slices.begin() + data_block_num_per_group[i] + global_parity_block_num_per_group[i]);
      local_parity_ptr_array.insert(local_parity_ptr_array.end(), node_slices.begin() + data_block_num_per_group[i] + global_parity_block_num_per_group[i], node_slices.begin() + data_block_num_per_group[i] + global_parity_block_num_per_group[i] + local_parity_block_num_per_group[i]);
    }
  }

  // add a stripe each time
  bool Client::set()
  {
    grpc::ClientContext get_proxy_ip_port;
    coordinator_proto::RequestProxyIPPort request;
    coordinator_proto::ReplyProxyIPsPorts reply;
    request.set_key(m_clientID);
    request.set_valuesizebytes(static_cast<size_t>(m_sys_config->BlockSize) *static_cast<size_t>(m_sys_config->k));
    request.set_append_mode("UNILRC_MODE");
    grpc::Status status = m_coordinator_ptr->uploadSetValue(&get_proxy_ip_port, request, &reply);

    if (!status.ok())
    {
      std::cout << "[SET402] upload data failed!" << std::endl;
      return false;
    }
    else
    {
      const int k = m_sys_config->k;
      const int r = m_sys_config->r;
      const int z = m_sys_config->z;
      const int block_size = static_cast<int>(m_sys_config->BlockSize);
      std::vector<char> plan_order_buffer;
      const char *send_buf = m_pre_allocated_buffer;
      if (reply_uses_cluster_ingress(&reply))
      {
        plan_order_buffer.resize(static_cast<size_t>(reply.sum_append_size()), 0);
        repack_logical_buffer_for_cluster_plans(plan_order_buffer.data(), m_pre_allocated_buffer,
                                                &reply, block_size);
        send_buf = plan_order_buffer.data();
      }
      std::vector<char *> cluster_slice_data = m_toolbox->splitCharPointer(send_buf, &reply);
      std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
      std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);

      assert(m_sys_config->CodeType == "UniLRC" || m_sys_config->CodeType == "OptimalLRC" || m_sys_config->CodeType == "UniformLRC" || is_azure_like_code(m_sys_config->CodeType));
      std::vector<char *> data_ptr_array, global_parity_ptr_array, local_parity_ptr_array;
      if (reply_uses_cluster_ingress(&reply))
      {
        split_for_set_data_and_parity_by_cluster_plans(&reply, cluster_slice_data, k, r, z,
                                                       m_sys_config->CodeType, block_size,
                                                       data_ptr_array, global_parity_ptr_array,
                                                       local_parity_ptr_array);
      }
      else
      {
        std::vector<int> data_block_num_per_group =
            get_data_block_num_per_group(k, r, z, m_sys_config->CodeType);
        std::vector<int> global_parity_block_num_per_group =
            get_global_parity_block_num_per_group(k, r, z, m_sys_config->CodeType);
        std::vector<int> local_parity_block_num_per_group =
            get_local_parity_block_num_per_group(k, r, z, m_sys_config->CodeType);
        split_for_set_data_and_parity(&reply, cluster_slice_data, data_block_num_per_group,
                                      global_parity_block_num_per_group,
                                      local_parity_block_num_per_group, data_ptr_array,
                                      global_parity_ptr_array, local_parity_ptr_array);
      }
      std::vector<char *> parity_ptr_array;
      parity_ptr_array.insert(parity_ptr_array.end(), global_parity_ptr_array.begin(), global_parity_ptr_array.end());
      parity_ptr_array.insert(parity_ptr_array.end(), local_parity_ptr_array.begin(), local_parity_ptr_array.end());
      if (m_sys_config->CodeType == "UniLRC")
      {
        //ECProject::encode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(global_parity_ptr_array.data()), reinterpret_cast<unsigned char **>(local_parity_ptr_array.data()), m_sys_config->BlockSize);
        ECProject::encode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      else if (m_sys_config->CodeType == "OptimalLRC")
      {
        //ECProject::encode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(global_parity_ptr_array.data()), reinterpret_cast<unsigned char **>(local_parity_ptr_array.data()), m_sys_config->BlockSize);
        ECProject::encode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      else if (m_sys_config->CodeType == "UniformLRC")
      {
        //ECProject::encode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(global_parity_ptr_array.data()), reinterpret_cast<unsigned char **>(local_parity_ptr_array.data()), m_sys_config->BlockSize);
        ECProject::encode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      else if (is_azure_like_code(m_sys_config->CodeType))
      {
        //ECProject::encode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(global_parity_ptr_array.data()), reinterpret_cast<unsigned char **>(local_parity_ptr_array.data()), m_sys_config->BlockSize);
        ECProject::encode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      launch_append_to_proxies_serial_per_endpoint(reply, send_buf, if_commit_arr.get());

      bool all_true = std::all_of(if_commit_arr.get(), if_commit_arr.get() + reply.append_keys_size(), [](bool val)
                                  { return val == true; });

      if (all_true)
      {
        std::cout << "Client " << m_clientID << " set successfully!" << std::endl;
        return true;
      }
      else
      {
        std::cout << "Client " << m_clientID << " set failed!" << std::endl;
        return false;
      }
    }

    return false;
  }

  bool Client::sub_set(int block_num)
  {
    grpc::ClientContext get_proxy_ip_port;
    coordinator_proto::RequestProxyIPPort request;
    coordinator_proto::ReplyProxyIPsPorts reply;
    request.set_key(m_clientID);
    request.set_valuesizebytes(static_cast<size_t>(m_sys_config->BlockSize) *static_cast<size_t>(block_num));
    request.set_append_mode("UNILRC_MODE");
    grpc::Status status = m_coordinator_ptr->uploadSubsetValue(&get_proxy_ip_port, request, &reply);

    if (!status.ok())
    {
      std::cout << "[SET402] upload data failed!" << std::endl;
      return false;
    }
    else
    {
      const int k = m_sys_config->k;
      const int r = m_sys_config->r;
      const int z = m_sys_config->z;
      const int block_size = static_cast<int>(m_sys_config->BlockSize);
      std::vector<char> plan_order_buffer;
      const char *send_buf = m_pre_allocated_buffer;
      if (reply_uses_cluster_ingress(&reply))
      {
        plan_order_buffer.resize(static_cast<size_t>(reply.sum_append_size()), 0);
        repack_logical_buffer_for_cluster_plans(plan_order_buffer.data(), m_pre_allocated_buffer,
                                                &reply, block_size);
        send_buf = plan_order_buffer.data();
      }
      std::vector<char *> cluster_slice_data = m_toolbox->splitCharPointer(send_buf, &reply);
      std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
      std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);

      assert(m_sys_config->CodeType == "UniLRC" || m_sys_config->CodeType == "OptimalLRC" || m_sys_config->CodeType == "UniformLRC" || is_azure_like_code(m_sys_config->CodeType));
      std::vector<char *> data_ptr_array, global_parity_ptr_array, local_parity_ptr_array;
      if (reply_uses_cluster_ingress(&reply))
      {
        split_for_set_data_and_parity_by_cluster_plans(&reply, cluster_slice_data, k, r, z,
                                                       m_sys_config->CodeType, block_size,
                                                       data_ptr_array, global_parity_ptr_array,
                                                       local_parity_ptr_array);
      }
      else
      {
        std::vector<int> data_block_num_per_group =
            get_data_block_num_per_group(k, r, z, m_sys_config->CodeType);
        int capacity = block_num;
        for (int i = 0; i < static_cast<int>(data_block_num_per_group.size()); i++)
        {
          if (data_block_num_per_group[i] > capacity)
          {
            data_block_num_per_group[i] = capacity;
          }
          capacity -= data_block_num_per_group[i];
        }
        std::vector<int> global_parity_block_num_per_group =
            get_global_parity_block_num_per_group(k, r, z, m_sys_config->CodeType);
        std::vector<int> local_parity_block_num_per_group =
            get_local_parity_block_num_per_group(k, r, z, m_sys_config->CodeType);
        m_toolbox->remove_common_zeros(data_block_num_per_group, global_parity_block_num_per_group,
                                       local_parity_block_num_per_group);
        split_for_set_data_and_parity(&reply, cluster_slice_data, data_block_num_per_group,
                                      global_parity_block_num_per_group,
                                      local_parity_block_num_per_group, data_ptr_array,
                                      global_parity_ptr_array, local_parity_ptr_array);
      }
      std::vector<char *> parity_ptr_array;
      parity_ptr_array.insert(parity_ptr_array.end(), global_parity_ptr_array.begin(), global_parity_ptr_array.end());
      parity_ptr_array.insert(parity_ptr_array.end(), local_parity_ptr_array.begin(), local_parity_ptr_array.end());
      if (m_sys_config->CodeType == "UniLRC")
      {
        //ECProject::encode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(global_parity_ptr_array.data()), reinterpret_cast<unsigned char **>(local_parity_ptr_array.data()), m_sys_config->BlockSize);
        ECProject::partial_encode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, block_num, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      else if (m_sys_config->CodeType == "OptimalLRC")
      {
        //ECProject::encode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(global_parity_ptr_array.data()), reinterpret_cast<unsigned char **>(local_parity_ptr_array.data()), m_sys_config->BlockSize);
        ECProject::partial_encode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, block_num, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      else if (m_sys_config->CodeType == "UniformLRC")
      {
        //ECProject::encode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(global_parity_ptr_array.data()), reinterpret_cast<unsigned char **>(local_parity_ptr_array.data()), m_sys_config->BlockSize);
        ECProject::partial_encode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, block_num, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      else if (is_azure_like_code(m_sys_config->CodeType))
      {
        //ECProject::encode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(global_parity_ptr_array.data()), reinterpret_cast<unsigned char **>(local_parity_ptr_array.data()), m_sys_config->BlockSize);
        ECProject::partial_encode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, block_num, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      launch_append_to_proxies_serial_per_endpoint(reply, send_buf, if_commit_arr.get());

      bool all_true = std::all_of(if_commit_arr.get(), if_commit_arr.get() + reply.append_keys_size(), [](bool val)
                                  { return val == true; });

      if (all_true)
      {
        return true;
      }
      else
      {
        std::cout << "Client " << m_clientID << " set failed!" << std::endl;
        return false;
      }
    }

    return false;
  }

  void Client::log_xue_client_timing_summary(int stripe_id, uint64_t xue_xfer_plan_id,
                                             const XueClientTimingSummary &timing) const
  {
    // std::cout << "[XUE][Timing] summary stripe=" << stripe_id
    //           << " xue_xfer_plan_id=" << xue_xfer_plan_id
    //           << " coordinator_uploadXueUpdate_s=" << timing.coordinator_uploadXueUpdate_s
    //           << " prepare_parse_pack_encode_s=" << timing.prepare_parse_pack_encode_s
    //           << " sum_tcp_to_proxy_s=" << timing.sum_tcp_to_proxy_s
    //           << " sum_coordinator_checkCommitAbort_s=" << timing.sum_coordinator_checkCommitAbort_s
    //           << " wait_all_ingress_ready_s=" << timing.wait_all_ingress_ready_s
    //           << " sum_release_schedule_wave_s=" << timing.sum_release_schedule_wave_s
    //           << " ingress_parallel_wave_tcp_wall_s=" << timing.ingress_parallel_wave_tcp_wall_s
    //           << " total_client_xue_s=" << timing.total_client_xue_s << std::endl;
  }

  bool Client::xue_update(int stripe_id, const std::vector<std::pair<int, int>> &logical_ranges)
  {
    if (logical_ranges.empty())
    {
      std::cout << "[XUE_UPDATE] Empty logical ranges." << std::endl;
      return false;
    }
    const int block_size = static_cast<int>(m_sys_config->BlockSize);
    const int unit_size = static_cast<int>(m_sys_config->UnitSize);
    const int k = m_sys_config->k;
    const int n = m_sys_config->n;

    for (const auto &r : logical_ranges)
    {
      if (r.second <= r.first)
      {
        std::cout << "[XUE_UPDATE] Invalid logical range: [" << r.first
                  << ", " << r.second << ") (require start < end)" << std::endl;
        return false;
      }
    }

    // 每个请求使用独立的本地 buffer，确保 detach 残留线程与新请求之间完全隔离。
    const size_t full_stripe_bytes = static_cast<size_t>(block_size) * static_cast<size_t>(n);
    std::vector<char> local_buffer(full_stripe_bytes);
    std::memset(local_buffer.data(), 0xaa, full_stripe_bytes);

    const std::vector<std::pair<int, int>> padded_ranges =
        pad_xue_logical_ranges_to_unit_size(logical_ranges, block_size, unit_size);
    if (padded_ranges != logical_ranges)
    {
      // std::cout << "[XUE_UPDATE] padded logical ranges to unit_size=" << unit_size
      //           << " for coordinator/plan alignment" << std::endl;
    }
    zero_fill_xue_unit_padding_gaps(local_buffer.data(), logical_ranges, block_size, unit_size);

    XueClientTimingSummary timing;
    const auto total_t0 = std::chrono::high_resolution_clock::now();

    grpc::ClientContext get_proxy_ip_port;
    coordinator_proto::XueUpdateRequest request;
    coordinator_proto::ReplyProxyIPsPorts reply;
    request.set_client_id(m_clientID);
    request.set_stripe_id(stripe_id);
    for (const auto &r : padded_ranges)
    {
      auto *range = request.add_ranges();
      range->set_logical_offset_start(r.first);
      range->set_logical_offset_end(r.second);
    }

    get_proxy_ip_port.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
    const auto coord_t0 = std::chrono::high_resolution_clock::now();
    grpc::Status status = m_coordinator_ptr->uploadXueUpdate(&get_proxy_ip_port, request, &reply);
    timing.coordinator_uploadXueUpdate_s =
        chron_elapsed_s(coord_t0, std::chrono::high_resolution_clock::now());
    if (!status.ok())
    {
      std::cout << "[XUE_UPDATE] upload failed: " << status.error_message() << std::endl;
      return false;
    }

    const auto prepare_t0 = std::chrono::high_resolution_clock::now();
    std::map<int, std::vector<std::pair<int, int>>> block_to_slices =
        build_xue_block_to_slices_from_ranges(padded_ranges, block_size);
    extend_xue_parity_slices_in_block_map(&block_to_slices, k, n, block_size, unit_size);

    std::vector<char> tcp_pack_buffer;
    const char *send_buf = local_buffer.data();
    if (reply_uses_cluster_ingress(&reply))
    {
      tcp_pack_buffer.resize(static_cast<size_t>(reply.sum_append_size()), 0);
      size_t off = 0;
      for (int i = 0; i < reply.append_keys_size(); ++i)
      {
        std::vector<char> cluster_payload;
        if (!pack_xue_tcp_payload_for_append_key(local_buffer.data(), block_to_slices,
                                                 reply.append_keys(i), block_size, &cluster_payload))
        {
          std::cout << "[XUE_UPDATE] failed to pack TCP payload for key=" << reply.append_keys(i)
                    << std::endl;
          return false;
        }
        if (cluster_payload.size() !=
            static_cast<size_t>(reply.cluster_slice_sizes(i)))
        {
          std::cout << "[XUE_UPDATE] cluster payload size mismatch key=" << reply.append_keys(i)
                    << " packed=" << cluster_payload.size()
                    << " expected=" << reply.cluster_slice_sizes(i) << std::endl;
          return false;
        }
        std::memcpy(tcp_pack_buffer.data() + off, cluster_payload.data(), cluster_payload.size());
        off += cluster_payload.size();
      }
      send_buf = tcp_pack_buffer.data();
    }
    timing.prepare_parse_pack_encode_s =
        chron_elapsed_s(prepare_t0, std::chrono::high_resolution_clock::now());

    std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
    std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);
    bool all_true = false;
    if (reply.xue_transfer_steps_size() > 0)
    {
      all_true = xue_update_strict_schedule(reply, send_buf, if_commit_arr.get(), &timing);
    }
    else if (reply.xue_client_waves_size() > 0 || reply.xue_commit_waves_size() > 0)
    {
      all_true = xue_update_follow_schedule(reply, send_buf, if_commit_arr.get(), &timing);
    }
    else
    {
      std::vector<int> all_indices;
      all_indices.reserve(static_cast<size_t>(reply.append_keys_size()));
      for (int i = 0; i < reply.append_keys_size(); ++i)
      {
        all_indices.push_back(i);
      }
      launch_append_subset_serial_per_endpoint(reply, send_buf, if_commit_arr.get(), all_indices,
                                               true, &timing);
      all_true = std::all_of(if_commit_arr.get(), if_commit_arr.get() + reply.append_keys_size(),
                              [](bool val) { return val == true; });
    }

    timing.total_client_xue_s = chron_elapsed_s(total_t0, std::chrono::high_resolution_clock::now());
    log_xue_client_timing_summary(stripe_id, reply.xue_xfer_plan_id(), timing);

    if (all_true && reply.xue_xfer_plan_id() > 0)
    {
      grpc::ClientContext pull_ctx;
      coordinator_proto::XueXferTimingPull pull_req;
      coordinator_proto::XueXferTimingSummary pull_rep;
      pull_req.set_stripe_id(stripe_id);
      pull_req.set_xue_xfer_plan_id(reply.xue_xfer_plan_id());
      pull_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
      const grpc::Status pull_st =
          m_coordinator_ptr->pullXueXferTiming(&pull_ctx, pull_req, &pull_rep);
      if (!pull_st.ok())
      {
        // std::cout << "[XUE][Timing] pullXueXferTiming failed: " << pull_st.error_message()
        //           << std::endl;
      }
    }

    if (!all_true)
    {
      // std::cout << "[XUE_UPDATE] commit check failed for at least one cluster slice." << std::endl;
    }
    return all_true;
  }

  std::shared_ptr<char[]> Client::get_degraded_read_block_breakdown(int stripe_id, int failed_block_id, double &total_time,double &disk_io_time, double &network_time, double &decode_time)
  {
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    unsigned int block_size = m_sys_config->BlockSize;
    grpc::ClientContext context;
    coordinator_proto::KeyAndClientIP request;
    request.set_key(std::to_string(stripe_id) + "_" + std::to_string(failed_block_id));
    request.set_clientip(m_clientIPForGet);
    request.set_clientport(m_clientPortForGet);
    coordinator_proto::DegradedReadReply reply;
    std::chrono::high_resolution_clock::time_point grpc_notify;
    std::thread t([&context, &request, &reply, &grpc_notify, this]() {
      grpc_notify = std::chrono::high_resolution_clock::now();
      grpc::Status status = m_coordinator_ptr->getDegradedReadBlockBreakdown(&context, request, &reply);
      if (!status.ok())
      {
        std::cout << "[Client] degraded read failed!" << std::endl;
      }
    });
    asio::ip::tcp::socket socket(io_context);
    acceptor.accept(socket);
    std::chrono::high_resolution_clock::time_point receive_start = std::chrono::high_resolution_clock::now();
    asio::error_code error;
    std::shared_ptr<char[]> buf(new char[block_size]);
    //std::cout << "start to read" << std::endl;
    size_t len = asio::read(socket, asio::buffer(buf.get(), block_size), error);
    if(len != block_size){
      std::cout << "[Error] len != block_size: " << len << std::endl;
      return nullptr;
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
    socket.close(ignore_ec); 
    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    total_time = std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
    t.join();
    disk_io_time = reply.disk_io_time();
    network_time = reply.network_time();
    decode_time = reply.decode_time();
    network_time += std::chrono::duration_cast<std::chrono::duration<double>>(end - receive_start).count();
    double coordinator_gRPC_delay = (reply.grpc_start_time() - std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count());
    network_time += coordinator_gRPC_delay;
    //std::cout << "[Client] degraded read success!" << std::endl;
    return buf;
  }

  std::shared_ptr<char[]> Client::get_degraded_read_block(int stripe_id, int failed_block_id)
  {
    unsigned int block_size = m_sys_config->BlockSize;
    grpc::ClientContext context;
    coordinator_proto::KeyAndClientIP request;
    request.set_key(std::to_string(stripe_id) + "_" + std::to_string(failed_block_id));
    request.set_clientip(m_clientIPForGet);
    request.set_clientport(m_clientPortForGet);
    coordinator_proto::DegradedReadReply reply;
    std::thread t([&context, &request, &reply, this]() {
      grpc::Status status = m_coordinator_ptr->getDegradedReadBlock(&context, request, &reply);
      if (!status.ok())
      {
        std::cout << "[Client] degraded read failed!" << std::endl;
      }
    });
    asio::ip::tcp::socket socket(io_context);
    acceptor.accept(socket);
    asio::error_code error;
    std::shared_ptr<char[]> buf(new char[block_size]);
    //std::cout << "start to read" << std::endl;
    size_t len = asio::read(socket, asio::buffer(buf.get(), block_size), error);
    if(len != block_size){
      std::cout << "[Error] len != block_size: " << len << std::endl;
      return nullptr;
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
    socket.close(ignore_ec); 
    t.join();
    //std::cout << "[Client] degraded read success!" << std::endl;
    return buf;
  }

  bool Client::recovery(int stripe_id, int failed_block_id)
  {
    grpc::ClientContext context;
    coordinator_proto::KeyAndClientIP request;
    request.set_key(std::to_string(stripe_id) + "_" + std::to_string(failed_block_id));
    request.set_clientip(m_clientIPForGet);
    request.set_clientport(m_clientPortForGet);

    coordinator_proto::RecoveryReply reply;
    grpc::Status status = m_coordinator_ptr->getRecovery(&context, request, &reply);

    if (!status.ok())
    {
      std::cout << "[Client] recovery failed!" << std::endl;
      return false;
    }

    return true;
  }

  bool Client::recovery_breakdown(int stripe_id, int failed_block_id, double &disk_read_time, double &network_time, double &decode_time, double &disk_write_time)
  {
    grpc::ClientContext context;
    coordinator_proto::KeyAndClientIP request;
    request.set_key(std::to_string(stripe_id) + "_" + std::to_string(failed_block_id));
    request.set_clientip(m_clientIPForGet);
    request.set_clientport(m_clientPortForGet);

    coordinator_proto::RecoveryReply reply;
    std::chrono::high_resolution_clock::time_point grpc_notify = std::chrono::high_resolution_clock::now();
    grpc::Status status = m_coordinator_ptr->getRecoveryBreakdown(&context, request, &reply);

    if (!status.ok())
    {
      std::cout << "[Client] recovery failed!" << std::endl;
      return false;
    }
    disk_read_time = reply.disk_read_time();
    network_time = reply.network_time();
    decode_time = reply.decode_time();
    double coordinator_gRPC_delay = (reply.grpc_start_time() - std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count());
    network_time += coordinator_gRPC_delay;
    disk_write_time = reply.disk_write_time();

    return true;
  }

  int Client::recovery_full_node(int node_id){
    grpc::ClientContext context;
    coordinator_proto::NodeIdFromClient request;
    request.set_node_id(node_id);

    coordinator_proto::RepBlockNum reply;
    grpc::Status status = m_coordinator_ptr->fullNodeRecovery(&context, request, &reply);
    if (!status.ok())
    {
      std::cout << "[Client] recovery full node failed!" << std::endl;
      return false;
    }
    return reply.block_num();
  }

  std::shared_ptr<char[]> Client::get(std::string key, size_t &data_size)
  {
    grpc::ClientContext context;
    coordinator_proto::KeyAndClientIP request;
    request.set_key(key);
    request.set_clientip(m_clientIPForGet);
    request.set_clientport(m_clientPortForGet);

    coordinator_proto::ReplyProxyIPsPorts reply;
    //std::cout << "getting stripe" << std::endl;
    grpc::Status status = m_coordinator_ptr->getStripe(&context, request, &reply);
    
    if(!status.ok())
    {
      std::cout << "[Client] get stripe failed!" << std::endl;
      return nullptr;
    }

    int data_block_num = m_sys_config->k;
    int block_size = m_sys_config->BlockSize;
    data_size = static_cast<size_t>(data_block_num) * static_cast<size_t>(block_size);
    
    std::shared_ptr<char[]> data_ptr_array(new char[data_size]);
    
    std::vector<std::thread> threads;
    for(int i = 0; i < data_block_num; i++)
    {
      threads.push_back(std::thread([this, i, data_ptr_array, block_size]() mutable {
        asio::io_context io_context;
        asio::ip::tcp::socket socket_data(io_context);
        this->acceptor.accept(socket_data);
        uint32_t block_id;
        asio::read(socket_data, asio::buffer(&block_id, sizeof(uint32_t)));
        asio::error_code error;
        size_t len = asio::read(socket_data, asio::buffer(data_ptr_array.get() + block_id * static_cast<size_t>(block_size), block_size), error);
        if(len != block_size)
        {
          std::cout << "[Client] get stripe block failed!" << std::endl;
        }
        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
        socket_data.close(ignore_ec);
      }));
    }
    
    for(auto &thread : threads)
    {
      thread.join();
    }
    
    return data_ptr_array;
  }
  
  //for workload
  std::shared_ptr<char[]> Client::get_blocks(int start_block_id, int end_block_id)
  {
    grpc::ClientContext context;
    coordinator_proto::BlockIDsAndClientIP request;
    request.set_start_block_id(start_block_id);
    request.set_end_block_id(end_block_id);
    request.set_clientip(m_clientIPForGet);
    request.set_clientport(m_clientPortForGet);

    coordinator_proto::ReplyProxyIPsPorts reply;
    bool is_get_blocks = false;
    std::thread notify_thread([&context, &request, &reply, this, &is_get_blocks]() {
      grpc::Status status;
      status = m_coordinator_ptr->getBlocks(&context, request, &reply);
      if (status.ok())
      {
        is_get_blocks = true;
      }
      else
      {
        std::cout << "[Client] get blocks failed!" << std::endl;
      }
    });

    int block_num = end_block_id - start_block_id + 1;
    int block_size = m_sys_config->BlockSize;
    //char * data_ptr_array = new char[static_cast<size_t>(block_num) * static_cast<size_t>(block_size)];
    std::shared_ptr<char[]> data_ptr_array(new char[static_cast<size_t>(block_num) * static_cast<size_t>(block_size)]);
    char * data_ptr_array_raw = data_ptr_array.get();
    std::vector<std::thread> threads;
    for(int i = 0; i < block_num; i++)
    {
      threads.push_back(std::thread(([this, &reply, i, data_ptr_array_raw, block_size]()mutable {
        asio::io_context io_context;
        asio::ip::tcp::socket socket_data(io_context);
        this->acceptor.accept(socket_data);
        uint32_t block_id;
        asio::read(socket_data, asio::buffer(&block_id, sizeof(uint32_t)));
        asio::error_code error;
        size_t len = asio::read(socket_data, asio::buffer(data_ptr_array_raw + block_id * static_cast<size_t>(block_size), block_size), error);
        if(len != block_size)
        {
          std::cout << "[Client] get blocks failed!" << std::endl;
        }
        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
        socket_data.close(ignore_ec);
      })));
    }
    for(auto &thread : threads)
    {
      thread.join();
    }
    notify_thread.join();
    if (!is_get_blocks)
    {
      std::cout << "[Client] get blocks failed!" << std::endl;
      return nullptr;
    }
    //std::cout << "[Client] get blocks success!" << std::endl;
    
    return data_ptr_array;
  }

  std::shared_ptr<char[]> Client::get_degraded_read_blocks(int start_block_id, int end_block_id)
  {
    grpc::ClientContext context;
    coordinator_proto::BlockIDsAndClientIP request;
    request.set_start_block_id(start_block_id);
    request.set_end_block_id(end_block_id);
    request.set_clientip(m_clientIPForGet);
    request.set_clientport(m_clientPortForGet);

    coordinator_proto::ReplyProxyIPsPorts reply;
    bool is_get_blocks = false;
    std::thread notify_thread([&context, &request, &reply, this, &is_get_blocks]() {
      grpc::Status status;
      status = m_coordinator_ptr->getDegradedReadBlocks(&context, request, &reply);
      if (status.ok())
      {
        is_get_blocks = true;
      }
      else
      {
        std::cout << "[Client] get blocks failed!" << std::endl;
      }
    });

    int block_num = end_block_id - start_block_id + 1;
    int block_size = m_sys_config->BlockSize;
    //char * data_ptr_array = new char[static_cast<size_t>(block_num) * static_cast<size_t>(block_size)];
    std::shared_ptr<char[]> data_ptr_array(new char[static_cast<size_t>(block_num) * static_cast<size_t>(block_size)]);
    char * data_ptr_array_raw = data_ptr_array.get();
    std::vector<std::thread> threads;
    for(int i = 0; i < block_num; i++)
    {
      threads.push_back(std::thread(([this, &reply, i, data_ptr_array_raw, block_size]()mutable {
        asio::io_context io_context;
        asio::ip::tcp::socket socket_data(io_context);
        this->acceptor.accept(socket_data);
        uint32_t block_id;
        asio::read(socket_data, asio::buffer(&block_id, sizeof(uint32_t)));
        asio::error_code error;
        size_t len = asio::read(socket_data, asio::buffer(data_ptr_array_raw + block_id * static_cast<size_t>(block_size), block_size), error);
        if(len != block_size)
        {
          std::cout << "[Client] get blocks failed!" << std::endl;
        }
        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
        socket_data.close(ignore_ec);
      })));
    }
    for(auto &thread : threads)
    {
      thread.join();
    }
    notify_thread.join();
    if (!is_get_blocks)
    {
      std::cout << "[Client] get blocks failed!" << std::endl;
      return nullptr;
    }
    //std::cout << "[Client] get blocks success!" << std::endl;
    
    return data_ptr_array;
  }

  /*
    Function: get
    1. send the get request including the information of key and clientipport to the coordinator
    2. accept the value transferred from the proxy
  */
  bool Client::get(std::string key, std::string &value)
  {
    grpc::ClientContext context;
    coordinator_proto::KeyAndClientIP request;
    request.set_key(key);
    request.set_clientip(m_clientIPForGet);
    request.set_clientport(m_clientPortForGet);
    // request
    coordinator_proto::RepIfGetSuccess reply;
    grpc::Status status = m_coordinator_ptr->getValue(&context, request, &reply);
    asio::ip::tcp::socket socket_data(io_context);
    int value_size = reply.valuesizebytes();
    acceptor.accept(socket_data);
    asio::error_code error;
    std::vector<char> buf_key(key.size());
    std::vector<char> buf(value_size);
    // read from socket
    size_t len = asio::read(socket_data, asio::buffer(buf_key, key.size()), error);
    int flag = 1;
    for (int i = 0; i < int(key.size()); i++)
    {
      if (key[i] != buf_key[i])
      {
        flag = 0;
      }
    }
    if (flag)
    {
      len = asio::read(socket_data, asio::buffer(buf, value_size), error);
    }
    else
    {
      std::cout << "[GET] key not matches!" << std::endl;
    }
    asio::error_code ignore_ec;
    socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
    socket_data.close(ignore_ec);
    if (flag)
    {
      std::cout << "[GET] get key: " << buf_key.data() << " ,valuesize: " << len << std::endl;
    }
    value = std::string(buf.data(), buf.size());
    return true;
  }


  /*
    Function: delete
    1. send the get request including the information of key to the coordinator
  */
  bool Client::delete_key(std::string key)
  {
    grpc::ClientContext context;
    coordinator_proto::KeyFromClient request;
    request.set_key(key);
    coordinator_proto::RepIfDeling reply;
    grpc::Status status = m_coordinator_ptr->delByKey(&context, request, &reply);
    if (status.ok())
    {
      if (reply.ifdeling())
      {
        std::cout << "[DEL] deleting " << key << std::endl;
      }
      else
      {
        std::cout << "[DEL] delete failed!" << std::endl;
      }
    }
    // check if metadata is saved successfully
    grpc::ClientContext check_commit;
    coordinator_proto::AskIfSuccess req;
    req.set_key(key);
    ECProject::OpperateType opp = DEL;
    req.set_opp(opp);
    req.set_stripe_id(-1);
    coordinator_proto::RepIfSuccess rep;
    grpc::Status stat;
    stat = m_coordinator_ptr->checkCommitAbort(&check_commit, req, &rep);
    if (stat.ok())
    {
      if (rep.ifcommit())
      {
        return true;
      }
      else
      {
        std::cout << "[DEL]" << key << " not delete!!!!!";
      }
    }
    else
    {
      std::cout << "[DEL]" << key << " Fail to check!!!!!";
    }
    return false;
  }

  bool Client::delete_stripe(int stripe_id)
  {
    grpc::ClientContext context;
    coordinator_proto::StripeIdFromClient request;
    request.set_stripe_id(stripe_id);
    coordinator_proto::RepIfDeling reply;
    grpc::Status status = m_coordinator_ptr->delByStripe(&context, request, &reply);
    if (status.ok())
    {
      if (reply.ifdeling())
      {
        std::cout << "[DEL] deleting Stripe " << stripe_id << std::endl;
      }
      else
      {
        std::cout << "[DEL] delete failed!" << std::endl;
      }
    }
    // check if metadata is saved successfully
    grpc::ClientContext check_commit;
    coordinator_proto::AskIfSuccess req;
    req.set_key("");
    ECProject::OpperateType opp = DEL;
    req.set_opp(opp);
    req.set_stripe_id(stripe_id);
    coordinator_proto::RepIfSuccess rep;
    grpc::Status stat;
    stat = m_coordinator_ptr->checkCommitAbort(&check_commit, req, &rep);
    if (stat.ok())
    {
      if (rep.ifcommit())
      {
        return true;
      }
      else
      {
        std::cout << "[DEL] Stripe" << stripe_id << " not delete!!!!!";
      }
    }
    else
    {
      std::cout << "[DEL] Stripe" << stripe_id << " Fail to check!!!!!";
    }
    return false;
  }

  bool Client::delete_all_stripes()
  {
    grpc::ClientContext context;
    coordinator_proto::RepStripeIds rep;
    coordinator_proto::RequestToCoordinator req;
    grpc::Status status = m_coordinator_ptr->listStripes(&context, req, &rep);
    if (status.ok())
    {
      std::cout << "Deleting all stripes!" << std::endl;
      for (int i = 0; i < int(rep.stripe_ids_size()); i++)
      {
        delete_stripe(rep.stripe_ids(i));
      }
      return true;
    }
    return false;
  }

  std::vector<int> Client::get_parameters()
  {
    std::vector<int> parameters;
    parameters.push_back(m_sys_config->k);
    parameters.push_back(m_sys_config->r);
    parameters.push_back(m_sys_config->z);
    parameters.push_back(m_sys_config->BlockSize);
    if(is_azure_like_code(m_sys_config->CodeType))
    {
      parameters.push_back(0);
    }
    else if(m_sys_config->CodeType == "OptimalLRC")
    {
      parameters.push_back(1);
    }
    else if(m_sys_config->CodeType == "UniformLRC")
    {
      parameters.push_back(2);
    }
    else if(m_sys_config->CodeType == "UniLRC")
    {
      parameters.push_back(3);
    }
    else
    {
      std::cout << "[Client] CodeType not supported!" << std::endl;
      return {};
    }
    return parameters;
  }

  bool Client::decode_test(int stripe_id, int failed_block_id, std::string client_ip, int client_port, double &decode_time)
  {
    grpc::ClientContext context;
    coordinator_proto::KeyAndClientIP request;
    request.set_key(std::to_string(stripe_id) + "_" + std::to_string(failed_block_id));
    request.set_clientip(client_ip);
    request.set_clientport(client_port);

    coordinator_proto::DegradedReadReply reply;
    grpc::Status status = m_coordinator_ptr->decodeTest(&context, request, &reply);
    decode_time = reply.decode_time();
    if (!status.ok())
    {
      std::cout << "[Client] decode test failed!" << std::endl;
      return false;
    }
    return true;
  }

  bool Client::multi_block_recovery(int stripe_id, std::vector<int> block_ids)
  {
    grpc::ClientContext context;
    coordinator_proto::StripeIdAndBlockIDsFromClient request;
    request.set_stripe_id(stripe_id);
    for(int i = 0; i < block_ids.size(); i++)
    {
      request.add_block_ids(block_ids[i]);
    }

    coordinator_proto::RecoveryReply reply;
    grpc::Status status = m_coordinator_ptr->multiBlockRecovery(&context, request, &reply);
    if (!status.ok())
    {
      std::cout << "[Client] multi block recovery failed!" << std::endl;
      return false;
    }
    return true;
  }
} // namespace ECProject