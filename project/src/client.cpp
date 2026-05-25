#include "client.h"
#include "coordinator.grpc.pb.h"
#include "proxy.grpc.pb.h"
#include "proxy.pb.h"

#include <asio.hpp>
#include <thread>
#include <assert.h>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <map>
#include <random>
#include <iomanip>
#include "unilrc_encoder.h"
namespace ECProject
{
  namespace
  {
    bool is_azure_like_code(const std::string &code_type)
    {
      return code_type == "AzureLRC" || code_type == "RandomLRC";
    }

    inline double chron_elapsed_s(std::chrono::high_resolution_clock::time_point a,
                                    std::chrono::high_resolution_clock::time_point b)
    {
      return std::chrono::duration<double>(b - a).count();
    }

    // 必须与 coordinator.cpp 匿名命名空间中的 RackCuClientStep 取值一致（与历史 group_ids 兼容）

    constexpr double kRackCuCommitWaitTimeoutSec = 0.5;

    bool check_append_committed_with_timeout(coordinator_proto::coordinatorService::Stub *stub,
                                             const std::string &append_key,
                                             int stripe_id,
                                             double timeout_sec)
    {
      coordinator_proto::AskIfSuccess request;
      request.set_key(append_key);
      request.set_opp(APPEND);
      if (stripe_id >= 0)
      {
        request.set_stripe_id(stripe_id);
      }
      grpc::ClientContext ctx;
      const auto deadline = std::chrono::system_clock::now() +
                            std::chrono::duration_cast<std::chrono::system_clock::duration>(
                                std::chrono::duration<double>(timeout_sec));
      ctx.set_deadline(deadline);
      coordinator_proto::RepIfSuccess reply;
      const grpc::Status status = stub->checkCommitAbort(&ctx, request, &reply);
      if (!status.ok())
      {
        if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED)
        {
          std::cout << "[RACKCU] commit wait timeout (" << timeout_sec << " s) append_key=" << append_key << std::endl;
        }
        else
        {
          std::cout << "[RACKCU] checkCommitAbort error: " << status.error_message() << " append_key=" << append_key
                    << std::endl;
        }
        return false;
      }
      if (!reply.ifcommit())
      {
        std::cout << "[RACKCU] append_key=" << append_key << " not committed" << std::endl;
        return false;
      }
      return true;
    }

    enum RackCuClientStep : int32_t
    {
      RACKCU_STEP_DATA_HOME = 1,
      RACKCU_STEP_DATA_TO_COLLECTOR = 2,
      RACKCU_STEP_PARITY_GLOBAL = 3,
      RACKCU_STEP_PARITY_GLOBAL_FROM_DATA = 4,
      RACKCU_STEP_LOCAL_PARITY = 5,
    };

    void merge_interval_in_map(std::map<int, std::vector<std::pair<int, int>>> *m, int key, int lo, int hi)
    {
      if (m == nullptr || hi <= lo)
      {
        return;
      }
      std::vector<std::pair<int, int>> &vec = (*m)[key];
      vec.push_back(std::make_pair(lo, hi));
      std::sort(vec.begin(), vec.end());
      std::vector<std::pair<int, int>> merged;
      for (const auto &p : vec)
      {
        if (merged.empty() || p.first > merged.back().second)
        {
          merged.push_back(p);
        }
        else
        {
          merged.back().second = std::max(merged.back().second, p.second);
        }
      }
      vec.swap(merged);
    }

  }

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
      std::vector<std::thread> threads;
      std::vector<char *> cluster_slice_data = m_toolbox->splitCharPointer(m_pre_allocated_buffer, &reply);
      std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
      std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);

      for (int i = 0; i < reply.append_keys_size(); i++)
      {
        threads.push_back(std::thread(&Client::async_append_to_proxies,
                                      this, cluster_slice_data[i], reply.append_keys(i), static_cast<int>(reply.cluster_slice_sizes(i)), reply.proxyips(i), reply.proxyports(i), i, if_commit_arr.get(), -1,
                                      static_cast<std::vector<std::vector<unsigned char>> *>(nullptr), static_cast<RackCuAppendNetworkTiming *>(nullptr), -1.0));
      }
      for (auto &thread : threads)
      {
        thread.join();
      }

      // check if all appends are successful
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

  void Client::async_append_to_proxies(char *cluster_slice_data, std::string append_key, int cluster_slice_size, std::string proxy_ip, int proxy_port, int index, bool *if_commit_arr, int stripe_id, std::vector<std::vector<unsigned char>> *rackcu_delta_by_block,
                                       RackCuAppendNetworkTiming *network_timing_out, double commit_wait_timeout_sec)
  {
    // std::cout << "[Append174] Appending size " << cluster_slice_size << " to proxy_address:" << proxy_ip << ":" << proxy_port << std::endl;
    const auto t_tcp0 = std::chrono::high_resolution_clock::now();
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
    const auto t_tcp1 = std::chrono::high_resolution_clock::now();

    const auto t_commit0 = std::chrono::high_resolution_clock::now();
    bool committed = false;
    if (commit_wait_timeout_sec >= 0.0)
    {
      committed = check_append_committed_with_timeout(m_coordinator_ptr.get(), append_key, stripe_id, commit_wait_timeout_sec);
    }
    else
    {
      grpc::ClientContext check_commit;
      coordinator_proto::AskIfSuccess request;
      request.set_key(append_key);
      OpperateType opp = APPEND;
      request.set_opp(opp);
      if (stripe_id >= 0)
      {
        request.set_stripe_id(stripe_id);
      }
      coordinator_proto::RepIfSuccess reply;
      grpc::Status status = m_coordinator_ptr->checkCommitAbort(&check_commit, request, &reply);
      committed = status.ok() && reply.ifcommit();
      if (status.ok() && !reply.ifcommit())
      {
        std::cout << "[APPEND205] " << append_key << " not commit!!!!!" << " cluster_slice_size: " << cluster_slice_size
                  << " proxy_ip: " << proxy_ip << " proxy_port: " << proxy_port << std::endl;
      }
      else if (!status.ok())
      {
        std::cout << "[APPEND210] " << append_key << " Fail to check!!!!!" << " cluster_slice_size: " << cluster_slice_size
                  << " proxy_ip: " << proxy_ip << " proxy_port: " << proxy_port << std::endl;
      }
    }
    const auto t_commit1 = std::chrono::high_resolution_clock::now();
    if (network_timing_out != nullptr)
    {
      network_timing_out->tcp_resolve_connect_write_shutdown_s = chron_elapsed_s(t_tcp0, t_tcp1);
      network_timing_out->coordinator_check_commit_abort_s = chron_elapsed_s(t_commit0, t_commit1);
    }
    if (committed)
    {
      if_commit_arr[index] = true;
    }
  }

  bool Client::rackcu_tcp_send_payload(const char *data, int size, const std::string &proxy_ip, int proxy_port)
  {
    if (data == nullptr || size < 0)
    {
      return false;
    }
    asio::io_context io_context;
    asio::error_code error;
    asio::ip::tcp::resolver resolver(io_context);
    asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(proxy_ip, std::to_string(proxy_port));
    asio::ip::tcp::socket sock_data(io_context);
    asio::connect(sock_data, endpoints);
    asio::write(sock_data, asio::buffer(data, static_cast<size_t>(size)), error);
    asio::error_code ignore_ec;
    sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    sock_data.close(ignore_ec);
    if (error)
    {
      std::cout << "[RACKCU] rackcu_tcp_send_payload: " << error.message() << " proxy=" << proxy_ip << ":" << proxy_port
                << " bytes=" << size << std::endl;
    }
    return !error;
  }

  bool Client::rackcu_wait_append_committed(const std::string &append_key, int stripe_id)
  {
    return check_append_committed_with_timeout(m_coordinator_ptr.get(), append_key, stripe_id, kRackCuCommitWaitTimeoutSec);
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
      std::vector<std::thread> threads;
      std::vector<char *> cluster_slice_data = m_toolbox->splitCharPointer(m_pre_allocated_buffer, &reply);
      std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
      std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);

      assert(m_sys_config->CodeType == "UniLRC" || m_sys_config->CodeType == "OptimalLRC" || m_sys_config->CodeType == "UniformLRC" || is_azure_like_code(m_sys_config->CodeType));
      std::vector<int> data_block_num_per_group = get_data_block_num_per_group(m_sys_config->k, m_sys_config->r, m_sys_config->z, m_sys_config->CodeType);
      std::vector<int> global_parity_block_num_per_group = get_global_parity_block_num_per_group(m_sys_config->k, m_sys_config->r, m_sys_config->z, m_sys_config->CodeType);
      std::vector<int> local_parity_block_num_per_group = get_local_parity_block_num_per_group(m_sys_config->k, m_sys_config->r, m_sys_config->z, m_sys_config->CodeType);
      std::vector<char *> data_ptr_array, global_parity_ptr_array, local_parity_ptr_array;
      split_for_set_data_and_parity(&reply, cluster_slice_data, data_block_num_per_group, global_parity_block_num_per_group, local_parity_block_num_per_group, data_ptr_array, global_parity_ptr_array, local_parity_ptr_array);
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
      for (int i = 0; i < reply.append_keys_size(); i++)
      {
        threads.push_back(std::thread(&Client::async_append_to_proxies,
                                      this, cluster_slice_data[i], reply.append_keys(i), static_cast<int>(reply.cluster_slice_sizes(i)), reply.proxyips(i), reply.proxyports(i), i, if_commit_arr.get(), -1,
                                      static_cast<std::vector<std::vector<unsigned char>> *>(nullptr), static_cast<RackCuAppendNetworkTiming *>(nullptr), -1.0));
      }
      for (auto &thread : threads)
      {
        thread.join();
      }

      // check if all appends are successful
      bool all_true = std::all_of(if_commit_arr.get(), if_commit_arr.get() + reply.append_keys_size(), [](bool val)
                                  { return val == true; });

      if (all_true)
      {
        std::cout << "[SET437] Client " << m_clientID << " set successfully!" << std::endl;
        return true;
      }
      else
      {
        std::cout << "[SET441] Client " << m_clientID << " set failed!" << std::endl;
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
      std::vector<std::thread> threads;
      std::vector<char *> cluster_slice_data = m_toolbox->splitCharPointer(m_pre_allocated_buffer, &reply);
      std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
      std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);

      assert(m_sys_config->CodeType == "UniLRC" || m_sys_config->CodeType == "OptimalLRC" || m_sys_config->CodeType == "UniformLRC" || is_azure_like_code(m_sys_config->CodeType));
      std::vector<int> data_block_num_per_group = get_data_block_num_per_group(m_sys_config->k, m_sys_config->r, m_sys_config->z, m_sys_config->CodeType);
      int capacity = block_num;
      for(int i = 0; i < data_block_num_per_group.size(); i++)
      {
        if(data_block_num_per_group[i] > capacity){
          data_block_num_per_group[i] = capacity;
        } 
        capacity -= data_block_num_per_group[i];
      }
      std::vector<int> global_parity_block_num_per_group = get_global_parity_block_num_per_group(m_sys_config->k, m_sys_config->r, m_sys_config->z, m_sys_config->CodeType);
      std::vector<int> local_parity_block_num_per_group = get_local_parity_block_num_per_group(m_sys_config->k, m_sys_config->r, m_sys_config->z, m_sys_config->CodeType);
      m_toolbox->remove_common_zeros(data_block_num_per_group, global_parity_block_num_per_group, local_parity_block_num_per_group);

      std::vector<char *> data_ptr_array, global_parity_ptr_array, local_parity_ptr_array;
      split_for_set_data_and_parity(&reply, cluster_slice_data, data_block_num_per_group, global_parity_block_num_per_group, local_parity_block_num_per_group, data_ptr_array, global_parity_ptr_array, local_parity_ptr_array);
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
      for (int i = 0; i < reply.append_keys_size(); i++)
      {
        threads.push_back(std::thread(&Client::async_append_to_proxies,
                                      this, cluster_slice_data[i], reply.append_keys(i), static_cast<int>(reply.cluster_slice_sizes(i)), reply.proxyips(i), reply.proxyports(i), i, if_commit_arr.get(), -1,
                                      static_cast<std::vector<std::vector<unsigned char>> *>(nullptr), static_cast<RackCuAppendNetworkTiming *>(nullptr), -1.0));
      }
      for (auto &thread : threads)
      {
        thread.join();
      }

      // check if all appends are successful
      bool all_true = std::all_of(if_commit_arr.get(), if_commit_arr.get() + reply.append_keys_size(), [](bool val)
                                  { return val == true; });

      if (all_true)
      {
        return true;
      }
      else
      {
        std::cout << "[SET441] Client " << m_clientID << " set failed!" << std::endl;
        return false;
      }
    }

    return false;
  }

  bool Client::xue_update(int stripe_id, const std::vector<std::pair<int, int>> &logical_ranges)
  {
    if (logical_ranges.empty())
    {
      std::cout << "[XUE_UPDATE] Empty logical ranges." << std::endl;
      return false;
    }
    grpc::ClientContext get_proxy_ip_port;
    coordinator_proto::XueUpdateRequest request;
    coordinator_proto::ReplyProxyIPsPorts reply;
    request.set_client_id(m_clientID);
    request.set_stripe_id(stripe_id);
    for (const auto &r : logical_ranges)
    {
      if (r.second <= r.first)
      {
        std::cout << "[XUE_UPDATE] Invalid logical range: [" << r.first
                  << ", " << r.second << ") (require start < end)" << std::endl;
        return false;
      }
      auto *range = request.add_ranges();
      range->set_logical_offset_start(r.first);
      range->set_logical_offset_end(r.second);
    }

    grpc::Status status = m_coordinator_ptr->uploadXueUpdate(&get_proxy_ip_port, request, &reply);
    if (!status.ok())
    {
      std::cout << "[XUE_UPDATE] upload failed: code=" << static_cast<int>(status.error_code()) << " " << status.error_message() << std::endl;
      return false;
    }

    std::vector<std::thread> threads;
    std::vector<char *> cluster_slice_data = m_toolbox->splitCharPointer(m_pre_allocated_buffer, &reply);
    std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
    std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);

    for (int i = 0; i < reply.append_keys_size(); i++)
    {
      threads.push_back(std::thread(&Client::async_append_to_proxies,
                                    this, cluster_slice_data[i], reply.append_keys(i), static_cast<int>(reply.cluster_slice_sizes(i)), reply.proxyips(i), reply.proxyports(i), i, if_commit_arr.get(), -1,
                                    static_cast<std::vector<std::vector<unsigned char>> *>(nullptr), static_cast<RackCuAppendNetworkTiming *>(nullptr), -1.0));
    }
    for (auto &thread : threads)
    {
      thread.join();
    }

    bool all_true = std::all_of(if_commit_arr.get(), if_commit_arr.get() + reply.append_keys_size(), [](bool val)
                                { return val == true; });
    if (!all_true)
    {
      std::cout << "[XUE_UPDATE] commit check failed for at least one cluster slice." << std::endl;
    }
    return all_true;
  }

  bool Client::rackcu_update(int stripe_id, const std::vector<std::pair<int, int>> &logical_ranges)
  {
    if (logical_ranges.empty())
    {
      std::cout << "[RACKCU] Empty logical ranges." << std::endl;
      return false;
    }
    if (!is_azure_like_code(m_sys_config->CodeType))
    {
      std::cout << "[RACKCU] unsupported CodeType: " << m_sys_config->CodeType << std::endl;
      return false;
    }

    const int k = m_sys_config->k;
    const int r = m_sys_config->r;
    const int z = m_sys_config->z;
    const int block_size = static_cast<int>(m_sys_config->BlockSize);
    const int stripe_data_bytes = k * block_size;

    std::map<int, std::vector<std::pair<int, int>>> block_intervals;
    for (const auto &r : logical_ranges)
    {
      if (r.second <= r.first)
      {
        std::cout << "[RACKCU] Invalid logical range: [" << r.first << ", " << r.second << ")" << std::endl;
        return false;
      }
      if (r.first < 0 || r.second > stripe_data_bytes)
      {
        std::cout << "[RACKCU] logical range out of data stripe range" << std::endl;
        return false;
      }
      int pos = r.first;
      const int logical_end = r.second - 1;
      while (pos <= logical_end)
      {
        const int bid = pos / block_size;
        const int off = pos % block_size;
        const int tail = block_size - off;
        const int len = std::min(tail, logical_end - pos + 1);
        merge_interval_in_map(&block_intervals, bid, off, off + len);
        pos += len;
      }
    }
    if (block_intervals.empty())
    {
      std::cout << "[RACKCU] no affected data blocks" << std::endl;
      return false;
    }

    std::vector<int> touched;
    touched.reserve(block_intervals.size());
    for (const auto &bp : block_intervals)
    {
      touched.push_back(bp.first);
    }
    std::sort(touched.begin(), touched.end());

    grpc::ClientContext ctx;
    coordinator_proto::RackCuUpdateRequest request;
    coordinator_proto::ReplyProxyIPsPorts reply;
    request.set_client_id(m_clientID);
    request.set_stripe_id(stripe_id);
    for (const auto &r : logical_ranges)
    {
      auto *range = request.add_ranges();
      range->set_logical_offset_start(r.first);
      range->set_logical_offset_end(r.second);
    }

    const auto t_coord0 = std::chrono::high_resolution_clock::now();
    grpc::Status status = m_coordinator_ptr->uploadRackCuUpdate(&ctx, request, &reply);
    const auto t_coord1 = std::chrono::high_resolution_clock::now();
    if (!status.ok())
    {
      std::cout << "[RACKCU] upload failed: code=" << static_cast<int>(status.error_code()) << " " << status.error_message() << std::endl;
      return false;
    }
    const int nsteps = reply.append_keys_size();
    if (nsteps != reply.cluster_slice_sizes_size() || nsteps != reply.proxyips_size() || nsteps != reply.proxyports_size() ||
        nsteps != reply.append_plans_size() || nsteps != reply.group_ids_size())
    {
      std::cout << "[RACKCU] malformed reply from coordinator" << std::endl;
      return false;
    }
    if (reply.rack_cu_scheduled_slots_size() != 0 && reply.rack_cu_scheduled_slots_size() != nsteps)
    {
      std::cout << "[RACKCU] malformed rack_cu_scheduled_slots size from coordinator" << std::endl;
      return false;
    }

    auto ptr_for_block = [&](int block_id) -> unsigned char * {
      return reinterpret_cast<unsigned char *>(m_pre_allocated_buffer + static_cast<size_t>(block_id) * static_cast<size_t>(block_size));
    };

    auto pack_slices_in_plan_order = [&](const proxy_proto::AppendStripeDataPlacement &plan, char *dst) -> size_t {
      size_t w = 0;
      for (int j = 0; j < plan.blockids_size(); j++)
      {
        const int bid = plan.blockids(j);
        const int off = static_cast<int>(plan.offsets(j));
        const int len = static_cast<int>(plan.sizes(j));
        if (len < 0 || off < 0 || off + len > block_size)
        {
          return 0;
        }
        std::memcpy(dst + w, ptr_for_block(bid) + off, static_cast<size_t>(len));
        w += static_cast<size_t>(len);
      }
      return w;
    };

    std::vector<int> dispatch_order;
    dispatch_order.reserve(static_cast<size_t>(nsteps));
    for (int i = 0; i < nsteps; i++)
    {
      dispatch_order.push_back(i);
    }
    // 先 DATA_HOME，再（若存在）DATA_TO_COLLECTOR，最后 parity；与历史 group_ids 分桶一致。
    auto rackcu_step_bucket = [](int32_t s) -> int {
      if (s == RACKCU_STEP_DATA_HOME)
      {
        return 0;
      }
      if (s == RACKCU_STEP_DATA_TO_COLLECTOR)
      {
        return 1;
      }
      return 2;
    };
    const bool rack_cu_use_slots = (reply.rack_cu_scheduled_slots_size() == nsteps);
    std::stable_sort(dispatch_order.begin(), dispatch_order.end(),
                     [&](int lhs, int rhs) {
                       if (rack_cu_use_slots)
                       {
                         const uint32_t sl = reply.rack_cu_scheduled_slots(lhs);
                         const uint32_t sr = reply.rack_cu_scheduled_slots(rhs);
                         if (sl != sr)
                         {
                           return sl < sr;
                         }
                       }
                       const int bl = rackcu_step_bucket(reply.group_ids(lhs));
                       const int br = rackcu_step_bucket(reply.group_ids(rhs));
                       if (bl != br)
                       {
                         return bl < br;
                       }
                       return lhs < rhs;
                     });

    double rackcu_sum_prepare_s = 0.0;
    double rackcu_sum_tcp_s = 0.0;
    double rackcu_sum_commit_check_s = 0.0;

    int oi_serial = 0;
    int home_prefix = 0;
    while (home_prefix < nsteps && reply.group_ids(dispatch_order[static_cast<size_t>(home_prefix)]) == RACKCU_STEP_DATA_HOME)
    {
      ++home_prefix;
    }

    if (home_prefix >= 2)
    {
      struct RackCuHomeWaveItem
      {
        int plan_idx = 0;
        int cluster_id = 0;
        int32_t step = 0;
        std::vector<char> buf;
        int slice_size = 0;
        std::string proxy_ip;
        int proxy_port = 0;
        std::string append_key;
        double prepare_s = 0.0;
        double tcp_s = 0.0;
        bool tcp_ok = false;
      };
      std::vector<RackCuHomeWaveItem> home_items;
      home_items.reserve(static_cast<size_t>(home_prefix));

      for (int oi = 0; oi < home_prefix; ++oi)
      {
        const auto t_step_begin = std::chrono::high_resolution_clock::now();
        const int i = dispatch_order[static_cast<size_t>(oi)];
        proxy_proto::AppendStripeDataPlacement plan;
        if (!plan.ParseFromString(reply.append_plans(i)))
        {
          std::cout << "[RACKCU] failed to parse append plan (DATA_HOME wave)" << std::endl;
          return false;
        }
        const size_t slice_size = static_cast<size_t>(reply.cluster_slice_sizes(i));
        std::vector<char> buf(std::max(slice_size, static_cast<size_t>(1)), 0);
        char *p = buf.data();
        const int32_t step = reply.group_ids(i);
        if (step != RACKCU_STEP_DATA_HOME)
        {
          std::cout << "[RACKCU] internal: DATA_HOME wave contains non-home step" << std::endl;
          return false;
        }
        std::cout << "[RACKCU][Dispatch] step=" << step << " (parallel wave prep) to cluster c" << plan.cluster_id() << " bytes=" << slice_size
                  << std::endl;
        const size_t w = pack_slices_in_plan_order(plan, p);
        if (w != slice_size)
        {
          std::cout << "[RACKCU] packed size mismatch for DATA_HOME wave step " << step << std::endl;
          return false;
        }
        const auto t_before_tcp = std::chrono::high_resolution_clock::now();
        RackCuHomeWaveItem it;
        it.plan_idx = i;
        it.cluster_id = plan.cluster_id();
        it.step = step;
        it.buf = std::move(buf);
        it.slice_size = static_cast<int>(slice_size);
        it.proxy_ip = reply.proxyips(i);
        it.proxy_port = reply.proxyports(i);
        it.append_key = reply.append_keys(i);
        it.prepare_s = chron_elapsed_s(t_step_begin, t_before_tcp);
        home_items.push_back(std::move(it));
      }

      // 同一 (proxy_ip, proxy_port) 上，Proxy 对每个 scheduleAppend 起一个线程各自 accept()；若对该端点并发连
      // 多个 TCP，accept 与 handler 会错配。按端点分组：组内按 dispatch 顺序串行 TCP，不同端点之间仍并行。
      std::map<std::string, std::vector<size_t>> endpoint_to_wave_indices;
      for (size_t wi = 0; wi < home_items.size(); ++wi)
      {
        const std::string ep = home_items[wi].proxy_ip + ":" + std::to_string(home_items[wi].proxy_port);
        endpoint_to_wave_indices[ep].push_back(wi);
      }

      const auto t_wave_tcp0 = std::chrono::high_resolution_clock::now();
      std::vector<std::thread> ep_threads;
      ep_threads.reserve(endpoint_to_wave_indices.size());
      for (const auto &kv : endpoint_to_wave_indices)
      {
        const std::vector<size_t> indices = kv.second;
        ep_threads.emplace_back([this, indices, &home_items]() {
          for (const size_t idx : indices)
          {
            const auto t0 = std::chrono::high_resolution_clock::now();
            RackCuHomeWaveItem &it = home_items[idx];
            it.tcp_ok = rackcu_tcp_send_payload(it.buf.data(), it.slice_size, it.proxy_ip, it.proxy_port);
            it.tcp_s = chron_elapsed_s(t0, std::chrono::high_resolution_clock::now());
            if (!it.tcp_ok)
            {
              return;
            }
          }
        });
      }
      for (auto &th : ep_threads)
      {
        th.join();
      }
      const auto t_wave_tcp1 = std::chrono::high_resolution_clock::now();
      const double wave_tcp_wall_s = chron_elapsed_s(t_wave_tcp0, t_wave_tcp1);

      for (size_t wi = 0; wi < home_items.size(); ++wi)
      {
        if (!home_items[wi].tcp_ok)
        {
          std::cout << "[RACKCU] DATA_HOME parallel wave: TCP send failed cluster_id=" << home_items[wi].cluster_id
                    << " proxy=" << home_items[wi].proxy_ip << ":" << home_items[wi].proxy_port << std::endl;
          return false;
        }
      }

      for (size_t wi = 0; wi < home_items.size(); ++wi)
      {
        RackCuHomeWaveItem &it = home_items[wi];
        rackcu_sum_prepare_s += it.prepare_s;
        rackcu_sum_tcp_s += it.tcp_s;
        const auto t_commit0 = std::chrono::high_resolution_clock::now();
        std::cout << "[RACKCU][Dispatch] wait_commit begin step=" << it.step << " c" << it.cluster_id << " proxy=" << it.proxy_ip << ":" << it.proxy_port
                  << " append_key=" << it.append_key << " (after parallel TCP wave)" << std::endl;
        const bool ok = rackcu_wait_append_committed(it.append_key, stripe_id);
        const auto t_commit1 = std::chrono::high_resolution_clock::now();
        const double commit_s = chron_elapsed_s(t_commit0, t_commit1);
        rackcu_sum_commit_check_s += commit_s;
        const double step_total_s = it.prepare_s + it.tcp_s + commit_s;
        std::cout << "[RACKCU][Timing] step=" << it.step << " cluster_id=" << it.cluster_id << " slice_bytes=" << it.slice_size
                  << " prepare_parse_pack_encode_s=" << std::fixed << std::setprecision(6) << it.prepare_s << " tcp_to_proxy_s=" << it.tcp_s
                  << " coordinator_checkCommitAbort_s=" << commit_s << " step_total_s=" << step_total_s << std::endl;
        std::cout << "[RACKCU][Dispatch] wait_commit end step=" << it.step << " c" << it.cluster_id << " ok=" << ok << std::endl;
        if (!ok)
        {
          return false;
        }
      }
      std::cout << "[RACKCU][Timing] data_home_parallel_wave n=" << home_prefix << " tcp_wall_s=" << std::fixed << std::setprecision(6) << wave_tcp_wall_s
                << std::endl;
      oi_serial = home_prefix;
    }

    for (int oi = oi_serial; oi < nsteps; oi++)
    {
      const auto t_step_begin = std::chrono::high_resolution_clock::now();
      const int i = dispatch_order[static_cast<size_t>(oi)];
      proxy_proto::AppendStripeDataPlacement plan;
      if (!plan.ParseFromString(reply.append_plans(i)))
      {
        std::cout << "[RACKCU] failed to parse append plan" << std::endl;
        return false;
      }

      const size_t slice_size = static_cast<size_t>(reply.cluster_slice_sizes(i));
      std::vector<char> buf(std::max(slice_size, static_cast<size_t>(1)), 0);
      char *p = buf.data();

      const int32_t step = reply.group_ids(i);
      // Parity / local 等仍串行；多个 DATA_HOME 时已在上方并行 TCP + 顺序 checkCommitAbort。
      std::cout << "[RACKCU][Dispatch] step=" << step
                << " to cluster c" << plan.cluster_id()
                << " bytes=" << slice_size << std::endl;
      switch (step)
      {
      case RACKCU_STEP_DATA_HOME:
      {
        const size_t w = pack_slices_in_plan_order(plan, p);
        if (w != slice_size)
        {
          std::cout << "[RACKCU] packed size mismatch for step " << step << std::endl;
          return false;
        }
        break;
      }
      case RACKCU_STEP_DATA_TO_COLLECTOR:
      {
        std::cout << "[RACKCU] 本 client 不再执行 DATA_TO_COLLECTOR（group_ids=2）；请升级 coordinator/proxy 至同版本，"
                     "或与仍下发该步的旧 coordinator 配套的旧 client 一起使用"
                  << std::endl;
        return false;
      }
      case RACKCU_STEP_PARITY_GLOBAL_FROM_DATA:
      case RACKCU_STEP_PARITY_GLOBAL:
      {
        if (plan.append_mode().find("HOME_DELTA_STAGING") != std::string::npos)
        {
          if (slice_size != 0)
          {
            std::cout << "[RACKCU] parity staging step expects cluster_slice_sizes==0" << std::endl;
            return false;
          }
          break;
        }
        std::cout << "[RACKCU] unsupported parity append_mode (expected *_HOME_DELTA_STAGING): " << plan.append_mode()
                  << std::endl;
        return false;
      }
      case RACKCU_STEP_LOCAL_PARITY:
      {
        if (!plan.is_merge_parity() || plan.blockids_size() < 2)
        {
          std::cout << "[RACKCU] invalid LOCAL_PARITY plan" << std::endl;
          return false;
        }
        const int tail_id = plan.blockids(plan.blockids_size() - 1);
        if (tail_id < k + r || tail_id >= k + r + z)
        {
          std::cout << "[RACKCU] LOCAL_PARITY tail block id invalid" << std::endl;
          return false;
        }
        const int poff = static_cast<int>(plan.offsets(plan.blockids_size() - 1));
        const int plen = static_cast<int>(plan.sizes(plan.blockids_size() - 1));
        if (poff < 0 || plen < 0 || poff + plen > block_size)
        {
          std::cout << "[RACKCU] LOCAL_PARITY tail slice bounds invalid" << std::endl;
          return false;
        }
        if (plan.append_mode().find("HOME_DELTA_STAGING") != std::string::npos)
        {
          if (slice_size != 0)
          {
            std::cout << "[RACKCU] local parity staging expects cluster_slice_sizes==0" << std::endl;
            return false;
          }
          break;
        }
        if (plan.append_mode().find("RACKCU_LOCAL_FROM_DATA") != std::string::npos)
        {
          std::cout << "[RACKCU] unsupported local parity append_mode: " << plan.append_mode() << std::endl;
          return false;
        }

        // 兼容旧路径：客户端本地计算 local parity delta 后发送
        const int group = tail_id - k - r;
        std::vector<int> local_touched;
        for (int j = 0; j + 1 < plan.blockids_size(); j++)
        {
          local_touched.push_back(plan.blockids(j));
        }
        std::sort(local_touched.begin(), local_touched.end());
        std::vector<unsigned char *> dptrs;
        dptrs.reserve(local_touched.size());
        std::vector<std::vector<unsigned char>> tmp_blocks(static_cast<size_t>(local_touched.size()),
                                                          std::vector<unsigned char>(static_cast<size_t>(block_size), 0));
        for (size_t t = 0; t < local_touched.size(); t++)
        {
          const int dbid = local_touched[t];
          unsigned char *row = tmp_blocks[t].data();
          std::memset(row, 0, static_cast<size_t>(block_size));
          for (const auto &seg : block_intervals[dbid])
          {
            std::memcpy(row + seg.first, ptr_for_block(dbid) + seg.first, static_cast<size_t>(seg.second - seg.first));
          }
          dptrs.push_back(row);
        }
        std::vector<std::vector<unsigned char>> parity_out(static_cast<size_t>(r + z), std::vector<unsigned char>(static_cast<size_t>(block_size), 0));
        std::vector<unsigned char *> pptrs;
        pptrs.reserve(static_cast<size_t>(r + z));
        for (int j = 0; j < r + z; j++)
        {
          pptrs.push_back(parity_out[static_cast<size_t>(j)].data());
        }
        ECProject::partial_encode_azure_lrc(k, r, z, static_cast<int>(dptrs.size()), dptrs.data(), pptrs.data(), block_size);
        const int local_idx = r + group;
        std::memcpy(p, parity_out[static_cast<size_t>(local_idx)].data() + poff, static_cast<size_t>(plen));
        if (slice_size != static_cast<size_t>(plen))
        {
          std::cout << "[RACKCU] LOCAL_PARITY slice size mismatch" << std::endl;
          return false;
        }
        break;
      }
      default:
        std::cout << "[RACKCU] unknown step tag: " << step << std::endl;
        return false;
      }

      const auto t_before_async = std::chrono::high_resolution_clock::now();
      const double step_prepare_s = chron_elapsed_s(t_step_begin, t_before_async);
      rackcu_sum_prepare_s += step_prepare_s;

      bool ok = true;
      RackCuAppendNetworkTiming net_t;
      std::cout << "[RACKCU][Dispatch] wait_commit begin step=" << step << " c" << plan.cluster_id()
                << " proxy=" << reply.proxyips(i) << ":" << reply.proxyports(i)
                << " append_key=" << reply.append_keys(i) << std::endl;
      async_append_to_proxies(p, reply.append_keys(i), static_cast<int>(slice_size), reply.proxyips(i), reply.proxyports(i), 0, &ok, stripe_id,
                              nullptr, &net_t, kRackCuCommitWaitTimeoutSec);
      const auto t_step_end = std::chrono::high_resolution_clock::now();
      rackcu_sum_tcp_s += net_t.tcp_resolve_connect_write_shutdown_s;
      rackcu_sum_commit_check_s += net_t.coordinator_check_commit_abort_s;
      const double step_total_s = chron_elapsed_s(t_step_begin, t_step_end);
      std::cout << "[RACKCU][Timing] step=" << step << " cluster_id=" << plan.cluster_id() << " slice_bytes=" << slice_size
                << " prepare_parse_pack_encode_s=" << std::fixed << std::setprecision(6) << step_prepare_s
                << " tcp_to_proxy_s=" << net_t.tcp_resolve_connect_write_shutdown_s
                << " coordinator_checkCommitAbort_s=" << net_t.coordinator_check_commit_abort_s
                << " step_total_s=" << step_total_s << std::endl;
      std::cout << "[RACKCU][Dispatch] wait_commit end step=" << step << " c" << plan.cluster_id() << " ok=" << ok
                << std::endl;
      if (!ok)
      {
        return false;
      }
    }

    const double s_coord_rack = chron_elapsed_s(t_coord0, t_coord1);

    // 全轮（含所有 parity）成功后：删除各 holder 上 datanode 的 home Δ 暂存；校验增量已合并进正式 parity 块，无单独暂存
    const auto t_cleanup0 = std::chrono::high_resolution_clock::now();
    for (int ci = 0; ci < reply.rack_cu_staging_cleanup_size(); ci++)
    {
      const coordinator_proto::RackCuStagingCleanupRef &r = reply.rack_cu_staging_cleanup(ci);
      const std::string channel = r.holder_proxy_ip() + ":" + std::to_string(r.holder_proxy_port());
      auto ch = grpc::CreateChannel(channel, grpc::InsecureChannelCredentials());
      auto stub = proxy_proto::proxyService::NewStub(ch);
      proxy_proto::RackCuHomeDeltaDeleteRequest dreq;
      dreq.set_staging_key(r.staging_key());
      dreq.set_staging_datanode_ip(r.staging_datanode_ip());
      dreq.set_staging_datanode_port(r.staging_datanode_port());
      grpc::ClientContext dctx;
      proxy_proto::RackCuHomeDeltaDeleteReply drep;
      grpc::Status dst = stub->deleteRackCuHomeDeltaStaging(&dctx, dreq, &drep);
      if (!dst.ok() || !drep.ok())
      {
        std::cout << "[RACKCU] staging cleanup failed key=" << r.staging_key() << " holder=" << channel
                  << " grpc=" << (dst.ok() ? "ok" : dst.error_message()) << " del_ok=" << drep.ok() << std::endl;
        return false;
      }
    }
    const auto t_cleanup1 = std::chrono::high_resolution_clock::now();
    const double s_cleanup = chron_elapsed_s(t_cleanup0, t_cleanup1);

    if (reply.rack_cu_xfer_plan_id() != 0)
    {
      grpc::ClientContext pull_ctx;
      coordinator_proto::RackCuXferTimingPullRequest pull_req;
      pull_req.set_stripe_id(stripe_id);
      pull_req.set_xfer_plan_id(reply.rack_cu_xfer_plan_id());
      coordinator_proto::ReplyFromCoordinator pull_rep;
      grpc::Status pst = m_coordinator_ptr->pullRackCuXferTiming(&pull_ctx, pull_req, &pull_rep);
      if (!pst.ok())
      {
        std::cout << "[RackCU][XferTiming] pullRackCuXferTiming failed: " << pst.error_message() << std::endl;
      }
    }

    const double dispatch_sum_s = rackcu_sum_prepare_s + rackcu_sum_tcp_s + rackcu_sum_commit_check_s;
    const double total_rackcu_s = s_coord_rack + dispatch_sum_s + s_cleanup;
    std::cout << "[RACKCU][Timing] summary: coordinator_uploadRackCuUpdate_s=" << std::fixed << std::setprecision(6)
              << s_coord_rack << " sum_prepare_parse_pack_encode_s=" << rackcu_sum_prepare_s << " sum_tcp_to_proxy_s="
              << rackcu_sum_tcp_s << " sum_coordinator_checkCommitAbort_s=" << rackcu_sum_commit_check_s
              << " dispatch_steps_sum_s=" << dispatch_sum_s << " staging_cleanup_s=" << s_cleanup
              << " total_client_rackcu_s=" << total_rackcu_s << " (nsteps=" << nsteps << ")" << std::endl;

    return true;
  }

  bool Client::randomize_preallocated_ranges(const std::vector<std::pair<int, int>> &logical_ranges)
  {
    if (logical_ranges.empty() || m_pre_allocated_buffer == nullptr)
    {
      std::cout << "[RACKCU] randomize_preallocated_ranges: empty ranges or no buffer" << std::endl;
      return false;
    }
    if (!is_azure_like_code(m_sys_config->CodeType))
    {
      std::cout << "[RACKCU] randomize_preallocated_ranges: unsupported CodeType" << std::endl;
      return false;
    }
    const int k = m_sys_config->k;
    const int block_size = static_cast<int>(m_sys_config->BlockSize);
    const int stripe_data_bytes = k * block_size;
    for (const auto &r : logical_ranges)
    {
      if (r.second <= r.first)
      {
        std::cout << "[RACKCU] randomize_preallocated_ranges: invalid range [" << r.first << ", " << r.second << ")" << std::endl;
        return false;
      }
      if (r.first < 0 || r.second > stripe_data_bytes)
      {
        std::cout << "[RACKCU] randomize_preallocated_ranges: range out of data stripe" << std::endl;
        return false;
      }
    }
    constexpr int k_preview_bytes = 16;
    auto print_range_preview = [this](const char *label, int range_idx, const std::pair<int, int> &r, int nbytes) {
      std::cout << "[RACKCU][buffer] range#" << range_idx << " logical [" << r.first << ", " << r.second << ") "
                << label << ", first " << nbytes << " byte(s) hex: ";
      std::ios::fmtflags old_flags = std::cout.flags();
      char old_fill = std::cout.fill();
      for (int i = 0; i < nbytes; i++)
      {
        unsigned char c = static_cast<unsigned char>(m_pre_allocated_buffer[r.first + i]);
        std::cout << std::hex << std::uppercase << std::setfill('0') << std::setw(2) << static_cast<int>(c);
        if (i + 1 < nbytes)
        {
          std::cout << ' ';
        }
      }
      std::cout.flags(old_flags);
      std::cout.fill(old_fill);
      std::cout << std::dec << std::endl;
    };

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 255);
    for (size_t ri = 0; ri < logical_ranges.size(); ri++)
    {
      const auto &r = logical_ranges[ri];
      const int len = r.second - r.first;
      const int preview = std::min(k_preview_bytes, len);
      print_range_preview("before randomize", static_cast<int>(ri), r, preview);
      for (int pos = r.first; pos < r.second; pos++)
      {
        m_pre_allocated_buffer[pos] = static_cast<char>(dist(gen));
      }
      print_range_preview("after randomize ", static_cast<int>(ri), r, preview);
    }
    return true;
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