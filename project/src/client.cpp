#include "client.h"
#include "coordinator.grpc.pb.h"

#include <asio.hpp>
#include <thread>
#include <atomic>
#include <memory>
#include <assert.h>
#include <chrono>
#include <iomanip>
#include <cstring>
#include <sstream>
#include <sys/socket.h>
#include <random>
#include "unilrc_encoder.h"
#include "cord_xue_lrc.h"
namespace ECProject
{
  namespace
  {
    void fill_random_bytes(char *buf, size_t n)
    {
      if (buf == nullptr || n == 0)
        return;
      thread_local std::mt19937 gen{std::random_device{}()};
      std::uniform_int_distribution<int> dist(0, 255);
      for (size_t i = 0; i < n; ++i)
        buf[i] = static_cast<char>(dist(gen));
    }

    std::string cord_client_hex_preview(const char *p, size_t len, size_t max_show = 48)
    {
      if (!p || len == 0)
        return "";
      std::ostringstream oss;
      oss << std::hex << std::setfill('0');
      const size_t n = std::min(len, max_show);
      for (size_t i = 0; i < n; ++i)
        oss << std::setw(2) << static_cast<unsigned>(static_cast<unsigned char>(p[i]));
      if (len > max_show)
        oss << "...+" << (len - max_show) << "b";
      return oss.str();
    }

    bool is_azure_like_code(const std::string &code_type)
    {
      // BoundedRandomLRC 改为 Uniform 风格编码/分组
      return code_type == "AzureLRC" || code_type == "RandomLRC" ||
             code_type == "SplitParityLRC" || code_type == "CordXueLRC";
    }

    bool is_bounded_random_uniform_code(const std::string &code_type)
    {
      return code_type == "BoundedRandomLRC";
    }

    using CordClock = std::chrono::steady_clock;
    thread_local const CordClock::time_point *g_cord_request_deadline = nullptr;
    thread_local int g_cord_request_timeout_sec = 2;

    bool cord_request_timed_out()
    {
      return g_cord_request_deadline != nullptr && CordClock::now() >= *g_cord_request_deadline;
    }

    int cord_remaining_ms()
    {
      if (g_cord_request_deadline == nullptr)
        return -1;
      const auto rem = std::chrono::duration_cast<std::chrono::milliseconds>(
                           *g_cord_request_deadline - CordClock::now())
                           .count();
      return rem > 0 ? static_cast<int>(rem) : 0;
    }

    void cord_apply_grpc_deadline(grpc::ClientContext &ctx)
    {
      const int ms = cord_remaining_ms();
      if (ms <= 0)
        ctx.set_deadline(std::chrono::system_clock::now());
      else
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(ms));
    }

    void cord_apply_socket_timeouts(asio::ip::tcp::socket &sock)
    {
      const int ms = cord_remaining_ms();
      if (ms <= 0)
        return;
      struct timeval tv;
      tv.tv_sec = ms / 1000;
      tv.tv_usec = static_cast<suseconds_t>((ms % 1000) * 1000);
      const auto native = sock.native_handle();
      if (native != -1)
      {
        setsockopt(native, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        setsockopt(native, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      }
    }

    bool cord_abort_if_timed_out()
    {
      if (!cord_request_timed_out())
        return false;
      std::cout << "[CoRD] request timeout (>" << g_cord_request_timeout_sec
                << "s), aborted" << std::endl;
      return true;
    }

    /** 并行 upload 工作线程继承主线程 CoRD 超时 deadline。 */
    struct CordThreadDeadlineScope
    {
      CordThreadDeadlineScope(const CordClock::time_point *dl, int timeout_sec)
      {
        g_cord_request_timeout_sec = timeout_sec;
        g_cord_request_deadline = dl;
      }
      ~CordThreadDeadlineScope() { g_cord_request_deadline = nullptr; }
    };
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
                                      this, cluster_slice_data[i], reply.append_keys(i), reply.cluster_slice_sizes(i), reply.proxyips(i), reply.proxyports(i), i, if_commit_arr.get()));
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


  void Client::async_cord_update_to_proxies(char *cluster_slice_data, std::string cord_key, int cluster_slice_size, std::string proxy_ip, int proxy_port, int index, bool *if_commit_arr,
                                            double *out_xfer_wait_sec, double *out_xfer_pure_sec)
  {
    if (out_xfer_wait_sec != nullptr)
      out_xfer_wait_sec[index] = 0.0;
    if (out_xfer_pure_sec != nullptr)
      out_xfer_pure_sec[index] = 0.0;
    if (cord_abort_if_timed_out())
      return;
    std::cout << "[CoRD][Client " << m_clientID << "] TCP send slice_idx=" << index << " bytes=" << cluster_slice_size
              << " -> proxy " << proxy_ip << ":" << proxy_port << " cord_key=" << cord_key
              << " payload_preview=" << cord_client_hex_preview(cluster_slice_data, static_cast<size_t>(cluster_slice_size))
              << std::endl;
    if (cord_abort_if_timed_out())
      return;
    asio::io_context io_context;
    asio::error_code error;
    asio::ip::tcp::resolver resolver(io_context);
    asio::ip::tcp::resolver::results_type endpoints =
        resolver.resolve(proxy_ip, std::to_string(proxy_port));
    asio::ip::tcp::socket sock_data(io_context);
    cord_apply_socket_timeouts(sock_data);
    asio::connect(sock_data, endpoints, error);
    if (error || cord_abort_if_timed_out())
      return;

    asio::write(sock_data, asio::buffer(cluster_slice_data, static_cast<size_t>(cluster_slice_size)), error);
    if (error || cord_abort_if_timed_out())
      return;
    asio::error_code ignore_ec;
    sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    sock_data.close(ignore_ec);

    if (cord_abort_if_timed_out())
      return;
    grpc::ClientContext check_commit;
    cord_apply_grpc_deadline(check_commit);
    coordinator_proto::AskIfSuccess request;
    request.set_key(cord_key);
    request.set_opp(CORD_UPDATE);
    coordinator_proto::RepIfSuccess reply;
    grpc::Status status = m_coordinator_ptr->checkCommitAbort(&check_commit, request, &reply);
    if (status.ok() && reply.ifcommit())
    {
      if_commit_arr[index] = true;
      if (reply.cord_xfer_timing_present())
      {
        const double pure = reply.cord_xfer_pure_sec();
        const double wait = pure + std::max(0.0, reply.cord_xfer_grpc_sec());
        if (out_xfer_pure_sec != nullptr)
          out_xfer_pure_sec[index] = pure;
        if (out_xfer_wait_sec != nullptr)
          out_xfer_wait_sec[index] = wait;
      }
    }
    else
    {
      std::cout << "[CoRD] commit check failed key=" << cord_key << " proxy=" << proxy_ip << ":" << proxy_port << std::endl;
    }
  }

  void Client::async_append_to_proxies(char *cluster_slice_data, std::string append_key, int cluster_slice_size, std::string proxy_ip, int proxy_port, int index, bool *if_commit_arr)
  {
    // std::cout << "[Append174] Appending size " << cluster_slice_size << " to proxy_address:" << proxy_ip << ":" << proxy_port << std::endl;
    {
      std::lock_guard<std::mutex> lk(m_proxy_tcp_mu);
      asio::io_context io_context;
      asio::error_code error;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints =
          resolver.resolve(proxy_ip, std::to_string(proxy_port));
      asio::ip::tcp::socket sock_data(io_context);
      asio::connect(sock_data, endpoints);

      asio::write(sock_data, asio::buffer(cluster_slice_data, static_cast<size_t>(cluster_slice_size)), error);
      asio::error_code ignore_ec;
      sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      sock_data.close(ignore_ec);
    }

    // check if metadata is saved successfully
    grpc::ClientContext check_commit;
    coordinator_proto::AskIfSuccess request;
    request.set_key(append_key);
    OpperateType opp = APPEND;
    request.set_opp(opp);
    coordinator_proto::RepIfSuccess reply;
    grpc::Status status;
    status = m_coordinator_ptr->checkCommitAbort(&check_commit, request, &reply);
    if (status.ok())
    {
      if (reply.ifcommit())
      {
        if_commit_arr[index] = true;
      }
      else
      {
        std::cout << "[APPEND205] " << append_key << " not commit!!!!!" << " cluster_slice_size: " << cluster_slice_size << " proxy_ip: " << proxy_ip << " proxy_port: " << proxy_port << std::endl;
      }
    }
    else
    {
      std::cout << "[APPEND210] " << append_key << " Fail to check!!!!!" << " cluster_slice_size: " << cluster_slice_size << " proxy_ip: " << proxy_ip << " proxy_port: " << proxy_port << std::endl;
    }
  }

  // 真正的异步版本（Asio 多路复用）：只负责异步 TCP 发送，不阻塞
  void Client::async_append_to_proxies_async(asio::io_context &io_context,
                                             char *cluster_slice_data,
                                             std::string append_key,
                                             int cluster_slice_size,
                                             std::string proxy_ip,
                                             int proxy_port,
                                             int index,
                                             bool *if_commit_arr,
                                             std::shared_ptr<std::atomic<int>> pending_counter)
  {
    std::cout << "[ASYNC_APPEND][START] idx=" << index << " key=" << append_key
              << " target=" << proxy_ip << ":" << proxy_port
              << " size=" << cluster_slice_size << "B" << std::endl;

    auto socket = std::make_shared<asio::ip::tcp::socket>(io_context);
    auto resolver = std::make_shared<asio::ip::tcp::resolver>(io_context);

    std::cout << "[ASYNC_APPEND][RESOLVE] idx=" << index << " calling async_resolve..." << std::endl;

    resolver->async_resolve(proxy_ip, std::to_string(proxy_port),
      [this, socket, resolver, cluster_slice_data, append_key, cluster_slice_size, proxy_ip, proxy_port, index, if_commit_arr, pending_counter]
      (const asio::error_code &ec, asio::ip::tcp::resolver::results_type endpoints) {
        std::cout << "[ASYNC_APPEND][RESOLVE_CB] idx=" << index << " ec=" << ec.message() << std::endl;
        if (ec)
        {
          std::cout << "[ASYNC_APPEND] resolve failed: " << ec.message() << std::endl;
          if (pending_counter) pending_counter->fetch_sub(1);
          return;
        }

        std::cout << "[ASYNC_APPEND][CONNECT] idx=" << index << " calling async_connect..." << std::endl;

        asio::async_connect(*socket, endpoints,
          [this, socket, cluster_slice_data, append_key, cluster_slice_size, proxy_ip, proxy_port, index, if_commit_arr, pending_counter]
          (const asio::error_code &ec, const asio::ip::tcp::endpoint &) {
            std::cout << "[ASYNC_APPEND][CONNECT_CB] idx=" << index << " ec=" << ec.message() << std::endl;
            if (ec)
            {
              std::cout << "[ASYNC_APPEND] connect failed: " << ec.message() << std::endl;
              if (pending_counter) pending_counter->fetch_sub(1);
              return;
            }

            std::cout << "[ASYNC_APPEND][WRITE] idx=" << index << " calling async_write size=" << cluster_slice_size << "B..." << std::endl;

            asio::async_write(*socket, asio::buffer(cluster_slice_data, static_cast<size_t>(cluster_slice_size)),
              [this, socket, append_key, proxy_ip, proxy_port, index, if_commit_arr, pending_counter]
              (const asio::error_code &ec, std::size_t bytes) {
                std::cout << "[ASYNC_APPEND][WRITE_CB] idx=" << index << " ec=" << ec.message() << " bytes=" << bytes << std::endl;
                asio::error_code ignore_ec;
                socket->shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
                socket->close(ignore_ec);

                if (ec)
                {
                  std::cout << "[ASYNC_APPEND] write failed: " << ec.message() << std::endl;
                }

                // TCP 发送完成，计数减一
                if (pending_counter) pending_counter->fetch_sub(1);
              });
          });
      });
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
    if (is_bounded_random_uniform_code(code_type))
    {
      for (int g = 0; g < z; ++g)
        data_block_num_per_group.push_back(cord_xue_lrc::data_block_count_in_local_group(g, k, r, z));
    }
    else if (is_azure_like_code(code_type))
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
    if (is_bounded_random_uniform_code(code_type))
    {
      for (int i = 0; i < z - 1; ++i)
        global_pairty_block_num_per_group.push_back(0);
      global_pairty_block_num_per_group.push_back(r);
    }
    else if (is_azure_like_code(code_type))
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
    if (is_bounded_random_uniform_code(code_type))
    {
      for (int i = 0; i < z; ++i)
        local_parity_block_num_per_group.push_back(1);
    }
    else if (is_azure_like_code(code_type))
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

  // BoundedRandomLRC：扁平编码整条带，再按物理机架 block_id 升序打包，并发发到各机架 proxy
  bool Client::set_bounded_random_by_physical_cluster(const coordinator_proto::ReplyProxyIPsPorts &reply,
                                                     int data_block_num)
  {
    const int k = m_sys_config->k;
    const int r = m_sys_config->r;
    const int z = m_sys_config->z;
    const int n = m_sys_config->n;
    const size_t bs = static_cast<size_t>(m_sys_config->BlockSize);
    if (data_block_num <= 0 || data_block_num > k)
    {
      std::cout << "[BoundedRandom][SET] invalid data_block_num=" << data_block_num << std::endl;
      return false;
    }
    if (reply.slice_block_ids_size() != reply.append_keys_size())
    {
      std::cout << "[BoundedRandom][SET] slice_block_ids size mismatch keys=" << reply.append_keys_size()
                << " ids=" << reply.slice_block_ids_size() << std::endl;
      return false;
    }

    std::vector<char> flat(static_cast<size_t>(n) * bs, 0);
    fill_random_bytes(flat.data(), static_cast<size_t>(data_block_num) * bs);

    std::vector<unsigned char *> data_ptrs(static_cast<size_t>(k));
    std::vector<unsigned char *> parity_ptrs(static_cast<size_t>(r + z));
    for (int i = 0; i < k; ++i)
      data_ptrs[static_cast<size_t>(i)] = reinterpret_cast<unsigned char *>(flat.data() + static_cast<size_t>(i) * bs);
    for (int j = 0; j < r + z; ++j)
      parity_ptrs[static_cast<size_t>(j)] =
          reinterpret_cast<unsigned char *>(flat.data() + static_cast<size_t>(k + j) * bs);
    ECProject::encode_uniform_lrc(k, r, z, data_ptrs.data(), parity_ptrs.data(), m_sys_config->BlockSize);

    std::vector<char> send_buf(static_cast<size_t>(reply.sum_append_size()));
    std::vector<char *> slice_ptrs(static_cast<size_t>(reply.append_keys_size()), nullptr);
    size_t off = 0;
    for (int i = 0; i < reply.append_keys_size(); ++i)
    {
      slice_ptrs[static_cast<size_t>(i)] = send_buf.data() + off;
      const auto &ids = reply.slice_block_ids(i);
      size_t slice_bytes = 0;
      for (int j = 0; j < ids.block_ids_size(); ++j)
      {
        const int bid = ids.block_ids(j);
        if (bid < 0 || bid >= n)
        {
          std::cout << "[BoundedRandom][SET] bad block_id=" << bid << std::endl;
          return false;
        }
        std::memcpy(send_buf.data() + off, flat.data() + static_cast<size_t>(bid) * bs, bs);
        off += bs;
        slice_bytes += bs;
      }
      if (slice_bytes != static_cast<size_t>(reply.cluster_slice_sizes(i)))
      {
        std::cout << "[BoundedRandom][SET] slice size mismatch i=" << i << " packed=" << slice_bytes
                  << " expect=" << reply.cluster_slice_sizes(i) << std::endl;
        return false;
      }
    }
    if (off != static_cast<size_t>(reply.sum_append_size()))
    {
      std::cout << "[BoundedRandom][SET] total pack size mismatch" << std::endl;
      return false;
    }

    std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
    std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);
    asio::io_context io_context;
    auto pending = std::make_shared<std::atomic<int>>(reply.append_keys_size());
    for (int i = 0; i < reply.append_keys_size(); ++i)
    {
      async_append_to_proxies_async(io_context, slice_ptrs[static_cast<size_t>(i)], reply.append_keys(i),
                                    static_cast<int>(reply.cluster_slice_sizes(i)), reply.proxyips(i),
                                    reply.proxyports(i), i, if_commit_arr.get(), pending);
    }
    io_context.run();

    const int slice_count = reply.append_keys_size();
    std::vector<std::thread> check_threads;
    check_threads.reserve(static_cast<size_t>(slice_count));
    for (int i = 0; i < slice_count; ++i)
    {
      check_threads.emplace_back([this, i, &reply, if_commit_arr = if_commit_arr.get()]() {
        grpc::ClientContext check_commit;
        coordinator_proto::AskIfSuccess req;
        req.set_key(reply.append_keys(i));
        req.set_opp(APPEND);
        coordinator_proto::RepIfSuccess reply_chk;
        grpc::Status st = m_coordinator_ptr->checkCommitAbort(&check_commit, req, &reply_chk);
        if (st.ok() && reply_chk.ifcommit())
          if_commit_arr[i] = true;
        else if (!st.ok())
          std::cout << "[BoundedRandom][SET] checkCommitAbort failed key=" << reply.append_keys(i) << std::endl;
      });
    }
    for (auto &t : check_threads)
      t.join();

    const bool all_true = std::all_of(if_commit_arr.get(), if_commit_arr.get() + slice_count,
                                      [](bool val) { return val; });
    if (all_true)
      std::cout << "[BoundedRandom][SET] Client " << m_clientID << " set by physical cluster ok, slices="
                << slice_count << std::endl;
    else
      std::cout << "[BoundedRandom][SET] Client " << m_clientID << " set failed!" << std::endl;
    return all_true;
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

    if (is_bounded_random_uniform_code(m_sys_config->CodeType))
      return set_bounded_random_by_physical_cluster(reply, m_sys_config->k);

    {
      // 每条 stripe 使用独立随机数据；校验块必须随数据重新编码
      const size_t buf_bytes =
          static_cast<size_t>(m_sys_config->BlockSize) * static_cast<size_t>(m_sys_config->n);
      fill_random_bytes(m_pre_allocated_buffer, buf_bytes);

      std::vector<char *> cluster_slice_data = m_toolbox->splitCharPointer(m_pre_allocated_buffer, &reply);
      std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
      std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);

      assert(m_sys_config->CodeType == "UniLRC" || m_sys_config->CodeType == "OptimalLRC" ||
             m_sys_config->CodeType == "UniformLRC" || is_azure_like_code(m_sys_config->CodeType));
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
        ECProject::encode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      else if (m_sys_config->CodeType == "OptimalLRC")
      {
        ECProject::encode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      else if (m_sys_config->CodeType == "UniformLRC")
      {
        ECProject::encode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      else if (is_azure_like_code(m_sys_config->CodeType))
      {
        ECProject::encode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char **>(data_ptr_array.data()), reinterpret_cast<unsigned char **>(parity_ptr_array.data()), m_sys_config->BlockSize);
      }
      (void)m_parity_precomputed;
      // 回退到 client 直接并发发送给所有 proxy（旧逻辑），先跑通带宽测试
      // 使用 Asio 多路复用实现真正的异步并发发送（单线程事件循环）
      asio::io_context io_context;
      auto pending = std::make_shared<std::atomic<int>>(reply.append_keys_size());

      for (int i = 0; i < reply.append_keys_size(); i++)
      {
        async_append_to_proxies_async(io_context,
                                      cluster_slice_data[i],
                                      reply.append_keys(i),
                                      reply.cluster_slice_sizes(i),
                                      reply.proxyips(i),
                                      reply.proxyports(i),
                                      i,
                                      if_commit_arr.get(),
                                      pending);
      }

      io_context.run();  // 等待所有异步 TCP 发送完成

      // 并行 gRPC 检查（每个 slice 独立 checkCommitAbort，减少尾延迟）
      const int slice_count = reply.append_keys_size();
      std::vector<std::thread> check_threads;
      check_threads.reserve(static_cast<size_t>(slice_count));
      for (int i = 0; i < slice_count; i++)
      {
        check_threads.emplace_back([this, i, &reply, if_commit_arr = if_commit_arr.get()]() {
          grpc::ClientContext check_commit;
          coordinator_proto::AskIfSuccess request;
          request.set_key(reply.append_keys(i));
          OpperateType opp = APPEND;
          request.set_opp(opp);
          coordinator_proto::RepIfSuccess reply_chk;
          grpc::Status st = m_coordinator_ptr->checkCommitAbort(&check_commit, request, &reply_chk);
          if (st.ok() && reply_chk.ifcommit())
          {
            if_commit_arr[i] = true;
          }
          else if (!st.ok())
          {
            std::cout << "[SET-ASYNC] checkCommitAbort failed for key=" << reply.append_keys(i) << std::endl;
          }
        });
      }
      for (auto &t : check_threads)
        t.join();

      // check if all appends are successful
      bool all_true = std::all_of(if_commit_arr.get(), if_commit_arr.get() + slice_count, [](bool val)
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

    if (is_bounded_random_uniform_code(m_sys_config->CodeType))
      return set_bounded_random_by_physical_cluster(reply, block_num);

    {
      const size_t buf_bytes =
          static_cast<size_t>(m_sys_config->BlockSize) * static_cast<size_t>(m_sys_config->n);
      fill_random_bytes(m_pre_allocated_buffer, buf_bytes);

      std::vector<char *> cluster_slice_data = m_toolbox->splitCharPointer(m_pre_allocated_buffer, &reply);
      std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
      std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);

      assert(m_sys_config->CodeType == "UniLRC" || m_sys_config->CodeType == "OptimalLRC" ||
             m_sys_config->CodeType == "UniformLRC" || is_azure_like_code(m_sys_config->CodeType));
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
      // 使用 Asio 多路复用实现真正的异步并发发送（单线程事件循环）
      asio::io_context io_context;
      auto pending = std::make_shared<std::atomic<int>>(reply.append_keys_size());

      for (int i = 0; i < reply.append_keys_size(); i++)
      {
        async_append_to_proxies_async(io_context,
                                      cluster_slice_data[i],
                                      reply.append_keys(i),
                                      reply.cluster_slice_sizes(i),
                                      reply.proxyips(i),
                                      reply.proxyports(i),
                                      i,
                                      if_commit_arr.get(),
                                      pending);
      }

      io_context.run();  // 等待所有异步 TCP 发送完成

      // 并行 gRPC 检查（每个 slice 独立 checkCommitAbort，减少尾延迟）
      const int slice_count = reply.append_keys_size();
      std::vector<std::thread> check_threads;
      check_threads.reserve(static_cast<size_t>(slice_count));
      for (int i = 0; i < slice_count; i++)
      {
        check_threads.emplace_back([this, i, &reply, if_commit_arr = if_commit_arr.get()]() {
          grpc::ClientContext check_commit;
          coordinator_proto::AskIfSuccess request;
          request.set_key(reply.append_keys(i));
          OpperateType opp = APPEND;
          request.set_opp(opp);
          coordinator_proto::RepIfSuccess reply_chk;
          grpc::Status st = m_coordinator_ptr->checkCommitAbort(&check_commit, request, &reply_chk);
          if (st.ok() && reply_chk.ifcommit())
          {
            if_commit_arr[i] = true;
          }
          else if (!st.ok())
          {
            std::cout << "[SET-ASYNC] checkCommitAbort failed for key=" << reply.append_keys(i) << std::endl;
          }
        });
      }
      for (auto &t : check_threads)
        t.join();

      // check if all appends are successful
      bool all_true = std::all_of(if_commit_arr.get(), if_commit_arr.get() + slice_count, [](bool val)
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
      std::cout << "[XUE_UPDATE] upload failed: " << status.error_message() << std::endl;
      return false;
    }

    fill_random_bytes(m_pre_allocated_buffer, static_cast<size_t>(reply.sum_append_size()));
    std::vector<char *> cluster_slice_data = m_toolbox->splitCharPointer(m_pre_allocated_buffer, &reply);
    std::unique_ptr<bool[]> if_commit_arr(new bool[reply.append_keys_size()]);
    std::fill_n(if_commit_arr.get(), reply.append_keys_size(), false);

    for (int i = 0; i < reply.append_keys_size(); i++)
    {
      async_append_to_proxies(cluster_slice_data[i], reply.append_keys(i), reply.cluster_slice_sizes(i),
                              reply.proxyips(i), reply.proxyports(i), i, if_commit_arr.get());
    }

    bool all_true = std::all_of(if_commit_arr.get(), if_commit_arr.get() + reply.append_keys_size(), [](bool val)
                                { return val == true; });
    if (!all_true)
    {
      std::cout << "[XUE_UPDATE] commit check failed for at least one cluster slice." << std::endl;
    }
    return all_true;
  }

  namespace
  {
    void cord_fill_timing(CordUpdateTiming *out, const std::chrono::steady_clock::time_point &wall_t0,
                          double plan_sec, double payload_prep_sec, double upload_sec,
                          double xfer_begin_sec, double xfer_wait_sec,
                          double xfer_pure_sec = 0.0, double xfer_grpc_sec = 0.0)
    {
      if (out == nullptr)
        return;
      out->plan_sec = plan_sec;
      out->payload_prep_sec = payload_prep_sec;
      out->upload_sec = upload_sec;
      out->xfer_begin_sec = xfer_begin_sec;
      out->xfer_wait_sec = xfer_wait_sec;
      out->xfer_pure_sec = xfer_pure_sec;
      out->xfer_grpc_sec = xfer_grpc_sec;
      out->wall_sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - wall_t0).count();
    }
  }

  bool Client::cord_update_start(int stripe_id, const std::vector<std::pair<int, int>> &logical_ranges,
                                 const char *update_payload, size_t update_payload_bytes,
                                 CordUpdatePending *pending, CordUpdateTiming *partial_timing)
  {
    if (pending == nullptr)
    {
      std::cout << "[CoRD] cord_update_start: pending is null." << std::endl;
      return false;
    }
    pending->stripe_id = stripe_id;
    pending->transfer_plan_key.clear();
    pending->plan_sec = 0.0;
    pending->payload_prep_sec = 0.0;
    pending->upload_sec = 0.0;
    pending->direct_xfer_wait_sec = 0.0;
    pending->direct_xfer_pure_sec = 0.0;
    pending->wall_t0 = std::chrono::steady_clock::now();

    if (logical_ranges.empty())
    {
      std::cout << "[CoRD] Empty update intervals." << std::endl;
      cord_fill_timing(partial_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, 0.0);
      return false;
    }
    g_cord_request_timeout_sec = m_sys_config->CordRequestTimeoutSec;
    const CordClock::time_point cord_deadline =
        CordClock::now() + std::chrono::seconds(g_cord_request_timeout_sec);
    g_cord_request_deadline = &cord_deadline;
    struct CordDeadlineGuard
    {
      ~CordDeadlineGuard() { g_cord_request_deadline = nullptr; }
    } cord_deadline_guard;

    grpc::ClientContext ctx;
    cord_apply_grpc_deadline(ctx);
    coordinator_proto::CordUpdateRequest request;
    coordinator_proto::ReplyProxyIPsPorts reply;
    request.set_client_id(m_clientID);
    request.set_stripe_id(stripe_id);
    request.set_interval_count(static_cast<int32_t>(logical_ranges.size()));
    for (const auto &r : logical_ranges)
    {
      if (r.second <= r.first)
      {
        std::cout << "[CoRD] Invalid half-open interval: [" << r.first << ", " << r.second << ")" << std::endl;
        cord_fill_timing(partial_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                         pending->upload_sec, 0.0, 0.0);
        return false;
      }
      auto *range = request.add_update_intervals();
      range->set_logical_offset_start(r.first);
      range->set_logical_offset_end(r.second);
    }

    const auto plan_t0 = std::chrono::steady_clock::now();
    grpc::Status status = m_coordinator_ptr->uploadCordUpdate(&ctx, request, &reply);
    pending->plan_sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - plan_t0).count();
    if (cord_abort_if_timed_out())
    {
      cord_fill_timing(partial_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, 0.0);
      return false;
    }
    if (!status.ok())
    {
      std::cout << "[CoRD] uploadCordUpdate failed: " << status.error_message() << std::endl;
      cord_fill_timing(partial_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, 0.0);
      return false;
    }
    if (reply.sum_append_size() == 0)
    {
      cord_fill_timing(partial_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, 0.0);
      return true;
    }

    std::vector<char> owned_payload;
    const char *payload_send = update_payload;
    if (update_payload == nullptr)
    {
      if (update_payload_bytes != 0)
      {
        std::cout << "[CoRD] auto fill payload: require update_payload_bytes==0 when payload is null" << std::endl;
        cord_fill_timing(partial_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                         pending->upload_sec, 0.0, 0.0);
        return false;
      }
      const auto prep_t0 = std::chrono::steady_clock::now();
      owned_payload.resize(static_cast<size_t>(reply.sum_append_size()));
      fill_random_bytes(owned_payload.data(), owned_payload.size());
      pending->payload_prep_sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - prep_t0).count();
      payload_send = owned_payload.data();
      std::cout << "[CoRD][Client " << m_clientID << "] stripe_id=" << stripe_id
                << " auto random fill payload total_bytes=" << owned_payload.size() << " intervals:";
      for (const auto &r : logical_ranges)
        std::cout << " [" << r.first << "," << r.second << ")";
      std::cout << '\n'
                << "[CoRD][Client " << m_clientID << "] fill_payload_preview="
                << cord_client_hex_preview(owned_payload.data(), owned_payload.size()) << std::endl;
    }
    else if (update_payload_bytes != static_cast<size_t>(reply.sum_append_size()))
    {
      std::cout << "[CoRD] payload size mismatch: got " << update_payload_bytes << " expected " << reply.sum_append_size() << std::endl;
      cord_fill_timing(partial_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, 0.0);
      return false;
    }

    std::cout << "[CoRD][Client " << m_clientID << "] coordinator replied append_keys=" << reply.append_keys_size()
              << " sum_append_size=" << reply.sum_append_size() << std::endl;
    for (int i = 0; i < reply.append_keys_size(); ++i)
    {
      std::cout << "[CoRD][Client " << m_clientID << "]   slice " << i << " cluster_gid=" << reply.group_ids(i)
                << " bytes=" << reply.cluster_slice_sizes(i) << " -> proxy " << reply.proxyips(i) << ":"
                << reply.proxyports(i) << " key=" << reply.append_keys(i) << std::endl;
    }

    const auto upload_t0 = std::chrono::steady_clock::now();
    std::vector<char *> cluster_slices = m_toolbox->splitCharPointer(payload_send, &reply);
    const int slice_count = reply.append_keys_size();
    std::unique_ptr<bool[]> if_commit_arr(new bool[slice_count]);
    std::fill_n(if_commit_arr.get(), slice_count, false);
    std::unique_ptr<double[]> xfer_wait_arr(new double[slice_count]);
    std::unique_ptr<double[]> xfer_pure_arr(new double[slice_count]);
    std::fill_n(xfer_wait_arr.get(), slice_count, 0.0);
    std::fill_n(xfer_pure_arr.get(), slice_count, 0.0);
    if (cord_abort_if_timed_out())
    {
      pending->upload_sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - upload_t0).count();
      cord_fill_timing(partial_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, 0.0);
      return false;
    }
    const int cord_timeout_sec = g_cord_request_timeout_sec;
    std::vector<std::thread> upload_threads;
    upload_threads.reserve(static_cast<size_t>(slice_count));
    for (int i = 0; i < slice_count; ++i)
    {
      upload_threads.emplace_back([this, i, &cord_deadline, cord_timeout_sec, &cluster_slices, &reply,
                                   if_commit_arr = if_commit_arr.get(), xfer_wait_arr = xfer_wait_arr.get(),
                                   xfer_pure_arr = xfer_pure_arr.get()]() {
        CordThreadDeadlineScope deadline_scope(&cord_deadline, cord_timeout_sec);
        async_cord_update_to_proxies(cluster_slices[static_cast<size_t>(i)], reply.append_keys(i),
                                     static_cast<int>(reply.cluster_slice_sizes(i)), reply.proxyips(i),
                                     reply.proxyports(i), i, if_commit_arr, xfer_wait_arr, xfer_pure_arr);
      });
    }
    for (auto &t : upload_threads)
      t.join();
    pending->upload_sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - upload_t0).count();
    for (int i = 0; i < slice_count; ++i)
    {
      pending->direct_xfer_wait_sec = std::max(pending->direct_xfer_wait_sec, xfer_wait_arr[static_cast<size_t>(i)]);
      pending->direct_xfer_pure_sec = std::max(pending->direct_xfer_pure_sec, xfer_pure_arr[static_cast<size_t>(i)]);
    }
    if (cord_abort_if_timed_out())
    {
      cord_fill_timing(partial_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, 0.0);
      return false;
    }
    if (!std::all_of(if_commit_arr.get(), if_commit_arr.get() + reply.append_keys_size(),
                     [](bool v) { return v; }))
    {
      cord_fill_timing(partial_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, 0.0);
      return false;
    }

    if (!reply.cord_transfer_plan_key().empty())
    {
      pending->transfer_plan_key = reply.cord_transfer_plan_key();
      std::cout << "[CoRD][Client " << m_clientID << "] upload done; deferred xfer wait for plan_key="
                << pending->transfer_plan_key << "\n";
    }
    else if (pending->direct_xfer_wait_sec > 0.0 || pending->direct_xfer_pure_sec > 0.0)
    {
      std::cout << "[BoundedRandom][Client " << m_clientID << "] direct fanout timing (max over clusters): "
                << "xfer_wait_sec=" << pending->direct_xfer_wait_sec
                << " xfer_pure_sec=" << pending->direct_xfer_pure_sec << "\n";
    }

    if (partial_timing != nullptr)
    {
      partial_timing->plan_sec = pending->plan_sec;
      partial_timing->payload_prep_sec = pending->payload_prep_sec;
      partial_timing->upload_sec = pending->upload_sec;
      partial_timing->xfer_begin_sec = 0.0;
      if (pending->transfer_plan_key.empty())
      {
        partial_timing->xfer_wait_sec = pending->direct_xfer_wait_sec;
        partial_timing->xfer_pure_sec = pending->direct_xfer_pure_sec;
        partial_timing->xfer_grpc_sec =
            std::max(0.0, pending->direct_xfer_wait_sec - pending->direct_xfer_pure_sec);
      }
      else
      {
        partial_timing->xfer_wait_sec = 0.0;
        partial_timing->xfer_pure_sec = 0.0;
        partial_timing->xfer_grpc_sec = 0.0;
      }
      partial_timing->wall_sec =
          std::chrono::duration<double>(std::chrono::steady_clock::now() - pending->wall_t0).count();
    }
    return true;
  }

  bool Client::cord_update_wait_xfer(CordUpdatePending *pending, CordUpdateTiming *out_timing)
  {
    if (pending == nullptr)
      return false;
    double xfer_wait_sec = 0.0;
    double xfer_pure_sec = 0.0;
    double xfer_grpc_sec = 0.0;
    if (pending->transfer_plan_key.empty())
    {
      // BoundedRandom 直推：扇出已在 upload 内完成，用各 cluster 上报的 max 填 xfer_*
      const double xw = pending->direct_xfer_wait_sec;
      const double xp = pending->direct_xfer_pure_sec;
      cord_fill_timing(out_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, xw, xp, std::max(0.0, xw - xp));
      return true;
    }

    g_cord_request_timeout_sec = m_sys_config->CordRequestTimeoutSec;
    const CordClock::time_point cord_deadline =
        CordClock::now() + std::chrono::seconds(g_cord_request_timeout_sec);
    g_cord_request_deadline = &cord_deadline;
    struct CordDeadlineGuard
    {
      ~CordDeadlineGuard() { g_cord_request_deadline = nullptr; }
    } cord_deadline_guard;

    std::cout << "[CoRD][Client " << m_clientID << "] waiting for cross-cluster transfer (auto-start after upload): "
              << pending->transfer_plan_key << " stripe_id=" << pending->stripe_id << "\n";
    if (cord_abort_if_timed_out())
    {
      cord_fill_timing(out_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, xfer_wait_sec, xfer_pure_sec, xfer_grpc_sec);
      return false;
    }
    grpc::ClientContext ctx_wait;
    cord_apply_grpc_deadline(ctx_wait);
    coordinator_proto::CordPlanWaitRequest wait_req;
    wait_req.set_plan_key(pending->transfer_plan_key);
    coordinator_proto::RepIfSuccess wait_rep;
    const auto xfer_wait_t0 = std::chrono::steady_clock::now();
    grpc::Status st_wait = m_coordinator_ptr->cordPlanWaitTransferComplete(&ctx_wait, wait_req, &wait_rep);
    xfer_wait_sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - xfer_wait_t0).count();
    if (wait_rep.cord_xfer_timing_present())
    {
      xfer_pure_sec = wait_rep.cord_xfer_pure_sec();
      xfer_grpc_sec = std::max(0., xfer_wait_sec - xfer_pure_sec);
    }
    else
    {
      xfer_grpc_sec = xfer_wait_sec;
    }
    if (cord_abort_if_timed_out())
    {
      cord_fill_timing(out_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, xfer_wait_sec, xfer_pure_sec, xfer_grpc_sec);
      return false;
    }
    if (!st_wait.ok() || !wait_rep.ifcommit())
    {
      std::cout << "[CoRD] cordPlanWaitTransferComplete failed: " << st_wait.error_message() << std::endl;
      cord_fill_timing(out_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                       pending->upload_sec, 0.0, xfer_wait_sec, xfer_pure_sec, xfer_grpc_sec);
      return false;
    }
    std::cout << "[CoRD][Client " << m_clientID << "] cross-cluster transfer complete stripe_id=" << pending->stripe_id
              << " xfer_wait_sec=" << xfer_wait_sec << " xfer_pure_sec=" << xfer_pure_sec
              << " xfer_grpc_sec=" << xfer_grpc_sec << "\n";

    pending->transfer_plan_key.clear();
    cord_fill_timing(out_timing, pending->wall_t0, pending->plan_sec, pending->payload_prep_sec,
                     pending->upload_sec, 0.0, xfer_wait_sec, xfer_pure_sec, xfer_grpc_sec);
    return true;
  }

  bool Client::cord_update(int stripe_id, const std::vector<std::pair<int, int>> &logical_ranges,
                           const char *update_payload, size_t update_payload_bytes,
                           CordUpdateTiming *out_timing)
  {
    CordUpdatePending pending;
    if (!cord_update_start(stripe_id, logical_ranges, update_payload, update_payload_bytes, &pending, out_timing))
      return false;
    return cord_update_wait_xfer(&pending, out_timing);
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
    else if(m_sys_config->CodeType == "UniformLRC" || is_bounded_random_uniform_code(m_sys_config->CodeType))
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