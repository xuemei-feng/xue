#include "client.h"
#include "coordinator.grpc.pb.h"
#include "proxy.grpc.pb.h"
#include "datanode.grpc.pb.h"
#include <grpcpp/create_channel.h>
#include <grpcpp/support/channel_arguments.h>

#include <asio.hpp>
#include <thread>
#include <assert.h>
#include <chrono>
#include <random>
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <string>
#if !defined(_WIN32)
#include <sys/socket.h>
#endif
#include <vector>
#include "unilrc_encoder.h"
namespace ECProject
{
  namespace
  {
    bool is_azure_like_code(const std::string &code_type)
    {
      return code_type == "AzureLRC" || code_type == "RandomLRC";
    }

    static grpc::ChannelArguments parix_channel_args()
    {
      grpc::ChannelArguments args;
      constexpr int k_max = 128 * 1024 * 1024;
      args.SetMaxReceiveMessageSize(k_max);
      args.SetMaxSendMessageSize(k_max);
      // 长 batch 不启用 client keepalive，避免 too_many_pings / GOAWAY。
      return args;
    }

    class ParixChannelCache
    {
    public:
      std::shared_ptr<grpc::Channel> get(const std::string &target)
      {
        std::lock_guard<std::mutex> lk(mu_);
        auto &ch = channels_[target];
        if (!ch)
        {
          ch = grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), parix_channel_args());
        }
        return ch;
      }

    private:
      std::mutex mu_;
      std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channels_;
    };

    ParixChannelCache g_parix_channel_cache;

    std::shared_ptr<grpc::Channel> parix_proxy_channel(const std::string &target)
    {
      return g_parix_channel_cache.get(target);
    }

    /** data proxy 只有一个 TCP acceptor；并发 parixScheduleDataUpdate 会导致 accept 错配并卡住。 */
    class ParixDataProxyLockManager
    {
    public:
      std::unique_lock<std::mutex> lock_endpoint(const std::string &endpoint)
      {
        std::mutex *mtx_ptr = nullptr;
        {
          std::lock_guard<std::mutex> map_lk(map_mutex_);
          auto &mtx = endpoint_mutexes_[endpoint];
          if (!mtx)
          {
            mtx = std::make_unique<std::mutex>();
          }
          mtx_ptr = mtx.get();
        }
        return std::unique_lock<std::mutex>(*mtx_ptr);
      }

    private:
      std::mutex map_mutex_;
      std::unordered_map<std::string, std::unique_ptr<std::mutex>> endpoint_mutexes_;
    };

    ParixDataProxyLockManager g_parix_data_proxy_locks;

    int parix_rpc_timeout_sec()
    {
      static const int timeout_sec = []() {
        const char *env = std::getenv("PARIX_RPC_TIMEOUT_SEC");
        if (env == nullptr || env[0] == '\0')
        {
          return 600;
        }
        try
        {
          return std::max(30, std::stoi(env));
        }
        catch (const std::exception &)
        {
          return 600;
        }
      }();
      return timeout_sec;
    }

    void parix_set_rpc_deadline(grpc::ClientContext &ctx)
    {
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(parix_rpc_timeout_sec()));
    }

    void parix_set_tcp_socket_timeouts(asio::ip::tcp::socket &s)
    {
#if defined(_WIN32)
      (void)s;
#else
      struct timeval tv {};
      tv.tv_sec = parix_rpc_timeout_sec();
      tv.tv_usec = 0;
      const int fd = s.native_handle();
      setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
#endif
    }

    // Hot-path: default off. Export PARIX_TRACE=1 for hex dumps and verbose Parix client logs.
    static bool parix_client_trace()
    {
      static const int enabled = []() {
        const char *e = std::getenv("PARIX_TRACE");
        return (e != nullptr && e[0] == '1' && e[1] == '\0') ? 1 : 0;
      }();
      return enabled != 0;
    }

    struct ParixPendingSlice
    {
      int off{};
      int len{};
      const char *payload{};
    };

    struct ParixCoalescedDiskWrite
    {
      int off{};
      int len{};
      const char *data{};
      std::vector<char> owned{};
    };

    static std::vector<ParixCoalescedDiskWrite> parix_coalesce_disk_writes(std::vector<ParixPendingSlice> pending)
    {
      std::vector<ParixCoalescedDiskWrite> out;
      if (pending.empty())
      {
        return out;
      }
      std::sort(pending.begin(), pending.end(),
                [](const ParixPendingSlice &a, const ParixPendingSlice &b) { return a.off < b.off; });
      size_t i = 0;
      while (i < pending.size())
      {
        size_t j = i;
        int run_end = pending[i].off + pending[i].len;
        bool mem_ok = true;
        while (j + 1 < pending.size() && pending[j + 1].off == run_end)
        {
          if (pending[j + 1].payload != pending[j].payload + pending[j].len)
          {
            mem_ok = false;
          }
          run_end += pending[j + 1].len;
          j++;
        }
        const int run_begin = pending[i].off;
        const int total_len = run_end - run_begin;
        if (j > i && mem_ok)
        {
          ParixCoalescedDiskWrite cw;
          cw.off = run_begin;
          cw.len = total_len;
          cw.data = pending[i].payload;
          out.push_back(std::move(cw));
          i = j + 1;
          continue;
        }
        if (j > i && !mem_ok)
        {
          std::vector<char> buf(static_cast<size_t>(total_len));
          int pos = 0;
          for (size_t t = i; t <= j; t++)
          {
            std::memcpy(buf.data() + pos, pending[t].payload, static_cast<size_t>(pending[t].len));
            pos += pending[t].len;
          }
          ParixCoalescedDiskWrite cw;
          cw.off = run_begin;
          cw.len = total_len;
          cw.owned = std::move(buf);
          cw.data = cw.owned.data();
          out.push_back(std::move(cw));
          i = j + 1;
          continue;
        }
        ParixCoalescedDiskWrite cw;
        cw.off = pending[i].off;
        cw.len = pending[i].len;
        cw.data = pending[i].payload;
        out.push_back(std::move(cw));
        i++;
      }
      return out;
    }

    bool parix_datanode_read_range_stub(datanode_proto::datanodeService::Stub *stub, const std::string &dn_ip, int dn_grpc_port,
                                        const std::string &block_key, int block_size, int range_offset, int range_length, char *out)
    {
      if (stub == nullptr || range_length <= 0 || range_offset < 0 || range_offset + range_length > block_size)
      {
        return false;
      }
      grpc::ClientContext ctx;
      parix_set_rpc_deadline(ctx);
      datanode_proto::GetInfo req;
      req.set_block_key(block_key);
      req.set_block_size(block_size);
      req.set_range_offset(range_offset);
      req.set_range_length(range_length);
      req.set_proxy_ip("127.0.0.1");
      req.set_proxy_port(0);
      datanode_proto::RequestResult res;
      if (!stub->handleGet(&ctx, req, &res).ok() || !res.message())
      {
        return false;
      }
      try
      {
        asio::io_context ioc;
        asio::ip::tcp::socket s(ioc);
        asio::ip::tcp::resolver r(ioc);
        asio::connect(s, r.resolve({dn_ip, std::to_string(dn_grpc_port + ECProject::DATANODE_PORT_SHIFT)}));
        parix_set_tcp_socket_timeouts(s);
        asio::error_code ec;
        size_t n = asio::read(s, asio::buffer(out, static_cast<size_t>(range_length)), ec);
        return !ec && n == static_cast<size_t>(range_length);
      }
      catch (...)
      {
        return false;
      }
    }

    bool parix_datanode_write_range_stub(datanode_proto::datanodeService::Stub *stub, const std::string &dn_ip, int dn_grpc_port,
                                         const std::string &block_key, int block_size, int range_offset, int range_length,
                                         const char *data)
    {
      if (stub == nullptr || range_length <= 0 || range_offset < 0 || range_offset + range_length > block_size)
      {
        return false;
      }
      grpc::ClientContext ctx;
      parix_set_rpc_deadline(ctx);
      datanode_proto::SetInfo req;
      req.set_block_key(block_key);
      req.set_block_size(block_size);
      req.set_block_id(0);
      req.set_proxy_ip("");
      req.set_proxy_port(0);
      req.set_ispull(false);
      req.set_range_offset(range_offset);
      req.set_range_length(range_length);
      datanode_proto::RequestResult res;
      if (!stub->handleSet(&ctx, req, &res).ok() || !res.message())
      {
        return false;
      }
      try
      {
        asio::io_context ioc;
        asio::ip::tcp::socket s(ioc);
        asio::ip::tcp::resolver r(ioc);
        asio::connect(s, r.resolve({dn_ip, std::to_string(dn_grpc_port + ECProject::DATANODE_PORT_SHIFT)}));
        parix_set_tcp_socket_timeouts(s);
        asio::error_code ec;
        asio::write(s, asio::buffer(data, static_cast<size_t>(range_length)), ec);
        return !ec;
      }
      catch (...)
      {
        return false;
      }
    }

    bool parix_ranges_disjoint_half_open(const std::vector<std::pair<int, int>> &ranges)
    {
      if (ranges.empty())
      {
        return false;
      }
      std::vector<std::pair<int, int>> sorted = ranges;
      std::sort(sorted.begin(), sorted.end(), [](const std::pair<int, int> &a, const std::pair<int, int> &b) {
        return a.first < b.first;
      });
      for (const auto &p : sorted)
      {
        if (p.second <= p.first)
        {
          return false;
        }
      }
      for (size_t i = 1; i < sorted.size(); ++i)
      {
        if (sorted[i - 1].second > sorted[i].first)
        {
          return false;
        }
      }
      return true;
    }

    int parix_packed_total_len(const std::vector<std::pair<int, int>> &ranges)
    {
      int t = 0;
      for (const auto &p : ranges)
      {
        if (p.second > p.first)
        {
          t += p.second - p.first;
        }
      }
      return t;
    }

    void parix_log_slice_hex(const std::string &ctx, const char *data, int nbytes, int max_show = 16)
    {
      if (!parix_client_trace())
      {
        return;
      }
      if (data == nullptr || nbytes <= 0)
      {
        std::cout << "[Client][Parix] " << ctx << " (empty)\n";
        return;
      }
      const int n = std::min(max_show, nbytes);
      std::cout << "[Client][Parix] " << ctx << " show_" << n << "_of_" << nbytes << "_bytes:";
      for (int i = 0; i < n; ++i)
      {
        std::cout << ' ' << std::hex << std::setfill('0') << std::setw(2)
                  << static_cast<unsigned>(static_cast<unsigned char>(data[i]));
      }
      std::cout << std::dec << std::endl;
    }

    bool parix_find_packed_payload_offset(int seg_lo, int rlen, const std::vector<std::pair<int, int>> &ranges, int *out_packed_off)
    {
      int acc = 0;
      for (const auto &pr : ranges)
      {
        const int rs = pr.first;
        const int re = pr.second;
        if (re <= rs)
        {
          continue;
        }
        if (seg_lo >= rs && seg_lo + rlen <= re)
        {
          *out_packed_off = acc + (seg_lo - rs);
          return true;
        }
        acc += (re - rs);
      }
      return false;
    }

    void fill_parix_placement_from_segment(const coordinator_proto::ParixDataSegmentPlan &seg, int stripe_id, uint64_t batch_id,
                                           proxy_proto::ParixDataUpdatePlacement *pl)
    {
      pl->set_key(seg.append_key());
      pl->set_stripe_id(stripe_id);
      pl->set_batch_id(batch_id);
      pl->set_write_generation(seg.write_generation());
      pl->set_cluster_id(seg.cluster_id());
      pl->set_block_key(seg.block_key());
      pl->set_block_id(seg.block_id());
      pl->set_range_offset(seg.range_offset());
      pl->set_range_length(seg.range_length());
      pl->set_datanode_ip(seg.datanode_ip());
      pl->set_datanode_port(seg.datanode_port());
      for (int i = 0; i < seg.global_parities_size(); ++i)
      {
        const auto &ep = seg.global_parities(i);
        auto *t = pl->add_global_parities();
        t->set_proxy_ip(ep.proxy_ip());
        t->set_proxy_grpc_port(ep.proxy_grpc_port());
        t->set_parity_block_id(ep.parity_block_id());
        t->set_parity_block_key(ep.parity_block_key());
        t->set_parity_datanode_ip(ep.parity_datanode_ip());
        t->set_parity_datanode_port(ep.parity_datanode_port());
      }
      if (!seg.local_parity().proxy_ip().empty())
      {
        const auto &ep = seg.local_parity();
        auto *t = pl->mutable_local_parity();
        t->set_proxy_ip(ep.proxy_ip());
        t->set_proxy_grpc_port(ep.proxy_grpc_port());
        t->set_parity_block_id(ep.parity_block_id());
        t->set_parity_block_key(ep.parity_block_key());
        t->set_parity_datanode_ip(ep.parity_datanode_ip());
        t->set_parity_datanode_port(ep.parity_datanode_port());
      }
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

  void Client::async_append_to_proxies(char *cluster_slice_data, std::string append_key, int cluster_slice_size, std::string proxy_ip, int proxy_port, int index, bool *if_commit_arr)
  {
    // std::cout << "[Append174] Appending size " << cluster_slice_size << " to proxy_address:" << proxy_ip << ":" << proxy_port << std::endl;
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

  bool Client::parix_partial_update(int stripe_id, int logical_offset_start, int logical_offset_end_exclusive, const char *new_span_bytes)
  {
    if (logical_offset_end_exclusive <= logical_offset_start)
    {
      std::cout << "[Client][Parix] invalid logical range" << std::endl;
      return false;
    }
    const std::vector<std::pair<int, int>> one{{logical_offset_start, logical_offset_end_exclusive}};
    return parix_partial_update_ranges(stripe_id, one, new_span_bytes);
  }

  bool Client::parix_ranges_cover_full_stripe_data(const std::vector<std::pair<int, int>> &ranges) const
  {
    if (!m_sys_config || ranges.empty())
    {
      return false;
    }
    const long long k = m_sys_config->k;
    const long long bs = m_sys_config->BlockSize;
    const long long total = k * bs;
    for (const auto &pr : ranges)
    {
      const long long lo = pr.first;
      const long long hi = pr.second;
      if (hi <= lo || lo < 0 || hi > total)
      {
        return false;
      }
    }
    for (long long bi = 0; bi < k; ++bi)
    {
      const long long b_lo = bi * bs;
      const long long b_hi = (bi + 1) * bs;
      bool hit = false;
      for (const auto &pr : ranges)
      {
        const long long lo = pr.first;
        const long long hi = pr.second;
        if (lo < b_hi && hi > b_lo)
        {
          hit = true;
          break;
        }
      }
      if (!hit)
      {
        return false;
      }
    }
    return true;
  }

  bool Client::parix_partial_update_ranges(int stripe_id, const std::vector<std::pair<int, int>> &ranges, const char *packed_new_bytes)
  {
    if (!m_sys_config || !packed_new_bytes)
    {
      return false;
    }
    if (!parix_ranges_disjoint_half_open(ranges))
    {
      std::cout << "[Client][Parix] ranges must be non-empty, half-open [start,end), pairwise disjoint" << std::endl;
      return false;
    }
    const int packed_len = parix_packed_total_len(ranges);
    if (packed_len <= 0)
    {
      std::cout << "[Client][Parix] invalid packed length" << std::endl;
      return false;
    }
    const int bs = m_sys_config->BlockSize;

    grpc::ClientContext pctx;
    parix_set_rpc_deadline(pctx);
    coordinator_proto::ParixPartialPlanRequest preq;
    preq.set_stripe_id(stripe_id);
    for (const auto &ab : ranges)
    {
      coordinator_proto::ParixLogicalRange *lr = preq.add_ranges();
      lr->set_logical_offset_start(ab.first);
      lr->set_logical_offset_end(ab.second);
    }
    coordinator_proto::ParixPartialPlanReply plan;
    grpc::Status pst = m_coordinator_ptr->planParixPartial(&pctx, preq, &plan);
    if (!pst.ok())
    {
      std::cout << "[Client][Parix] planParixPartial failed: " << pst.error_message() << std::endl;
      return false;
    }
    if (plan.segments_size() == 0)
    {
      std::cout << "[Client][Parix] empty plan segments" << std::endl;
      return false;
    }

    std::map<std::string, std::vector<int>> seg_indices_by_block;
    std::vector<std::string> block_key_plan_order;
    for (int si = 0; si < plan.segments_size(); ++si)
    {
      const std::string &bk = plan.segments(si).block_key();
      if (seg_indices_by_block[bk].empty())
      {
        block_key_plan_order.push_back(bk);
      }
      seg_indices_by_block[bk].push_back(si);
    }

    for (const std::string &bk : block_key_plan_order)
    {
      const std::vector<int> &idxs = seg_indices_by_block[bk];
      const coordinator_proto::ParixDataSegmentPlan &seg_read = plan.segments(idxs[0]);
      if (parix_client_trace())
      {
        std::cout << "[Client][Parix] --- data block group block_key=" << bk << " segments_in_block=" << idxs.size() << std::endl;
      }

      const std::string dn_ep = seg_read.datanode_ip() + ":" + std::to_string(seg_read.datanode_port());
      auto dn_channel = parix_proxy_channel(dn_ep);
      auto dn_stub = datanode_proto::datanodeService::NewStub(dn_channel);

      std::unordered_map<int, std::vector<char>> old_seg_by_idx;
      {
        std::vector<int> idx_sort(idxs);
        std::sort(idx_sort.begin(), idx_sort.end(), [&](int a, int b) {
          return plan.segments(a).range_offset() < plan.segments(b).range_offset();
        });
        size_t g = 0;
        while (g < idx_sort.size())
        {
          int i0 = idx_sort[g];
          int run_lo = static_cast<int>(plan.segments(i0).range_offset());
          int run_hi = run_lo + static_cast<int>(plan.segments(i0).range_length());
          size_t h = g;
          while (h + 1 < idx_sort.size())
          {
            int inext = idx_sort[h + 1];
            int lo = static_cast<int>(plan.segments(inext).range_offset());
            if (lo != run_hi)
            {
              break;
            }
            run_hi = lo + static_cast<int>(plan.segments(inext).range_length());
            h++;
          }
          if (h > g)
          {
            const int run_len = run_hi - run_lo;
            std::vector<char> run_buf(static_cast<size_t>(run_len));
            if (!parix_datanode_read_range_stub(dn_stub.get(), seg_read.datanode_ip(), seg_read.datanode_port(), seg_read.block_key(), bs,
                                                  run_lo, run_len, run_buf.data()))
            {
              std::cout << "[Client][Parix] read data slice failed " << seg_read.block_key() << std::endl;
              return false;
            }
            for (size_t t = g; t <= h; t++)
            {
              const int idx = idx_sort[t];
              const auto &s = plan.segments(idx);
              const int o = static_cast<int>(s.range_offset());
              const int l = static_cast<int>(s.range_length());
              old_seg_by_idx[idx].assign(run_buf.begin() + (o - run_lo), run_buf.begin() + (o - run_lo) + l);
            }
            g = h + 1;
          }
          else
          {
            const int idx = idx_sort[g];
            const auto &s = plan.segments(idx);
            const int l = static_cast<int>(s.range_length());
            old_seg_by_idx[idx].resize(static_cast<size_t>(l));
            if (!parix_datanode_read_range_stub(dn_stub.get(), seg_read.datanode_ip(), seg_read.datanode_port(), seg_read.block_key(), bs,
                                                 static_cast<int>(s.range_offset()), l, old_seg_by_idx[idx].data()))
            {
              std::cout << "[Client][Parix] read data slice failed " << seg_read.block_key() << std::endl;
              return false;
            }
            g++;
          }
        }
      }

      std::unordered_map<std::string, std::unique_ptr<proxy_proto::proxyService::Stub>> data_proxy_stubs;
      std::unordered_map<std::string, std::unique_ptr<proxy_proto::proxyService::Stub>> parity_stub_by_ep;

      std::vector<ParixPendingSlice> pending_writes;
      pending_writes.reserve(idxs.size());

      for (int idx : idxs)
      {
        const coordinator_proto::ParixDataSegmentPlan &seg = plan.segments(idx);
        const int seg_lo = seg.block_id() * bs + seg.range_offset();
        const int rlen = static_cast<int>(seg.range_length());
        int buf_off = 0;
        if (!parix_find_packed_payload_offset(seg_lo, rlen, ranges, &buf_off))
        {
          std::cout << "[Client][Parix] segment not covered by any input range" << std::endl;
          return false;
        }
        if (buf_off < 0 || buf_off + rlen > packed_len)
        {
          std::cout << "[Client][Parix] segment buffer mapping error" << std::endl;
          return false;
        }
        const char *payload_ptr = packed_new_bytes + static_cast<size_t>(buf_off);

        std::vector<char> &old_seg = old_seg_by_idx.at(idx);

        if (parix_client_trace())
        {
          std::cout << "[Client][Parix] segment plan_idx=" << idx << " logical[" << seg_lo << "," << (seg_lo + rlen)
                    << ") block_intra_off=" << seg.range_offset() << " len=" << rlen << std::endl;
        }
        parix_log_slice_hex("BEFORE update (on block slice)", old_seg.data(), rlen);
        parix_log_slice_hex("UPDATE content (new payload)", payload_ptr, rlen);

        if (parix_client_trace())
        {
          std::cout << "[Client][Parix] call data_proxy gRPC=" << seg.data_proxy_ip() << ":" << seg.data_proxy_grpc_port()
                    << " tcp_payload_port=" << seg.data_proxy_tcp_shift_port()
                    << " (proxy will fan-out parixJournalAppend to parities)" << std::endl;
        }

        proxy_proto::ParixDataUpdatePlacement placement;
        fill_parix_placement_from_segment(seg, stripe_id, plan.batch_id(), &placement);

        const std::string dpe = seg.data_proxy_ip() + ":" + std::to_string(seg.data_proxy_grpc_port());
        auto data_proxy_guard = g_parix_data_proxy_locks.lock_endpoint(dpe);
        const int dp_tcp = seg.data_proxy_tcp_shift_port();
        const std::string dp_ip = seg.data_proxy_ip();
        std::thread tcp_thr([payload_ptr, rlen, dp_ip, dp_tcp]() {
          try
          {
            asio::io_context ioc;
            asio::ip::tcp::socket s(ioc);
            asio::ip::tcp::resolver r(ioc);
            asio::connect(s, r.resolve({dp_ip, std::to_string(dp_tcp)}));
            asio::error_code ec;
            asio::write(s, asio::buffer(payload_ptr, static_cast<size_t>(rlen)), ec);
            asio::error_code ign;
            s.shutdown(asio::ip::tcp::socket::shutdown_send, ign);
            s.close(ign);
          }
          catch (const std::exception &e)
          {
            std::cerr << "[Client][Parix] tcp thread: " << e.what() << std::endl;
          }
        });

        grpc::ClientContext sched_ctx;
        parix_set_rpc_deadline(sched_ctx);
        proxy_proto::ParixScheduleDataUpdateReply sched_rep;
        auto ds_it = data_proxy_stubs.find(dpe);
        if (ds_it == data_proxy_stubs.end())
        {
          ds_it = data_proxy_stubs
                      .emplace(dpe, proxy_proto::proxyService::NewStub(parix_proxy_channel(dpe)))
                      .first;
        }
        grpc::Status sched_st = ds_it->second->parixScheduleDataUpdate(&sched_ctx, placement, &sched_rep);
        tcp_thr.join();
        if (!sched_st.ok() || !sched_rep.ifcommit())
        {
          std::cout << "[Client][Parix] parixScheduleDataUpdate failed" << std::endl;
          return false;
        }

        for (int ai = 0; ai < sched_rep.journal_acks_size(); ++ai)
        {
          const proxy_proto::ParixJournalAckItem &ack = sched_rep.journal_acks(ai);
          if (parix_client_trace())
          {
            const char *ack_str = (ack.ack() == proxy_proto::PARIX_ACK_SUCCESS) ? "SUCCESS" : "NEED_D0";
            std::cout << "[Client][Parix]   schedule reply: parity_proxy " << ack.parity_proxy_ip() << ":"
                      << ack.parity_proxy_grpc_port() << " parity_block_id=" << ack.parity_block_id() << " -> " << ack_str
                      << std::endl;
          }
          if (ack.ack() != proxy_proto::PARIX_ACK_NEED_D0)
          {
            continue;
          }
          grpc::ClientContext sup_ctx;
          parix_set_rpc_deadline(sup_ctx);
          proxy_proto::ParixSupplyD0Request sreq;
          sreq.set_stripe_id(stripe_id);
          sreq.set_batch_id(plan.batch_id());
          sreq.set_write_generation(seg.write_generation());
          sreq.set_parity_block_id(ack.parity_block_id());
          sreq.set_data_block_id(seg.block_id());
          sreq.set_range_offset(seg.range_offset());
          sreq.set_range_length(static_cast<uint64_t>(rlen));
          sreq.set_old_payload(old_seg.data(), static_cast<size_t>(rlen));
          const std::string sup_ep = ack.parity_proxy_ip() + ":" + std::to_string(ack.parity_proxy_grpc_port());
          auto pit = parity_stub_by_ep.find(sup_ep);
          if (pit == parity_stub_by_ep.end())
          {
            auto sup_ch = parix_proxy_channel(sup_ep);
            pit = parity_stub_by_ep.emplace(sup_ep, proxy_proto::proxyService::NewStub(sup_ch)).first;
          }
          proxy_proto::SetReply sup_rep;
          grpc::Status sup_st = pit->second->parixSupplyD0(&sup_ctx, sreq, &sup_rep);
          if (!sup_st.ok() || !sup_rep.ifcommit())
          {
            std::cout << "[Client][Parix] parixSupplyD0 failed parity_block_id=" << ack.parity_block_id() << std::endl;
            return false;
          }
          if (parix_client_trace())
          {
            std::cout << "[Client][Parix]   parixSupplyD0 ok -> parity_proxy " << ack.parity_proxy_ip() << ":"
                      << ack.parity_proxy_grpc_port() << " parity_block_id=" << ack.parity_block_id() << std::endl;
          }
        }

        pending_writes.push_back(ParixPendingSlice{static_cast<int>(seg.range_offset()), rlen, payload_ptr});
        parix_log_slice_hex("AFTER update (new bytes staged for writeback)", payload_ptr, rlen);
      }

      const std::vector<ParixCoalescedDiskWrite> coalesced = parix_coalesce_disk_writes(std::move(pending_writes));
      for (const ParixCoalescedDiskWrite &cw : coalesced)
      {
        if (!parix_datanode_write_range_stub(dn_stub.get(), seg_read.datanode_ip(), seg_read.datanode_port(), seg_read.block_key(), bs,
                                            cw.off, cw.len, cw.data))
        {
          std::cout << "[Client][Parix] write data slice failed " << seg_read.block_key() << std::endl;
          return false;
        }
      }
      if (parix_client_trace())
      {
        std::cout << "[Client][Parix] data block slice writeback done datanode " << seg_read.datanode_ip() << ":" << seg_read.datanode_port()
                  << " key=" << seg_read.block_key() << " logical_slices=" << idxs.size() << " disk_writes=" << coalesced.size()
                  << std::endl;
      }
    }

    grpc::ClientContext cctx;
    parix_set_rpc_deadline(cctx);
    coordinator_proto::ParixCommitBatchRequest creq;
    creq.set_stripe_id(stripe_id);
    creq.set_batch_id(plan.batch_id());
    coordinator_proto::ReplyFromCoordinator crpl;
    std::cout << "[Client][Parix] commitParixBatch -> coordinator (parity flush is journal-threshold driven on proxies)" << std::endl;
    grpc::Status cst = m_coordinator_ptr->commitParixBatch(&cctx, creq, &crpl);
    if (!cst.ok())
    {
      std::cout << "[Client][Parix] commitParixBatch failed: " << cst.error_message() << std::endl;
      return false;
    }
    std::cout << "[Client][Parix] partial update committed batch_id=" << plan.batch_id() << std::endl;
    return true;
  }

  bool Client::parix_full_stripe_rewrite(int stripe_id, const char *new_stripe_data)
  {
    if (!m_sys_config || !new_stripe_data)
    {
      return false;
    }
    const int k = m_sys_config->k;
    const int r = m_sys_config->r;
    const int z = m_sys_config->z;
    const int bs = m_sys_config->BlockSize;

    std::vector<char *> data_ptrs(static_cast<size_t>(k));
    for (int i = 0; i < k; ++i)
    {
      data_ptrs[static_cast<size_t>(i)] = const_cast<char *>(new_stripe_data) + static_cast<size_t>(i) * static_cast<size_t>(bs);
    }
    std::vector<std::vector<char>> parity_store(static_cast<size_t>(r + z));
    std::vector<char *> parity_ptrs;
    parity_ptrs.reserve(static_cast<size_t>(r + z));
    for (int i = 0; i < r + z; ++i)
    {
      parity_store[static_cast<size_t>(i)].assign(static_cast<size_t>(bs), 0);
      parity_ptrs.push_back(parity_store[static_cast<size_t>(i)].data());
    }

    if (m_sys_config->CodeType == "UniLRC")
    {
      ECProject::encode_unilrc(k, r, z, reinterpret_cast<unsigned char **>(data_ptrs.data()),
                               reinterpret_cast<unsigned char **>(parity_ptrs.data()), bs);
    }
    else if (m_sys_config->CodeType == "OptimalLRC")
    {
      ECProject::encode_optimal_lrc(k, r, z, reinterpret_cast<unsigned char **>(data_ptrs.data()),
                                    reinterpret_cast<unsigned char **>(parity_ptrs.data()), bs);
    }
    else if (m_sys_config->CodeType == "UniformLRC")
    {
      ECProject::encode_uniform_lrc(k, r, z, reinterpret_cast<unsigned char **>(data_ptrs.data()),
                                    reinterpret_cast<unsigned char **>(parity_ptrs.data()), bs);
    }
    else if (is_azure_like_code(m_sys_config->CodeType))
    {
      ECProject::encode_azure_lrc(k, r, z, reinterpret_cast<unsigned char **>(data_ptrs.data()),
                                  reinterpret_cast<unsigned char **>(parity_ptrs.data()), bs);
    }
    else
    {
      std::cout << "[Client][Parix] full stripe: CodeType not supported" << std::endl;
      return false;
    }

    grpc::ClientContext fctx;
    parix_set_rpc_deadline(fctx);
    coordinator_proto::ParixFullStripePlanRequest freq;
    freq.set_stripe_id(stripe_id);
    coordinator_proto::ParixFullStripePlanReply fplan;
    grpc::Status fst = m_coordinator_ptr->planParixFullStripe(&fctx, freq, &fplan);
    if (!fst.ok() || !fplan.ok())
    {
      std::cout << "[Client][Parix] planParixFullStripe failed: " << (fst.ok() ? fplan.err() : fst.error_message()) << std::endl;
      return false;
    }

    for (int i = 0; i < fplan.parity_targets_size(); ++i)
    {
      const coordinator_proto::ParixParityEndpoint &ep = fplan.parity_targets(i);
      const int pbid = ep.parity_block_id();
      const int idx = pbid - k;
      if (idx < 0 || idx >= r + z)
      {
        std::cout << "[Client][Parix] full stripe: bad parity_block_id " << pbid << std::endl;
        return false;
      }
      proxy_proto::ParixParityFullOverwriteRequest oreq;
      oreq.set_stripe_id(stripe_id);
      oreq.set_new_write_generation(fplan.new_write_generation());
      oreq.set_parity_block_id(pbid);
      oreq.set_parity_block_key(ep.parity_block_key());
      oreq.set_full_parity_block(parity_ptrs[static_cast<size_t>(idx)], static_cast<size_t>(bs));
      oreq.set_datanode_ip(ep.parity_datanode_ip());
      oreq.set_datanode_port(ep.parity_datanode_port());
      for (int j = 0; j < fplan.journal_invalidations_size(); ++j)
      {
        const coordinator_proto::ParixJournalInvalidationRange &inv = fplan.journal_invalidations(j);
        proxy_proto::ParixJournalInvalidationRange *jr = oreq.add_journal_invalidations();
        jr->set_data_block_id(inv.data_block_id());
        jr->set_range_offset(inv.range_offset());
        jr->set_range_length(inv.range_length());
      }
      grpc::ClientContext po_ctx;
      parix_set_rpc_deadline(po_ctx);
      auto pch = parix_proxy_channel(ep.proxy_ip() + ":" + std::to_string(ep.proxy_grpc_port()));
      std::unique_ptr<proxy_proto::proxyService::Stub> pstub = proxy_proto::proxyService::NewStub(pch);
      proxy_proto::SetReply prepl;
      grpc::Status pst = pstub->parixParityFullOverwrite(&po_ctx, oreq, &prepl);
      if (!pst.ok() || !prepl.ifcommit())
      {
        std::cout << "[Client][Parix] parixParityFullOverwrite failed parity_block_id=" << pbid << std::endl;
        return false;
      }
    }
    std::cout << "[Client][Parix] full stripe rewrite done stripe_id=" << stripe_id << std::endl;
    return true;
  }
} // namespace ECProject