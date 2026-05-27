#include "proxy.h"
#include <grpcpp/create_channel.h>
#include <grpcpp/support/channel_arguments.h>
#include "jerasure.h"
#include "reed_sol.h"
#include "tinyxml2.h"
#include "toolbox.h"
#include "lrc.h"
#include <thread>
#include <mutex>
#include <cassert>
#include <string>
#include <cstring>
#include <fstream>
#include <sys/mman.h>
#include "unilrc_encoder.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <map>
#if defined(__linux__)
#include <netinet/tcp.h>
#include <sys/socket.h>
#ifndef SO_MAX_PACING_RATE
#define SO_MAX_PACING_RATE 47
#endif
#endif
template <typename T>
inline T ceil(T const &A, T const &B)
{
  return T((A + B - 1) / B);
};
namespace ECProject
{
  namespace
  {
    inline bool is_azure_like_code(const std::string &code_type)
    {
      return code_type == "AzureLRC" || code_type == "RandomLRC";
    }

    /** gRPC default max message is 4MB; RackCU home-delta fetch / Parix payloads need larger. */
    inline grpc::ChannelArguments grpc_large_payload_channel_args()
    {
      grpc::ChannelArguments args;
      constexpr int k_max = 128 * 1024 * 1024;
      args.SetMaxReceiveMessageSize(k_max);
      args.SetMaxSendMessageSize(k_max);
      return args;
    }
    inline int64_t rackcu_wall_unix_ms_now()
    {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
          .count();
    }

    struct RackCuBatchXferAcc
    {
      bool have{false};
      double pure_xfer_sec_sum{0.0};
      int64_t wall_min_ms{0};
      int64_t wall_max_ms{0};
    };

    std::mutex g_rackcu_batch_xfer_mu;
    std::map<std::pair<int, uint64_t>, RackCuBatchXferAcc> g_rackcu_batch_xfer;

    void rackcu_batch_xfer_add(int stripe_id, uint64_t xfer_plan_id, double pure_sec, int64_t w0, int64_t w1)
    {
      if (xfer_plan_id == 0u)
      {
        return;
      }
      if (w1 < w0)
      {
        std::swap(w0, w1);
      }
      const std::pair<int, uint64_t> key{stripe_id, xfer_plan_id};
      std::lock_guard<std::mutex> lk(g_rackcu_batch_xfer_mu);
      RackCuBatchXferAcc &acc = g_rackcu_batch_xfer[key];
      if (!acc.have)
      {
        acc.have = true;
        acc.wall_min_ms = w0;
        acc.wall_max_ms = w1;
        acc.pure_xfer_sec_sum = pure_sec;
        return;
      }
      acc.wall_min_ms = std::min(acc.wall_min_ms, w0);
      acc.wall_max_ms = std::max(acc.wall_max_ms, w1);
      acc.pure_xfer_sec_sum += pure_sec;
    }


    constexpr int kRackcuParityHexPreview = 16;

    inline void log_rackcu_parity_range_hex(
        int proxy_cluster,
        int stripe_id,
        const char *phase,
        int parity_block_id,
        int range_off,
        int range_len,
        const unsigned char *bytes,
        int nbytes_available)
    {
      const int n = std::min(kRackcuParityHexPreview,
                             std::min(range_len, nbytes_available));
      std::cout << "[Proxy" << proxy_cluster << "][RACKCU][Parity] " << phase
                << " stripe=" << stripe_id << " parity_block_id=" << parity_block_id
                << " range=[" << range_off << "," << (range_off + range_len)
                << ") first " << n << " byte(s) hex:";
      for (int i = 0; i < n; i++)
      {
        std::cout << ' ' << std::hex << std::setfill('0') << std::setw(2)
                  << static_cast<unsigned>(bytes[i]);
      }
      std::cout << std::dec << std::endl;
    }

    constexpr uint32_t k_rackcu_home_delta_magic = 0x52434448u;
    inline uint32_t rackcu_rd_u32_le(const unsigned char *p)
    {
      return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) | (static_cast<uint32_t>(p[2]) << 16) |
             (static_cast<uint32_t>(p[3]) << 24);
    }
    inline int32_t rackcu_rd_i32_le(const unsigned char *p)
    {
      return static_cast<int32_t>(rackcu_rd_u32_le(p));
    }

    bool merge_one_home_delta_blob_into_rows(std::vector<std::vector<unsigned char>> *rows, int k, int block_size,
                                               const std::string &blob)
    {
      if (rows == nullptr || static_cast<int>(rows->size()) < k)
      {
        return false;
      }
      if (blob.size() < 8u)
      {
        return false;
      }
      const auto *base = reinterpret_cast<const unsigned char *>(blob.data());
      if (rackcu_rd_u32_le(base) != k_rackcu_home_delta_magic)
      {
        return false;
      }
      const uint32_t nseg = rackcu_rd_u32_le(base + 4);
      size_t pos = 8u;
      for (uint32_t si = 0; si < nseg; si++)
      {
        if (pos + 12u > blob.size())
        {
          return false;
        }
        const int32_t bid = rackcu_rd_i32_le(base + pos);
        pos += 4u;
        const int32_t off = rackcu_rd_i32_le(base + pos);
        pos += 4u;
        const int32_t len = rackcu_rd_i32_le(base + pos);
        pos += 4u;
        if (bid < 0 || bid >= k || off < 0 || len < 0 || off + len > block_size)
        {
          return false;
        }
        if (pos + static_cast<size_t>(len) > blob.size())
        {
          return false;
        }
        if ((*rows)[static_cast<size_t>(bid)].size() != static_cast<size_t>(block_size))
        {
          (*rows)[static_cast<size_t>(bid)].assign(static_cast<size_t>(block_size), 0);
        }
        std::memcpy((*rows)[static_cast<size_t>(bid)].data() + off, base + pos, static_cast<size_t>(len));
        pos += static_cast<size_t>(len);
      }
      return pos == blob.size();
    }

    bool merge_rackcu_home_delta_blobs(std::vector<std::vector<unsigned char>> *rows, int k, int block_size,
                                       const std::vector<std::string> &blobs)
    {
      rows->assign(static_cast<size_t>(k), std::vector<unsigned char>(static_cast<size_t>(block_size), 0));
      for (const auto &b : blobs)
      {
        if (!merge_one_home_delta_blob_into_rows(rows, k, block_size, b))
        {
          return false;
        }
      }
      return true;
    }
  }

  bool ProxyImpl::init_coordinator()
  {
    m_coordinator_ptr = coordinator_proto::coordinatorService::NewStub(grpc::CreateChannel(m_coordinator_address, grpc::InsecureChannelCredentials()));
    // coordinator_proto::RequestToCoordinator req;
    // coordinator_proto::ReplyFromCoordinator rep;
    // grpc::ClientContext context;
    // std::string proxy_info = "Proxy [" + proxy_ip_port + "]";
    // req.set_name(proxy_info);
    // grpc::Status status;
    // status = m_coordinator_ptr->checkalive(&context, req, &rep);
    // if (status.ok())
    // {
    //   std::cout << "[Coordinator Check] ok from " << m_coordinator_address << std::endl;
    // }
    // else
    // {
    //   std::cout << "[Coordinator Check] failed to connect " << m_coordinator_address << std::endl;
    // }
    return true;
  }

  bool ProxyImpl::init_datanodes(std::string m_datanodeinfo_path)
  {
    tinyxml2::XMLDocument xml;
    xml.LoadFile(m_datanodeinfo_path.c_str());
    tinyxml2::XMLElement *root = xml.RootElement();
    for (tinyxml2::XMLElement *cluster = root->FirstChildElement(); cluster != nullptr; cluster = cluster->NextSiblingElement())
    {
      std::string cluster_id(cluster->Attribute("id"));
      std::string proxy(cluster->Attribute("proxy"));
      if (proxy == proxy_ip_port)
      {
        m_self_cluster_id = std::stoi(cluster_id);
      }
      for (tinyxml2::XMLElement *node = cluster->FirstChildElement()->FirstChildElement(); node != nullptr; node = node->NextSiblingElement())
      {
        std::string node_uri(node->Attribute("uri"));
        auto _stub = datanode_proto::datanodeService::NewStub(grpc::CreateChannel(node_uri, grpc::InsecureChannelCredentials()));
        // datanode_proto::CheckaliveCMD cmd;
        // datanode_proto::RequestResult result;
        // grpc::ClientContext context;
        // std::string proxy_info = "Proxy [" + proxy_ip_port + "]";
        // cmd.set_name(proxy_info);
        // grpc::Status status;
        // status = _stub->checkalive(&context, cmd, &result);
        // if (status.ok())
        // {
        //   // std::cout << "[Datanode Check] ok from " << node_uri << std::endl;
        // }
        // else
        // {
        //   std::cout << "[Datanode Check] failed to connect " << node_uri << std::endl;
        // }
        m_datanode_ptrs.insert(std::make_pair(node_uri, std::move(_stub)));
      }
    }
    return true;
  }

  namespace
  {
    std::string rackcu_ip_only_host(std::string const &hostport)
    {
      const auto c = hostport.find(':');
      if (c == std::string::npos)
      {
        return hostport;
      }
      return hostport.substr(0, c);
    }

    void tcp_read_all_bytes(asio::ip::tcp::socket &sock, void *buf, size_t nbytes, asio::error_code &ec)
    {
      ec.clear();
      char *p = static_cast<char *>(buf);
      size_t off = 0;
      while (off < nbytes)
      {
        const size_t m = asio::read(sock, asio::buffer(p + off, nbytes - off), ec);
        if (ec)
        {
          return;
        }
        off += m;
      }
    }

    std::string rackcu_tcp_read_append_key(asio::ip::tcp::socket &sock, asio::error_code &ec)
    {
      uint16_t key_len = 0;
      tcp_read_all_bytes(sock, &key_len, sizeof(key_len), ec);
      if (ec)
      {
        return {};
      }
      if (key_len == 0 || key_len > static_cast<uint16_t>(ECProject::RACKCU_APPEND_KEY_MAX))
      {
        std::cout << "[RACKCU] invalid append key length on wire: " << key_len << std::endl;
        ec = asio::error::make_error_code(asio::error::invalid_argument);
        return {};
      }
      std::string key(static_cast<size_t>(key_len), '\0');
      tcp_read_all_bytes(sock, &key[0], static_cast<size_t>(key_len), ec);
      return key;
    }

    void rackcu_tcp_write_keyed_frame(asio::ip::tcp::socket &sock, const std::string &append_key, const char *data,
                                      size_t nbytes, asio::error_code &ec)
    {
      ec.clear();
      if (append_key.empty() || append_key.size() > static_cast<size_t>(ECProject::RACKCU_APPEND_KEY_MAX))
      {
        ec = asio::error::make_error_code(asio::error::invalid_argument);
        return;
      }
      const uint16_t klen = static_cast<uint16_t>(append_key.size());
      asio::write(sock, asio::buffer(&klen, sizeof(klen)), ec);
      if (ec)
      {
        return;
      }
      asio::write(sock, asio::buffer(append_key.data(), append_key.size()), ec);
      if (ec)
      {
        return;
      }
      if (nbytes > 0 && data != nullptr)
      {
        asio::write(sock, asio::buffer(data, nbytes), ec);
      }
    }
  }

  bool ProxyImpl::init_ip_to_cluster_map(std::string cluster_xml_path)
  {
    m_ip_to_cluster_id.clear();
    tinyxml2::XMLDocument xml;
    if (xml.LoadFile(cluster_xml_path.c_str()) != tinyxml2::XML_SUCCESS)
    {
      std::cout << "[Proxy] init_ip_to_cluster_map: failed to load " << cluster_xml_path << std::endl;
      return false;
    }
    tinyxml2::XMLElement *root = xml.RootElement();
    if (root == nullptr)
    {
      return false;
    }
    for (tinyxml2::XMLElement *cluster = root->FirstChildElement(); cluster != nullptr; cluster = cluster->NextSiblingElement())
    {
      const int cid = std::stoi(cluster->Attribute("id"));
      const std::string proxy(cluster->Attribute("proxy"));
      m_ip_to_cluster_id[rackcu_ip_only_host(proxy)] = cid;
      tinyxml2::XMLElement *dn_root = cluster->FirstChildElement();
      if (dn_root == nullptr)
      {
        continue;
      }
      for (tinyxml2::XMLElement *node = dn_root->FirstChildElement(); node != nullptr; node = node->NextSiblingElement())
      {
        const std::string uri(node->Attribute("uri"));
        m_ip_to_cluster_id[rackcu_ip_only_host(uri)] = cid;
      }
    }
    return true;
  }

  bool ProxyImpl::init_bandwidth_matrix(std::string cluster_xml_path)
  {
    m_bw_matrix_enabled = false;
    std::string dir = std::move(cluster_xml_path);
    const auto pos = dir.find_last_of('/');
    if (pos != std::string::npos)
    {
      dir = dir.substr(0, pos);
    }
    else
    {
      dir = ".";
    }
    const std::string bwfile = dir + "/bw_limit_matrix.txt";
    if (!m_bw_limit.loadFromFile(bwfile))
    {
      std::cout << "[Proxy] cross-cluster bandwidth: matrix not loaded (optional) " << bwfile << std::endl;
      return false;
    }
    m_bw_matrix_enabled = true;
#if defined(__linux__)
    std::cout << "[Proxy] cross-cluster bandwidth: kernel pacing (SO_MAX_PACING_RATE) + RCVBUF, matrix=" << bwfile
              << std::endl;
#else
    std::cout << "[Proxy] cross-cluster bandwidth matrix loaded but kernel pacing only on Linux; " << bwfile
              << std::endl;
#endif
    return true;
  }

  int ProxyImpl::cluster_for_datapath_ip(const char *ip) const
  {
    if (ip == nullptr)
    {
      return -1;
    }
    const auto it = m_ip_to_cluster_id.find(std::string(ip));
    if (it == m_ip_to_cluster_id.end())
    {
      return -1;
    }
    return it->second;
  }

  void ProxyImpl::apply_kernel_bandwidth_to_peer_socket(asio::ip::tcp::socket &sock, const char *peer_ip) const
  {
#if !defined(__linux__)
    (void)sock;
    (void)peer_ip;
    return;
#else
    if (!m_bw_matrix_enabled || peer_ip == nullptr || peer_ip[0] == '\0')
    {
      return;
    }
    const int peer = cluster_for_datapath_ip(peer_ip);
    if (peer < 0 || peer == m_self_cluster_id)
    {
      return;
    }
    const double mbps = m_bw_limit.getBandwidthMBps(m_self_cluster_id, peer, 0.0);
    if (mbps <= 0.0)
    {
      return;
    }
    const double bps_d = mbps * 1024.0 * 1024.0;
    const auto fd = sock.native_handle();
    uint32_t pacing_bps = 0;
    if (bps_d >= static_cast<double>(UINT32_MAX))
    {
      pacing_bps = UINT32_MAX;
    }
    else
    {
      pacing_bps = static_cast<uint32_t>(bps_d);
    }
    if (pacing_bps > 0u)
    {
      if (setsockopt(fd, SOL_SOCKET, SO_MAX_PACING_RATE, &pacing_bps, sizeof(pacing_bps)) != 0)
      {
        static std::atomic<int> warn_pace{0};
        if (warn_pace.fetch_add(1) == 0)
        {
          std::cout << "[Proxy] SO_MAX_PACING_RATE failed (kernel/fq?): errno=" << errno << " " << std::strerror(errno)
                    << std::endl;
        }
      }
    }
    int rcv = static_cast<int>(bps_d * 0.05);
    if (rcv < 8192)
    {
      rcv = 8192;
    }
    if (rcv > 4 * 1024 * 1024)
    {
      rcv = 4 * 1024 * 1024;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv)) != 0)
    {
      static std::atomic<int> warn_rcv{0};
      if (warn_rcv.fetch_add(1) == 0)
      {
        std::cout << "[Proxy] SO_RCVBUF tune failed: errno=" << errno << " " << std::strerror(errno) << std::endl;
      }
    }
#endif
  }

  grpc::Status ProxyImpl::checkalive(grpc::ServerContext *context,
                                     const proxy_proto::CheckaliveCMD *request,
                                     proxy_proto::RequestResult *response)
  {

    std::cout << "[Proxy] checkalive" << request->name() << std::endl;
    response->set_message(false);
    init_coordinator();
    return grpc::Status::OK;
  }

  bool ProxyImpl::fetch_rack_cu_home_staging_blob(const proxy_proto::RackCuHomeDeltaStagingRef &ref, std::string *out_blob,
                                                  int rack_cu_xfer_stripe_id, uint64_t rack_cu_xfer_plan_id)
  {
    if (out_blob == nullptr)
    {
      return false;
    }
    const std::string &cache_key = ref.staging_key();
    constexpr auto k_cache_ttl = std::chrono::minutes(5);
    {
      std::lock_guard<std::mutex> lk(m_rackcu_home_staging_cache_mu);
      auto it = m_rackcu_home_staging_blob_cache.find(cache_key);
      if (it != m_rackcu_home_staging_blob_cache.end())
      {
        const auto age = std::chrono::steady_clock::now() - it->second.second;
        if (age < k_cache_ttl)
        {
          *out_blob = it->second.first;
          return true;
        }
        m_rackcu_home_staging_blob_cache.erase(it);
      }
    }

    const uint64_t nb = ref.blob_bytes();
    if (nb == 0u || nb > (1ull << 30))
    {
      return false;
    }
    const size_t nbytes = static_cast<size_t>(nb);
    out_blob->assign(nbytes, '\0');
    if (ref.holder_proxy_ip() == m_ip && ref.holder_proxy_port() == m_port)
    {
      if (!GetFromDatanode(ref.staging_key(), &(*out_blob)[0], nbytes, ref.staging_datanode_ip().c_str(),
                           ref.staging_datanode_port()))
      {
        return false;
      }
    }
    else
    {
      const std::string target = ref.holder_proxy_ip() + ":" + std::to_string(ref.holder_proxy_port());
      auto channel = grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), grpc_large_payload_channel_args());
      auto stub = proxy_proto::proxyService::NewStub(channel);
      proxy_proto::RackCuHomeDeltaFetchRequest req;
      req.set_staging_key(ref.staging_key());
      req.set_staging_datanode_ip(ref.staging_datanode_ip());
      req.set_staging_datanode_port(ref.staging_datanode_port());
      req.set_blob_bytes(ref.blob_bytes());
      if (rack_cu_xfer_plan_id != 0u)
      {
        req.set_rack_cu_xfer_stripe_id(rack_cu_xfer_stripe_id);
        req.set_rack_cu_xfer_plan_id(rack_cu_xfer_plan_id);
      }
      grpc::ClientContext ctx;
      proxy_proto::RackCuHomeDeltaFetchReply rep;
      grpc::Status st = stub->fetchRackCuHomeDeltaStaging(&ctx, req, &rep);
      if (!st.ok() || !rep.ok() || static_cast<size_t>(rep.blob().size()) != nbytes)
      {
        return false;
      }
      *out_blob = rep.blob();
    }

    {
      std::lock_guard<std::mutex> lk(m_rackcu_home_staging_cache_mu);
      m_rackcu_home_staging_blob_cache[cache_key] = {*out_blob, std::chrono::steady_clock::now()};
    }
    return true;
  }

  grpc::Status ProxyImpl::fetchRackCuHomeDeltaStaging(grpc::ServerContext *context,
                                                      const proxy_proto::RackCuHomeDeltaFetchRequest *request,
                                                      proxy_proto::RackCuHomeDeltaFetchReply *response)
  {
    (void)context;
    const auto rackcu_fetch_t0 = std::chrono::steady_clock::now();
    const int64_t rackcu_fetch_w0 = rackcu_wall_unix_ms_now();
    const uint64_t nb = request->blob_bytes();
    if (nb == 0u || nb > (1ull << 30))
    {
      response->set_ok(false);
      return grpc::Status::OK;
    }
    const size_t nbytes = static_cast<size_t>(nb);
    std::vector<char> buf(nbytes);
    const bool ok = GetFromDatanode(request->staging_key(), buf.data(), nbytes, request->staging_datanode_ip().c_str(),
                                    request->staging_datanode_port());
    response->set_ok(ok);
    if (ok)
    {
      response->set_blob(buf.data(), buf.size());
    }
    if (request->rack_cu_xfer_plan_id() != 0u)
    {
      const auto rackcu_fetch_t1 = std::chrono::steady_clock::now();
      const int64_t rackcu_fetch_w1 = rackcu_wall_unix_ms_now();
      rackcu_batch_xfer_add(request->rack_cu_xfer_stripe_id(), request->rack_cu_xfer_plan_id(),
                            std::chrono::duration<double>(rackcu_fetch_t1 - rackcu_fetch_t0).count(), rackcu_fetch_w0,
                            rackcu_fetch_w1);
    }
    return grpc::Status::OK;
  }

  bool ProxyImpl::delete_rack_cu_staging_on_datanode(const std::string &key, const std::string &dn_ip, int dn_port)
  {
    const std::string node_ip_port = dn_ip + ":" + std::to_string(dn_port);
    const auto it = m_datanode_ptrs.find(node_ip_port);
    if (it == m_datanode_ptrs.end())
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][RACKCU][DEL-STAGING] unknown datanode " << node_ip_port << std::endl;
      return false;
    }
    grpc::ClientContext context;
    datanode_proto::DelInfo delinfo;
    datanode_proto::RequestResult response_dn;
    delinfo.set_block_key(key);
    grpc::Status status = it->second->handleDelete(&context, delinfo, &response_dn);
    if (!status.ok())
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][RACKCU][DEL-STAGING] handleDelete failed key=" << key
                << " err=" << status.error_message() << std::endl;
      return false;
    }
    std::cout << "[Proxy" << m_self_cluster_id << "][RACKCU][DEL-STAGING] removed staging key=" << key << " at " << node_ip_port
              << std::endl;
    {
      std::lock_guard<std::mutex> lk(m_rackcu_home_staging_cache_mu);
      m_rackcu_home_staging_blob_cache.erase(key);
    }
    return true;
  }

  grpc::Status ProxyImpl::deleteRackCuHomeDeltaStaging(grpc::ServerContext *context,
                                                       const proxy_proto::RackCuHomeDeltaDeleteRequest *request,
                                                       proxy_proto::RackCuHomeDeltaDeleteReply *response)
  {
    (void)context;
    const bool ok =
        delete_rack_cu_staging_on_datanode(request->staging_key(), request->staging_datanode_ip(), request->staging_datanode_port());
    response->set_ok(ok);
    return grpc::Status::OK;
  }

  bool ProxyImpl::MergeParityOnDatanode(const char *block_key, int block_id, const char *ip, int port, const std::string &append_mode)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::MergeParityInfo merge_parity_info;
      datanode_proto::RequestResult result;
      merge_parity_info.set_block_key(std::string(block_key));
      merge_parity_info.set_block_id(block_id);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      // REP_MODE; UNILRC_MODE; CACHED_MODE
      if (append_mode == "UNILRC_MODE" || append_mode == "CACHED_MODE")
      {
        grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleMergeParity(&context, merge_parity_info, &result);
      }
      /*else if (append_mode == "REP_MODE")
      {
        grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleMergeParityWithRep(&context, merge_parity_info, &result);
      }*/
      else
      {
        throw std::runtime_error("Invalid append mode: " + append_mode);
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  // slice_offset is the physical offset of the data block
  bool ProxyImpl::AppendToDatanode(const char *block_key, int block_id, size_t slice_size, const char *slice_buf, int slice_offset, const char *ip, int port, bool is_serialized)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::AppendInfo append_info;
      datanode_proto::RequestResult result;
      append_info.set_block_key(std::string(block_key));
      append_info.set_block_id(block_id);
      append_info.set_append_size(slice_size);
      append_info.set_append_offset(slice_offset);
      append_info.set_is_serialized(is_serialized);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleAppend(&context, append_info, &result);

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}), con_error);
      if (!con_error && IF_DEBUG)
      {
        std::cout << "Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " success! block_key: " << block_key << " block_id: " << block_id << " slice_size: " << slice_size << " slice_offset: " << slice_offset << " is_serialized: " << is_serialized << std::endl;
      }
      else if (IF_DEBUG)
      {
        std::cout << "Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " failed! block_key: " << block_key << " block_id: " << block_id << " slice_size: " << slice_size << " slice_offset: " << slice_offset << " is_serialized: " << is_serialized << std::endl;
        exit(-1);
      }
      apply_kernel_bandwidth_to_peer_socket(socket, ip);
      asio::write(socket, asio::buffer(slice_buf, slice_size), error);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Append139]"
                  << "Append to " << block_key << " with length of " << slice_size << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::RecoveryToDatanode(const char *block_key, int block_id, const char *buf, const char *ip, int port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::MergeParityInfo recovery_info;
      datanode_proto::RequestResult result;
      recovery_info.set_block_key(std::string(block_key));
      recovery_info.set_block_id(block_id);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      std::thread notify_datanode_thread([this, &context, &recovery_info, &result, &node_ip_port, &block_key, &block_id]()
      {
        grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleRecovery(&context, recovery_info, &result);
        if (!stat.ok())
        {
          std::cout << "[RecoveryToDatanode] notify datanode failed! block_key: " << block_key << " block_id: " << block_id << std::endl;
          exit(-1);
        }
      });
      //grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleRecovery(&context, recovery_info, &result);

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}), con_error);
      if (!con_error)
      {
        std::cout << "[RecoveryToDatanode] Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " success! block_key: " << block_key << " block_id: " << block_id << std::endl;
      }
      else
      {
        std::cout << "[RecoveryToDatanode] Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " failed! block_key: " << block_key << " block_id: " << block_id << std::endl;
        exit(-1);
      }
      apply_kernel_bandwidth_to_peer_socket(socket, ip);
      asio::write(socket, asio::buffer(buf, m_sys_config->BlockSize), error);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      notify_datanode_thread.join();
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::RecoveryToDatanodeBreakdown(const char *block_key, int block_id, const char *buf, const char *ip, int port, double *network_time, double *disk_io_time)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::MergeParityInfo recovery_info;
      datanode_proto::RequestResult result;
      recovery_info.set_block_key(std::string(block_key));
      recovery_info.set_block_id(block_id);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      std::chrono::high_resolution_clock::time_point grpc_notify_time;
      std::thread notify_datanode_thread([this, &context, &recovery_info, &result, &grpc_notify_time, &node_ip_port, &block_key, &block_id]()
      {
        grpc_notify_time = std::chrono::high_resolution_clock::now();
        grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleRecoveryBreakdown(&context, recovery_info, &result);
        if (!stat.ok())
        {
          std::cout << "[RecoveryToDatanode] notify datanode failed! block_key: " << block_key << " block_id: " << block_id << std::endl;
          exit(-1);
        }
      });

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}), con_error);
      std::chrono::high_resolution_clock::time_point begin = std::chrono::high_resolution_clock::now(); // start time for network
      if (!con_error)
      {
        std::cout << "[RecoveryToDatanode] Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " success! block_key: " << block_key << " block_id: " << block_id << std::endl;
      }
      else
      {
        std::cout << "[RecoveryToDatanode] Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " failed! block_key: " << block_key << " block_id: " << block_id << std::endl;
        exit(-1);
      }
      apply_kernel_bandwidth_to_peer_socket(socket, ip);
      asio::write(socket, asio::buffer(buf, m_sys_config->BlockSize), error);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now(); // end time for network
      *network_time = std::chrono::duration_cast<std::chrono::duration<double>>(end - begin).count();
      notify_datanode_thread.join();
      *disk_io_time = result.disk_io_end_time() - result.disk_io_start_time();
      *network_time += result.grpc_start_time() - std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify_time.time_since_epoch()).count();
  
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::SetToDatanode(const char *key, size_t key_length, const char *value, size_t value_length, const char *ip, int port, int offset)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::SetInfo set_info;
      datanode_proto::RequestResult result;
      set_info.set_block_key(std::string(key));
      set_info.set_block_size(value_length);
      set_info.set_proxy_ip(m_ip);
      set_info.set_proxy_port(m_port + offset);
      set_info.set_ispull(false);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleSet(&context, set_info, &result);

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}), con_error);
      if (!con_error && IF_DEBUG)
      {
        std::cout << "Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " success!" << std::endl;
      }

      apply_kernel_bandwidth_to_peer_socket(socket, ip);
      asio::write(socket, asio::buffer(value, value_length), error);

      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                  << "Write " << key << " to socket finish! With length of " << strlen(value) << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }
  bool ProxyImpl::GetFromDatanode(const std::string &key, char *value, const size_t value_length, const char *ip, const int port, 
    double *disk_io_start_time, double *disk_io_end_time, double *network_start_time, double *network_end_time, double* grpc_notify_time, double *grpc_start_time)
  {
    try
    {

      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << " Ready to recieve data from datanode " << std::endl;

      grpc::ClientContext context;
      datanode_proto::GetInfo get_info;
      datanode_proto::RequestResult result;
      get_info.set_block_key(key);
      get_info.set_block_size(value_length);
      // set proxy ip and port is useless, however, to competitive with the original code, we still need to set it
      get_info.set_proxy_ip(m_ip);
      get_info.set_proxy_port(m_port);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      std::chrono::high_resolution_clock::time_point grpc_notify = std::chrono::high_resolution_clock::now();
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleGetBreakdown(&context, get_info, &result);
      if (stat.ok() && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Call datanode to handle get " << key << std::endl;
      }
      else if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Call datanode to handle get " << key << " failed!" << std::endl;
        return false;
      }
      *disk_io_start_time = result.disk_io_start_time();
      *disk_io_end_time = result.disk_io_end_time();
      *grpc_notify_time = std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count();
      *grpc_start_time = result.grpc_start_time();
      

      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      std::chrono::high_resolution_clock::time_point begin = std::chrono::high_resolution_clock::now(); // start time for network
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}));
      asio::error_code ec;
      apply_kernel_bandwidth_to_peer_socket(socket, ip);
      tcp_read_all_bytes(socket, value, value_length, ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now(); // end time for network
      *network_start_time = std::chrono::duration_cast<std::chrono::duration<double>>(begin.time_since_epoch()).count();
      *network_end_time = std::chrono::duration_cast<std::chrono::duration<double>>(end.time_since_epoch()).count();
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Read data from socket with length of " << value_length << std::endl;
      }
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
      << " Read data from socket with length of " << value_length << std::endl;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }
  bool ProxyImpl::GetFromDatanode(const char *key, size_t key_length, char *value, size_t value_length, const char *ip, int port, int offset)
  {
    try
    {
      // ready to recieve
      char *buf = new char[value_length];
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Ready to recieve data from datanode " << std::endl;
      }

      grpc::ClientContext context;
      datanode_proto::GetInfo get_info;
      datanode_proto::RequestResult result;
      get_info.set_block_key(std::string(key));
      get_info.set_block_size(value_length);
      get_info.set_proxy_ip(m_ip);
      get_info.set_proxy_port(m_port + offset);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleGet(&context, get_info, &result);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Call datanode to handle get " << key << std::endl;
      }

      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}));
      asio::error_code ec;
      apply_kernel_bandwidth_to_peer_socket(socket, ip);
      tcp_read_all_bytes(socket, buf, value_length, ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Read data from socket with length of " << value_length << std::endl;
      }
      memcpy(value, buf, value_length);
      delete buf;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::GetFromDatanode(const std::string &key, char *value, const size_t value_length, const char *ip, const int port)
  {
    try
    {

      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << " Ready to recieve data from datanode " << std::endl;

      grpc::ClientContext context;
      datanode_proto::GetInfo get_info;
      datanode_proto::RequestResult result;
      get_info.set_block_key(key);
      get_info.set_block_size(value_length);
      // set proxy ip and port is useless, however, to competitive with the original code, we still need to set it
      get_info.set_proxy_ip(m_ip);
      get_info.set_proxy_port(m_port);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleGet(&context, get_info, &result);
      if (stat.ok() && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Call datanode to handle get " << key << std::endl;
      }
      else if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Call datanode to handle get " << key << " failed!" << std::endl;
        return false;
      }

      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}));
      asio::error_code ec;
      apply_kernel_bandwidth_to_peer_socket(socket, ip);
      tcp_read_all_bytes(socket, value, value_length, ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Read data from socket with length of " << value_length << std::endl;
      }
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
      << " Read data from socket with length of " << value_length << std::endl;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::DelInDatanode(std::string key, std::string node_ip_port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::DelInfo delinfo;
      datanode_proto::RequestResult response;
      delinfo.set_block_key(key);
      grpc::Status status = m_datanode_ptrs[node_ip_port]->handleDelete(&context, delinfo, &response);
      if (status.ok() && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][DEL] delete block " << key << " success!" << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  void ProxyImpl::printAppendStripeDataPlacement(const proxy_proto::AppendStripeDataPlacement *append_stripe_data_placement)
  {
    // Print basic info
    std::cout << "=== AppendStripeDataPlacement Info ===" << std::endl;
    std::cout << "Key: " << append_stripe_data_placement->key() << std::endl;
    std::cout << "Stripe ID: " << append_stripe_data_placement->stripe_id() << std::endl;
    std::cout << "Cluster ID: " << append_stripe_data_placement->cluster_id() << std::endl;
    std::cout << "Total Append Size: " << append_stripe_data_placement->append_size() << std::endl;
    std::cout << "Is Merge Parity: " << (append_stripe_data_placement->is_merge_parity() ? "true" : "false") << std::endl;
    std::cout << "Append Mode: " << append_stripe_data_placement->append_mode() << std::endl;
    std::cout << "Is Serialized: " << (append_stripe_data_placement->is_serialized() ? "true" : "false") << std::endl;

    // Print datanode info
    std::cout << "\n=== Datanode Info ===" << std::endl;
    for (int i = 0; i < append_stripe_data_placement->datanodeip_size(); i++)
    {
      std::cout << "Datanode " << i << ":" << std::endl;
      std::cout << "  IP: " << append_stripe_data_placement->datanodeip(i) << std::endl;
      std::cout << "  Port: " << append_stripe_data_placement->datanodeport(i) << std::endl;
    }

    // Print block info
    std::cout << "\n=== Block Info ===" << std::endl;
    for (int i = 0; i < append_stripe_data_placement->blockkeys_size(); i++)
    {
      std::cout << "Block " << i << ":" << std::endl;
      std::cout << "  Key: " << append_stripe_data_placement->blockkeys(i) << std::endl;
      std::cout << "  ID: " << append_stripe_data_placement->blockids(i) << std::endl;
      std::cout << "  Offset: " << append_stripe_data_placement->offsets(i) << std::endl;
      std::cout << "  Size: " << append_stripe_data_placement->sizes(i) << std::endl;
    }
    std::cout << "===================================" << std::endl;
  }

  grpc::Status ProxyImpl::scheduleAppend2Datanode(
      grpc::ServerContext *context,
      const proxy_proto::AppendStripeDataPlacement *append_stripe_data_placement,
      proxy_proto::SetReply *response)
  {
    // printAppendStripeDataPlacement(append_stripe_data_placement);

    int stripe_id = append_stripe_data_placement->stripe_id();
    // sum of all append slices allocated to this proxy
    size_t cluster_append_size = append_stripe_data_placement->append_size();
    // number of slices allocated to this proxy
    int slice_num = append_stripe_data_placement->blockkeys_size();
    bool is_serialized = append_stripe_data_placement->is_serialized();

    struct RackCuHomeStagingCommit
    {
      bool used = false;
      std::string staging_key;
      uint64_t blob_bytes = 0;
      std::string dn_ip;
      int dn_port = 0;
    };
    auto placement_copy = std::make_shared<proxy_proto::AppendStripeDataPlacement>(*append_stripe_data_placement);
    auto rackcu_home_delta_out = std::make_shared<std::string>();
    auto rackcu_home_staging = std::make_shared<RackCuHomeStagingCommit>();

    const bool rack_cu_keyed = append_stripe_data_placement->rack_cu_xfer_plan_id() != 0u;
    if (rack_cu_keyed)
    {
      std::lock_guard<std::mutex> lk(m_rackcu_pending_appends_mu);
      m_rackcu_pending_appends[placement_copy->key()] = placement_copy;
    }

    auto append_and_save = [this, rack_cu_keyed, stripe_id, cluster_append_size, slice_num, placement_copy, is_serialized, rackcu_home_delta_out, rackcu_home_staging]() mutable
    {
      try
      {
        asio::ip::tcp::socket socket_data(io_context);
        asio::error_code error;
        std::string append_peer_ip;
        std::vector<char> append_buf;

        {
          std::lock_guard<std::mutex> accept_io_lk(m_rackcu_accept_io_mu);
          acceptor.accept(socket_data);
          try
          {
            append_peer_ip = socket_data.remote_endpoint().address().to_string();
          }
          catch (...)
          {
          }

          if (rack_cu_keyed)
          {
            const std::string wire_key = rackcu_tcp_read_append_key(socket_data, error);
            if (error)
            {
              throw asio::system_error(error);
            }
            if (wire_key.empty())
            {
              throw std::runtime_error("RACKCU: empty append key on wire");
            }
            {
              std::lock_guard<std::mutex> lk(m_rackcu_pending_appends_mu);
              const auto pit = m_rackcu_pending_appends.find(wire_key);
              if (pit == m_rackcu_pending_appends.end())
              {
                throw std::runtime_error("RACKCU: append_key not scheduled on proxy: " + wire_key);
              }
              placement_copy = pit->second;
              m_rackcu_pending_appends.erase(pit);
            }
            cluster_append_size = static_cast<size_t>(placement_copy->append_size());
            slice_num = placement_copy->blockkeys_size();
            is_serialized = placement_copy->is_serialized();
            stripe_id = placement_copy->stripe_id();
          }

          append_buf.assign(cluster_append_size, 0);
          if (cluster_append_size > 0)
          {
            apply_kernel_bandwidth_to_peer_socket(socket_data, append_peer_ip.c_str());
            tcp_read_all_bytes(socket_data, append_buf.data(), cluster_append_size, error);
            if (error == asio::error::eof)
            {
              std::cout << "error == asio::error::eof" << std::endl;
            }
            else if (error)
            {
              throw asio::system_error(error);
            }
          }

          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][Append339]"
                      << "Append to Stripe " << stripe_id << " with length of " << cluster_append_size << std::endl;
          }

          asio::error_code ignore_ec;
          socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
          socket_data.close(ignore_ec);
        }

        const uint64_t rack_cu_xfer_plan_id_cap = placement_copy->rack_cu_xfer_plan_id();
        const auto rack_cu_xfer_t0_steady = std::chrono::steady_clock::now();
        const int64_t rack_cu_xfer_w0_wall = rackcu_wall_unix_ms_now();

        const std::string &am0 = placement_copy->append_mode();
        const bool rackcu_parity_staging =
            am0 == "RACKCU_GLOBAL_FROM_DATA_HOME_DELTA_STAGING" ||
            am0 == "RACKCU_PARITY_GLOBAL_BY_COLLECTOR_HOME_DELTA_STAGING_BATCH" ||
            am0 == "RACKCU_LOCAL_FROM_DATA_HOME_DELTA_STAGING";

        std::vector<char *> slices;
        if (!rackcu_parity_staging)
        {
          slices = m_toolbox->splitCharPointer(append_buf.data(), placement_copy);
        }

        // RackCU DATA_HOME：写回 new 之前在本 cluster 读盘 old，计算 ΔD=new⊕old，序列化后经 coordinator
        // 交给 client；校验步仅带 delta，不再读数据块旧值。
        if (placement_copy->append_mode() == "RACKCU_DATA_HOME_XOR_FIRST")
        {
          const int k_bs = static_cast<int>(m_sys_config->k);
          const int bsz = static_cast<int>(m_sys_config->BlockSize);
          std::map<int, std::vector<unsigned char>> old_blk_cache;
          auto load_old_home = [&](int j) -> std::vector<unsigned char> & {
            const int bid = placement_copy->blockids(j);
            auto it = old_blk_cache.find(bid);
            if (it != old_blk_cache.end())
            {
              return it->second;
            }
            std::vector<unsigned char> ov(static_cast<size_t>(bsz), 0);
            const bool ok = GetFromDatanode(
                placement_copy->blockkeys(j),
                reinterpret_cast<char *>(ov.data()),
                static_cast<size_t>(bsz),
                placement_copy->datanodeip(j).c_str(),
                placement_copy->datanodeport(j));
            if (!ok)
            {
              throw std::runtime_error("RACKCU_DATA_HOME_XOR_FIRST: failed to read old data block before home write");
            }
            auto ins = old_blk_cache.emplace(bid, std::move(ov));
            return ins.first->second;
          };
          constexpr int k_preview = 16;
          constexpr uint32_t k_rackcu_delta_magic = 0x52434448u;
          std::vector<char> payload;
          auto append_u32 = [](std::vector<char> &b, uint32_t v) {
            b.push_back(static_cast<char>(v & 0xffu));
            b.push_back(static_cast<char>((v >> 8) & 0xffu));
            b.push_back(static_cast<char>((v >> 16) & 0xffu));
            b.push_back(static_cast<char>((v >> 24) & 0xffu));
          };
          auto append_i32 = [&append_u32](std::vector<char> &b, int32_t v) {
            append_u32(b, static_cast<uint32_t>(v));
          };
          uint32_t nseg = 0;
          for (int j = 0; j < slice_num; j++)
          {
            const int bid = placement_copy->blockids(j);
            if (bid < 0 || bid >= k_bs)
            {
              continue;
            }
            const int off = static_cast<int>(placement_copy->offsets(j));
            const int len = static_cast<int>(placement_copy->sizes(j));
            if (len <= 0 || off < 0 || off + len > bsz)
            {
              throw std::runtime_error("RACKCU_DATA_HOME_XOR_FIRST: invalid data slice bounds");
            }
            std::vector<unsigned char> &oldv = load_old_home(j);
            int nz = 0;
            const int pv = std::min(k_preview, len);
            std::cout << "[Proxy" << m_self_cluster_id << "][RACKCU][HOME] pre_write stripe=" << stripe_id
                      << " block_id=" << bid << " range=[" << off << "," << (off + len) << ") ";
            std::cout << "old_fp" << pv << ":";
            for (int t = 0; t < pv; t++)
            {
              std::cout << ' ' << std::hex << std::setfill('0') << std::setw(2)
                        << static_cast<unsigned>(oldv[static_cast<size_t>(off + t)]);
            }
            std::cout << " new_fp" << pv << ":";
            for (int t = 0; t < pv; t++)
            {
              std::cout << ' ' << std::hex << std::setfill('0') << std::setw(2)
                        << static_cast<unsigned>(static_cast<unsigned char>(slices[static_cast<size_t>(j)][t]));
            }
            std::cout << " delta_fp" << pv << ":";
            append_i32(payload, static_cast<int32_t>(bid));
            append_i32(payload, static_cast<int32_t>(off));
            append_i32(payload, static_cast<int32_t>(len));
            for (int t = 0; t < len; t++)
            {
              const unsigned char d = static_cast<unsigned char>(
                  static_cast<unsigned char>(slices[static_cast<size_t>(j)][t]) ^
                  oldv[static_cast<size_t>(off + t)]);
              if (t < pv)
              {
                std::cout << ' ' << std::hex << std::setfill('0') << std::setw(2) << static_cast<unsigned>(d);
              }
              if (d != 0)
              {
                nz++;
              }
              payload.push_back(static_cast<char>(d));
            }
            std::cout << std::dec << " nonzero_delta_bytes=" << nz << "/" << len << std::endl;
            nseg++;
          }
          std::vector<char> full;
          append_u32(full, k_rackcu_delta_magic);
          append_u32(full, nseg);
          full.insert(full.end(), payload.begin(), payload.end());
          const std::string staging_key = placement_copy->key() + ".rackcu_home_delta";
          if (slice_num <= 0)
          {
            throw std::runtime_error("RACKCU_DATA_HOME_XOR_FIRST: empty placement");
          }
          SetToDatanode(
              staging_key.c_str(),
              staging_key.size(),
              full.data(),
              full.size(),
              placement_copy->datanodeip(0).c_str(),
              placement_copy->datanodeport(0),
              2);
          rackcu_home_staging->used = true;
          rackcu_home_staging->staging_key = staging_key;
          rackcu_home_staging->blob_bytes = static_cast<uint64_t>(full.size());
          rackcu_home_staging->dn_ip = placement_copy->datanodeip(0);
          rackcu_home_staging->dn_port = placement_copy->datanodeport(0);
          rackcu_home_delta_out->clear();
        }

        if (placement_copy->append_mode() == "RACKCU_GLOBAL_FROM_DATA_HOME_DELTA" ||
            placement_copy->append_mode() == "RACKCU_GLOBAL_FROM_DATA_HOME_DELTA_STAGING" ||
            placement_copy->append_mode() == "RACKCU_PARITY_GLOBAL_BY_COLLECTOR_HOME_DELTA" ||
            placement_copy->append_mode() == "RACKCU_PARITY_GLOBAL_BY_COLLECTOR_HOME_DELTA_STAGING_BATCH" ||
            placement_copy->append_mode() == "RACKCU_LOCAL_FROM_DATA_HOME_DELTA" ||
            placement_copy->append_mode() == "RACKCU_LOCAL_FROM_DATA_HOME_DELTA_STAGING")
        {
          const int k = m_sys_config->k;
          const int r = m_sys_config->r;
          const int z = m_sys_config->z;
          const int block_size = static_cast<int>(m_sys_config->BlockSize);
          const bool rackcu_global_collector_batch =
              placement_copy->append_mode() == "RACKCU_PARITY_GLOBAL_BY_COLLECTOR_HOME_DELTA_STAGING_BATCH";
          const int slice_num_local = placement_copy->blockids_size();
          if (rackcu_global_collector_batch)
          {
            if (slice_num_local < 1)
            {
              throw std::runtime_error("RACKCU_GLOBAL_COLLECTOR_BATCH: invalid slice num");
            }
            if (placement_copy->rackcu_global_parity_batch_size() < 1)
            {
              throw std::runtime_error("RACKCU_GLOBAL_COLLECTOR_BATCH: empty parity batch");
            }
          }
          else if (slice_num_local < 2)
          {
            throw std::runtime_error("RACKCU_GLOBAL_FROM_DATA: invalid slice num");
          }

          int tail_idx;
          int parity_block_id = -1;
          int gidx = 0;
          int poff = 0;
          int plen = 0;
          if (!rackcu_global_collector_batch)
          {
            tail_idx = slice_num_local - 1;
            parity_block_id = placement_copy->blockids(tail_idx);
            gidx = parity_block_id - k;
            poff = static_cast<int>(placement_copy->offsets(tail_idx));
            plen = static_cast<int>(placement_copy->sizes(tail_idx));
            if (poff < 0 || plen < 0 || poff + plen > block_size)
            {
              throw std::runtime_error("RACKCU_GLOBAL_FROM_DATA: invalid parity tail range");
            }
          }
          else
          {
            tail_idx = slice_num_local;
          }

          // RackCU：*_HOME_DELTA 模式下 TCP 载荷已是各 home 算好的 ΔD；*_STAGING 从各 holder 拉取同格式 blob。
          // 否则 ΔD = D_new XOR D_old（在 proxy 上读旧块）。
          const bool from_home_delta_tcp =
              (placement_copy->append_mode() == "RACKCU_GLOBAL_FROM_DATA_HOME_DELTA" ||
               placement_copy->append_mode() == "RACKCU_PARITY_GLOBAL_BY_COLLECTOR_HOME_DELTA" ||
               placement_copy->append_mode() == "RACKCU_LOCAL_FROM_DATA_HOME_DELTA");
          const bool from_precomputed_home_delta = from_home_delta_tcp || rackcu_parity_staging;
          std::vector<char *> wire_slices = slices;
          std::vector<std::vector<char>> staging_wire_storage;
          if (rackcu_parity_staging)
          {
            if (placement_copy->rackcu_home_delta_staging_refs_size() <= 0)
            {
              throw std::runtime_error("RACKCU: missing home delta staging refs");
            }
            std::vector<std::string> blobs;
            blobs.reserve(static_cast<size_t>(placement_copy->rackcu_home_delta_staging_refs_size()));
            for (int si = 0; si < placement_copy->rackcu_home_delta_staging_refs_size(); si++)
            {
              std::string one;
              if (!fetch_rack_cu_home_staging_blob(placement_copy->rackcu_home_delta_staging_refs(si), &one, placement_copy->stripe_id(),
                                              placement_copy->rack_cu_xfer_plan_id()))
              {
                throw std::runtime_error("RACKCU: failed to fetch home delta staging blob");
              }
              blobs.push_back(std::move(one));
            }
            std::vector<std::vector<unsigned char>> merged_rows;
            if (!merge_rackcu_home_delta_blobs(&merged_rows, k, block_size, blobs))
            {
              throw std::runtime_error("RACKCU: failed to parse/merge home delta staging blobs");
            }
            staging_wire_storage.resize(static_cast<size_t>(tail_idx));
            wire_slices.resize(static_cast<size_t>(tail_idx));
            for (int j = 0; j < tail_idx; j++)
            {
              const int bid = placement_copy->blockids(j);
              const int off = static_cast<int>(placement_copy->offsets(j));
              const int len = static_cast<int>(placement_copy->sizes(j));
              staging_wire_storage[static_cast<size_t>(j)].assign(static_cast<size_t>(len), '\0');
              std::memcpy(staging_wire_storage[static_cast<size_t>(j)].data(),
                            merged_rows[static_cast<size_t>(bid)].data() + static_cast<size_t>(off),
                            static_cast<size_t>(len));
              wire_slices[static_cast<size_t>(j)] = staging_wire_storage[static_cast<size_t>(j)].data();
            }
          }
          std::map<int, std::vector<unsigned char>> old_data_cache;
          auto load_old_block = [&](int j) -> std::vector<unsigned char> &
          {
            const int bid = placement_copy->blockids(j);
            auto it = old_data_cache.find(bid);
            if (it != old_data_cache.end())
            {
              return it->second;
            }
            std::vector<unsigned char> old_block(static_cast<size_t>(block_size), 0);
            const bool ok = GetFromDatanode(
                placement_copy->blockkeys(j),
                reinterpret_cast<char *>(old_block.data()),
                static_cast<size_t>(block_size),
                placement_copy->datanodeip(j).c_str(),
                placement_copy->datanodeport(j));
            if (!ok)
            {
              throw std::runtime_error("RACKCU: failed to read old data block from datanode");
            }
            auto inserted = old_data_cache.emplace(bid, std::move(old_block));
            return inserted.first->second;
          };

          std::vector<std::vector<unsigned char>> delta_slices(static_cast<size_t>(tail_idx));
          for (int j = 0; j < tail_idx; j++)
          {
            const int bid = placement_copy->blockids(j);
            if (bid < 0 || bid >= k)
            {
              throw std::runtime_error("RACKCU: non-data block appears before parity tail");
            }
            const int off = static_cast<int>(placement_copy->offsets(j));
            const int len = static_cast<int>(placement_copy->sizes(j));
            if (off < 0 || len < 0 || off + len > block_size)
            {
              throw std::runtime_error("RACKCU: invalid data slice range");
            }
            std::vector<unsigned char> delta(static_cast<size_t>(len), 0);
            if (from_precomputed_home_delta)
            {
              std::memcpy(delta.data(), wire_slices[static_cast<size_t>(j)], static_cast<size_t>(len));
            }
            else
            {
              std::vector<unsigned char> &old_block = load_old_block(j);
              for (int t = 0; t < len; t++)
              {
                const unsigned char new_v = static_cast<unsigned char>(wire_slices[static_cast<size_t>(j)][t]);
                const unsigned char old_v = old_block[static_cast<size_t>(off + t)];
                delta[static_cast<size_t>(t)] = static_cast<unsigned char>(new_v ^ old_v);
              }
            }
            delta_slices[static_cast<size_t>(j)] = std::move(delta);
            if (IF_DEBUG)
            {
              std::cout << "[Proxy" << m_self_cluster_id << "][RACKCU][DeltaTag] stripe=" << stripe_id
                        << " block=" << bid << " offset=" << off << " size=" << len
                        << (from_precomputed_home_delta ? " (home_delta_precomputed)" : "") << std::endl;
            }
          }

          std::vector<std::vector<unsigned char>> parity_rows(static_cast<size_t>(r + z), std::vector<unsigned char>(static_cast<size_t>(block_size), 0));
          if (placement_copy->append_mode() == "RACKCU_LOCAL_FROM_DATA" ||
              placement_copy->append_mode() == "RACKCU_LOCAL_FROM_DATA_HOME_DELTA" ||
              placement_copy->append_mode() == "RACKCU_LOCAL_FROM_DATA_HOME_DELTA_STAGING")
          {
            if (parity_block_id < k + r || parity_block_id >= k + r + z)
            {
              throw std::runtime_error("RACKCU_LOCAL_FROM_DATA: invalid local parity block id");
            }
            // Build sparse k-row delta matrix with original block IDs preserved.
            // Do not collapse touched rows to [0..n), otherwise block-id positions
            // are lost and local parity row can be computed incorrectly.
            std::vector<std::vector<unsigned char>> delta_rows(static_cast<size_t>(k), std::vector<unsigned char>(static_cast<size_t>(block_size), 0));
            for (int j = 0; j < tail_idx; j++)
            {
              const int bid = placement_copy->blockids(j);
              if (bid < 0 || bid >= k)
              {
                throw std::runtime_error("RACKCU_LOCAL_FROM_DATA: non-data block appears before parity tail");
              }
              const int off = static_cast<int>(placement_copy->offsets(j));
              const int len = static_cast<int>(placement_copy->sizes(j));
              if (off < 0 || len < 0 || off + len > block_size)
              {
                throw std::runtime_error("RACKCU_LOCAL_FROM_DATA: invalid data slice range");
              }
              std::memcpy(delta_rows[static_cast<size_t>(bid)].data() + off, delta_slices[static_cast<size_t>(j)].data(), static_cast<size_t>(len));
            }

            std::vector<unsigned char *> data_ptrs;
            data_ptrs.reserve(static_cast<size_t>(k));
            for (int bid = 0; bid < k; bid++)
            {
              data_ptrs.push_back(delta_rows[static_cast<size_t>(bid)].data());
            }
            std::vector<unsigned char *> parity_ptrs;
            parity_ptrs.reserve(static_cast<size_t>(r + z));
            for (int pid = 0; pid < r + z; pid++)
            {
              parity_ptrs.push_back(parity_rows[static_cast<size_t>(pid)].data());
            }
            ECProject::encode_azure_lrc(k, r, z, data_ptrs.data(), parity_ptrs.data(), block_size);
          }
          else
          {
            if (!rackcu_global_collector_batch)
            {
              if (parity_block_id < k || parity_block_id >= k + r)
              {
                throw std::runtime_error("RACKCU_GLOBAL_FROM_DATA: invalid global parity block id");
              }
            }
            // build k-row sparse delta data blocks, then full azure encode to get parity deltas
            std::vector<std::vector<unsigned char>> delta_rows(static_cast<size_t>(k), std::vector<unsigned char>(static_cast<size_t>(block_size), 0));
            for (int j = 0; j < tail_idx; j++)
            {
              const int bid = placement_copy->blockids(j);
              if (bid < 0 || bid >= k)
              {
                throw std::runtime_error("RACKCU_GLOBAL_FROM_DATA: non-data block appears before parity tail");
              }
              const int off = static_cast<int>(placement_copy->offsets(j));
              const int len = static_cast<int>(placement_copy->sizes(j));
              if (off < 0 || len < 0 || off + len > block_size)
              {
                throw std::runtime_error("RACKCU_GLOBAL_FROM_DATA: invalid data slice range");
              }
              std::memcpy(delta_rows[static_cast<size_t>(bid)].data() + off, delta_slices[static_cast<size_t>(j)].data(), static_cast<size_t>(len));
            }
            std::vector<unsigned char *> data_ptrs;
            data_ptrs.reserve(static_cast<size_t>(k));
            for (int bid = 0; bid < k; bid++)
            {
              data_ptrs.push_back(delta_rows[static_cast<size_t>(bid)].data());
            }
            std::vector<unsigned char *> parity_ptrs;
            parity_ptrs.reserve(static_cast<size_t>(r + z));
            for (int pid = 0; pid < r + z; pid++)
            {
              parity_ptrs.push_back(parity_rows[static_cast<size_t>(pid)].data());
            }
            ECProject::encode_azure_lrc(k, r, z, data_ptrs.data(), parity_ptrs.data(), block_size);
          }

          if (rackcu_global_collector_batch)
          {
            for (int bi = 0; bi < placement_copy->rackcu_global_parity_batch_size(); bi++)
            {
              const auto &pt = placement_copy->rackcu_global_parity_batch(bi);
              const int parity_block_id_b = pt.parity_block_id();
              const int gidx_b = parity_block_id_b - k;
              if (parity_block_id_b < k || parity_block_id_b >= k + r)
              {
                throw std::runtime_error("RACKCU_GLOBAL_COLLECTOR_BATCH: invalid global parity block id");
              }
              const int poff_b = static_cast<int>(pt.offset());
              const int plen_b = static_cast<int>(pt.size());
              if (poff_b < 0 || plen_b < 0 || poff_b + plen_b > block_size)
              {
                throw std::runtime_error("RACKCU_GLOBAL_COLLECTOR_BATCH: invalid parity slice range");
              }
              const unsigned char *delta_at_poff_b = parity_rows[static_cast<size_t>(gidx_b)].data() + static_cast<size_t>(poff_b);
              log_rackcu_parity_range_hex(
                  m_self_cluster_id, stripe_id, "xor_delta_encoded", parity_block_id_b,
                  poff_b, plen_b, delta_at_poff_b, plen_b);

              bool forwarded = false;
              if (!pt.target_proxy_ip().empty() && pt.target_proxy_port() > 0)
              {
                proxy_proto::AppendStripeDataPlacement forward_plan;
                forward_plan.set_key(placement_copy->key());
                forward_plan.set_cluster_id(placement_copy->cluster_id());
                forward_plan.set_stripe_id(stripe_id);
                forward_plan.set_append_size(static_cast<uint64_t>(plen_b));
                forward_plan.add_datanodeip(pt.datanode_ip());
                forward_plan.add_datanodeport(pt.datanode_port());
                forward_plan.add_blockkeys(pt.blockkey());
                forward_plan.add_blockids(parity_block_id_b);
                forward_plan.add_offsets(static_cast<uint64_t>(poff_b));
                forward_plan.add_sizes(static_cast<uint64_t>(plen_b));
                forward_plan.set_is_merge_parity(true);
                forward_plan.set_append_mode("RACKCU_PARITY_APPLY_ONLY");
                forward_plan.set_is_serialized(true);
                forward_plan.set_rack_cu_xfer_plan_id(placement_copy->rack_cu_xfer_plan_id());

                const std::string rpc_target = pt.target_proxy_ip() + ":" + std::to_string(pt.target_proxy_port());
                auto target_stub = proxy_proto::proxyService::NewStub(
                    grpc::CreateChannel(rpc_target, grpc::InsecureChannelCredentials()));
                grpc::ClientContext fctx;
                proxy_proto::SetReply freply;
                grpc::Status st = target_stub->scheduleAppend2Datanode(&fctx, forward_plan, &freply);
                if (st.ok())
                {
                  asio::io_context fwd_io;
                  asio::error_code ec;
                  asio::ip::tcp::resolver resolver(fwd_io);
                  const int target_data_port = pt.target_proxy_port() + ECProject::PROXY_PORT_SHIFT;
                  auto endpoints = resolver.resolve(pt.target_proxy_ip(), std::to_string(target_data_port), ec);
                  if (!ec)
                  {
                    asio::ip::tcp::socket sock(fwd_io);
                    asio::connect(sock, endpoints, ec);
                    if (!ec)
                    {
                      apply_kernel_bandwidth_to_peer_socket(sock, pt.target_proxy_ip().c_str());
                      rackcu_tcp_write_keyed_frame(sock, forward_plan.key(),
                                                   reinterpret_cast<const char *>(delta_at_poff_b),
                                                   static_cast<size_t>(plen_b), ec);
                      if (!ec)
                      {
                        asio::error_code ignore_ec;
                        sock.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
                        sock.close(ignore_ec);
                        forwarded = true;
                      }
                    }
                  }
                }
                if (!forwarded && IF_DEBUG)
                {
                  std::cout << "[Proxy" << m_self_cluster_id << "][RACKCU][ForwardBatch] failed target="
                            << rpc_target << std::endl;
                }
              }
              if (!forwarded)
              {
                AppendToDatanode(pt.blockkey().c_str(),
                                 parity_block_id_b,
                                 static_cast<size_t>(plen_b),
                                 reinterpret_cast<const char *>(delta_at_poff_b),
                                 poff_b,
                                 pt.datanode_ip().c_str(),
                                 pt.datanode_port(),
                                 true);
                MergeParityOnDatanode(pt.blockkey().c_str(),
                                      parity_block_id_b,
                                      pt.datanode_ip().c_str(),
                                      pt.datanode_port(),
                                      "UNILRC_MODE");
              }
            }

            coordinator_proto::CommitAbortKey commit_abort_key;
            coordinator_proto::ReplyFromCoordinator result;
            grpc::ClientContext context2;
            ECProject::OpperateType opp = APPEND;
            commit_abort_key.set_opp(opp);
            commit_abort_key.set_key(placement_copy->key());
            commit_abort_key.set_stripe_id(stripe_id);
            commit_abort_key.set_ifcommitmetadata(true);
            if (rackcu_home_delta_out != nullptr && !rackcu_home_delta_out->empty())
            {
              commit_abort_key.set_rackcu_home_delta_blob(*rackcu_home_delta_out);
            }
            grpc::Status status = m_coordinator_ptr->reportCommitAbort(&context2, commit_abort_key, &result);
            if (!status.ok() && IF_DEBUG)
            {
              std::cout << "[Proxy][RACKCU] report commit failed!" << std::endl;
            }
            return;
          }

          const unsigned char *delta_at_poff = parity_rows[static_cast<size_t>(gidx)].data() + static_cast<size_t>(poff);
          log_rackcu_parity_range_hex(
              m_self_cluster_id, stripe_id, "xor_delta_encoded", parity_block_id,
              poff, plen, delta_at_poff, plen);
          auto apply_delta_locally = [&]() {
            AppendToDatanode(placement_copy->blockkeys(tail_idx).c_str(),
                             parity_block_id,
                             static_cast<size_t>(plen),
                             reinterpret_cast<const char *>(delta_at_poff),
                             poff,
                             placement_copy->datanodeip(tail_idx).c_str(),
                             placement_copy->datanodeport(tail_idx),
                             true);
            MergeParityOnDatanode(placement_copy->blockkeys(tail_idx).c_str(),
                                  parity_block_id,
                                  placement_copy->datanodeip(tail_idx).c_str(),
                                  placement_copy->datanodeport(tail_idx),
                                  "UNILRC_MODE");
          };
          auto forward_delta_to_target_proxy = [&]() -> bool {
            if (placement_copy->target_proxy_ip().empty() || placement_copy->target_proxy_port() <= 0)
            {
              return false;
            }
            proxy_proto::AppendStripeDataPlacement forward_plan;
            forward_plan.set_key(placement_copy->key());
            forward_plan.set_cluster_id(placement_copy->cluster_id());
            forward_plan.set_stripe_id(stripe_id);
            forward_plan.set_append_size(static_cast<uint64_t>(plen));
            forward_plan.add_datanodeip(placement_copy->datanodeip(tail_idx));
            forward_plan.add_datanodeport(placement_copy->datanodeport(tail_idx));
            forward_plan.add_blockkeys(placement_copy->blockkeys(tail_idx));
            forward_plan.add_blockids(parity_block_id);
            forward_plan.add_offsets(static_cast<uint64_t>(poff));
            forward_plan.add_sizes(static_cast<uint64_t>(plen));
            forward_plan.set_is_merge_parity(true);
            forward_plan.set_append_mode("RACKCU_PARITY_APPLY_ONLY");
            forward_plan.set_is_serialized(true);
            forward_plan.set_rack_cu_xfer_plan_id(placement_copy->rack_cu_xfer_plan_id());

            const std::string rpc_target = placement_copy->target_proxy_ip() + ":" + std::to_string(placement_copy->target_proxy_port());
            auto target_stub = proxy_proto::proxyService::NewStub(
                grpc::CreateChannel(rpc_target, grpc::InsecureChannelCredentials()));
            grpc::ClientContext fctx;
            proxy_proto::SetReply freply;
            grpc::Status st = target_stub->scheduleAppend2Datanode(&fctx, forward_plan, &freply);
            if (!st.ok())
            {
              std::cout << "[Proxy" << m_self_cluster_id << "][RACKCU][Forward] schedule failed to "
                        << rpc_target << " err=" << st.error_message() << std::endl;
              return false;
            }

            asio::io_context fwd_io;
            asio::error_code ec;
            asio::ip::tcp::resolver resolver(fwd_io);
            const int target_data_port = placement_copy->target_proxy_port() + ECProject::PROXY_PORT_SHIFT;
            auto endpoints = resolver.resolve(placement_copy->target_proxy_ip(), std::to_string(target_data_port), ec);
            if (ec)
            {
              std::cout << "[Proxy" << m_self_cluster_id << "][RACKCU][Forward] resolve failed ip="
                        << placement_copy->target_proxy_ip() << " port=" << target_data_port
                        << " err=" << ec.message() << std::endl;
              return false;
            }
            asio::ip::tcp::socket sock(fwd_io);
            asio::connect(sock, endpoints, ec);
            if (ec)
            {
              std::cout << "[Proxy" << m_self_cluster_id << "][RACKCU][Forward] connect failed ip="
                        << placement_copy->target_proxy_ip() << " port=" << target_data_port
                        << " err=" << ec.message() << std::endl;
              return false;
            }
            apply_kernel_bandwidth_to_peer_socket(sock, placement_copy->target_proxy_ip().c_str());
            rackcu_tcp_write_keyed_frame(sock, forward_plan.key(), reinterpret_cast<const char *>(delta_at_poff),
                                       static_cast<size_t>(plen), ec);
            if (ec)
            {
              std::cout << "[Proxy" << m_self_cluster_id << "][RACKCU][Forward] send failed ip="
                        << placement_copy->target_proxy_ip() << " port=" << target_data_port
                        << " err=" << ec.message() << std::endl;
              return false;
            }
            asio::error_code ignore_ec;
            sock.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
            sock.close(ignore_ec);
            return true;
          };

          if (!forward_delta_to_target_proxy())
          {
            apply_delta_locally();
          }

          coordinator_proto::CommitAbortKey commit_abort_key;
          coordinator_proto::ReplyFromCoordinator result;
          grpc::ClientContext context2;
          ECProject::OpperateType opp = APPEND;
          commit_abort_key.set_opp(opp);
          commit_abort_key.set_key(placement_copy->key());
          commit_abort_key.set_stripe_id(stripe_id);
          commit_abort_key.set_ifcommitmetadata(true);
          if (rackcu_home_delta_out != nullptr && !rackcu_home_delta_out->empty())
          {
            commit_abort_key.set_rackcu_home_delta_blob(*rackcu_home_delta_out);
          }
          grpc::Status status = m_coordinator_ptr->reportCommitAbort(&context2, commit_abort_key, &result);
          if (!status.ok() && IF_DEBUG)
          {
            std::cout << "[Proxy][RACKCU] report commit failed!" << std::endl;
          }
          return;
        }

        if (placement_copy->append_mode() == "RACKCU_PARITY_APPLY_ONLY")
        {
          auto append_to_datanode = [this](const char *block_key, int block_id, size_t slice_size, const char *slice_buf, int slice_offset, const char *ip, int port, bool is_serialized)
          {
            AppendToDatanode(block_key, block_id, slice_size, slice_buf, slice_offset, ip, port, is_serialized);
          };
          std::vector<std::thread> senders;
          for (int j = 0; j < slice_num; j++)
          {
            senders.push_back(std::thread(append_to_datanode, placement_copy->blockkeys(j).c_str(), placement_copy->blockids(j), placement_copy->sizes(j), slices[j], placement_copy->offsets(j), placement_copy->datanodeip(j).c_str(), placement_copy->datanodeport(j), true));
          }
          for (int j = 0; j < int(senders.size()); j++)
          {
            senders[j].join();
          }
          for (int j = 0; j < slice_num; j++)
          {
            if (placement_copy->blockids(j) >= m_sys_config->k)
            {
              MergeParityOnDatanode(placement_copy->blockkeys(j).c_str(), placement_copy->blockids(j), placement_copy->datanodeip(j).c_str(), placement_copy->datanodeport(j), "UNILRC_MODE");
            }
          }
          return;
        }

        auto append_to_datanode = [this](const char *block_key, int block_id, size_t slice_size, const char *slice_buf, int slice_offset, const char *ip, int port, bool is_serialized)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][Append353]"
                      << "Append to Block " << block_key << " of block_id " << block_id << " at the offset of " << slice_offset << " with length of " << slice_size << std::endl;
          }
          AppendToDatanode(block_key, block_id, slice_size, slice_buf, slice_offset, ip, port, is_serialized);
        };

        std::vector<std::thread> senders;
        for (int j = 0; j < slice_num; j++)
        {
          senders.push_back(std::thread(append_to_datanode, placement_copy->blockkeys(j).c_str(), placement_copy->blockids(j), placement_copy->sizes(j), slices[j], placement_copy->offsets(j), placement_copy->datanodeip(j).c_str(), placement_copy->datanodeport(j), is_serialized));
        }
        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }

        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Append371]"
                    << "Finish appending to Stripe " << stripe_id << std::endl;
        }

        if (placement_copy->is_merge_parity())
        {
          for (int j = 0; j < slice_num; j++)
          {
            if (placement_copy->blockids(j) >= m_sys_config->k)
            {
              MergeParityOnDatanode(placement_copy->blockkeys(j).c_str(), placement_copy->blockids(j), placement_copy->datanodeip(j).c_str(), placement_copy->datanodeport(j), placement_copy->append_mode());
            }
          }

          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][Append387]"
                      << "Async merging parities of Stripe " << stripe_id << std::endl;
          }
        }

        // report to coordinator
        coordinator_proto::CommitAbortKey commit_abort_key;
        coordinator_proto::ReplyFromCoordinator result;
        grpc::ClientContext context;
        ECProject::OpperateType opp = APPEND;
        commit_abort_key.set_opp(opp);
        commit_abort_key.set_key(placement_copy->key());
        commit_abort_key.set_stripe_id(stripe_id);
        commit_abort_key.set_ifcommitmetadata(true);
        if (rackcu_home_staging->used)
        {
          commit_abort_key.set_rackcu_home_staging_key(rackcu_home_staging->staging_key);
          commit_abort_key.set_rackcu_home_staging_bytes(rackcu_home_staging->blob_bytes);
          commit_abort_key.set_rackcu_home_staging_dn_ip(rackcu_home_staging->dn_ip);
          commit_abort_key.set_rackcu_home_staging_dn_port(rackcu_home_staging->dn_port);
        }
        else if (rackcu_home_delta_out != nullptr && !rackcu_home_delta_out->empty())
        {
          commit_abort_key.set_rackcu_home_delta_blob(*rackcu_home_delta_out);
        }
        grpc::Status status;
        status = m_coordinator_ptr->reportCommitAbort(&context, commit_abort_key, &result);
        if (status.ok() && IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][APPEND405]"
                    << " report to coordinator success" << std::endl;
        }
        else if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][APPEND410]"
                    << " report to coordinator fail!" << std::endl;
        }
        if (rack_cu_xfer_plan_id_cap != 0u)
        {
          const auto rack_cu_xfer_t1_steady = std::chrono::steady_clock::now();
          const int64_t rack_cu_xfer_w1_wall = rackcu_wall_unix_ms_now();
          rackcu_batch_xfer_add(stripe_id, rack_cu_xfer_plan_id_cap,
                                std::chrono::duration<double>(rack_cu_xfer_t1_steady - rack_cu_xfer_t0_steady).count(),
                                rack_cu_xfer_w0_wall, rack_cu_xfer_w1_wall);
        }
      }
      catch (std::exception &e)
      {
        std::cout << "exception in append_and_save" << std::endl;
        std::cout << e.what() << std::endl;
      }
    };
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy][APPEND424] Handle append_and_save" << std::endl;
      }
      std::thread my_thread(append_and_save);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::encodeAndSetObject(
      grpc::ServerContext *context,
      const proxy_proto::ObjectAndPlacement *object_and_placement,
      proxy_proto::SetReply *response)
  {
    std::string key = object_and_placement->key();
    int value_size_bytes = object_and_placement->valuesizebyte();
    int k = object_and_placement->k();
    int g_m = object_and_placement->g_m();
    int l = object_and_placement->l();
    // int stripe_id = object_and_placement->stripe_id();
    int block_size = object_and_placement->block_size();
    ECProject::EncodeType encode_type = (ECProject::EncodeType)object_and_placement->encode_type();
    std::vector<std::pair<std::string, std::pair<std::string, int>>> keys_nodes;
    for (int i = 0; i < object_and_placement->datanodeip_size(); i++)
    {
      keys_nodes.push_back(std::make_pair(object_and_placement->blockkeys(i), std::make_pair(object_and_placement->datanodeip(i), object_and_placement->datanodeport(i))));
    }
    auto encode_and_save = [this, key, value_size_bytes, k, g_m, l, block_size, keys_nodes, encode_type]() mutable
    {
      try
      {
        // read the key and value in the socket sent by client
        // initialize the socket of reading key and value
        asio::ip::tcp::socket socket_data(io_context);
        acceptor.accept(socket_data);
        asio::error_code error;

        int extend_value_size_byte = block_size * k;
        std::vector<char> buf_key(key.size());
        std::vector<char> v_buf(extend_value_size_byte);
        for (int i = value_size_bytes; i < extend_value_size_byte; i++)
        {
          v_buf[i] = '0';
        }

        // read the key
        asio::read(socket_data, asio::buffer(buf_key, key.size()), error);
        if (error == asio::error::eof)
        {
          std::cout << "error == asio::error::eof" << std::endl;
        }
        else if (error)
        {
          throw asio::system_error(error);
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Check key " << buf_key.data() << std::endl;
        }

        // check the key
        bool flag = true;
        for (int i = 0; i < int(key.size()); i++)
        {
          if (key[i] != buf_key[i])
          {
            flag = false;
          }
        }
        if (flag)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                      << "Read value of " << buf_key.data() << std::endl;
          }
          // read the value
          asio::read(socket_data, asio::buffer(v_buf.data(), value_size_bytes), error);
        }
        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
        socket_data.close(ignore_ec);

        // set the blocks to the datanode
        char *buf = v_buf.data();
        // define a lambda function to send to datanode
        auto send_to_datanode = [this](int j, int k, std::string block_key, char **data, char **coding, int block_size, std::pair<std::string, int> ip_and_port)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                      << "Thread " << j << " send " << block_key << " to Datanode" << ip_and_port.second << std::endl;
          }
          if (j < k)
          {
            // send to data node
            SetToDatanode(block_key.c_str(), block_key.size(), data[j], block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
          }
          else
          {
            // send to parity node
            SetToDatanode(block_key.c_str(), block_key.size(), coding[j - k], block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
          }
        };

        // calculate parity blocks
        // initialize the area of parity blocks
        std::vector<char *> v_data(k);
        std::vector<char *> v_coding(g_m + l + 1);
        char **data = (char **)v_data.data();
        char **coding = (char **)v_coding.data();

        std::vector<std::vector<char>> v_coding_area(g_m + l + 1, std::vector<char>(block_size));
        for (int j = 0; j < k; j++)
        {
          data[j] = &buf[j * block_size];
        }
        for (int j = 0; j < g_m + l + 1; j++)
        {
          coding[j] = v_coding_area[j].data();
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Encode value with size of " << v_buf.size() << std::endl;
        }
        int send_num;
        if (encode_type == Azure_LRC || encode_type == Optimal_Cauchy_LRC)
        {
          encode(k, g_m, l, data, coding, block_size, encode_type);
          send_num = k + g_m + l;
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Distribute blocks to datanodes" << std::endl;
        }

        // call the lambda function send_to_datanode to send data and parity blocks
        std::vector<std::thread> senders;
        for (int j = 0; j < send_num; j++)
        {
          std::string block_key = keys_nodes[j].first;
          std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
          senders.push_back(std::thread(send_to_datanode, j, k, block_key, data, coding, block_size, ip_and_port));
        }
        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Finish distributing blocks!" << std::endl;
        }

        // report to coordinator
        coordinator_proto::CommitAbortKey commit_abort_key;
        coordinator_proto::ReplyFromCoordinator result;
        grpc::ClientContext context;
        ECProject::OpperateType opp = SET;
        commit_abort_key.set_opp(opp);
        commit_abort_key.set_key(key);
        commit_abort_key.set_ifcommitmetadata(true);
        grpc::Status status;
        status = m_coordinator_ptr->reportCommitAbort(&context, commit_abort_key, &result);
        if (status.ok() && IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "[SET] report to coordinator success" << std::endl;
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << " report to coordinator fail!" << std::endl;
        }
      }
      catch (std::exception &e)
      {
        std::cout << "exception in encode_and_save" << std::endl;
        std::cout << e.what() << std::endl;
      }
    };
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy][SET] Handle encode and set" << std::endl;
      }
      std::thread my_thread(encode_and_save);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::decodeAndGetObject(
      grpc::ServerContext *context,
      const proxy_proto::ObjectAndPlacement *object_and_placement,
      proxy_proto::GetReply *response)
  {
    ECProject::EncodeType encode_type = (ECProject::EncodeType)object_and_placement->encode_type();
    std::string key = object_and_placement->key();
    int k = object_and_placement->k();
    int g_m = object_and_placement->g_m();
    int l = object_and_placement->l();
    // int block_size = object_and_placement->block_size();
    int value_size_bytes = object_and_placement->valuesizebyte();
    int block_size = ceil(value_size_bytes, k);
    std::string clientip = object_and_placement->clientip();
    int clientport = object_and_placement->clientport();
    int stripe_id = object_and_placement->stripe_id();

    std::vector<std::pair<std::string, std::pair<std::string, int>>> keys_nodes;
    std::vector<int> block_idxs;
    for (int i = 0; i < object_and_placement->datanodeip_size(); i++)
    {
      block_idxs.push_back(object_and_placement->blockids(i));
      keys_nodes.push_back(std::make_pair(object_and_placement->blockkeys(i), std::make_pair(object_and_placement->datanodeip(i), object_and_placement->datanodeport(i))));
    }

    auto decode_and_get = [this, key, k, g_m, l, block_size, value_size_bytes, stripe_id,
                           clientip, clientport, keys_nodes, block_idxs, encode_type]() mutable
    {
      int expect_block_number = (encode_type == Azure_LRC) ? (k + l) : k;
      int all_expect_blocks = (encode_type == Azure_LRC) ? (k + g_m + l) : (k + g_m);

      auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto blocks_key_ptr = std::make_shared<std::vector<std::string>>();
      auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto myLock_ptr = std::make_shared<std::mutex>();
      auto cv_ptr = std::make_shared<std::condition_variable>();

      std::vector<char *> v_data(k);
      std::vector<char *> v_coding(all_expect_blocks - k);
      char **data = v_data.data();
      char **coding = v_coding.data();

      auto getFromNode = [this, k, blocks_ptr, blocks_key_ptr, blocks_idx_ptr, myLock_ptr, cv_ptr](int expect_block_number, int block_idx, std::string block_key, int block_size, std::string ip, int port)
      {
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                    << "Block " << block_idx << " with key " << block_key << " from Datanode" << ip << ":" << port << std::endl;
        }

        std::vector<char> temp(block_size);
        bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, ip.c_str(), port, block_idx + 2);

        if (!ret)
        {
          std::cout << "getFromNode !ret" << std::endl;
          return;
        }
        myLock_ptr->lock();
        // get any k blocks and decode
        if (!check_received_block(k, expect_block_number, blocks_idx_ptr, blocks_ptr->size()))
        {
          blocks_ptr->push_back(temp);
          blocks_key_ptr->push_back(block_key);
          blocks_idx_ptr->push_back(block_idx);
          if (check_received_block(k, expect_block_number, blocks_idx_ptr, blocks_ptr->size()))
          {
            cv_ptr->notify_all();
          }
        }
        // get all the blocks
        // blocks_ptr->push_back(temp);
        // blocks_key_ptr->push_back(block_key);
        // blocks_idx_ptr->push_back(block_idx);
        myLock_ptr->unlock();
      };

      std::vector<std::vector<char>> v_data_area(k, std::vector<char>(block_size));
      std::vector<std::vector<char>> v_coding_area(all_expect_blocks - k, std::vector<char>(block_size));
      for (int j = 0; j < k; j++)
      {
        data[j] = v_data_area[j].data();
      }
      for (int j = 0; j < all_expect_blocks - k; j++)
      {
        coding[j] = v_coding_area[j].data();
      }
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "ready to get blocks from datanodes!" << std::endl;
      }
      std::vector<std::thread> read_treads;
      // for (int j = 0; j < k; j++)
      for (int j = 0; j < all_expect_blocks; j++)
      {
        int block_idx = block_idxs[j];
        std::string block_key = keys_nodes[j].first;
        std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
        // std::vector<char> temp(block_size);
        // GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
        // blocks_ptr->push_back(temp);
        // blocks_key_ptr->push_back(block_key);
        // blocks_idx_ptr->push_back(j);
        read_treads.push_back(std::thread(getFromNode, expect_block_number, block_idx, block_key, block_size, ip_and_port.first, ip_and_port.second));
      }
      for (int j = 0; j < all_expect_blocks; j++)
      {
        read_treads[j].detach();
        // read_treads[j].join();
      }

      std::unique_lock<std::mutex> lck(*myLock_ptr);
      while (!check_received_block(k, expect_block_number, blocks_idx_ptr, blocks_ptr->size()))
      {
        cv_ptr->wait(lck);
      }
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "ready to decode!" << std::endl;
      }
      for (int j = 0; j < int(blocks_idx_ptr->size()); j++)
      {
        int idx = (*blocks_idx_ptr)[j];
        if (idx < k)
        {
          memcpy(data[idx], (*blocks_ptr)[j].data(), block_size);
        }
        else
        {
          memcpy(coding[idx - k], (*blocks_ptr)[j].data(), block_size);
        }
      }

      auto erasures = std::make_shared<std::vector<int>>();
      for (int j = 0; j < all_expect_blocks; j++)
      {
        if (std::find(blocks_idx_ptr->begin(), blocks_idx_ptr->end(), j) == blocks_idx_ptr->end())
        {
          erasures->push_back(j);
        }
      }
      erasures->push_back(-1);
      if (encode_type == Azure_LRC)
      {
        if (!decode(k, g_m, l, data, coding, erasures, block_size, encode_type))
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][GET] proxy cannot decode!" << std::endl;
        }
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET] proxy decode error!" << std::endl;
      }
      std::string value;
      for (int j = 0; j < k; j++)
      {
        value += std::string(data[j]);
      }

      if (IF_DEBUG)
      {
        std::cout << "\033[1;31m[Proxy" << m_self_cluster_id << "][GET]"
                  << "send " << key << " to client with length of " << value.size() << "\033[0m" << std::endl;
      }

      // send to the client
      asio::error_code error;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(clientip, std::to_string(clientport));
      asio::ip::tcp::socket sock_data(io_context);
      asio::connect(sock_data, endpoints);

      apply_kernel_bandwidth_to_peer_socket(sock_data, clientip.c_str());
      asio::write(sock_data, asio::buffer(key.data(), key.size()), error);
      if(error)
      {
        std::cout << "error in write key" << std::endl;
      }
      asio::write(sock_data, asio::buffer(value.data(), value_size_bytes), error);
      if(error)
      {
        std::cout << "error in write value" << std::endl;
      }
      asio::error_code ignore_ec;
      sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      sock_data.close(ignore_ec);
    };
    try
    {
      // std::cerr << "decode_and_get_thread start" << std::endl;
      if (IF_DEBUG)
      {
        std::cout << "[Proxy] Handle get and decode" << std::endl;
      }
      std::thread my_thread(decode_and_get);
      my_thread.detach();
      // std::cerr << "decode_and_get_thread detach" << std::endl;
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  std::vector<unsigned char *> ProxyImpl::convertToUnsignedCharArray(std::vector<char*> &input)
  {
    std::vector<unsigned char *> output;

    for (auto &row : input)
    {
      output.push_back(reinterpret_cast<unsigned char *>(row));
    }

    return output;
  }

  void ProxyImpl::get_from_node(const std::string &block_key, char *block_value, const size_t block_size, const char *datanode_ip, const int datanode_port, bool *status, int index)
  {
    if (IF_DEBUG)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "Block key " << block_key << " from Datanode" << datanode_ip << ":" << datanode_port << std::endl;
    }
    status[index] = GetFromDatanode(block_key, block_value, block_size, datanode_ip, datanode_port);
  }

  void ProxyImpl::get_from_node_breakdown(const std::string &block_key, char *block_value, const size_t block_size, const char *datanode_ip, const int datanode_port, bool *status, int index, 
    double *disk_io_start_time, double *disk_io_end_time, double *network_start_time, double *network_end_time, double *grpc_notify_time, double *grpc_start_time)
  {
    if (IF_DEBUG)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "Block key " << block_key << " from Datanode" << datanode_ip << ":" << datanode_port << std::endl;
    }
    status[index] = GetFromDatanode(block_key, block_value, block_size, datanode_ip, datanode_port, disk_io_start_time, disk_io_end_time, network_start_time, network_end_time, grpc_notify_time, grpc_start_time);
  }
  // degraded read
  grpc::Status ProxyImpl::degradedRead(
      grpc::ServerContext *context,
      const proxy_proto::DegradedReadRequest *degraded_read_request,
      proxy_proto::DegradedReadReply *response)
  {
    auto request_copy = std::make_shared<proxy_proto::DegradedReadRequest>(*degraded_read_request);

    auto degraded_read = [this, request_copy, &response]() mutable
    {
      std::string code_type = m_sys_config->CodeType;
      // auto status = std::make_shared<std::vector<bool>>(request_copy->datanodeip_size(), false);
      std::unique_ptr<bool[]> status(new bool[request_copy->datanodeip_size()]);
      std::fill_n(status.get(), request_copy->datanodeip_size(), false);

      //std::vector<std::vector<char>> get_bufs(request_copy->datanodeip_size(), std::vector<char>(m_sys_config->BlockSize, 0));
      std::vector<char*> get_bufs(request_copy->datanodeip_size());

      for(int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      }

      //std::vector<char> res_buf(m_sys_config->BlockSize, 0);
      char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      std::vector<std::thread> get_threads;
      //std::chrono::high_resolution_clock::time_point t0 = std::chrono::high_resolution_clock::now();
      for (int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_threads.push_back(std::thread(&ProxyImpl::get_from_node, this,
                                          request_copy->blockkeys(i), // block key
                                          get_bufs[i],               // buffer to store the block value
                                          m_sys_config->BlockSize,   // block size
                                          request_copy->datanodeip(i).c_str(), // datanode IP
                                          request_copy->datanodeport(i),       // datanode port
                                          status.get(),              // status array to track success/failure
                                          i                          // index for status array
        ));
      }
      for (int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_threads[i].join();
      }
      bool all_true = std::all_of(status.get(), status.get() + request_copy->datanodeip_size(), [](bool val)
                                  { return val == true; });
      if (!all_true)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes failed!" << std::endl;
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes success!" << std::endl;

        std::vector<int> block_idxs;
        for (int i = 0; i < request_copy->datanodeip_size(); i++)
        {
          block_idxs.push_back(request_copy->blockids(i));
        }
        std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);
        if (code_type == "UniLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_unilrc" << std::endl;
          decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
        }
        else if (is_azure_like_code(code_type))
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc" << std::endl;
          decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc success!" << std::endl;
        }
        else if (code_type == "OptimalLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc" << std::endl;
          decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc success!" << std::endl;
        }
        else if (code_type == "UniformLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc" << " failed block id: " << request_copy->failed_block_id() << std::endl;
          decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc success!" << std::endl;
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
          exit(1);
        }
        std::string client_ip = request_copy->clientip();
        int client_port = request_copy->clientport();

        // send to the client
        asio::error_code error;
        asio::ip::tcp::resolver resolver(io_context);
        asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(client_ip, std::to_string(client_port));
        //asio::io_context io_context;
        asio::ip::tcp::socket socket_data(io_context);
        asio::connect(socket_data, endpoints);
        //acceptor.accept(socket_data);
        if (error)
        {
          std::cout << "error in connect" << std::endl;
        }
        asio::write(socket_data, asio::buffer(res_buf, m_sys_config->BlockSize), error);
        if (error)
        {
          std::cout << "error in write" << std::endl;
        }
        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
        socket_data.close(ignore_ec);
        delete res_buf;
        for(int i = 0; i < request_copy->datanodeip_size(); i++)
        {
          delete get_bufs[i];
        }
        //std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] send to the client done" << std::endl;
      }
    };

    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] Handle degraded read" << std::endl;
      }

      std::thread my_thread(degraded_read);
      my_thread.join();
    }
    catch (const std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cerr << e.what() << '\n';
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::degradedReadBreakdown(
      grpc::ServerContext *context,
      const proxy_proto::DegradedReadRequest *degraded_read_request,
      proxy_proto::DegradedReadReply *response)
  {
    std::chrono::high_resolution_clock::time_point START = std::chrono::high_resolution_clock::now();
    response->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(START.time_since_epoch()).count());

    auto request_copy = std::make_shared<proxy_proto::DegradedReadRequest>(*degraded_read_request);

    auto degraded_read = [this, request_copy, &response]() mutable
    {
      std::string code_type = m_sys_config->CodeType;
      // auto status = std::make_shared<std::vector<bool>>(request_copy->datanodeip_size(), false);
      std::unique_ptr<bool[]> status(new bool[request_copy->datanodeip_size()]);
      std::fill_n(status.get(), request_copy->datanodeip_size(), false);

      //std::vector<std::vector<char>> get_bufs(request_copy->datanodeip_size(), std::vector<char>(m_sys_config->BlockSize, 0));
      std::vector<char*> get_bufs(request_copy->datanodeip_size());
      std::vector<double> data_node_disk_io_start_time(request_copy->datanodeip_size(), 0.0);
      std::vector<double> data_node_disk_io_end_time(request_copy->datanodeip_size(), 0.0);
      std::vector<double> data_node_network_start_time(request_copy->datanodeip_size(), 0.0);
      std::vector<double> data_node_network_end_time(request_copy->datanodeip_size(), 0.0);
      std::vector<double> data_node_grpc_notify_time(request_copy->datanodeip_size(), 0.0);
      std::vector<double> data_node_grpc_start_time(request_copy->datanodeip_size(), 0.0);
      for(int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      }

      //std::vector<char> res_buf(m_sys_config->BlockSize, 0);
      char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      std::vector<std::thread> get_threads;
      //std::chrono::high_resolution_clock::time_point t0 = std::chrono::high_resolution_clock::now();
      for (int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_threads.push_back(std::thread(&ProxyImpl::get_from_node_breakdown, this,
                                          request_copy->blockkeys(i), // block key
                                          get_bufs[i],               // buffer to store the block value
                                          m_sys_config->BlockSize,   // block size
                                          request_copy->datanodeip(i).c_str(), // datanode IP
                                          request_copy->datanodeport(i),       // datanode port
                                          status.get(),              // status array to track success/failure
                                          i,                         // index for status array
                                          &data_node_disk_io_start_time[i],         // pointer to store disk IO time
                                          &data_node_disk_io_end_time[i],           // pointer to store disk IO time
                                          &data_node_network_start_time[i],          // pointer to store network time
                                          &data_node_network_end_time[i],            // pointer to store network time
                                          &data_node_grpc_notify_time[i],            // pointer to store grpc notify time
                                          &data_node_grpc_start_time[i]              // pointer to store grpc start time
        ));
      }
      for (int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_threads[i].join();
      }
      //std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
      //std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0);
      bool all_true = std::all_of(status.get(), status.get() + request_copy->datanodeip_size(), [](bool val)
                                  { return val == true; });
      if (!all_true)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes failed!" << std::endl;
      }
      else
      {
        //response->set_disk_io_time(*std::max_element(data_node_disk_io_end_time.begin(), data_node_disk_io_end_time.end()) - *std::min_element(data_node_disk_io_start_time.begin(), data_node_disk_io_start_time.end()));
        //response->set_network_time(*std::max_element(data_node_network_end_time.begin(), data_node_network_end_time.end()) - *std::min_element(data_node_network_start_time.begin(), data_node_network_start_time.end()));
        response->set_disk_io_start_time(*std::min_element(data_node_disk_io_start_time.begin(), data_node_disk_io_start_time.end()));
        response->set_disk_io_end_time(*std::max_element(data_node_disk_io_end_time.begin(), data_node_disk_io_end_time.end()));
        response->set_network_start_time(*std::min_element(data_node_network_start_time.begin(), data_node_network_start_time.end()));
        response->set_network_end_time(*std::max_element(data_node_network_end_time.begin(), data_node_network_end_time.end()));
        response->set_data_node_grpc_notify_time(*std::min_element(data_node_grpc_notify_time.begin(), data_node_grpc_notify_time.end()));
        response->set_data_node_grpc_start_time(*std::max_element(data_node_grpc_start_time.begin(), data_node_grpc_start_time.end()));
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes success!" << std::endl;

        std::vector<int> block_idxs;
        for (int i = 0; i < request_copy->datanodeip_size(); i++)
        {
          block_idxs.push_back(request_copy->blockids(i));
        }
        std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        if (code_type == "UniLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_unilrc" << std::endl;
          decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
        }
        else if (is_azure_like_code(code_type))
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc" << std::endl;
          decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc success!" << std::endl;
        }
        else if (code_type == "OptimalLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc" << std::endl;
          decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc success!" << std::endl;
        }
        else if (code_type == "UniformLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc" << " failed block id: " << request_copy->failed_block_id() << std::endl;
          decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc success!" << std::endl;
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
          exit(1);
        }
        std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
        response->set_decode_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(t2.time_since_epoch()).count());
        response->set_decode_end_time(std::chrono::duration_cast<std::chrono::duration<double>>(t3.time_since_epoch()).count());
        std::string client_ip = request_copy->clientip();
        int client_port = request_copy->clientport();

        // send to the client
        asio::error_code error;
        asio::ip::tcp::resolver resolver(io_context);
        asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(client_ip, std::to_string(client_port));
        asio::ip::tcp::socket sock_data(io_context);
        //asio::connect(sock_data, endpoints);
        sock_data.connect(*endpoints, error);
        if (error)
        {
          std::cout << "error in connect" << std::endl;
        }
        asio::write(sock_data, asio::buffer(res_buf, m_sys_config->BlockSize), error);
        if (error)
        {
          std::cout << "error in write" << std::endl;
        }
        asio::error_code ignore_ec;
        sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
        sock_data.close(ignore_ec);
        delete res_buf;
        for(int i = 0; i < request_copy->datanodeip_size(); i++)
        {
          delete get_bufs[i];
        }
        //std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] send to the client done" << std::endl;
      }
    };

    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] Handle degraded read" << std::endl;
      }

      std::thread my_thread(degraded_read);
      my_thread.join();
    }
    catch (const std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cerr << e.what() << '\n';
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::degradedReadWithBlockStripeID(
    grpc::ServerContext *context,
    const proxy_proto::DegradedReadRequest *degraded_read_request,
    proxy_proto::GetReply *response)
{
  auto request_copy = std::make_shared<proxy_proto::DegradedReadRequest>(*degraded_read_request);

  auto degraded_read = [this, request_copy]() mutable
  {
    int stripe_id = request_copy->failed_block_stripe_id();
    std::string code_type = m_sys_config->CodeType;
    // auto status = std::make_shared<std::vector<bool>>(request_copy->datanodeip_size(), false);
    std::unique_ptr<bool[]> status(new bool[request_copy->datanodeip_size()]);
    std::fill_n(status.get(), request_copy->datanodeip_size(), false);

    //std::vector<std::vector<char>> get_bufs(request_copy->datanodeip_size(), std::vector<char>(m_sys_config->BlockSize, 0));
    std::vector<char*> get_bufs(request_copy->datanodeip_size());
    for(int i = 0; i < request_copy->datanodeip_size(); i++)
    {
      get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    }

    //std::vector<char> res_buf(m_sys_config->BlockSize, 0);
    char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    std::vector<std::thread> get_threads;
    for (int i = 0; i < request_copy->datanodeip_size(); i++)
    {
      get_threads.push_back(std::thread(&ProxyImpl::get_from_node, this, request_copy->blockkeys(i), get_bufs[i], m_sys_config->BlockSize, request_copy->datanodeip(i).c_str(), request_copy->datanodeport(i), status.get(), i));
    }
    for (int i = 0; i < request_copy->datanodeip_size(); i++)
    {
      get_threads[i].join();
    }

    bool all_true = std::all_of(status.get(), status.get() + request_copy->datanodeip_size(), [](bool val)
                                { return val == true; });
    if (!all_true)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes failed!" << std::endl;
    }
    else
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes success!" << std::endl;

      std::vector<int> block_idxs;
      for (int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        block_idxs.push_back(request_copy->blockids(i));
      }
      std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

      std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
      if (code_type == "UniLRC")
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_unilrc" << std::endl;
        decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
      }
      else if (is_azure_like_code(code_type))
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc" << std::endl;
        decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc success!" << std::endl;
      }
      else if (code_type == "OptimalLRC")
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc" << std::endl;
        decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc success!" << std::endl;
      }
      else if (code_type == "UniformLRC")
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc" << " failed block id: " << request_copy->failed_block_id() << std::endl;
        decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc success!" << std::endl;
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
        exit(1);
      }

      std::string client_ip = request_copy->clientip();
      int client_port = request_copy->clientport();

      // send to the client
      asio::error_code error;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(client_ip, std::to_string(client_port));
      asio::ip::tcp::socket sock_data(io_context);
      //asio::connect(sock_data, endpoints);
      sock_data.connect(*endpoints, error);
      if (error)
      {
        std::cout << "error in connect" << std::endl;
      }
      asio::write(sock_data, asio::buffer(&stripe_id, sizeof(int)), error);
      asio::write(sock_data, asio::buffer(res_buf, m_sys_config->BlockSize), error);
      if (error)
      {
        std::cout << "error in write" << std::endl;
      }
      asio::error_code ignore_ec;
      sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      sock_data.close(ignore_ec);
      delete res_buf;
      for(int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        delete get_bufs[i];
      }
      //std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] send to the client done" << std::endl;
    }
  };

  try
  {
    if (IF_DEBUG)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] Handle degraded read" << std::endl;
    }

    std::thread my_thread(degraded_read);
    my_thread.detach();
  }
  catch (const std::exception &e)
  {
    std::cout << "exception" << std::endl;
    std::cerr << e.what() << '\n';
  }

  return grpc::Status::OK;
}

  grpc::Status ProxyImpl::degradedRead2Client(
    grpc::ServerContext *context,
    const proxy_proto::RecoveryRequest *recovery_request,
    proxy_proto::DegradedReadReply *response)
{
  try
  {
    if (IF_DEBUG)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] Handle Degraded Read" << std::endl;
    }

    std::string code_type = m_sys_config->CodeType;
    int cross_rack_num = recovery_request->cross_rack_num();
    std::unique_ptr<bool[]> status(new bool[recovery_request->datanodeip_size()]);
    std::fill_n(status.get(), recovery_request->datanodeip_size(), false);
    std::vector<char*> get_bufs(recovery_request->datanodeip_size());
    for(int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    }

    char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    char *real_res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    std::vector<std::thread> get_threads;
    //std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_threads.push_back(std::thread(&ProxyImpl::get_from_node, this, recovery_request->blockkeys(i), get_bufs[i], m_sys_config->BlockSize, 
      recovery_request->datanodeip(i).c_str(), recovery_request->datanodeport(i), status.get(), i));
    }
    for (int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_threads[i].join();
    }

    bool all_true = std::all_of(status.get(), status.get() + recovery_request->datanodeip_size(), [](bool val)
                                { return val == true; });
    if (!all_true)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes failed!" << std::endl;
    }

    else
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes success!" << std::endl;
      std::vector<int> block_idxs;
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        block_idxs.push_back(recovery_request->blockids(i));
      }
      std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

      std::string failed_block_key = recovery_request->failed_block_key();
      int failed_block_id = recovery_request->failed_block_id();
      std::string replaced_node_ip = recovery_request->replaced_node_ip();
      int replaced_node_port = recovery_request->replaced_node_port();

      if (code_type == "UniLRC")
      {
        decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
      }
      else if (is_azure_like_code(code_type))
      {
        decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else if (code_type == "OptimalLRC")
      {
        decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else if (code_type == "UniformLRC")
      {
        decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
        exit(1);
      }
      if(cross_rack_num){
        std::cout << "start to recover cross rack" << std::endl;
        char **cross_rack_bufs = new char*[cross_rack_num];
        for(int i = 0; i < cross_rack_num; i++)
        {
          cross_rack_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
        }
        std::vector<std::thread> get_from_proxies_threads;
        std::vector<double> accept_start_time(cross_rack_num, 0.0);
        for(int i = 0; i < cross_rack_num; i++)
        {
          get_from_proxies_threads.push_back(std::thread([i, this, &cross_rack_bufs]()mutable{
            //asio::io_context io_context;
            asio::ip::tcp::socket socket(this->io_context);
            //asio::ip::tcp::resolver resolver(io_context);
            this->acceptor.accept(socket);
            std::cout << "connected to porxy" << std::endl;
            asio::error_code error;
            size_t len = asio::read(socket, asio::buffer(cross_rack_bufs[i], this->m_sys_config->BlockSize), error);
            if(len != this->m_sys_config->BlockSize)
            {
              std::cout << "error in read" << std::endl;
            }
            asio::error_code ignore_ec;
            socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
            socket.close(ignore_ec);
          }));
        }
        for(int i = 0; i < cross_rack_num; i++)
        {
          get_from_proxies_threads[i].join();
        }
        std::cout << "start to xor" << std::endl;
        char **buf_ptrs = new char*[cross_rack_num + 2];
        for(int i = 0; i < cross_rack_num; i++)
        {
          buf_ptrs[i] = cross_rack_bufs[i];
        }
        buf_ptrs[cross_rack_num] = res_buf;
        buf_ptrs[cross_rack_num + 1] = real_res_buf;
        xor_avx(cross_rack_num + 2, m_sys_config->BlockSize, (void**)buf_ptrs);
        for(int i = 0; i < cross_rack_num; i++)
        {
          delete cross_rack_bufs[i];
        }
        delete cross_rack_bufs;
        delete buf_ptrs;
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode success!" << std::endl;
      }
    }
  
    std::string replaced_node_ip = recovery_request->replaced_node_ip();
    int replaced_node_port = recovery_request->replaced_node_port();
    std::cout << "[Proxy" << m_self_cluster_id << "][Degraded] send to the client" << replaced_node_ip << ":" << replaced_node_port << std::endl;
    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    asio::ip::tcp::resolver resolver(io_context);
    asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(replaced_node_ip, std::to_string(replaced_node_port));
    asio::connect(socket, endpoints);
    if(recovery_request->is_to_send_block_id()){
      int32_t block_id_to_send = recovery_request->block_id_to_send();
      asio::write(socket, asio::buffer(&block_id_to_send, sizeof(int32_t)));
    }
    if(cross_rack_num){
      asio::write(socket, asio::buffer(real_res_buf, m_sys_config->BlockSize));
    }
    else{
      asio::write(socket, asio::buffer(res_buf, m_sys_config->BlockSize));
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    socket.close(ignore_ec);
    std::cout << "[Proxy" << m_self_cluster_id << "][Degraded Read] send to the client done" << std::endl;
    delete res_buf;
    delete real_res_buf;
    for(int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      delete get_bufs[i];
    }
  }
  catch (const std::exception &e)
  {
    std::cout << "exception" << std::endl;
    std::cerr << e.what() << '\n';
  }
  return grpc::Status::OK;
}


  grpc::Status ProxyImpl::degradedRead2ClientBreakdown(
    grpc::ServerContext *context,
    const proxy_proto::RecoveryRequest *recovery_request,
    proxy_proto::DegradedReadReply *response)
{
  std::chrono::high_resolution_clock::time_point START = std::chrono::high_resolution_clock::now();
  response->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(START.time_since_epoch()).count());
  try
  {
    if (IF_DEBUG)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] Handle Degraded Read" << std::endl;
    }

    std::string code_type = m_sys_config->CodeType;
    int cross_rack_num = recovery_request->cross_rack_num();
    std::unique_ptr<bool[]> status(new bool[recovery_request->datanodeip_size()]);
    std::fill_n(status.get(), recovery_request->datanodeip_size(), false);


    std::vector<double> data_node_disk_io_start_time(recovery_request->datanodeip_size(), 0.0);
    std::vector<double> data_node_disk_io_end_time(recovery_request->datanodeip_size(), 0.0);
    std::vector<double> data_node_network_start_time(recovery_request->datanodeip_size(), 0.0);
    std::vector<double> data_node_network_end_time(recovery_request->datanodeip_size(), 0.0);
    std::vector<double> data_node_grpc_notify_time(recovery_request->datanodeip_size(), 0.0);
    std::vector<double> data_node_grpc_start_time(recovery_request->datanodeip_size(), 0.0);

    std::vector<char*> get_bufs(recovery_request->datanodeip_size());
    for(int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    }

    char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    char *real_res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    std::vector<std::thread> get_threads;
    //std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_threads.push_back(std::thread(&ProxyImpl::get_from_node_breakdown, this, recovery_request->blockkeys(i), get_bufs[i], m_sys_config->BlockSize, recovery_request->datanodeip(i).c_str(), recovery_request->datanodeport(i), status.get(), i, 
        &data_node_disk_io_start_time[i], &data_node_disk_io_end_time[i], &data_node_network_start_time[i], &data_node_network_end_time[i], &data_node_grpc_notify_time[i], &data_node_grpc_start_time[i]));
    }
    for (int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_threads[i].join();
    }

    bool all_true = std::all_of(status.get(), status.get() + recovery_request->datanodeip_size(), [](bool val)
                                { return val == true; });
    if (!all_true)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes failed!" << std::endl;
    }

    else
    {
      //response->set_disk_io_time(*std::max_element(data_node_disk_io_end_time.begin(), data_node_disk_io_end_time.end()) - *std::min_element(data_node_disk_io_start_time.begin(), data_node_disk_io_start_time.end()));
      //response->set_network_time(*std::max_element(data_node_network_end_time.begin(), data_node_network_end_time.end()) - *std::min_element(data_node_network_start_time.begin(), data_node_network_start_time.end()));
      response->set_disk_io_start_time(*std::min_element(data_node_disk_io_start_time.begin(), data_node_disk_io_start_time.end()));
      response->set_disk_io_end_time(*std::max_element(data_node_disk_io_end_time.begin(), data_node_disk_io_end_time.end()));
      response->set_network_start_time(*std::min_element(data_node_network_start_time.begin(), data_node_network_start_time.end()));
      response->set_network_end_time(*std::max_element(data_node_network_end_time.begin(), data_node_network_end_time.end()));
      response->set_data_node_grpc_notify_time(*std::min_element(data_node_grpc_notify_time.begin(), data_node_grpc_notify_time.end()));
      response->set_data_node_grpc_start_time(*std::max_element(data_node_grpc_start_time.begin(), data_node_grpc_start_time.end()));

      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes success!" << std::endl;
      std::chrono::high_resolution_clock::time_point decode_start_time = std::chrono::high_resolution_clock::now();
      std::vector<int> block_idxs;
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        block_idxs.push_back(recovery_request->blockids(i));
      }
      std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

      std::string failed_block_key = recovery_request->failed_block_key();
      int failed_block_id = recovery_request->failed_block_id();
      std::string replaced_node_ip = recovery_request->replaced_node_ip();
      int replaced_node_port = recovery_request->replaced_node_port();

      if (code_type == "UniLRC")
      {
        decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
      }
      else if (is_azure_like_code(code_type))
      {
        decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else if (code_type == "OptimalLRC")
      {
        decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else if (code_type == "UniformLRC")
      {
        decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
        exit(1);
      }
      std::chrono::high_resolution_clock::time_point decode_end_time = std::chrono::high_resolution_clock::now();
      response->set_decode_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(decode_start_time.time_since_epoch()).count());
      response->set_decode_end_time(std::chrono::duration_cast<std::chrono::duration<double>>(decode_end_time.time_since_epoch()).count());

      if(cross_rack_num){
        std::cout << "start to recover cross rack" << std::endl;
        char **cross_rack_bufs = new char*[cross_rack_num];
        for(int i = 0; i < cross_rack_num; i++)
        {
          cross_rack_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
        }
        std::vector<std::thread> get_from_proxies_threads;
        std::vector<double> accept_start_time(cross_rack_num, 0.0);
        for(int i = 0; i < cross_rack_num; i++)
        {
          get_from_proxies_threads.push_back(std::thread([i, this, &cross_rack_bufs, &accept_start_time]()mutable{
            //asio::io_context io_context;
            asio::ip::tcp::socket socket(this->io_context);
            //asio::ip::tcp::resolver resolver(io_context);
            this->acceptor.accept(socket);
            std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
            accept_start_time[i] = std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count();
            std::cout << "connected to porxy" << std::endl;
            asio::error_code error;
            size_t len = asio::read(socket, asio::buffer(cross_rack_bufs[i], this->m_sys_config->BlockSize), error);
            if(len != this->m_sys_config->BlockSize)
            {
              std::cout << "error in read" << std::endl;
            }
            asio::error_code ignore_ec;
            socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
            socket.close(ignore_ec);
          }));
        }
        for(int i = 0; i < cross_rack_num; i++)
        {
          get_from_proxies_threads[i].join();
        }
        double min_accept_start_time = *std::min_element(accept_start_time.begin(), accept_start_time.end());
        std::chrono::high_resolution_clock::time_point accept_end_time = std::chrono::high_resolution_clock::now();
        double time_span3 = std::chrono::duration_cast<std::chrono::duration<double>>(accept_end_time.time_since_epoch()).count() - min_accept_start_time;
        response->set_cross_rack_time(time_span3);
        std::cout << "start to xor" << std::endl;
        char **buf_ptrs = new char*[cross_rack_num + 2];
        for(int i = 0; i < cross_rack_num; i++)
        {
          buf_ptrs[i] = cross_rack_bufs[i];
        }
        buf_ptrs[cross_rack_num] = res_buf;
        buf_ptrs[cross_rack_num + 1] = real_res_buf;
        std::chrono::high_resolution_clock::time_point cross_rack_xor_start_time = std::chrono::high_resolution_clock::now();
        xor_avx(cross_rack_num + 2, m_sys_config->BlockSize, (void**)buf_ptrs);
        std::chrono::high_resolution_clock::time_point cross_rack_xor_end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> xor_time = std::chrono::duration_cast<std::chrono::duration<double>>(cross_rack_xor_end_time - cross_rack_xor_start_time);
        response->set_cross_rack_xor_time(xor_time.count());
        for(int i = 0; i < cross_rack_num; i++)
        {
          delete cross_rack_bufs[i];
        }
        delete cross_rack_bufs;
        delete buf_ptrs;
      }
      else
      {
        response->set_cross_rack_time(0);
        response->set_cross_rack_xor_time(0);
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode success!" << std::endl;
      }
    }
  
    std::string replaced_node_ip = recovery_request->replaced_node_ip();
    int replaced_node_port = recovery_request->replaced_node_port();
    std::cout << "[Proxy" << m_self_cluster_id << "][Degraded] send to the client" << replaced_node_ip << ":" << replaced_node_port << std::endl;
    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    asio::ip::tcp::resolver resolver(io_context);
    asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(replaced_node_ip, std::to_string(replaced_node_port));
    asio::connect(socket, endpoints);
    if(cross_rack_num){
      asio::write(socket, asio::buffer(real_res_buf, m_sys_config->BlockSize));
    }
    else{
      asio::write(socket, asio::buffer(res_buf, m_sys_config->BlockSize));
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    socket.close(ignore_ec);
    std::cout << "[Proxy" << m_self_cluster_id << "][Degraded Read] send to the client done" << std::endl;
    delete res_buf;
    delete real_res_buf;
    for(int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      delete get_bufs[i];
    }
  }
  catch (const std::exception &e)
  {
    std::cout << "exception" << std::endl;
    std::cerr << e.what() << '\n';
  }
  return grpc::Status::OK;
}


  // recovery
  grpc::Status ProxyImpl::recovery(
    grpc::ServerContext *context,
    const proxy_proto::RecoveryRequest *recovery_request,
    proxy_proto::RecoveryReply *response)
  {
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] Handle recovery" << std::endl;
      }

      std::string code_type = m_sys_config->CodeType;
      int cross_rack_num = recovery_request->cross_rack_num();
      // auto status = std::make_shared<std::vector<bool>>(recovery_request->datanodeip_size(), false);
      std::unique_ptr<bool[]> status(new bool[recovery_request->datanodeip_size()]);
      std::fill_n(status.get(), recovery_request->datanodeip_size(), false);

      //std::vector<std::vector<char>> get_bufs(recovery_request->datanodeip_size(), std::vector<char>(m_sys_config->BlockSize, 0));
      std::vector<char*> get_bufs(recovery_request->datanodeip_size());
      for(int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      }
      //std::vector<char> res_buf(m_sys_config->BlockSize, 0);
      char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      char *real_res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));

      std::vector<std::thread> get_threads;
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_threads.push_back(std::thread(&ProxyImpl::get_from_node, this, 
          recovery_request->blockkeys(i), get_bufs[i], m_sys_config->BlockSize, recovery_request->datanodeip(i).c_str(), 
          recovery_request->datanodeport(i), status.get(), i));
      }
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_threads[i].join();
      }
      
      bool all_true = std::all_of(status.get(), status.get() + recovery_request->datanodeip_size(), [](bool val)
                                  { return val == true; });
      if (!all_true)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes failed!" << std::endl;
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes success!" << std::endl;

        std::vector<int> block_idxs;
        for (int i = 0; i < recovery_request->datanodeip_size(); i++)
        {
          block_idxs.push_back(recovery_request->blockids(i));
        }
        std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

        std::string failed_block_key = recovery_request->failed_block_key();
        int failed_block_id = recovery_request->failed_block_id();
        std::string replaced_node_ip = recovery_request->replaced_node_ip();
        int replaced_node_port = recovery_request->replaced_node_port();

        if (code_type == "UniLRC")
        {
          decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
        }
        else if (is_azure_like_code(code_type))
        {
          decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else if (code_type == "OptimalLRC")
        {
          decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else if (code_type == "UniformLRC")
        {
          decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
          exit(1);
        }

        if(cross_rack_num){
          std::cout << "start to recover cross rack" << std::endl;
          char **cross_rack_bufs = new char*[cross_rack_num];
          for(int i = 0; i < cross_rack_num; i++)
          {
            cross_rack_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
          }
          std::vector<std::thread> get_from_proxies_threads;
          std::vector<std::string> cross_rack_ips;
          //std::vector<int> cross_rack_ports;
          //for(int i = 0; i < cross_rack_num; i++)
          //{
          //  cross_rack_ips.push_back(recovery_request->proxyip(i));
          //  cross_rack_ports.push_back(recovery_request->proxyport(i));
          //}
          //std::lock_guard<std::mutex> lock(m_mutex);
          for(int i = 0; i < cross_rack_num; i++)
          {
            get_from_proxies_threads.push_back(std::thread([i, this, &cross_rack_bufs]()mutable{
              asio::ip::tcp::socket socket(this->io_context);
              std::cout << "connecting to proxy" << std::endl;
              this->acceptor.accept(socket);
              std::cout << "connected to porxy" << std::endl;
              asio::error_code error;
              asio::read(socket, asio::buffer(cross_rack_bufs[i], this->m_sys_config->BlockSize), error);
              std::cout << "read from proxy"  << std::endl;
              if(error)
              {
                std::cout << "error in read" << std::endl;
              }
              asio::error_code ignore_ec;
              socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
              socket.close(ignore_ec);
            }));
          }
          for(int i = 0; i < cross_rack_num; i++)
          {
            get_from_proxies_threads[i].join();
          }

          std::cout << "start to xor" << std::endl;
          char **buf_ptrs = new char*[cross_rack_num + 2];
          for(int i = 0; i < cross_rack_num; i++)
          {
            buf_ptrs[i] = cross_rack_bufs[i];
          }
          buf_ptrs[cross_rack_num] = res_buf;
          buf_ptrs[cross_rack_num + 1] = real_res_buf;
          xor_avx(cross_rack_num + 2, m_sys_config->BlockSize, (void**)buf_ptrs);
          for(int i = 0; i < cross_rack_num; i++)
          {
            delete cross_rack_bufs[i];
          }
          delete cross_rack_bufs;
          delete buf_ptrs;
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode success!" << std::endl;
        }
        std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] send to the replaced node" << std::endl;
        // send to the replaced node
        if(cross_rack_num){
          RecoveryToDatanode(failed_block_key.c_str(), failed_block_id, real_res_buf, replaced_node_ip.c_str(), replaced_node_port);
        }
        else{
          RecoveryToDatanode(failed_block_key.c_str(), failed_block_id, res_buf, replaced_node_ip.c_str(), replaced_node_port);
        }
      }
      delete res_buf;
      delete real_res_buf;
      for(int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        delete get_bufs[i];
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cerr << e.what() << '\n';
    }
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::recoveryBreakdown(
    grpc::ServerContext *context,
    const proxy_proto::RecoveryRequest *recovery_request,
    proxy_proto::RecoveryReply *response)
  {
    std::chrono::high_resolution_clock::time_point START = std::chrono::high_resolution_clock::now();
    response->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(START.time_since_epoch()).count());
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] Handle recovery" << std::endl;
      }

      std::string code_type = m_sys_config->CodeType;
      int cross_rack_num = recovery_request->cross_rack_num();
      // auto status = std::make_shared<std::vector<bool>>(recovery_request->datanodeip_size(), false);
      std::unique_ptr<bool[]> status(new bool[recovery_request->datanodeip_size()]);
      std::fill_n(status.get(), recovery_request->datanodeip_size(), false);

      //std::vector<std::vector<char>> get_bufs(recovery_request->datanodeip_size(), std::vector<char>(m_sys_config->BlockSize, 0));
      std::vector<char*> get_bufs(recovery_request->datanodeip_size());
      for(int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      }
      //std::vector<char> res_buf(m_sys_config->BlockSize, 0);
      char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      char *real_res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));

      std::vector<double> data_node_disk_io_start_time(recovery_request->datanodeip_size(), 0.0);
      std::vector<double> data_node_disk_io_end_time(recovery_request->datanodeip_size(), 0.0);
      std::vector<double> data_node_network_start_time(recovery_request->datanodeip_size(), 0.0);
      std::vector<double> data_node_network_end_time(recovery_request->datanodeip_size(), 0.0);
      std::vector<double> data_node_grpc_notify_time(recovery_request->datanodeip_size(), 0.0);
      std::vector<double> data_node_grpc_start_time(recovery_request->datanodeip_size(), 0.0);

      std::vector<std::thread> get_threads;
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_threads.push_back(std::thread(&ProxyImpl::get_from_node_breakdown, this, 
          recovery_request->blockkeys(i), get_bufs[i], m_sys_config->BlockSize, recovery_request->datanodeip(i).c_str(), 
          recovery_request->datanodeport(i), status.get(), i, &data_node_disk_io_start_time[i], &data_node_disk_io_end_time[i],
          &data_node_network_start_time[i], &data_node_network_end_time[i], &data_node_grpc_notify_time[i], &data_node_grpc_start_time[i]));
      }
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_threads[i].join();
      }

      response->set_disk_io_start_time(*std::min_element(data_node_disk_io_start_time.begin(), data_node_disk_io_start_time.end()));
      response->set_disk_io_end_time(*std::max_element(data_node_disk_io_end_time.begin(), data_node_disk_io_end_time.end()));
      response->set_network_start_time(*std::min_element(data_node_network_start_time.begin(), data_node_network_start_time.end()));
      response->set_network_end_time(*std::max_element(data_node_network_end_time.begin(), data_node_network_end_time.end()));
      response->set_data_node_grpc_notify_time(*std::min_element(data_node_grpc_notify_time.begin(), data_node_grpc_notify_time.end()));
      response->set_data_node_grpc_start_time(*std::max_element(data_node_grpc_start_time.begin(), data_node_grpc_start_time.end()));
      
      bool all_true = std::all_of(status.get(), status.get() + recovery_request->datanodeip_size(), [](bool val)
                                  { return val == true; });
      if (!all_true)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes failed!" << std::endl;
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes success!" << std::endl;

        std::vector<int> block_idxs;
        for (int i = 0; i < recovery_request->datanodeip_size(); i++)
        {
          block_idxs.push_back(recovery_request->blockids(i));
        }
        std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

        std::string failed_block_key = recovery_request->failed_block_key();
        int failed_block_id = recovery_request->failed_block_id();
        std::string replaced_node_ip = recovery_request->replaced_node_ip();
        int replaced_node_port = recovery_request->replaced_node_port();

        std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
        if (code_type == "UniLRC")
        {
          decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
        }
        else if (is_azure_like_code(code_type))
        {
          decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else if (code_type == "OptimalLRC")
        {
          decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else if (code_type == "UniformLRC")
        {
          decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
          exit(1);
        }
        std::chrono::high_resolution_clock::time_point t4 = std::chrono::high_resolution_clock::now();
        response->set_decode_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(t3.time_since_epoch()).count());
        response->set_decode_end_time(std::chrono::duration_cast<std::chrono::duration<double>>(t4.time_since_epoch()).count());

        if(cross_rack_num){
          std::cout << "start to recover cross rack" << std::endl;
          char **cross_rack_bufs = new char*[cross_rack_num];
          for(int i = 0; i < cross_rack_num; i++)
          {
            cross_rack_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
          }
          std::vector<std::thread> get_from_proxies_threads;
          std::vector<double> accept_start_time(cross_rack_num, 0.0);
          for(int i = 0; i < cross_rack_num; i++)
          {
            get_from_proxies_threads.push_back(std::thread([i, this, &cross_rack_bufs, &accept_start_time]()mutable{
              //asio::io_context io_context;
              asio::ip::tcp::socket socket(this->io_context);
              //asio::ip::tcp::resolver resolver(io_context);
              std::cout << "connecting to proxy" << std::endl;
              this->acceptor.accept(socket);
              std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
              accept_start_time[i] = std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count();
              std::cout << "connected to porxy" << std::endl;
              asio::error_code error;
              asio::read(socket, asio::buffer(cross_rack_bufs[i], this->m_sys_config->BlockSize), error);
              std::cout << "read from proxy"  << std::endl;
              if(error)
              {
                std::cout << "error in read" << std::endl;
              }
              asio::error_code ignore_ec;
              socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
              socket.close(ignore_ec);
            }));
          }
          for(int i = 0; i < cross_rack_num; i++)
          {
            get_from_proxies_threads[i].join();
          }
          double min_accept_start_time = *std::min_element(accept_start_time.begin(), accept_start_time.end());
          std::chrono::high_resolution_clock::time_point accept_end_time = std::chrono::high_resolution_clock::now();
          double time_span3 = std::chrono::duration_cast<std::chrono::duration<double>>(accept_end_time.time_since_epoch()).count() - min_accept_start_time;
          response->set_cross_rack_time(time_span3);
          std::cout << "start to xor" << std::endl;
          char **buf_ptrs = new char*[cross_rack_num + 2];
          for(int i = 0; i < cross_rack_num; i++)
          {
            buf_ptrs[i] = cross_rack_bufs[i];
          }
          buf_ptrs[cross_rack_num] = res_buf;
          buf_ptrs[cross_rack_num + 1] = real_res_buf;
          std::chrono::high_resolution_clock::time_point t7 = std::chrono::high_resolution_clock::now();
          xor_avx(cross_rack_num + 2, m_sys_config->BlockSize, (void**)buf_ptrs);
          std::chrono::high_resolution_clock::time_point t8 = std::chrono::high_resolution_clock::now();
          std::chrono::duration<double> time_span4 = std::chrono::duration_cast<std::chrono::duration<double>>(t8 - t7);
          response->set_cross_rack_xor_time(time_span4.count());
          for(int i = 0; i < cross_rack_num; i++)
          {
            delete cross_rack_bufs[i];
          }
          delete cross_rack_bufs;
          delete buf_ptrs;
        }
        else
        {
          response->set_cross_rack_time(0);
          response->set_cross_rack_xor_time(0);
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode success!" << std::endl;
        }
        std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] send to the replaced node" << std::endl;
        // send to the replaced node
        double dest_data_node_network_time, dest_data_node_disk_io_time;
        if(cross_rack_num){
          RecoveryToDatanodeBreakdown(failed_block_key.c_str(), failed_block_id, real_res_buf, replaced_node_ip.c_str(), replaced_node_port, 
            &dest_data_node_network_time, &dest_data_node_disk_io_time);
        }
        else{
          RecoveryToDatanodeBreakdown(failed_block_key.c_str(), failed_block_id, res_buf, replaced_node_ip.c_str(), replaced_node_port, 
            &dest_data_node_network_time, &dest_data_node_disk_io_time);
        }
        response->set_dest_data_node_network_time(dest_data_node_network_time);
        response->set_dest_data_node_disk_io_time(dest_data_node_disk_io_time);
      }
      delete res_buf;
      delete real_res_buf;
      for(int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        delete get_bufs[i];
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cerr << e.what() << '\n';
    }
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::partialDecoding(
    grpc::ServerContext *context,
    const proxy_proto::PartialDecodingRequest *partial_decoding_request,
    proxy_proto::DegradedReadReply *response)
  {
    int block_size = m_sys_config->BlockSize;
    int source_num = partial_decoding_request->source_block_ids_size();
    int decode_num = partial_decoding_request->decode_num();
    unsigned char *source_buf_ptrs[source_num];
    unsigned char *decode_buf_ptrs[decode_num];
    for(int i = 0; i < source_num; i++)
    {
      source_buf_ptrs[i] = static_cast<unsigned char*>(std::aligned_alloc(32, block_size));
    }
    std::vector<std::thread> get_threads;
    for(int i = 0; i < source_num; i++)
    {
      get_threads.push_back(std::thread([this, i, &partial_decoding_request, &source_buf_ptrs, block_size]() {
        this->GetFromDatanode(
            partial_decoding_request->source_block_keys(i), 
            reinterpret_cast<char*>(source_buf_ptrs[i]),
            static_cast<size_t>(block_size),
            partial_decoding_request->source_datanode_ips(i).c_str(),
            static_cast<int>(partial_decoding_request->source_datanode_ports(i))
        );
      }));
    }
    for(int i = 0; i < decode_num; i++)
    {
      decode_buf_ptrs[i] = static_cast<unsigned char*>(std::aligned_alloc(32, block_size));
    }
    unsigned char decode_matrix[source_num * decode_num];
    for(int i = 0; i < source_num * decode_num; i++)
    {
      decode_matrix[i] = partial_decoding_request->decode_factors(i);
    }
    for(int i = 0; i < source_num; i++)
    {
      get_threads[i].join();
    }
    ECProject::encode_data(block_size, source_num, decode_num, decode_matrix, source_buf_ptrs, decode_buf_ptrs);
    std::string dest_proxy_ip = partial_decoding_request->dest_ip();
    int dest_proxy_port = partial_decoding_request->dest_port();
    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    asio::ip::tcp::resolver resolver(io_context);
    asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(dest_proxy_ip, std::to_string(dest_proxy_port));
    asio::connect(socket, endpoints);
    for(int i = 0; i < decode_num; i++)
    {
      asio::write(socket, asio::buffer(decode_buf_ptrs[i], block_size));
      delete decode_buf_ptrs[i];
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    socket.close(ignore_ec);
    for(int i = 0; i < source_num; i++)
    {
      delete source_buf_ptrs[i];
    }
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::multipleRecovery(
    grpc::ServerContext *context,
    const proxy_proto::MultipleRecoveryRequest *multiple_recovery_request,
    proxy_proto::GetReply *response)
  {
  try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][MultipleRecovery] Handle multiple recovery" << std::endl;
      }

      int resource_proxy_num = multiple_recovery_request->cross_rack_num();
      int block_num = multiple_recovery_request->failed_block_keys_size();
      int block_size = m_sys_config->BlockSize;
      std::string replacing_node_ip = multiple_recovery_request->replacing_node_ip();
      int replacing_node_port = multiple_recovery_request->replacing_node_port();

      // collect failed block keys (order matters: 横向放置顺序)
      std::vector<std::string> block_keys;
      for (int i = 0; i < block_num; ++i)
        block_keys.push_back(multiple_recovery_request->failed_block_keys(i));

      if (resource_proxy_num <= 0 || block_num <= 0)
      {
        std::cerr << "[MultipleRecovery] invalid parameters: resource_proxy_num=" << resource_proxy_num << " block_num=" << block_num << std::endl;
        return grpc::Status::OK;
      }

      size_t per_proxy_len = static_cast<size_t>(block_num) * static_cast<size_t>(block_size);

      // allocate buffers to receive contiguous data from each resource proxy
      char **proxy_bufs = new char *[resource_proxy_num];
      for (int i = 0; i < resource_proxy_num; ++i)
      {
        proxy_bufs[i] = static_cast<char *>(std::aligned_alloc(32, per_proxy_len));
        memset(proxy_bufs[i], 0, per_proxy_len);
      }

      // accept resource_proxy_num connections and read their contiguous payloads
      for (int i = 0; i < resource_proxy_num; ++i)
      {
        try
        {
          asio::ip::tcp::socket socket(this->io_context);
          this->acceptor.accept(socket);
          asio::error_code ec;
          size_t read_bytes = 0;
          while (read_bytes < per_proxy_len)
          {
            read_bytes += asio::read(socket, asio::buffer(proxy_bufs[i] + read_bytes, per_proxy_len - read_bytes), ec);
            if (ec && ec != asio::error::eof)
            {
              std::cerr << "[MultipleRecovery] read error from proxy #" << i << " : " << ec.message() << std::endl;
              break;
            }
            if (ec == asio::error::eof) break;
          }
          asio::error_code ignore_ec;
          socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
          socket.close(ignore_ec);
          if (IF_DEBUG)
            std::cout << "[MultipleRecovery] received " << read_bytes << " bytes from proxy #" << i << std::endl;
        }
        catch (const std::exception &e)
        {
          std::cerr << "[MultipleRecovery] exception while receiving from proxy #" << i << " : " << e.what() << std::endl;
        }
      }

      // prepare per-block output buffers and compute XOR across proxies for each block slice
      char **out_blocks = new char *[block_num];
      for (int b = 0; b < block_num; ++b)
      {
        out_blocks[b] = static_cast<char *>(std::aligned_alloc(32, block_size));
        // initialize to zero
        memset(out_blocks[b], 0, block_size);

        // XOR all proxy buffers' slice b into out_blocks[b]
        for (int p = 0; p < resource_proxy_num; ++p)
        {
          char *src = proxy_bufs[p] + static_cast<size_t>(b) * block_size;
          // use 64-bit XOR when possible for speed
          size_t t = 0;
          const size_t WORD_SZ = sizeof(uint64_t);
          for (; t + WORD_SZ <= static_cast<size_t>(block_size); t += WORD_SZ)
          {
            uint64_t *dstw = reinterpret_cast<uint64_t *>(out_blocks[b] + t);
            uint64_t *srcw = reinterpret_cast<uint64_t *>(src + t);
            *dstw ^= *srcw;
          }
          // tail bytes
          for (; t < static_cast<size_t>(block_size); ++t)
            out_blocks[b][t] ^= src[t];
        }
      }

      // send each recovered block to the replacing datanode
      for (int b = 0; b < block_num; ++b)
      {
        try
        {
          // Use RecoveryToDatanode to notify datanode and push block content.
          // block id: here we don't have explicit id mapping in the request, use index b.
          // If your system needs specific block ids, adapt to include them in the request.
          RecoveryToDatanode(block_keys[b].c_str(), b, out_blocks[b], replacing_node_ip.c_str(), replacing_node_port);
          if (IF_DEBUG)
            std::cout << "[MultipleRecovery] sent recovered block " << block_keys[b] << " (index " << b << ") to " << replacing_node_ip << ":" << replacing_node_port << std::endl;
        }
        catch (const std::exception &e)
        {
          std::cerr << "[MultipleRecovery] exception while sending block " << b << " : " << e.what() << std::endl;
        }
      }

      // free temp buffers
      for (int i = 0; i < resource_proxy_num; ++i)
      {
        delete proxy_bufs[i];
      }
      delete[] proxy_bufs;

      for (int b = 0; b < block_num; ++b)
      {
        delete out_blocks[b];
      }
      delete[] out_blocks;
    }
    catch (const std::exception &e)
    {
      std::cerr << "[MultipleRecovery] top-level exception: " << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }
  // delete
  grpc::Status ProxyImpl::deleteBlock(
      grpc::ServerContext *context,
      const proxy_proto::NodeAndBlock *node_and_block,
      proxy_proto::DelReply *response)
  {
    std::vector<std::string> blocks_id;
    std::vector<std::string> nodes_ip_port;
    std::string key = node_and_block->key();
    int stripe_id = node_and_block->stripe_id();
    for (int i = 0; i < node_and_block->blockkeys_size(); i++)
    {
      blocks_id.push_back(node_and_block->blockkeys(i));
      std::string ip_port = node_and_block->datanodeip(i) + ":" + std::to_string(node_and_block->datanodeport(i));
      nodes_ip_port.push_back(ip_port);
    }
    auto delete_blocks = [this, key, blocks_id, stripe_id, nodes_ip_port]() mutable
    {
      auto request_and_delete = [this](std::string block_key, std::string node_ip_port)
      {
        bool ret = DelInDatanode(block_key, node_ip_port);
        if (!ret)
        {
          std::cout << "Delete value no return!" << std::endl;
          return;
        }
      };
      try
      {
        std::vector<std::thread> senders;
        for (int j = 0; j < int(blocks_id.size()); j++)
        {
          senders.push_back(std::thread(request_and_delete, blocks_id[j], nodes_ip_port[j]));
        }

        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }

        if (stripe_id != -1 || key != "")
        {
          grpc::ClientContext c_context;
          coordinator_proto::CommitAbortKey commit_abort_key;
          coordinator_proto::ReplyFromCoordinator rep;
          ECProject::OpperateType opp = DEL;
          commit_abort_key.set_opp(opp);
          commit_abort_key.set_key(key);
          commit_abort_key.set_ifcommitmetadata(true);
          commit_abort_key.set_stripe_id(stripe_id);
          grpc::Status stat;
          stat = m_coordinator_ptr->reportCommitAbort(&c_context, commit_abort_key, &rep);
        }
      }
      catch (const std::exception &e)
      {
        std::cout << "exception" << std::endl;
        std::cerr << e.what() << '\n';
      }
    };
    try
    {
      std::thread my_thread(delete_blocks);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::rackCuPullXferTiming(grpc::ServerContext *context,
                                                    const proxy_proto::RackCuXferTimingPullRequest *request,
                                                    proxy_proto::RackCuXferTimingReply *response)
  {
    (void)context;
    const std::pair<int, uint64_t> key{request->stripe_id(), request->xfer_plan_id()};
    std::lock_guard<std::mutex> lk(g_rackcu_batch_xfer_mu);
    const auto it = g_rackcu_batch_xfer.find(key);
    if (it == g_rackcu_batch_xfer.end() || !it->second.have)
    {
      response->set_had_samples(false);
      response->set_proxy_pure_xfer_sec(0.0);
      response->set_wall_span_start_unix_ms(0);
      response->set_wall_span_end_unix_ms(0);
      return grpc::Status::OK;
    }
    response->set_had_samples(true);
    response->set_proxy_pure_xfer_sec(it->second.pure_xfer_sec_sum);
    response->set_wall_span_start_unix_ms(it->second.wall_min_ms);
    response->set_wall_span_end_unix_ms(it->second.wall_max_ms);
    g_rackcu_batch_xfer.erase(it);
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::getBlocks(grpc::ServerContext *context,
    const proxy_proto::StripeAndBlockIDs *request, proxy_proto::GetReply *response)
  {
    std::cout << "getting blocks" << "[" << request->block_ids(0) << "]" << "to" << "[" << request->block_ids(request->block_ids_size() - 1) << "]" << std::endl;
    int BlockSize = m_sys_config->BlockSize;
    size_t total_size = static_cast<size_t> (BlockSize) * request->block_ids_size();
    char *blocks = new char[total_size];
    uint32_t group_id = request->group_id();

    std::vector<std::thread> get_threads;
    for(int i = 0; i < request->block_ids_size(); i++)
    {
      get_threads.push_back(std::thread([this, i, &blocks, &request, BlockSize]() {
        this->GetFromDatanode(
            request->block_keys(i), 
            blocks + i * BlockSize,
            static_cast<size_t>(m_sys_config->BlockSize), 
            request->datanodeips(i).c_str(), 
            static_cast<int>(request->datanodeports(i))
        );    
      }));
      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket_data(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(request->clientip(), std::to_string(request->clientport()));;
      socket_data.connect(*endpoints, error);
      if (error)
      {
        std::cout << "error in connect" << std::endl;
      }
      std::cout << "connected to client" << std::endl;
      u_int32_t block_id = request->block_ids(i);
      apply_kernel_bandwidth_to_peer_socket(socket_data, request->clientip().c_str());
      asio::write(socket_data, asio::buffer(&block_id, sizeof(u_int32_t)), error);
      asio::write(socket_data, asio::buffer(blocks + i * static_cast<size_t>(BlockSize), BlockSize), error);
      asio::error_code ignore_ec;
      socket_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      socket_data.close(ignore_ec);
    }

    for(int i = 0; i < request->block_ids_size(); i++)
    {
      get_threads[i].join();
    }

    delete blocks;
    return grpc::Status();
  }

} // namespace ECProject