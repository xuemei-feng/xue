#include "proxy.h"
#include "jerasure.h"
#include "jerasure/galois.h"
#include "reed_sol.h"
#include "tinyxml2.h"
#include "toolbox.h"
#include "lrc.h"
#include <thread>
#include <atomic>
#include <cassert>
#include <string>
#include <fstream>
#include <sys/mman.h>
#include "unilrc_encoder.h"
#include <chrono>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <map>
#include <set>
#include <sstream>

namespace
{
  std::mutex &xue_forward_dest_cluster_mutex(int stripe_id, int dest_cluster_id)
  {
    static std::mutex map_mutex;
    static std::map<std::pair<int, int>, std::unique_ptr<std::mutex>> mutexes;
    const std::pair<int, int> key(stripe_id, dest_cluster_id);
    std::lock_guard<std::mutex> lk(map_mutex);
    std::unique_ptr<std::mutex> &slot = mutexes[key];
    if (!slot)
    {
      slot = std::make_unique<std::mutex>();
    }
    return *slot;
  }

  std::string proxy_xfer_timestamp()
  {
    const auto tp = std::chrono::system_clock::now();
    const std::time_t tt = std::chrono::system_clock::to_time_t(tp);
    std::tm st{};
#if defined(_WIN32)
    localtime_s(&st, &tt);
#else
    localtime_r(&tt, &st);
#endif
    std::ostringstream oss;
    oss << std::put_time(&st, "%F %T");
    return oss.str();
  }

  std::string fmt_proxy_cluster(int cluster_id)
  {
    return "cluster-" + std::to_string(cluster_id);
  }

  void log_xfert_line(const std::string &ts, const std::string &line)
  {
    std::cout << "[Proxy][XFERT] " << ts << " " << line << std::endl;
  }

  int64_t xue_wall_unix_ms_now()
  {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  struct XueXferScopeTimer
  {
    ECProject::ProxyImpl *self = nullptr;
    int stripe_id = -1;
    uint64_t plan_id = 0;
    bool active = false;
    std::chrono::steady_clock::time_point t0{};
    int64_t w0 = 0;

    void flush()
    {
      if (!active || self == nullptr || plan_id == 0)
      {
        active = false;
        return;
      }
      active = false;
      self->record_xue_xfer_sample(stripe_id, plan_id, t0, std::chrono::steady_clock::now(), w0,
                                   xue_wall_unix_ms_now());
    }

    ~XueXferScopeTimer() { flush(); }
  };

  std::string fmt_proxy_tcp_route(int from_cluster, int to_cluster)
  {
    if (from_cluster < 0)
    {
      return "client->proxy_cluster=" + std::to_string(to_cluster);
    }
    return "proxy_cluster=" + std::to_string(from_cluster) + "->proxy_cluster=" +
           std::to_string(to_cluster);
  }

  int effective_tcp_slice_count(const proxy_proto::AppendStripeDataPlacement &placement,
                                int plan_block_count)
  {
    if (placement.xue_tcp_slice_count() > 0)
    {
      return placement.xue_tcp_slice_count();
    }
    return plan_block_count;
  }

  bool is_xue_proxy_forward_tcp_mode(const std::string &append_mode)
  {
    return append_mode == "XUE_DELTA_TO_RELAY" || append_mode == "XUE_DELTA_TO_GLOBAL" ||
           append_mode == "XUE_COMPUTE_LOCAL_PARITY" || append_mode == "XUE_LOCAL_PARITY_DELTA";
  }

  struct ParityDeltaSlice
  {
    int offset = 0;
    int length = 0;
    std::vector<char> delta;
  };

  void xor_delta_into_merged(std::vector<char> *merged, int merged_base_off, const ParityDeltaSlice &slice)
  {
    if (merged == nullptr || slice.length <= 0)
    {
      return;
    }
    const int rel = slice.offset - merged_base_off;
    if (rel < 0 || rel + slice.length > static_cast<int>(merged->size()))
    {
      return;
    }
    for (int t = 0; t < slice.length; ++t)
    {
      (*merged)[static_cast<size_t>(rel + t)] = static_cast<char>(
          static_cast<unsigned char>((*merged)[static_cast<size_t>(rel + t)]) ^
          static_cast<unsigned char>(slice.delta[static_cast<size_t>(t)]));
    }
  }

  std::vector<ParityDeltaSlice> merge_parity_delta_slices(std::vector<ParityDeltaSlice> slices)
  {
    if (slices.empty())
    {
      return slices;
    }
    std::sort(slices.begin(), slices.end(),
              [](const ParityDeltaSlice &a, const ParityDeltaSlice &b) { return a.offset < b.offset; });
    std::vector<ParityDeltaSlice> merged;
    ParityDeltaSlice cur = std::move(slices[0]);
    for (size_t i = 1; i < slices.size(); ++i)
    {
      const ParityDeltaSlice &n = slices[i];
      const int cur_r = cur.offset + cur.length - 1;
      if (n.offset <= cur_r + 1)
      {
        const int new_r = std::max(cur_r, n.offset + n.length - 1);
        const int new_off = cur.offset;
        const int new_len = new_r - new_off + 1;
        std::vector<char> acc(static_cast<size_t>(new_len), 0);
        xor_delta_into_merged(&acc, new_off, cur);
        xor_delta_into_merged(&acc, new_off, n);
        cur.offset = new_off;
        cur.length = new_len;
        cur.delta = std::move(acc);
      }
      else
      {
        merged.push_back(std::move(cur));
        cur = std::move(slices[i]);
      }
    }
    merged.push_back(std::move(cur));
    return merged;
  }

  int find_local_parity_plan_index(const proxy_proto::AppendStripeDataPlacement &placement, int k, int r)
  {
    const int local_begin = k;
    const int local_end = k + r;
    for (int j = 0; j < placement.blockids_size(); ++j)
    {
      const int bid = placement.blockids(j);
      if (bid >= local_begin && bid < local_end)
      {
        return j;
      }
    }
    return -1;
  }

  int infer_data_block_cluster_from_placement(const proxy_proto::AppendStripeDataPlacement &placement,
                                              int tcp_slice_count, int k)
  {
    for (int si = 0; si < tcp_slice_count && si < placement.blockids_size(); ++si)
    {
      if (placement.blockids(si) >= 0 && placement.blockids(si) < k &&
          si < placement.block_cluster_ids_size())
      {
        return placement.block_cluster_ids(si);
      }
    }
    return -1;
  }

  bool infer_local_parity_cluster_from_plan(const proxy_proto::AppendStripeDataPlacement &placement,
                                            int k, int r, int *out_local_cluster)
  {
    if (out_local_cluster == nullptr)
    {
      return false;
    }
    *out_local_cluster = -1;
    const int lp_idx = find_local_parity_plan_index(placement, k, r);
    if (lp_idx >= 0 && lp_idx < placement.block_cluster_ids_size())
    {
      *out_local_cluster = placement.block_cluster_ids(lp_idx);
      return *out_local_cluster >= 0;
    }
    return false;
  }

  bool infer_class2_remote_local_parity_cluster(
      const proxy_proto::AppendStripeDataPlacement &placement, int k, int r, int tcp_slice_count,
      int proxy_cluster_id, int *out_local_cluster)
  {
    if (out_local_cluster == nullptr || !placement.xue_compute_global_parity())
    {
      return false;
    }
    const int data_cluster = infer_data_block_cluster_from_placement(placement, tcp_slice_count, k);
    const int global_cluster = placement.xue_global_parity_cluster_id();
    if (data_cluster < 0 || global_cluster < 0 || data_cluster != global_cluster ||
        proxy_cluster_id != data_cluster)
    {
      return false;
    }
    if (infer_local_parity_cluster_from_plan(placement, k, r, out_local_cluster) &&
        *out_local_cluster >= 0 && *out_local_cluster != proxy_cluster_id)
    {
      return true;
    }
    *out_local_cluster = -1;
    for (int j = 0; j < placement.blockids_size(); ++j)
    {
      const int bid = placement.blockids(j);
      if (bid >= k && bid < k + r && j < placement.block_cluster_ids_size())
      {
        const int cl = placement.block_cluster_ids(j);
        if (cl >= 0 && cl != proxy_cluster_id)
        {
          *out_local_cluster = cl;
          return true;
        }
      }
    }
    return false;
  }

  // 仅用于日志：推断本 proxy TCP 接收的上一跳 cluster（不改变传输）
  int infer_tcp_source_cluster_for_recv(const proxy_proto::AppendStripeDataPlacement &placement,
                                        const std::string &append_mode, int recv_proxy_cluster,
                                        int tcp_slice_count, int k)
  {
    if (append_mode == "XUE_DELTA_TO_RELAY")
    {
      return infer_data_block_cluster_from_placement(placement, tcp_slice_count, k);
    }
    if (append_mode == "XUE_DELTA_TO_GLOBAL")
    {
      const int data_cluster =
          infer_data_block_cluster_from_placement(placement, tcp_slice_count, k);
      if (data_cluster >= 0 && data_cluster != recv_proxy_cluster)
      {
        return data_cluster;
      }
      if (placement.xue_relay_cluster_id() >= 0 &&
          placement.xue_relay_cluster_id() != recv_proxy_cluster)
      {
        return placement.xue_relay_cluster_id();
      }
      return -1;
    }
    if (append_mode == "XUE_COMPUTE_LOCAL_PARITY" || append_mode == "XUE_LOCAL_PARITY_DELTA")
    {
      return infer_data_block_cluster_from_placement(placement, tcp_slice_count, k);
    }
    return -1;
  }

  bool is_client_tcp_ingress(const std::string &append_mode,
                             const proxy_proto::AppendStripeDataPlacement &placement)
  {
    return append_mode == "XUE_UPDATE" && !placement.xue_data_slices_are_delta();
  }

  bool is_layout_stripe_append_mode(const std::string &append_mode)
  {
    return append_mode == "UNILRC_MODE" || append_mode == "CACHED_MODE";
  }

  std::string format_block_id_list(const std::vector<int> &block_ids)
  {
    std::ostringstream oss;
    for (size_t i = 0; i < block_ids.size(); ++i)
    {
      if (i > 0)
      {
        oss << ",";
      }
      oss << block_ids[i];
    }
    return oss.str();
  }

  void append_key_tcp_meta_suffix(std::ostringstream &oss, const std::string &append_key)
  {
    std::vector<int> tcp_ids;
    std::vector<int> meta_ids;
    if (ECProject::ToolBox::getInstance()->parse_append_key_tcp_block_ids(append_key, &tcp_ids))
    {
      oss << " tcp_blocks=" << format_block_id_list(tcp_ids);
    }
    if (ECProject::ToolBox::getInstance()->parse_append_key_meta_block_ids(append_key, &meta_ids))
    {
      oss << " meta_blocks=" << format_block_id_list(meta_ids);
    }
  }

  void log_layout_client_batch(int proxy_cluster, int stripe_id,
                               const std::vector<int> &tcp_block_ids, size_t bytes, int tcp_slices)
  {
    std::ostringstream oss;
    oss << "layout client->proxy_cluster=" << proxy_cluster << " stripe_id=" << stripe_id
        << " tcp_blocks=" << format_block_id_list(tcp_block_ids) << " bytes=" << bytes
        << " slices=" << tcp_slices;
    log_xfert_line(proxy_xfer_timestamp(), oss.str());
  }

  void log_layout_set_done(int proxy_cluster, const std::string &object_key, int blocks,
                           int block_size, bool grpc_ok)
  {
    std::ostringstream oss;
    oss << "layout_set proxy_cluster=" << proxy_cluster << " object_key=" << object_key
        << " blocks=" << blocks << " block_size=" << block_size
        << " grpc_ok=" << (grpc_ok ? "true" : "false");
    log_xfert_line(proxy_xfer_timestamp(), oss.str());
  }

  void log_proxy_tcp_hop(const std::string &ts, int from_cluster, int to_cluster,
                         const std::string &append_key, size_t byte_count, int tcp_slices)
  {
    std::ostringstream oss;
    oss << "proxy_tcp " << fmt_proxy_tcp_route(from_cluster, to_cluster) << " append_key="
        << append_key << " bytes=" << byte_count << " slices=" << tcp_slices;
    log_xfert_line(ts, oss.str());
  }

  void log_proxy_tcp_relay_passthrough(const std::string &ts, int relay_cluster, int in_from_cluster,
                                       int out_to_cluster, const std::string &append_key,
                                       size_t byte_count, int tcp_slices)
  {
    std::ostringstream oss;
    oss << "proxy_tcp_relay proxy_cluster=" << relay_cluster << " append_key=" << append_key
        << " in=" << in_from_cluster << "->" << relay_cluster << " out=" << relay_cluster << "->"
        << out_to_cluster << " bytes=" << byte_count << " slices=" << tcp_slices;
    log_xfert_line(ts, oss.str());
  }

  void log_proxy_tcp_global_done(const std::string &ts, int global_cluster, int from_cluster,
                                 const std::string &append_key, size_t tcp_byte_count, int tcp_slices,
                                 const ECProject::XueParityWriteStats &parity_write)
  {
    std::ostringstream oss;
    oss << "proxy_tcp_global proxy_cluster=" << global_cluster << " from=" << from_cluster
        << " append_key=" << append_key << " tcp_bytes=" << tcp_byte_count
        << " tcp_slices=" << tcp_slices << " parity_ranges=" << parity_write.ranges
        << " parity_bytes=" << parity_write.bytes;
    log_xfert_line(ts, oss.str());
  }

  void log_xue_parity_write_applied(const std::string &ts, const char *event, int proxy_cluster,
                                    const std::string &append_key,
                                    const ECProject::XueParityWriteStats &parity_write,
                                    const char *route_tag = nullptr, size_t tcp_bytes = 0,
                                    int tcp_slices = 0)
  {
    std::ostringstream oss;
    oss << event << " proxy_cluster=" << proxy_cluster << " append_key=" << append_key;
    if (tcp_bytes > 0 || tcp_slices > 0)
    {
      oss << " tcp_bytes=" << tcp_bytes << " tcp_slices=" << tcp_slices;
    }
    oss << " parity_ranges=" << parity_write.ranges << " parity_bytes=" << parity_write.bytes;
    if (route_tag != nullptr && route_tag[0] != '\0')
    {
      oss << " " << route_tag;
    }
    log_xfert_line(ts, oss.str());
  }

  void log_append_commit_report(const std::string &ts, int proxy_cluster,
                                const std::string &append_key, int stripe_id, bool grpc_ok)
  {
    std::ostringstream oss;
    oss << "append_commit_report proxy_cluster=" << proxy_cluster << " append_key=" << append_key
        << " stripe_id=" << stripe_id << " grpc_ok=" << (grpc_ok ? "true" : "false");
    log_xfert_line(ts, oss.str());
  }

  std::string xue_xfer_payload_desc(const std::string &append_mode)
  {
    if (append_mode == "XUE_LOCAL_PARITY_DELTA")
    {
      return "本地校验增量(local_parity_delta)";
    }
    if (append_mode == "XUE_DELTA_TO_RELAY" || append_mode == "XUE_DELTA_TO_GLOBAL" ||
        append_mode == "XUE_COMPUTE_LOCAL_PARITY")
    {
      return "数据增量(data_delta)";
    }
    return "载荷(" + append_mode + ")";
  }

  std::string xue_tcp_payload_desc(const proxy_proto::AppendStripeDataPlacement &placement)
  {
    const std::string &mode = placement.append_mode();
    if (mode == "XUE_UPDATE")
    {
      if (placement.xue_data_slices_are_delta())
      {
        return "数据增量(data_delta)";
      }
      return "数据新值(client_new_value)";
    }
    return xue_xfer_payload_desc(mode);
  }

  std::string xue_route_tag(const proxy_proto::AppendStripeDataPlacement &placement)
  {
    if (placement.append_mode() != "XUE_UPDATE")
    {
      return "";
    }
    if (placement.xue_class1_relay_path())
    {
      return "route=class1_relay";
    }
    if (placement.xue_data_slices_are_delta())
    {
      return "route=proxy_forward_hop";
    }
    if (placement.xue_compute_global_parity())
    {
      return "route=class2_ingress_data_global";
    }
    const std::string &plan_key = placement.key();
    const size_t class_pos = plan_key.find("_class");
    if (class_pos != std::string::npos)
    {
      const size_t num_begin = class_pos + 6;
      const size_t cpos = plan_key.find('c', num_begin);
      if (cpos != std::string::npos && cpos > num_begin)
      {
        const int class_sub = std::stoi(plan_key.substr(num_begin, cpos - num_begin));
        if (class_sub == 1)
        {
          return "route=class1_subgroup";
        }
        if (class_sub == 2)
        {
          return "route=class2_subgroup";
        }
        if (class_sub == 3)
        {
          return "route=class3_subgroup";
        }
      }
    }
    return "route=xue_update";
  }

  void append_xue_global_parity_plan_suffix(std::ostringstream &oss,
                                            const proxy_proto::AppendStripeDataPlacement &placement)
  {
    if (placement.append_mode() != "XUE_UPDATE")
    {
      return;
    }
    oss << " global_parity_cluster=" << placement.xue_global_parity_cluster_id()
        << " compute_global_parity="
        << (placement.xue_compute_global_parity() ? "true" : "false")
        << " class1_relay=" << (placement.xue_class1_relay_path() ? "true" : "false");
  }

  // 第1类：本地校验块仅在 plan 元数据中（j >= tcp_slice_count），且与数据块同 cluster
  bool infer_class1_local_parity_cluster(const proxy_proto::AppendStripeDataPlacement &placement,
                                         int k, int r, int *out_local_cluster)
  {
    if (out_local_cluster == nullptr)
    {
      return false;
    }
    *out_local_cluster = -1;
    if (placement.append_mode() != "XUE_UPDATE")
    {
      return false;
    }
    const int tcp = placement.xue_tcp_slice_count() > 0 ? placement.xue_tcp_slice_count() : 0;
    int data_cluster = -1;
    bool has_local_meta = false;
    int local_cluster = -1;
    const int slice_num = placement.blockids_size();
    for (int j = 0; j < slice_num; ++j)
    {
      const int bid = placement.blockids(j);
      const int cl = (j < placement.block_cluster_ids_size()) ? placement.block_cluster_ids(j) : -1;
      if (bid >= 0 && bid < k && j < tcp)
      {
        if (data_cluster < 0)
        {
          data_cluster = cl;
        }
        else if (cl >= 0 && data_cluster >= 0 && cl != data_cluster)
        {
          return false;
        }
      }
      if (bid >= k && bid < k + r && j >= tcp)
      {
        has_local_meta = true;
        if (local_cluster < 0)
        {
          local_cluster = cl;
        }
        else if (cl >= 0 && local_cluster >= 0 && cl != local_cluster)
        {
          return false;
        }
      }
    }
    if (!has_local_meta || data_cluster < 0 || local_cluster < 0 || data_cluster != local_cluster)
    {
      return false;
    }
    *out_local_cluster = local_cluster;
    return true;
  }
} // namespace

template <typename T>
inline T ceil(T const &A, T const &B)
{
  return T((A + B - 1) / B);
};
namespace ECProject
{
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
      const size_t colon = proxy.find(':');
      if (colon != std::string::npos)
      {
        m_cluster_proxy_endpoints[std::stoi(cluster_id)] = std::make_pair(
            proxy.substr(0, colon), std::stoi(proxy.substr(colon + 1)));
      }
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
        m_datanode_endpoint_to_cluster[node_uri] = std::stoi(cluster_id);
      }
    }
    return true;
  }

  int ProxyImpl::clusterIdForDatanodeEndpoint(const std::string &ip, int port) const
  {
    const std::string key = ip + ":" + std::to_string(port);
    const auto it = m_datanode_endpoint_to_cluster.find(key);
    if (it == m_datanode_endpoint_to_cluster.end())
    {
      return -1;
    }
    return it->second;
  }

  proxy_proto::proxyService::Stub *ProxyImpl::getProxyStubForCluster(int cluster_id)
  {
    auto it = m_proxy_peer_stubs.find(cluster_id);
    if (it != m_proxy_peer_stubs.end())
    {
      return it->second.get();
    }
    auto ep_it = m_cluster_proxy_endpoints.find(cluster_id);
    if (ep_it == m_cluster_proxy_endpoints.end())
    {
      return nullptr;
    }
    const std::string channel_addr = ep_it->second.first + ":" + std::to_string(ep_it->second.second);
    auto stub = proxy_proto::proxyService::NewStub(
        grpc::CreateChannel(channel_addr, grpc::InsecureChannelCredentials()));
    proxy_proto::proxyService::Stub *raw = stub.get();
    m_proxy_peer_stubs[cluster_id] = std::move(stub);
    return raw;
  }

  void ProxyImpl::waitXueScheduleHopBeforeForward(int stripe_id, const std::string &append_key,
                                                  int dest_cluster_id)
  {
    if (m_coordinator_ptr == nullptr || stripe_id < 0)
    {
      return;
    }
    grpc::ClientContext ctx;
    coordinator_proto::XueScheduleHopWait req;
    coordinator_proto::ReplyFromCoordinator rep;
    req.set_stripe_id(stripe_id);
    req.set_append_key(append_key);
    req.set_from_cluster(m_self_cluster_id);
    req.set_to_cluster(dest_cluster_id);
    const grpc::Status st = m_coordinator_ptr->waitXueScheduleHop(&ctx, req, &rep);
    if (!st.ok())
    {
      std::cerr << "[Proxy][XFERT] waitXueScheduleHop failed stripe=" << stripe_id
                << " append_key=" << append_key << " " << m_self_cluster_id << "->" << dest_cluster_id
                << " err=" << st.error_message() << std::endl;
    }
  }

  bool ProxyImpl::waitXueScheduleStepBeforeForward(int stripe_id, const std::string &append_key,
                                                   int from_cluster, int to_cluster)
  {
    if (m_coordinator_ptr == nullptr || stripe_id < 0)
    {
      return false;
    }
    grpc::ClientContext ctx;
    coordinator_proto::XueScheduleStepWait req;
    coordinator_proto::ReplyFromCoordinator rep;
    req.set_stripe_id(stripe_id);
    req.set_step_no(0);
    req.set_append_key(append_key);
    req.set_from_cluster(from_cluster);
    req.set_to_cluster(to_cluster);
    const grpc::Status st = m_coordinator_ptr->waitXueScheduleStep(&ctx, req, &rep);
    if (!st.ok())
    {
      std::cerr << "[Proxy][XFERT] waitXueScheduleStep failed stripe=" << stripe_id
                << " append_key=" << append_key << " " << from_cluster << "->" << to_cluster
                << " err=" << st.error_message() << std::endl;
      return false;
    }
    return true;
  }

  void ProxyImpl::reportXueScheduleStepDoneAfterForward(int stripe_id, const std::string &append_key,
                                                        int from_cluster, int to_cluster,
                                                        bool success)
  {
    if (m_coordinator_ptr == nullptr || stripe_id < 0)
    {
      return;
    }
    grpc::ClientContext ctx;
    coordinator_proto::XueScheduleStepDone req;
    coordinator_proto::ReplyFromCoordinator rep;
    req.set_stripe_id(stripe_id);
    req.set_step_no(0);
    req.set_success(success);
    req.set_append_key(append_key);
    req.set_from_cluster(from_cluster);
    req.set_to_cluster(to_cluster);
    const grpc::Status st = m_coordinator_ptr->reportXueScheduleStepDone(&ctx, req, &rep);
    if (!st.ok())
    {
      std::cerr << "[Proxy][XFERT] reportXueScheduleStepDone failed stripe=" << stripe_id
                << " append_key=" << append_key << " err=" << st.error_message() << std::endl;
    }
  }

  void ProxyImpl::reportXueIngressReadyToCoordinator(int stripe_id, const std::string &append_key)
  {
    if (m_coordinator_ptr == nullptr || stripe_id < 0)
    {
      return;
    }
    grpc::ClientContext ctx;
    coordinator_proto::XueIngressReadyReport req;
    coordinator_proto::ReplyFromCoordinator rep;
    req.set_stripe_id(stripe_id);
    req.set_append_key(append_key);
    const grpc::Status st = m_coordinator_ptr->reportXueIngressReady(&ctx, req, &rep);
    if (!st.ok())
    {
      std::cerr << "[Proxy] reportXueIngressReady failed stripe=" << stripe_id
                << " append_key=" << append_key << " err=" << st.error_message() << std::endl;
    }
  }

  void ProxyImpl::record_xue_xfer_sample(int stripe_id, uint64_t xue_xfer_plan_id,
                                         const std::chrono::steady_clock::time_point &t0,
                                         const std::chrono::steady_clock::time_point &t1,
                                         int64_t wall_ms_start, int64_t wall_ms_end)
  {
    if (xue_xfer_plan_id == 0)
    {
      return;
    }
    const double pure_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();
    std::lock_guard<std::mutex> lk(m_xue_xfer_timing_mutex);
    XueXferBatchAccumulator &acc = m_xue_xfer_batches[{stripe_id, xue_xfer_plan_id}];
    acc.pure_xfer_sec_sum += pure_sec;
    if (!acc.has_wall)
    {
      acc.wall_span_start_ms = wall_ms_start;
      acc.wall_span_end_ms = wall_ms_end;
      acc.has_wall = true;
    }
    else
    {
      acc.wall_span_start_ms = std::min(acc.wall_span_start_ms, wall_ms_start);
      acc.wall_span_end_ms = std::max(acc.wall_span_end_ms, wall_ms_end);
    }
  }

  grpc::Status ProxyImpl::xuePullXferTiming(grpc::ServerContext *context,
                                          const proxy_proto::XueXferTimingPull *request,
                                          proxy_proto::XueXferTimingProxyReply *response)
  {
    (void)context;
    if (request == nullptr || response == nullptr)
    {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "null request/response");
    }
    const int stripe_id = request->stripe_id();
    const uint64_t xfer_plan_id = request->xue_xfer_plan_id();
    response->set_proxy_pure_xfer_sec(0);
    response->set_wall_span_start_unix_ms(0);
    response->set_wall_span_end_unix_ms(0);
    if (xfer_plan_id == 0)
    {
      return grpc::Status::OK;
    }
    std::lock_guard<std::mutex> lk(m_xue_xfer_timing_mutex);
    const auto it = m_xue_xfer_batches.find({stripe_id, xfer_plan_id});
    if (it == m_xue_xfer_batches.end())
    {
      return grpc::Status::OK;
    }
    const XueXferBatchAccumulator &acc = it->second;
    response->set_proxy_pure_xfer_sec(acc.pure_xfer_sec_sum);
    if (acc.has_wall)
    {
      response->set_wall_span_start_unix_ms(acc.wall_span_start_ms);
      response->set_wall_span_end_unix_ms(acc.wall_span_end_ms);
    }
    return grpc::Status::OK;
  }

  bool ProxyImpl::forwardXueDataDeltaSync(int dest_cluster_id, const std::string &append_mode,
                                          const proxy_proto::AppendStripeDataPlacement &placement,
                                          const char *delta_buf, size_t delta_size, bool log_send,
                                          bool gate_strict_schedule_step)
  {
    std::lock_guard<std::mutex> dest_forward_lk(
        xue_forward_dest_cluster_mutex(placement.stripe_id(), dest_cluster_id));
    if (placement.xue_strict_schedule() && gate_strict_schedule_step)
    {
      if (!waitXueScheduleStepBeforeForward(placement.stripe_id(), placement.key(), m_self_cluster_id,
                                            dest_cluster_id))
      {
        return false;
      }
    }
    else if (is_xue_proxy_forward_tcp_mode(append_mode) ||
             append_mode == "XUE_COMPUTE_LOCAL_PARITY")
    {
      if (!placement.xue_strict_schedule())
      {
        waitXueScheduleHopBeforeForward(placement.stripe_id(), placement.key(), dest_cluster_id);
      }
    }
    const std::string ts = proxy_xfer_timestamp();
    const int tcp_slices =
        effective_tcp_slice_count(placement, placement.blockkeys_size());
    if (log_send)
    {
      std::ostringstream oss;
      oss << "proxy_tcp " << fmt_proxy_tcp_route(m_self_cluster_id, dest_cluster_id)
          << " append_key=" << placement.key() << " append_mode=" << append_mode
          << " payload=数据增量(data_delta) bytes=" << delta_size << " tcp_slices=" << tcp_slices;
      append_key_tcp_meta_suffix(oss, placement.key());
      log_xfert_line(ts, oss.str());
    }

    proxy_proto::proxyService::Stub *stub = getProxyStubForCluster(dest_cluster_id);
    if (stub == nullptr)
    {
      std::cerr << "[Proxy][XFERT] " << ts << " proxy_tcp_failed "
                << fmt_proxy_tcp_route(m_self_cluster_id, dest_cluster_id)
                << " reason=no_stub append_key=" << placement.key() << std::endl;
      if (placement.xue_strict_schedule() && gate_strict_schedule_step)
      {
        reportXueScheduleStepDoneAfterForward(placement.stripe_id(), placement.key(),
                                              m_self_cluster_id, dest_cluster_id, false);
      }
      return false;
    }
    auto ep_it = m_cluster_proxy_endpoints.find(dest_cluster_id);
    if (ep_it == m_cluster_proxy_endpoints.end())
    {
      std::cerr << "[Proxy][XFERT] " << ts << " proxy_tcp_failed "
                << fmt_proxy_tcp_route(m_self_cluster_id, dest_cluster_id)
                << " reason=no_endpoint append_key=" << placement.key() << std::endl;
      if (placement.xue_strict_schedule() && gate_strict_schedule_step)
      {
        reportXueScheduleStepDoneAfterForward(placement.stripe_id(), placement.key(),
                                              m_self_cluster_id, dest_cluster_id, false);
      }
      return false;
    }

    proxy_proto::AppendStripeDataPlacement fwd = placement;
    fwd.set_cluster_id(dest_cluster_id);
    fwd.set_append_mode(append_mode);
    fwd.set_append_size(delta_size);
    const int fwd_tcp_slices = effective_tcp_slice_count(placement, placement.blockkeys_size());
    fwd.set_xue_tcp_slice_count(fwd_tcp_slices > 0 ? fwd_tcp_slices : 1);
    fwd.set_xue_data_slices_are_delta(true);
    fwd.set_xue_send_ack(true);
    fwd.set_xue_class1_relay_path(false);
    fwd.set_xue_compute_global_parity(append_mode == "XUE_DELTA_TO_GLOBAL");

    grpc::ClientContext ctx;
    proxy_proto::SetReply rep;
    grpc::Status st = stub->scheduleAppend2Datanode(&ctx, fwd, &rep);
    if (!st.ok())
    {
      std::cerr << "[Proxy][XFERT] " << ts << " proxy_tcp_failed "
                << fmt_proxy_tcp_route(m_self_cluster_id, dest_cluster_id)
                << " reason=schedule_grpc append_key=" << placement.key() << std::endl;
      if (placement.xue_strict_schedule() && gate_strict_schedule_step)
      {
        reportXueScheduleStepDoneAfterForward(placement.stripe_id(), placement.key(),
                                              m_self_cluster_id, dest_cluster_id, false);
      }
      return false;
    }

    try
    {
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      const int tcp_port = ep_it->second.second + ECProject::PROXY_PORT_SHIFT;
      asio::error_code con_error;
      asio::connect(socket,
                    resolver.resolve({ep_it->second.first, std::to_string(tcp_port)}),
                    con_error);
      if (con_error)
      {
        std::cerr << "[Proxy][XFERT] " << ts << " proxy_tcp_failed "
                  << fmt_proxy_tcp_route(m_self_cluster_id, dest_cluster_id)
                  << " reason=tcp_connect append_key=" << placement.key() << std::endl;
        if (placement.xue_strict_schedule() && gate_strict_schedule_step)
        {
          reportXueScheduleStepDoneAfterForward(placement.stripe_id(), placement.key(),
                                                m_self_cluster_id, dest_cluster_id, false);
        }
        return false;
      }
      asio::error_code write_ec;
      asio::write(socket, asio::buffer(delta_buf, delta_size), write_ec);
      if (write_ec)
      {
        std::cerr << "[Proxy][XFERT] " << ts << " proxy_tcp_failed "
                  << fmt_proxy_tcp_route(m_self_cluster_id, dest_cluster_id)
                  << " reason=tcp_write append_key=" << placement.key() << std::endl;
        return false;
      }
      char ack = 0;
      asio::error_code read_ec;
      asio::read(socket, asio::buffer(&ack, 1), read_ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      const bool ok = !read_ec && ack == 1;
      if (!ok)
      {
        std::cerr << "[Proxy][XFERT] " << ts << " proxy_tcp_failed "
                  << fmt_proxy_tcp_route(m_self_cluster_id, dest_cluster_id)
                  << " reason=peer_ack"
                  << " append_mode=" << append_mode << " append_key=" << placement.key()
                  << " (downstream returned ack=" << static_cast<int>(ack) << ")" << std::endl;
      }
      if (placement.xue_strict_schedule() && gate_strict_schedule_step)
      {
        reportXueScheduleStepDoneAfterForward(placement.stripe_id(), placement.key(),
                                              m_self_cluster_id, dest_cluster_id, ok);
      }
      return ok;
    }
    catch (const std::exception &e)
    {
      std::cerr << "[Proxy][XFERT] " << ts << " proxy_tcp_failed "
                << fmt_proxy_tcp_route(m_self_cluster_id, dest_cluster_id)
                << " reason=exception what=" << e.what() << std::endl;
      if (placement.xue_strict_schedule() && gate_strict_schedule_step)
      {
        reportXueScheduleStepDoneAfterForward(placement.stripe_id(), placement.key(),
                                              m_self_cluster_id, dest_cluster_id, false);
      }
      return false;
    }
  }

  int ProxyImpl::applyXueDataBlocksNewValueToDataDelta(const proxy_proto::AppendStripeDataPlacement &placement,
                                                       std::vector<char *> &slices, int tcp_slice_count)
  {
    const int k0 = m_sys_config->k;
    const int slice_num = placement.blockkeys_size();
    int updated = 0;
    for (int j = 0; j < tcp_slice_count && j < slice_num; ++j)
    {
      const int bid = placement.blockids(j);
      if (bid < 0 || bid >= k0)
      {
        continue;
      }
      const std::string &bk = placement.blockkeys(j);
      const size_t sz = placement.sizes(j);
      const int off = static_cast<int>(placement.offsets(j));
      const std::string dip = placement.datanodeip(j);
      const int dport = placement.datanodeport(j);
      std::vector<char> oldbuf(sz);
      if (!ReadRangeFromDatanode(bk.c_str(), bid, off, static_cast<int>(sz), oldbuf.data(), dip.c_str(), dport))
      {
        std::memset(oldbuf.data(), 0, sz);
      }
      std::vector<char> newbuf(sz);
      std::memcpy(newbuf.data(), slices[j], sz);
      for (size_t i = 0; i < sz; ++i)
      {
        slices[j][i] = static_cast<char>(
            static_cast<unsigned char>(newbuf[i]) ^ static_cast<unsigned char>(oldbuf[i]));
      }
      if (WriteRangeToDatanode(bk.c_str(), bid, off, newbuf.data(), static_cast<int>(sz), dip.c_str(), dport))
      {
        ++updated;
      }
      else
      {
        std::cerr << "[Proxy] XUE data block WriteRange failed block " << bk << std::endl;
      }
    }
    return updated;
  }

  bool ProxyImpl::handleXueClass1RelayAtDataCluster(const proxy_proto::AppendStripeDataPlacement &placement,
                                                    std::vector<char *> &slices, int tcp_slice_count,
                                                    const char *delta_buf, size_t delta_size)
  {
    const bool azure_like =
        m_sys_config->CodeType == "AzureLRC" || m_sys_config->CodeType == "XueLRC";
    const std::string ts = proxy_xfer_timestamp();
    (void)applyXueDataBlocksNewValueToDataDelta(placement, slices, tcp_slice_count);

    if (azure_like)
    {
      int local_parity_cluster = -1;
      if (infer_class1_local_parity_cluster(placement, m_sys_config->k, m_sys_config->r,
                                            &local_parity_cluster) &&
          local_parity_cluster >= 0 && m_self_cluster_id == local_parity_cluster)
      {
        const XueParityWriteStats local_write =
            applyXueLocalParityFromDataDeltas(placement, slices, tcp_slice_count);
        if (local_write.ranges > 0)
        {
          log_xue_parity_write_applied(ts, "XUE_COMPUTE_LOCAL_PARITY applied", local_parity_cluster,
                                       placement.key(), local_write, "route=class1_data_cluster");
        }
      }
    }

    if (placement.xue_strict_schedule())
    {
      return true;
    }
    if (placement.xue_relay_cluster_id() < 0)
    {
      std::cerr << "[Proxy] class1 relay: missing relay cluster id" << std::endl;
      return false;
    }
    proxy_proto::AppendStripeDataPlacement fwd_placement = placement;
    fwd_placement.set_xue_data_slices_are_delta(true);
    return forwardXueDataDeltaSync(placement.xue_relay_cluster_id(), "XUE_DELTA_TO_RELAY",
                                   fwd_placement, delta_buf, delta_size);
  }

  void ProxyImpl::runXueStrictDeferredForwards(
      std::shared_ptr<proxy_proto::AppendStripeDataPlacement> placement,
      std::shared_ptr<std::vector<char>> append_buf, std::vector<char *> slices, int tcp_slice_count)
  {
    if (!placement || !append_buf)
    {
      return;
    }
    const int stripe_id = placement->stripe_id();
    const uint64_t xfer_plan_id = placement->xue_xfer_plan_id();
    const auto xfer_t0 = std::chrono::steady_clock::now();
    const int64_t xfer_w0 = xue_wall_unix_ms_now();
    if (m_coordinator_ptr != nullptr && stripe_id >= 0)
    {
      grpc::ClientContext wait_ctx;
      coordinator_proto::XueStripeScheduleId wait_req;
      coordinator_proto::ReplyFromCoordinator wait_rep;
      wait_req.set_stripe_id(stripe_id);
      const grpc::Status wait_st =
          m_coordinator_ptr->waitXueAllIngressReady(&wait_ctx, wait_req, &wait_rep);
      if (!wait_st.ok())
      {
        std::cerr << "[Proxy] strict schedule: waitXueAllIngressReady failed stripe=" << stripe_id
                  << " append_key=" << placement->key() << " err=" << wait_st.error_message()
                  << std::endl;
        return;
      }
    }

    const size_t cluster_append_size = append_buf->size();
    bool ok = true;

    if (placement->xue_strict_outgoing_size() == 0)
    {
      const bool class2_ingress_only =
          placement->xue_compute_global_parity() &&
          placement->xue_global_parity_cluster_id() == m_self_cluster_id &&
          m_self_cluster_id == placement->cluster_id();
      // class3：data cluster ingress 已在 strict ingress 侧完成 merged local parity forward，
      // coordinator 不会把 4->local 放进 strict_outgoing，deferred 无 hop 属预期。
      const bool class3_ingress_only =
          m_self_cluster_id == placement->cluster_id() &&
          needsClass3MergedLocalParityForward(*placement, tcp_slice_count);
      if (!class2_ingress_only && !class3_ingress_only)
      {
        std::cerr << "[Proxy] strict schedule: no outgoing hops for append_key=" << placement->key()
                  << " proxy_cluster=" << m_self_cluster_id << std::endl;
        ok = false;
      }
    }
    for (int hi = 0; hi < placement->xue_strict_outgoing_size(); ++hi)
    {
      const auto &hop = placement->xue_strict_outgoing(hi);
      int lp_cluster = -1;
      if (needsClass3MergedLocalParityForward(*placement, tcp_slice_count) &&
          infer_local_parity_cluster_from_plan(*placement, m_sys_config->k, m_sys_config->r,
                                               &lp_cluster) &&
          lp_cluster >= 0 && hop.to_cluster() == lp_cluster &&
          (hop.forward_append_mode() == "XUE_COMPUTE_LOCAL_PARITY" ||
           hop.forward_append_mode() == "XUE_UPDATE"))
      {
        continue;
      }
      if (placement->xue_compute_global_parity() &&
          hop.forward_append_mode() == "XUE_COMPUTE_LOCAL_PARITY")
      {
        if (!waitXueScheduleStepBeforeForward(placement->stripe_id(), placement->key(),
                                              m_self_cluster_id, hop.to_cluster()))
        {
          ok = false;
          continue;
        }
        const bool merged_ok =
            forwardMergedLocalParityDelta(hop.to_cluster(), *placement, slices, tcp_slice_count);
        reportXueScheduleStepDoneAfterForward(placement->stripe_id(), placement->key(),
                                              m_self_cluster_id, hop.to_cluster(), merged_ok);
        if (!merged_ok)
        {
          std::cerr << "[Proxy] strict class2 scheduled local parity forward failed append_key="
                    << placement->key() << " " << m_self_cluster_id << "->" << hop.to_cluster()
                    << std::endl;
          ok = false;
        }
        continue;
      }
      proxy_proto::AppendStripeDataPlacement fwd_placement = *placement;
      fwd_placement.set_xue_data_slices_are_delta(true);
      const bool hop_ok =
          forwardXueDataDeltaSync(hop.to_cluster(), hop.forward_append_mode(), fwd_placement,
                                  append_buf->data(), cluster_append_size);
      if (!hop_ok)
      {
        std::cerr << "[Proxy] strict scheduled hop failed append_key=" << placement->key()
                  << " " << m_self_cluster_id << "->" << hop.to_cluster()
                  << " mode=" << hop.forward_append_mode() << std::endl;
        ok = false;
      }
    }

    coordinator_proto::CommitAbortKey commit_abort_key;
    coordinator_proto::ReplyFromCoordinator result;
    grpc::ClientContext context;
    commit_abort_key.set_opp(APPEND);
    commit_abort_key.set_key(placement->key());
    commit_abort_key.set_stripe_id(placement->stripe_id());
    commit_abort_key.set_ifcommitmetadata(ok);
    const grpc::Status status =
        m_coordinator_ptr->reportCommitAbort(&context, commit_abort_key, &result);
    log_append_commit_report(proxy_xfer_timestamp(), m_self_cluster_id, placement->key(),
                             placement->stripe_id(), status.ok());
    record_xue_xfer_sample(stripe_id, xfer_plan_id, xfer_t0, std::chrono::steady_clock::now(),
                          xfer_w0, xue_wall_unix_ms_now());
  }

  void ProxyImpl::mergeXueGlobalParityDeltaIntoBlock(
      std::vector<XueGlobalParityIngressBatch::RangeAccum> &intervals,
      XueGlobalParityIngressBatch::RangeAccum incoming)
  {
    if (incoming.length <= 0 || incoming.delta_xor.empty())
    {
      return;
    }
    const int block_id = incoming.block_id;
    const std::string block_key = incoming.block_key;
    const std::string datanode_ip = incoming.datanode_ip;
    const int datanode_port = incoming.datanode_port;
    std::vector<XueGlobalParityIngressBatch::RangeAccum> all = intervals;
    all.push_back(std::move(incoming));

    std::set<int> cut_points;
    for (const auto &r : all)
    {
      cut_points.insert(r.offset);
      cut_points.insert(r.offset + r.length);
    }

    std::vector<XueGlobalParityIngressBatch::RangeAccum> merged;
    std::vector<int> pts(cut_points.begin(), cut_points.end());
    for (size_t i = 0; i + 1 < pts.size(); ++i)
    {
      const int seg_off = pts[i];
      const int seg_len = pts[i + 1] - pts[i];
      if (seg_len <= 0)
      {
        continue;
      }
      bool covered = false;
      std::vector<char> acc(static_cast<size_t>(seg_len), 0);
      for (const auto &r : all)
      {
        if (r.offset > seg_off || r.offset + r.length < seg_off + seg_len)
        {
          continue;
        }
        covered = true;
        const int rel = seg_off - r.offset;
        for (int t = 0; t < seg_len; ++t)
        {
          acc[static_cast<size_t>(t)] = static_cast<char>(
              static_cast<unsigned char>(acc[static_cast<size_t>(t)]) ^
              static_cast<unsigned char>(r.delta_xor[static_cast<size_t>(rel + t)]));
        }
      }
      if (!covered)
      {
        continue;
      }
      XueGlobalParityIngressBatch::RangeAccum seg;
      seg.block_id = block_id;
      seg.offset = seg_off;
      seg.length = seg_len;
      seg.delta_xor = std::move(acc);
      seg.block_key = block_key;
      seg.datanode_ip = datanode_ip;
      seg.datanode_port = datanode_port;
      if (!merged.empty() && merged.back().offset + merged.back().length == seg_off &&
          merged.back().block_id == seg.block_id && merged.back().block_key == seg.block_key &&
          merged.back().datanode_ip == seg.datanode_ip && merged.back().datanode_port == seg.datanode_port)
      {
        XueGlobalParityIngressBatch::RangeAccum &prev = merged.back();
        prev.length += seg_len;
        prev.delta_xor.insert(prev.delta_xor.end(), seg.delta_xor.begin(), seg.delta_xor.end());
      }
      else
      {
        merged.push_back(std::move(seg));
      }
    }
    intervals = std::move(merged);
  }

  void ProxyImpl::registerXueGlobalParityIngressExpected(int stripe_id, const std::string &append_key)
  {
    std::lock_guard<std::mutex> lk(m_xue_global_parity_ingress_mutex);
    m_xue_global_parity_ingress_batches[stripe_id].expected_append_keys.insert(append_key);
  }

  XueParityWriteStats ProxyImpl::flushXueGlobalParityIngressBatch(int stripe_id)
  {
    XueParityWriteStats stats;
    auto it = m_xue_global_parity_ingress_batches.find(stripe_id);
    if (it == m_xue_global_parity_ingress_batches.end())
    {
      return stats;
    }
    XueGlobalParityIngressBatch &batch = it->second;
    for (auto &kv : batch.merged_by_block)
    {
      for (XueGlobalParityIngressBatch::RangeAccum &acc : kv.second)
      {
        if (acc.length <= 0 || acc.delta_xor.empty())
        {
          continue;
        }
        std::vector<char> oldbuf(static_cast<size_t>(acc.length), 0);
        if (!ReadRangeFromDatanode(acc.block_key.c_str(), acc.block_id, acc.offset, acc.length,
                                   oldbuf.data(), acc.datanode_ip.c_str(), acc.datanode_port))
        {
          std::memset(oldbuf.data(), 0, static_cast<size_t>(acc.length));
        }
        std::vector<char> newbuf(static_cast<size_t>(acc.length));
        for (int t = 0; t < acc.length; ++t)
        {
          newbuf[static_cast<size_t>(t)] = static_cast<char>(
              static_cast<unsigned char>(oldbuf[static_cast<size_t>(t)]) ^
              static_cast<unsigned char>(acc.delta_xor[static_cast<size_t>(t)]));
        }
        if (WriteRangeToDatanode(acc.block_key.c_str(), acc.block_id, acc.offset, newbuf.data(),
                                 acc.length, acc.datanode_ip.c_str(), acc.datanode_port))
        {
          ++stats.ranges;
          stats.bytes += static_cast<size_t>(acc.length);
        }
        else
        {
          std::cerr << "[Proxy] XUE global parity merged WriteRange failed block " << acc.block_key
                    << " off=" << acc.offset << " len=" << acc.length << std::endl;
        }
      }
    }
    m_xue_global_parity_ingress_batches.erase(it);
    return stats;
  }

  XueParityWriteStats ProxyImpl::applyXueGlobalParityIngressMerged(
      const proxy_proto::AppendStripeDataPlacement &placement, const std::vector<char *> &slices,
      int tcp_slice_count)
  {
    XueParityWriteStats stats;
    const bool azure_like =
        m_sys_config->CodeType == "AzureLRC" || m_sys_config->CodeType == "XueLRC";
    if (!azure_like)
    {
      return stats;
    }
    const int k0 = m_sys_config->k;
    const int r0 = m_sys_config->r;
    const int z0 = m_sys_config->z;
    const int slice_num = placement.blockkeys_size();
    const bool xue_lrc = m_sys_config->CodeType == "XueLRC";
    const int global_begin = xue_lrc ? (k0 + r0) : k0;
    const int global_end = xue_lrc ? (k0 + r0 + z0) : (k0 + r0);
    const int global_count = global_end - global_begin;

    std::vector<int> global_parity_indices;
    global_parity_indices.reserve(static_cast<size_t>(global_count));
    for (int j = 0; j < slice_num; ++j)
    {
      const int bid = placement.blockids(j);
      if (bid >= global_begin && bid < global_end)
      {
        global_parity_indices.push_back(j);
      }
    }
    if (global_parity_indices.empty())
    {
      return stats;
    }
    std::map<int, int> global_block_plan_idx;
    for (int idx : global_parity_indices)
    {
      const int bid = placement.blockids(idx);
      if (global_block_plan_idx.find(bid) == global_block_plan_idx.end())
      {
        global_block_plan_idx[bid] = idx;
      }
    }

    static std::atomic<bool> gf8_inited{false};
    if (!gf8_inited.exchange(true))
    {
      galois_init_default_field(8);
    }
    const int m = k0 + r0;
    std::vector<unsigned char> enc(static_cast<size_t>((m + z0) * k0));
    gen_azure_lrc_matrix(enc.data(), k0, r0, z0);

    using RangeKey = std::pair<int, int>;
    std::map<RangeKey, std::vector<int>> data_slices_by_range;
    for (int j = 0; j < tcp_slice_count && j < slice_num; ++j)
    {
      const int bid = placement.blockids(j);
      if (bid < 0 || bid >= k0)
      {
        continue;
      }
      const int o = static_cast<int>(placement.offsets(j));
      const int l = static_cast<int>(placement.sizes(j));
      data_slices_by_range[{o, l}].push_back(j);
    }

    const int stripe_id = placement.stripe_id();
    const std::string &append_key = placement.key();
    bool should_flush = false;

    {
      std::lock_guard<std::mutex> lk(m_xue_global_parity_ingress_mutex);
      XueGlobalParityIngressBatch &batch = m_xue_global_parity_ingress_batches[stripe_id];
      batch.staged_append_keys.insert(append_key);

      for (const auto &range_kv : data_slices_by_range)
      {
        const int ref_off = range_kv.first.first;
        const int ref_len = range_kv.first.second;
        if (ref_len <= 0)
        {
          continue;
        }
        std::vector<std::vector<char>> p_acc(static_cast<size_t>(global_count),
                                             std::vector<char>(static_cast<size_t>(ref_len), 0));
        for (int j : range_kv.second)
        {
          const int bid = placement.blockids(j);
          for (int gi = 0; gi < global_count; ++gi)
          {
            const int matrix_row = xue_lrc ? (m + gi) : (k0 + gi);
            const unsigned char coeff = enc[static_cast<size_t>(matrix_row * k0 + bid)];
            if (coeff == 0)
            {
              continue;
            }
            for (int t = 0; t < ref_len; ++t)
            {
              const unsigned char v = static_cast<unsigned char>(galois_single_multiply(
                  coeff, static_cast<unsigned char>(slices[j][static_cast<size_t>(t)]), 8));
              p_acc[static_cast<size_t>(gi)][static_cast<size_t>(t)] = static_cast<char>(
                  static_cast<unsigned char>(p_acc[static_cast<size_t>(gi)][static_cast<size_t>(t)]) ^ v);
            }
          }
        }
        for (const auto &gkv : global_block_plan_idx)
        {
          const int bid = gkv.first;
          const int idx = gkv.second;
          const int gi = bid - global_begin;
          if (gi < 0 || gi >= global_count)
          {
            continue;
          }
          XueGlobalParityIngressBatch::RangeAccum acc;
          acc.block_id = bid;
          acc.offset = ref_off;
          acc.length = ref_len;
          acc.delta_xor = p_acc[static_cast<size_t>(gi)];
          acc.block_key = placement.blockkeys(idx);
          acc.datanode_ip = placement.datanodeip(idx);
          acc.datanode_port = placement.datanodeport(idx);
          mergeXueGlobalParityDeltaIntoBlock(batch.merged_by_block[bid], std::move(acc));
        }
      }

      if (!batch.expected_append_keys.empty() &&
          batch.staged_append_keys == batch.expected_append_keys)
      {
        should_flush = true;
      }
      else if (batch.expected_append_keys.empty())
      {
        should_flush = true;
      }

      if (should_flush)
      {
        stats = flushXueGlobalParityIngressBatch(stripe_id);
      }
    }
    return stats;
  }

  XueParityWriteStats ProxyImpl::applyXueGlobalParityFromDataDeltas(
      const proxy_proto::AppendStripeDataPlacement &placement, const std::vector<char *> &slices,
      int tcp_slice_count)
  {
    const bool ingress_global_compute =
        placement.append_mode() == "XUE_UPDATE" && placement.xue_compute_global_parity() &&
        placement.xue_global_parity_cluster_id() == m_self_cluster_id &&
        placement.cluster_id() == m_self_cluster_id;
    if (ingress_global_compute)
    {
      return applyXueGlobalParityIngressMerged(placement, slices, tcp_slice_count);
    }

    XueParityWriteStats stats;
    const bool azure_like =
        m_sys_config->CodeType == "AzureLRC" || m_sys_config->CodeType == "XueLRC";
    if (!azure_like)
    {
      return stats;
    }
    const int k0 = m_sys_config->k;
    const int r0 = m_sys_config->r;
    const int z0 = m_sys_config->z;
    const int slice_num = placement.blockkeys_size();
    const bool xue_lrc = m_sys_config->CodeType == "XueLRC";
    const int global_begin = xue_lrc ? (k0 + r0) : k0;
    const int global_end = xue_lrc ? (k0 + r0 + z0) : (k0 + r0);
    const int global_count = global_end - global_begin;

    std::vector<int> global_parity_indices;
    global_parity_indices.reserve(static_cast<size_t>(global_count));
    for (int j = 0; j < slice_num; ++j)
    {
      const int bid = placement.blockids(j);
      if (bid >= global_begin && bid < global_end)
      {
        global_parity_indices.push_back(j);
      }
    }
    if (global_parity_indices.empty())
    {
      return stats;
    }
    std::map<int, int> global_block_plan_idx;
    for (int idx : global_parity_indices)
    {
      const int bid = placement.blockids(idx);
      if (global_block_plan_idx.find(bid) == global_block_plan_idx.end())
      {
        global_block_plan_idx[bid] = idx;
      }
    }

    static std::atomic<bool> gf8_inited_direct{false};
    if (!gf8_inited_direct.exchange(true))
    {
      galois_init_default_field(8);
    }
    const int m = k0 + r0;
    std::vector<unsigned char> enc(static_cast<size_t>((m + z0) * k0));
    gen_azure_lrc_matrix(enc.data(), k0, r0, z0);

    using RangeKey = std::pair<int, int>;
    std::map<RangeKey, std::vector<int>> data_slices_by_range;
    for (int j = 0; j < tcp_slice_count && j < slice_num; ++j)
    {
      const int bid = placement.blockids(j);
      if (bid < 0 || bid >= k0)
      {
        continue;
      }
      const int o = static_cast<int>(placement.offsets(j));
      const int l = static_cast<int>(placement.sizes(j));
      data_slices_by_range[{o, l}].push_back(j);
    }

    std::map<int, std::vector<XueGlobalParityIngressBatch::RangeAccum>> merged_by_block;
    for (const auto &range_kv : data_slices_by_range)
    {
      const int ref_off = range_kv.first.first;
      const int ref_len = range_kv.first.second;
      if (ref_len <= 0)
      {
        continue;
      }
      std::vector<std::vector<char>> p_acc(static_cast<size_t>(global_count),
                                           std::vector<char>(static_cast<size_t>(ref_len), 0));
      for (int j : range_kv.second)
      {
        const int bid = placement.blockids(j);
        for (int gi = 0; gi < global_count; ++gi)
        {
          const int matrix_row = xue_lrc ? (m + gi) : (k0 + gi);
          const unsigned char coeff = enc[static_cast<size_t>(matrix_row * k0 + bid)];
          if (coeff == 0)
          {
            continue;
          }
          for (int t = 0; t < ref_len; ++t)
          {
            const unsigned char v = static_cast<unsigned char>(galois_single_multiply(
                coeff, static_cast<unsigned char>(slices[j][static_cast<size_t>(t)]), 8));
            p_acc[static_cast<size_t>(gi)][static_cast<size_t>(t)] = static_cast<char>(
                static_cast<unsigned char>(p_acc[static_cast<size_t>(gi)][static_cast<size_t>(t)]) ^ v);
          }
        }
      }
      for (const auto &gkv : global_block_plan_idx)
      {
        const int bid = gkv.first;
        const int idx = gkv.second;
        const int gi = bid - global_begin;
        if (gi < 0 || gi >= global_count)
        {
          continue;
        }
        XueGlobalParityIngressBatch::RangeAccum acc;
        acc.block_id = bid;
        acc.offset = ref_off;
        acc.length = ref_len;
        acc.delta_xor = p_acc[static_cast<size_t>(gi)];
        acc.block_key = placement.blockkeys(idx);
        acc.datanode_ip = placement.datanodeip(idx);
        acc.datanode_port = placement.datanodeport(idx);
        mergeXueGlobalParityDeltaIntoBlock(merged_by_block[bid], std::move(acc));
      }
    }

    for (auto &kv : merged_by_block)
    {
      for (XueGlobalParityIngressBatch::RangeAccum &acc : kv.second)
      {
        if (acc.length <= 0 || acc.delta_xor.empty())
        {
          continue;
        }
        std::vector<char> oldbuf(static_cast<size_t>(acc.length), 0);
        if (!ReadRangeFromDatanode(acc.block_key.c_str(), acc.block_id, acc.offset, acc.length,
                                   oldbuf.data(), acc.datanode_ip.c_str(), acc.datanode_port))
        {
          std::memset(oldbuf.data(), 0, static_cast<size_t>(acc.length));
        }
        std::vector<char> newbuf(static_cast<size_t>(acc.length));
        for (int t = 0; t < acc.length; ++t)
        {
          newbuf[static_cast<size_t>(t)] = static_cast<char>(
              static_cast<unsigned char>(oldbuf[static_cast<size_t>(t)]) ^
              static_cast<unsigned char>(acc.delta_xor[static_cast<size_t>(t)]));
        }
        if (WriteRangeToDatanode(acc.block_key.c_str(), acc.block_id, acc.offset, newbuf.data(),
                                 acc.length, acc.datanode_ip.c_str(), acc.datanode_port))
        {
          ++stats.ranges;
          stats.bytes += static_cast<size_t>(acc.length);
        }
        else
        {
          std::cerr << "[Proxy] XUE global parity WriteRange failed block " << acc.block_key
                    << " off=" << acc.offset << " len=" << acc.length << std::endl;
        }
      }
    }
    return stats;
  }

  static std::vector<ParityDeltaSlice> compute_local_parity_delta_slices(
      int k0, int r0, int z0, const proxy_proto::AppendStripeDataPlacement &placement,
      const std::vector<char *> &slices, int tcp_slice_count, int local_parity_plan_idx)
  {
    std::vector<ParityDeltaSlice> out;
    if (local_parity_plan_idx < 0 || tcp_slice_count <= 0)
    {
      return out;
    }
    const int slice_num = placement.blockids_size();
    const int local_begin = k0;
    const int local_end = k0 + r0;
    const int local_count = local_end - local_begin;
    const int local_bid = placement.blockids(local_parity_plan_idx);
    const int li = local_bid - local_begin;
    if (li < 0 || li >= local_count)
    {
      return out;
    }

    static std::atomic<bool> gf8_inited_local{false};
    if (!gf8_inited_local.exchange(true))
    {
      galois_init_default_field(8);
    }
    const int m = k0 + r0;
    std::vector<unsigned char> enc(static_cast<size_t>((m + z0) * k0));
    gen_azure_lrc_matrix(enc.data(), k0, r0, z0);

    using RangeKey = std::pair<int, int>;
    std::map<RangeKey, std::vector<int>> data_slices_by_range;
    for (int j = 0; j < tcp_slice_count && j < slice_num; ++j)
    {
      const int bid = placement.blockids(j);
      if (bid < 0 || bid >= k0)
      {
        continue;
      }
      const int o = static_cast<int>(placement.offsets(j));
      const int l = static_cast<int>(placement.sizes(j));
      data_slices_by_range[{o, l}].push_back(j);
    }

    for (const auto &range_kv : data_slices_by_range)
    {
      const int ref_off = range_kv.first.first;
      const int ref_len = range_kv.first.second;
      if (ref_len <= 0)
      {
        continue;
      }
      std::vector<char> p_acc(static_cast<size_t>(ref_len), 0);
      for (int j : range_kv.second)
      {
        const int bid = placement.blockids(j);
        const int matrix_row = k0 + li;
        const unsigned char coeff = enc[static_cast<size_t>(matrix_row * k0 + bid)];
        if (coeff == 0)
        {
          continue;
        }
        for (int t = 0; t < ref_len; ++t)
        {
          const unsigned char v = static_cast<unsigned char>(galois_single_multiply(
              coeff, static_cast<unsigned char>(slices[j][static_cast<size_t>(t)]), 8));
          p_acc[static_cast<size_t>(t)] = static_cast<char>(
              static_cast<unsigned char>(p_acc[static_cast<size_t>(t)]) ^ v);
        }
      }
      ParityDeltaSlice sl;
      sl.offset = ref_off;
      sl.length = ref_len;
      sl.delta = std::move(p_acc);
      out.push_back(std::move(sl));
    }
    return out;
  }

  bool ProxyImpl::needsClass3MergedLocalParityForward(
      const proxy_proto::AppendStripeDataPlacement &placement, int tcp_slice_count) const
  {
    if (placement.append_mode() != "XUE_UPDATE" || placement.xue_class1_relay_path() || tcp_slice_count <= 0)
    {
      return false;
    }
    const int k0 = m_sys_config->k;
    const int r0 = m_sys_config->r;
    int local_cluster = -1;
    if (!infer_local_parity_cluster_from_plan(placement, k0, r0, &local_cluster) || local_cluster < 0)
    {
      return false;
    }
    if (local_cluster == m_self_cluster_id)
    {
      return false;
    }
    const int data_cluster = infer_data_block_cluster_from_placement(placement, tcp_slice_count, k0);
    if (data_cluster < 0 || data_cluster == local_cluster)
    {
      return false;
    }
    const int global_cluster = placement.xue_global_parity_cluster_id();
    return global_cluster < 0 || data_cluster != global_cluster;
  }

  XueParityWriteStats ProxyImpl::applyReceivedLocalParityDelta(
      const proxy_proto::AppendStripeDataPlacement &placement, const char *delta_buf,
      size_t delta_size)
  {
    XueParityWriteStats stats;
    const int k0 = m_sys_config->k;
    const int r0 = m_sys_config->r;
    const int lp_idx = find_local_parity_plan_index(placement, k0, r0);
    if (lp_idx < 0)
    {
      return stats;
    }
    const int tcp_slices = placement.xue_tcp_slice_count() > 0 ? placement.xue_tcp_slice_count()
                                                                 : placement.blockkeys_size();
    std::vector<size_t> tcp_slice_sizes;
    tcp_slice_sizes.reserve(static_cast<size_t>(tcp_slices));
    for (int i = 0; i < tcp_slices && i < placement.sizes_size(); ++i)
    {
      tcp_slice_sizes.push_back(placement.sizes(i));
    }
    std::vector<char *> slices =
        m_toolbox->splitCharPointer(delta_buf, delta_size, tcp_slice_sizes);
    const int local_bid = placement.blockids(lp_idx);
    const std::string &pbk = placement.blockkeys(lp_idx);
    const std::string dip = placement.datanodeip(lp_idx);
    const int dport = placement.datanodeport(lp_idx);
    for (int i = 0; i < tcp_slices && i < placement.blockids_size(); ++i)
    {
      if (placement.blockids(i) != local_bid)
      {
        continue;
      }
      const int off = static_cast<int>(placement.offsets(i));
      const int len = static_cast<int>(placement.sizes(i));
      if (XorWriteRangeToDatanode(pbk.c_str(), local_bid, off, slices[i], len, dip.c_str(), dport))
      {
        ++stats.ranges;
        stats.bytes += static_cast<size_t>(len);
      }
      else
      {
        std::cerr << "[Proxy] XUE_LOCAL_PARITY_DELTA XorWrite failed block " << pbk << " off=" << off
                  << " len=" << len << std::endl;
      }
    }
    return stats;
  }

  bool ProxyImpl::forwardMergedLocalParityDelta(
      int dest_local_cluster, const proxy_proto::AppendStripeDataPlacement &placement,
      const std::vector<char *> &data_delta_slices, int tcp_slice_count)
  {
    const int k0 = m_sys_config->k;
    const int r0 = m_sys_config->r;
    const int z0 = m_sys_config->z;
    const int lp_idx = find_local_parity_plan_index(placement, k0, r0);
    if (lp_idx < 0)
    {
      return false;
    }
    std::vector<ParityDeltaSlice> raw =
        compute_local_parity_delta_slices(k0, r0, z0, placement, data_delta_slices, tcp_slice_count, lp_idx);
    if (raw.empty())
    {
      return false;
    }
    const size_t slices_before_merge = raw.size();
    const std::vector<ParityDeltaSlice> merged = merge_parity_delta_slices(std::move(raw));

    proxy_proto::AppendStripeDataPlacement fwd = placement;
    fwd.clear_datanodeip();
    fwd.clear_datanodeport();
    fwd.clear_blockkeys();
    fwd.clear_blockids();
    fwd.clear_block_cluster_ids();
    fwd.clear_offsets();
    fwd.clear_sizes();
    const int local_bid = placement.blockids(lp_idx);
    const std::string lp_key = placement.blockkeys(lp_idx);
    const std::string lp_ip = placement.datanodeip(lp_idx);
    const int lp_port = placement.datanodeport(lp_idx);
    const int lp_cluster = (lp_idx < placement.block_cluster_ids_size())
                               ? placement.block_cluster_ids(lp_idx)
                               : dest_local_cluster;

    size_t total = 0;
    std::vector<char> wire;
    for (const auto &sl : merged)
    {
      fwd.add_datanodeip(lp_ip);
      fwd.add_datanodeport(lp_port);
      fwd.add_blockkeys(lp_key);
      fwd.add_blockids(local_bid);
      fwd.add_block_cluster_ids(lp_cluster);
      fwd.add_offsets(static_cast<uint64_t>(sl.offset));
      fwd.add_sizes(static_cast<uint64_t>(sl.length));
      wire.insert(wire.end(), sl.delta.begin(), sl.delta.end());
      total += static_cast<size_t>(sl.length);
    }
    fwd.set_cluster_id(dest_local_cluster);
    fwd.set_append_size(total);
    fwd.set_xue_tcp_slice_count(static_cast<int>(merged.size()));
    fwd.set_xue_data_slices_are_delta(true);
    fwd.set_xue_class1_relay_path(false);
    fwd.set_xue_compute_global_parity(false);

    const std::string ts = proxy_xfer_timestamp();
    std::ostringstream oss;
    oss << "class3_local_parity_delta_merge proxy_cluster=" << m_self_cluster_id << " -> "
        << dest_local_cluster << " slices_before=" << slices_before_merge
        << " slices_after=" << merged.size()
        << " bytes=" << total << " ranges=";
    for (size_t i = 0; i < merged.size(); ++i)
    {
      if (i > 0)
      {
        oss << ";";
      }
      oss << "[" << merged[i].offset << "," << (merged[i].offset + merged[i].length) << ")";
    }
    log_xfert_line(ts, oss.str());

    // ingress 侧 class3 合并 local parity：调度表通常无 3->local 步（global-first 仅 3->0）
    return forwardXueDataDeltaSync(dest_local_cluster, "XUE_LOCAL_PARITY_DELTA", fwd, wire.data(),
                                   total, true, false);
  }

  XueParityWriteStats ProxyImpl::applyXueLocalParityFromDataDeltas(
      const proxy_proto::AppendStripeDataPlacement &placement, const std::vector<char *> &slices,
      int tcp_slice_count)
  {
    XueParityWriteStats stats;
    const bool azure_like =
        m_sys_config->CodeType == "AzureLRC" || m_sys_config->CodeType == "XueLRC";
    if (!azure_like)
    {
      return stats;
    }
    const int k0 = m_sys_config->k;
    const int r0 = m_sys_config->r;
    const int z0 = m_sys_config->z;
    const int lp_idx = find_local_parity_plan_index(placement, k0, r0);
    if (lp_idx < 0)
    {
      return stats;
    }
    std::vector<ParityDeltaSlice> raw =
        compute_local_parity_delta_slices(k0, r0, z0, placement, slices, tcp_slice_count, lp_idx);
    const std::vector<ParityDeltaSlice> merged = merge_parity_delta_slices(std::move(raw));
    const int local_bid = placement.blockids(lp_idx);
    const std::string &pbk = placement.blockkeys(lp_idx);
    const std::string dip = placement.datanodeip(lp_idx);
    const int dport = placement.datanodeport(lp_idx);
    for (const auto &sl : merged)
    {
      if (XorWriteRangeToDatanode(pbk.c_str(), local_bid, sl.offset, sl.delta.data(), sl.length, dip.c_str(),
                                  dport))
      {
        ++stats.ranges;
        stats.bytes += static_cast<size_t>(sl.length);
      }
      else
      {
        std::cerr << "[Proxy] XUE local parity XorWrite failed block " << pbk << " off=" << sl.offset
                  << " len=" << sl.length << std::endl;
      }
    }
    return stats;
  }

  void ProxyImpl::handleXueClass2Or3LocalParityOnDataCluster(
      const proxy_proto::AppendStripeDataPlacement &placement, const std::vector<char *> &slices,
      int tcp_slice_count, const char *append_buf, size_t cluster_append_size)
  {
    const int k0 = m_sys_config->k;
    const int r0 = m_sys_config->r;
    const int data_cluster =
        infer_data_block_cluster_from_placement(placement, tcp_slice_count, k0);
    int local_cluster = -1;
    if (!infer_local_parity_cluster_from_plan(placement, k0, r0, &local_cluster) || local_cluster < 0 ||
        data_cluster < 0 || data_cluster == local_cluster || m_self_cluster_id != data_cluster)
    {
      if (m_self_cluster_id == data_cluster && data_cluster >= 0 && local_cluster < 0)
      {
        log_xfert_line(proxy_xfer_timestamp(),
                       "class2_local_parity_skipped proxy_cluster=" +
                           std::to_string(m_self_cluster_id) + " append_key=" + placement.key() +
                           " reason=no_local_parity_meta_in_plan");
      }
      return;
    }
    const int global_cluster = placement.xue_global_parity_cluster_id();
    const std::string ts = proxy_xfer_timestamp();
    if (global_cluster >= 0 && data_cluster == global_cluster)
    {
      const bool fwd_ok = forwardXueDataDeltaSync(local_cluster, "XUE_COMPUTE_LOCAL_PARITY", placement,
                                                  append_buf, cluster_append_size);
      if (fwd_ok)
      {
        log_xfert_line(ts, "class2_local_parity_forward proxy_cluster=" +
                               std::to_string(m_self_cluster_id) + " -> " +
                               std::to_string(local_cluster) + " append_key=" + placement.key());
      }
      else
      {
        std::cerr << "[Proxy] class2 local parity forward failed append_key=" << placement.key()
                  << std::endl;
      }
      return;
    }
    if (needsClass3MergedLocalParityForward(placement, tcp_slice_count))
    {
      const bool fwd_ok =
          forwardMergedLocalParityDelta(local_cluster, placement, slices, tcp_slice_count);
      if (!fwd_ok)
      {
        std::cerr << "[Proxy] class3 merged local parity delta forward failed append_key="
                  << placement.key() << std::endl;
      }
    }
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

  bool ProxyImpl::ReadRangeFromDatanode(const char *block_key, int block_id, int range_offset, int range_size, char *out_buf, const char *ip, int port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::ReadRangeInfo req;
      datanode_proto::RequestResult result;
      req.set_block_key(std::string(block_key));
      req.set_block_id(block_id);
      req.set_range_offset(range_offset);
      req.set_range_size(range_size);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleReadRange(&context, req, &result);
      if (!stat.ok() || !result.message())
      {
        return false;
      }

      asio::error_code con_error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}), con_error);
      if (con_error)
      {
        return false;
      }
      asio::error_code ec;
      asio::read(socket, asio::buffer(out_buf, static_cast<size_t>(range_size)), ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      return !ec;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
      return false;
    }
  }

  bool ProxyImpl::WriteRangeToDatanode(const char *block_key, int block_id, int range_offset, const char *data, int range_size, const char *ip, int port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::WriteRangeInfo req;
      datanode_proto::RequestResult result;
      req.set_block_key(std::string(block_key));
      req.set_block_id(block_id);
      req.set_range_offset(range_offset);
      req.set_range_size(range_size);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleWriteRange(&context, req, &result);
      if (!stat.ok() || !result.message())
      {
        return false;
      }

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}), con_error);
      if (con_error)
      {
        return false;
      }
      asio::write(socket, asio::buffer(data, static_cast<size_t>(range_size)), error);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      return !error;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
      return false;
    }
  }

  bool ProxyImpl::XorWriteRangeToDatanode(const char *block_key, int block_id, int range_offset, const char *delta,
                                          int range_size, const char *ip, int port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::WriteRangeInfo req;
      datanode_proto::RequestResult result;
      req.set_block_key(std::string(block_key));
      req.set_block_id(block_id);
      req.set_range_offset(range_offset);
      req.set_range_size(range_size);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleXorWriteRange(&context, req, &result);
      if (!stat.ok() || !result.message())
      {
        return false;
      }

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}),
                    con_error);
      if (con_error)
      {
        return false;
      }
      asio::write(socket, asio::buffer(delta, static_cast<size_t>(range_size)), error);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      return !error;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
      return false;
    }
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
      asio::read(socket, asio::buffer(value, value_length), ec);
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
      asio::read(socket, asio::buffer(buf, value_length), ec);
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
      asio::read(socket, asio::buffer(value, value_length), ec);
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

    auto placement_copy = std::make_shared<proxy_proto::AppendStripeDataPlacement>(*append_stripe_data_placement);

    const bool layout_append =
        is_layout_stripe_append_mode(append_stripe_data_placement->append_mode());
    if (!is_xue_proxy_forward_tcp_mode(append_stripe_data_placement->append_mode()) && !layout_append)
    {
      const std::string ts = proxy_xfer_timestamp();
      const int plan_cluster = append_stripe_data_placement->cluster_id();
      const std::string route = xue_route_tag(*append_stripe_data_placement);
      const int tcp_slices = effective_tcp_slice_count(*append_stripe_data_placement, slice_num);
      std::ostringstream oss;
      oss << "schedule_append proxy_cluster=" << m_self_cluster_id
          << " ingress_proxy_cluster=" << plan_cluster
          << " append_key=" << append_stripe_data_placement->key() << " stripe_id=" << stripe_id
          << " mode=" << append_stripe_data_placement->append_mode()
          << " payload=" << xue_tcp_payload_desc(*append_stripe_data_placement)
          << " expect_tcp_bytes=" << cluster_append_size << " tcp_slices=" << tcp_slices
          << " plan_blocks=" << slice_num;
      append_key_tcp_meta_suffix(oss, append_stripe_data_placement->key());
      append_xue_global_parity_plan_suffix(oss, *append_stripe_data_placement);
      if (!route.empty())
      {
        oss << " " << route;
      }
      log_xfert_line(ts, oss.str());
    }

    if (append_stripe_data_placement->append_mode() == "XUE_UPDATE" &&
        append_stripe_data_placement->xue_compute_global_parity() &&
        append_stripe_data_placement->cluster_id() == m_self_cluster_id &&
        append_stripe_data_placement->xue_global_parity_cluster_id() == m_self_cluster_id)
    {
      registerXueGlobalParityIngressExpected(stripe_id, append_stripe_data_placement->key());
    }

    auto append_and_save = [this, stripe_id, cluster_append_size, slice_num, placement_copy, is_serialized]() mutable
    {
      try
      {
        asio::ip::tcp::socket socket_data(io_context);
        acceptor.accept(socket_data);
        asio::error_code error;

        // assert(m_pre_allocated_buffer_queue.size() > 0 && "Pre-allocated buffer queue is empty");
        // std::shared_ptr<char[]> append_buf = m_pre_allocated_buffer_queue.front();
        // m_pre_allocated_buffer_queue.pop();
        // char *append_buf = new char[cluster_append_size];
        // memset(append_buf, 0, cluster_append_size);
        // std::shared_ptr<char> append_buf_ptr(append_buf, [](char* p) { delete[] p; }); // 使用智能指针管理内存
        std::vector<char> append_buf(cluster_append_size, 0);
        asio::read(socket_data, asio::buffer(append_buf.data(), cluster_append_size), error);
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
          std::cout << "[Proxy" << m_self_cluster_id << "][Append339]"
                    << "Append to Stripe " << stripe_id << " with length of " << cluster_append_size << std::endl;
        }

        const bool send_ack = placement_copy->xue_send_ack();
        const int tcp_slice_count = effective_tcp_slice_count(*placement_copy, slice_num);
        std::vector<size_t> tcp_slice_sizes;
        tcp_slice_sizes.reserve(static_cast<size_t>(tcp_slice_count));
        size_t tcp_payload_sum = 0;
        for (int i = 0; i < tcp_slice_count && i < slice_num; ++i)
        {
          tcp_slice_sizes.push_back(placement_copy->sizes(i));
          tcp_payload_sum += placement_copy->sizes(i);
        }
        if (cluster_append_size > 0 && tcp_payload_sum > 0 &&
            tcp_payload_sum != cluster_append_size)
        {
          std::cerr << "[Proxy] tcp_slice_size_sum mismatch append_key=" << placement_copy->key()
                    << " proxy_cluster=" << m_self_cluster_id << " append_size=" << cluster_append_size
                    << " slice_sum=" << tcp_payload_sum << " tcp_slices=" << tcp_slice_count
                    << std::endl;
        }
        auto close_socket = [&socket_data]() {
          asio::error_code ignore_ec;
          socket_data.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
          socket_data.close(ignore_ec);
        };

        if (cluster_append_size == 0 || tcp_slice_sizes.empty())
        {
          std::cerr << "[Proxy] skip append: zero tcp payload append_key=" << placement_copy->key()
                    << " proxy_cluster=" << m_self_cluster_id
                    << " mode=" << placement_copy->append_mode() << std::endl;
          close_socket();
          return;
        }
        XueXferScopeTimer xue_xfer_timer;
        xue_xfer_timer.self = this;
        xue_xfer_timer.stripe_id = stripe_id;
        xue_xfer_timer.plan_id = placement_copy->xue_xfer_plan_id();
        if (xue_xfer_timer.plan_id > 0)
        {
          xue_xfer_timer.active = true;
          xue_xfer_timer.t0 = std::chrono::steady_clock::now();
          xue_xfer_timer.w0 = xue_wall_unix_ms_now();
        }
        std::vector<char *> slices =
            m_toolbox->splitCharPointer(append_buf.data(), cluster_append_size, tcp_slice_sizes);

        const std::string append_mode_str = placement_copy->append_mode();
        const bool azure_like =
            m_sys_config->CodeType == "AzureLRC" || m_sys_config->CodeType == "XueLRC";

        if (is_layout_stripe_append_mode(append_mode_str) && tcp_slice_count > 0)
        {
          std::vector<int> layout_block_ids;
          layout_block_ids.reserve(static_cast<size_t>(tcp_slice_count));
          for (int j = 0; j < tcp_slice_count && j < placement_copy->blockids_size(); ++j)
          {
            layout_block_ids.push_back(placement_copy->blockids(j));
          }
          log_layout_client_batch(m_self_cluster_id, stripe_id, layout_block_ids,
                                  cluster_append_size, tcp_slice_count);
        }
        else if (is_client_tcp_ingress(append_mode_str, *placement_copy))
        {
          log_proxy_tcp_hop(proxy_xfer_timestamp(), -1, m_self_cluster_id, placement_copy->key(),
                            cluster_append_size, tcp_slice_count);
        }

        if (append_mode_str == "XUE_UPDATE" && placement_copy->xue_class1_relay_path() &&
            !placement_copy->xue_data_slices_are_delta())
        {
          const bool ingress_ok = handleXueClass1RelayAtDataCluster(*placement_copy, slices, tcp_slice_count,
                                                                    append_buf.data(), cluster_append_size);
          if (placement_copy->xue_strict_schedule())
          {
            reportXueIngressReadyToCoordinator(stripe_id, placement_copy->key());
            auto append_shared = std::make_shared<std::vector<char>>(append_buf.begin(), append_buf.end());
            std::vector<char *> slice_ptrs = slices;
            std::thread([this, placement_copy, append_shared, slice_ptrs, tcp_slice_count]() {
              runXueStrictDeferredForwards(placement_copy, append_shared, slice_ptrs, tcp_slice_count);
            }).detach();
            if (send_ack)
            {
              const char ack = ingress_ok ? static_cast<char>(1) : static_cast<char>(0);
              asio::error_code ack_ec;
              asio::write(socket_data, asio::buffer(&ack, 1), ack_ec);
            }
            close_socket();
            return;
          }
          if (!ingress_ok)
          {
            std::cerr << "[Proxy] class1 relay: data cluster -> relay 转发 data_delta 失败" << std::endl;
          }
          if (send_ack)
          {
            const char ack = ingress_ok ? static_cast<char>(1) : static_cast<char>(0);
            asio::error_code ack_ec;
            asio::write(socket_data, asio::buffer(&ack, 1), ack_ec);
          }
          close_socket();
          coordinator_proto::CommitAbortKey commit_abort_key;
          coordinator_proto::ReplyFromCoordinator result;
          grpc::ClientContext context;
          commit_abort_key.set_opp(APPEND);
          commit_abort_key.set_key(placement_copy->key());
          commit_abort_key.set_stripe_id(stripe_id);
          commit_abort_key.set_ifcommitmetadata(ingress_ok);
          grpc::Status status =
              m_coordinator_ptr->reportCommitAbort(&context, commit_abort_key, &result);
          log_append_commit_report(proxy_xfer_timestamp(), m_self_cluster_id, placement_copy->key(),
                                   stripe_id, status.ok() && ingress_ok);
          return;
        }

        if (append_mode_str == "XUE_DELTA_TO_RELAY")
        {
          const std::string ts = proxy_xfer_timestamp();
          if (!placement_copy->xue_data_slices_are_delta())
          {
            std::cerr << "[Proxy][XFERT] " << ts
                      << " proxy_tcp_warn append_key=" << placement_copy->key()
                      << " expected data_delta payload" << std::endl;
          }
          const int src_cluster = infer_tcp_source_cluster_for_recv(
              *placement_copy, append_mode_str, m_self_cluster_id, tcp_slice_count, m_sys_config->k);
          const int global_cluster = placement_copy->xue_global_parity_cluster_id();
          log_proxy_tcp_hop(ts, src_cluster, m_self_cluster_id, placement_copy->key(),
                            cluster_append_size, tcp_slice_count);

          if (placement_copy->xue_strict_schedule())
          {
            // Ack 2->3 before waitXueScheduleStep(3->0): upstream reports step done only after ack.
            if (send_ack)
            {
              const char ack = static_cast<char>(1);
              asio::error_code ack_ec;
              asio::write(socket_data, asio::buffer(&ack, 1), ack_ec);
            }
            close_socket();

            auto append_shared =
                std::make_shared<std::vector<char>>(append_buf.begin(), append_buf.end());
            std::thread([this, placement_copy, append_shared, tcp_slice_count, src_cluster,
                         global_cluster, ts]() {
              const int stripe_id_local = placement_copy->stripe_id();
              const uint64_t xfer_plan_id = placement_copy->xue_xfer_plan_id();
              const auto xfer_t0 = std::chrono::steady_clock::now();
              const int64_t xfer_w0 = xue_wall_unix_ms_now();
              proxy_proto::AppendStripeDataPlacement fwd_placement = *placement_copy;
              fwd_placement.set_xue_data_slices_are_delta(true);
              const bool ok = forwardXueDataDeltaSync(
                  global_cluster, "XUE_DELTA_TO_GLOBAL", fwd_placement, append_shared->data(),
                  append_shared->size(), true);
              if (ok)
              {
                log_proxy_tcp_relay_passthrough(ts, m_self_cluster_id, src_cluster, global_cluster,
                                                placement_copy->key(), append_shared->size(),
                                                tcp_slice_count);
              }
              record_xue_xfer_sample(stripe_id_local, xfer_plan_id, xfer_t0,
                                     std::chrono::steady_clock::now(), xfer_w0,
                                     xue_wall_unix_ms_now());
            }).detach();
            return;
          }

          proxy_proto::AppendStripeDataPlacement fwd_placement = *placement_copy;
          fwd_placement.set_xue_data_slices_are_delta(true);
          bool ok = forwardXueDataDeltaSync(global_cluster, "XUE_DELTA_TO_GLOBAL", fwd_placement,
                                            append_buf.data(), cluster_append_size, false);
          if (ok)
          {
            log_proxy_tcp_relay_passthrough(ts, m_self_cluster_id, src_cluster, global_cluster,
                                            placement_copy->key(), cluster_append_size, tcp_slice_count);
          }
          if (send_ack)
          {
            const char ack = ok ? static_cast<char>(1) : static_cast<char>(0);
            asio::error_code ack_ec;
            asio::write(socket_data, asio::buffer(&ack, 1), ack_ec);
          }
          close_socket();
          return;
        }

        if (append_mode_str == "XUE_DELTA_TO_GLOBAL")
        {
          const std::string ts = proxy_xfer_timestamp();
          const int src_cluster = infer_tcp_source_cluster_for_recv(
              *placement_copy, append_mode_str, m_self_cluster_id, tcp_slice_count, m_sys_config->k);
          const XueParityWriteStats parity_write =
              applyXueGlobalParityFromDataDeltas(*placement_copy, slices, tcp_slice_count);
          const bool ok = parity_write.ranges > 0;
          if (ok)
          {
            log_proxy_tcp_global_done(ts, m_self_cluster_id, src_cluster, placement_copy->key(),
                                      cluster_append_size, tcp_slice_count, parity_write);
          }
          else
          {
            std::cerr << "[Proxy][XFERT] " << ts
                      << " XUE_GLOBAL_PARITY_FAILED proxy_cluster=" << m_self_cluster_id
                      << " append_key=" << placement_copy->key()
                      << " plan_blocks=" << placement_copy->blockids_size()
                      << " tcp_slices=" << tcp_slice_count
                      << " (no global parity metadata in plan or WriteRange failed)" << std::endl;
          }
          if (send_ack)
          {
            const char ack = ok ? static_cast<char>(1) : static_cast<char>(0);
            asio::error_code ack_ec;
            asio::write(socket_data, asio::buffer(&ack, 1), ack_ec);
          }
          close_socket();
          return;
        }

        if (append_mode_str == "XUE_LOCAL_PARITY_DELTA")
        {
          const std::string ts = proxy_xfer_timestamp();
          const XueParityWriteStats parity_write = applyReceivedLocalParityDelta(
              *placement_copy, append_buf.data(), cluster_append_size);
          const bool ok = parity_write.ranges > 0;
          if (ok)
          {
            log_xue_parity_write_applied(ts, "XUE_LOCAL_PARITY_DELTA applied", m_self_cluster_id,
                                         placement_copy->key(), parity_write, nullptr,
                                         cluster_append_size, tcp_slice_count);
          }
          else
          {
            std::cerr << "[Proxy][XFERT] " << ts << " XUE_LOCAL_PARITY_DELTA_FAILED proxy_cluster="
                      << m_self_cluster_id << " append_key=" << placement_copy->key() << std::endl;
          }
          if (send_ack)
          {
            const char ack = ok ? static_cast<char>(1) : static_cast<char>(0);
            asio::error_code ack_ec;
            asio::write(socket_data, asio::buffer(&ack, 1), ack_ec);
          }
          close_socket();
          return;
        }

        if (append_mode_str == "XUE_COMPUTE_LOCAL_PARITY")
        {
          const std::string ts = proxy_xfer_timestamp();
          const XueParityWriteStats parity_write =
              applyXueLocalParityFromDataDeltas(*placement_copy, slices, tcp_slice_count);
          const bool ok = parity_write.ranges > 0;
          if (ok)
          {
            log_xue_parity_write_applied(ts, "proxy_tcp_local_parity applied", m_self_cluster_id,
                                         placement_copy->key(), parity_write, nullptr,
                                         cluster_append_size, tcp_slice_count);
          }
          if (send_ack)
          {
            const char ack = ok ? static_cast<char>(1) : static_cast<char>(0);
            asio::error_code ack_ec;
            asio::write(socket_data, asio::buffer(&ack, 1), ack_ec);
          }
          close_socket();
          return;
        }

        if (!is_layout_stripe_append_mode(append_mode_str) && !is_client_tcp_ingress(append_mode_str, *placement_copy) &&
            !is_xue_proxy_forward_tcp_mode(append_mode_str))
        {
          log_proxy_tcp_hop(proxy_xfer_timestamp(), -1, m_self_cluster_id, placement_copy->key(),
                            cluster_append_size, tcp_slice_count);
        }

        if (!send_ack)
        {
          asio::error_code ignore_ec;
          socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
          socket_data.close(ignore_ec);
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

        const bool slices_are_delta = placement_copy->xue_data_slices_are_delta();
        std::vector<int> global_parity_indices;
        std::vector<std::thread> senders;
        for (int j = 0; j < slice_num; j++)
        {
          if (j >= tcp_slice_count)
          {
            continue;
          }
          const int bid = placement_copy->blockids(j);
          const bool xue_data_path = (append_mode_str == "XUE_UPDATE" && !slices_are_delta && bid >= 0 &&
                                      bid < m_sys_config->k);
          const int xue_global_begin = (m_sys_config->CodeType == "XueLRC")
                                           ? (m_sys_config->k + m_sys_config->r)
                                           : m_sys_config->k;
          const int xue_global_end = (m_sys_config->CodeType == "XueLRC")
                                         ? (m_sys_config->k + m_sys_config->r + m_sys_config->z)
                                         : (m_sys_config->k + m_sys_config->r);
          const bool xue_global_parity_deferred = (append_mode_str == "XUE_UPDATE" && azure_like &&
                                                   bid >= xue_global_begin && bid < xue_global_end &&
                                                   placement_copy->xue_compute_global_parity());
          const int block_cluster = (j < placement_copy->block_cluster_ids_size())
                                        ? placement_copy->block_cluster_ids(j)
                                        : clusterIdForDatanodeEndpoint(placement_copy->datanodeip(j),
                                                                       placement_copy->datanodeport(j));
          if (xue_data_path && block_cluster == m_self_cluster_id)
          {
            senders.push_back(std::thread(
                [this, placement_copy, slices, j]() {
                  const std::string bk = placement_copy->blockkeys(j);
                  const int block_id = placement_copy->blockids(j);
                  const size_t sz = placement_copy->sizes(j);
                  const int off = static_cast<int>(placement_copy->offsets(j));
                  const std::string dip = placement_copy->datanodeip(j);
                  const int dport = placement_copy->datanodeport(j);
                  std::vector<char> oldbuf(sz);
                  if (!ReadRangeFromDatanode(bk.c_str(), block_id, off, static_cast<int>(sz), oldbuf.data(), dip.c_str(), dport))
                  {
                    std::memset(oldbuf.data(), 0, sz);
                  }
                  std::vector<char> newbuf(sz);
                  std::memcpy(newbuf.data(), slices[j], sz);
                  for (size_t i = 0; i < sz; ++i)
                  {
                    slices[j][i] = static_cast<char>(
                        static_cast<unsigned char>(newbuf[i]) ^ static_cast<unsigned char>(oldbuf[i]));
                  }
                  if (!WriteRangeToDatanode(bk.c_str(), block_id, off, newbuf.data(), static_cast<int>(sz), dip.c_str(), dport))
                  {
                    std::cerr << "[Proxy] XUE_UPDATE WriteRangeToDatanode failed block " << bk << std::endl;
                  }
                }));
          }
          else if (xue_global_parity_deferred)
          {
            global_parity_indices.push_back(j);
          }
          else if (block_cluster == m_self_cluster_id)
          {
            senders.push_back(std::thread(append_to_datanode, placement_copy->blockkeys(j).c_str(), bid, placement_copy->sizes(j), slices[j], placement_copy->offsets(j), placement_copy->datanodeip(j).c_str(), placement_copy->datanodeport(j), is_serialized));
          }
          else if (!is_layout_stripe_append_mode(append_mode_str) &&
                   (append_mode_str == "UNILRC_MODE" ||
                    (append_mode_str == "XUE_UPDATE" && !slices_are_delta)))
          {
            std::cerr << "[Proxy] skip cross-cluster datanode_write proxy_cluster="
                      << m_self_cluster_id << " block_cluster=" << block_cluster
                      << " block_id=" << bid << " append_key=" << placement_copy->key() << std::endl;
          }
        }
        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }

        if (placement_copy->xue_strict_schedule() && append_mode_str == "XUE_UPDATE" &&
            !placement_copy->xue_data_slices_are_delta())
        {
          if (azure_like)
          {
            int local_parity_cluster = -1;
            if (infer_class1_local_parity_cluster(*placement_copy, m_sys_config->k, m_sys_config->r,
                                                  &local_parity_cluster) &&
                local_parity_cluster >= 0 && m_self_cluster_id == local_parity_cluster)
            {
              const XueParityWriteStats local_write =
                  applyXueLocalParityFromDataDeltas(*placement_copy, slices, tcp_slice_count);
              if (local_write.ranges > 0)
              {
                log_xue_parity_write_applied(proxy_xfer_timestamp(),
                                             "XUE_COMPUTE_LOCAL_PARITY applied", local_parity_cluster,
                                             placement_copy->key(), local_write,
                                             "route=strict_ingress");
              }
            }
            else if (tcp_slice_count > 0 &&
                     needsClass3MergedLocalParityForward(*placement_copy, tcp_slice_count))
            {
              int lp_cluster = -1;
              if (infer_local_parity_cluster_from_plan(*placement_copy, m_sys_config->k, m_sys_config->r,
                                                       &lp_cluster) &&
                  lp_cluster >= 0)
              {
                const bool fwd_ok = forwardMergedLocalParityDelta(lp_cluster, *placement_copy, slices,
                                                                  tcp_slice_count);
                if (!fwd_ok)
                {
                  std::cerr << "[Proxy] strict class3 local parity forward failed append_key="
                            << placement_copy->key() << std::endl;
                }
              }
            }
            if (placement_copy->xue_compute_global_parity() &&
                placement_copy->xue_global_parity_cluster_id() == m_self_cluster_id &&
                m_self_cluster_id == placement_copy->cluster_id())
            {
              const XueParityWriteStats global_write =
                  applyXueGlobalParityFromDataDeltas(*placement_copy, slices, tcp_slice_count);
              if (global_write.ranges > 0)
              {
                log_xue_parity_write_applied(
                    proxy_xfer_timestamp(), "XUE_UPDATE global_parity applied", m_self_cluster_id,
                    placement_copy->key(), global_write, "route=strict_class2_ingress_data_global");
              }
              int local_parity_cluster = -1;
              if (infer_class2_remote_local_parity_cluster(
                      *placement_copy, m_sys_config->k, m_sys_config->r, tcp_slice_count,
                      m_self_cluster_id, &local_parity_cluster))
              {
                bool scheduled_local_hop = false;
                for (int hi = 0; hi < placement_copy->xue_strict_outgoing_size(); ++hi)
                {
                  if (placement_copy->xue_strict_outgoing(hi).to_cluster() == local_parity_cluster)
                  {
                    scheduled_local_hop = true;
                    break;
                  }
                }
                if (!scheduled_local_hop)
                {
                  const bool fwd_ok = forwardMergedLocalParityDelta(
                      local_parity_cluster, *placement_copy, slices, tcp_slice_count);
                  if (fwd_ok)
                  {
                    log_xfert_line(
                        proxy_xfer_timestamp(),
                        "class2_local_parity_forward proxy_cluster=" +
                            std::to_string(m_self_cluster_id) + " -> " +
                            std::to_string(local_parity_cluster) +
                            " append_key=" + placement_copy->key() + " route=strict_ingress");
                  }
                  else
                  {
                    std::cerr << "[Proxy] strict class2 local parity forward failed append_key="
                              << placement_copy->key() << std::endl;
                  }
                }
              }
            }
          }
          reportXueIngressReadyToCoordinator(stripe_id, placement_copy->key());
          auto append_shared = std::make_shared<std::vector<char>>(append_buf.begin(), append_buf.end());
          std::vector<char *> slice_ptrs = slices;
          std::thread([this, placement_copy, append_shared, slice_ptrs, tcp_slice_count]() {
            runXueStrictDeferredForwards(placement_copy, append_shared, slice_ptrs, tcp_slice_count);
          }).detach();
          if (send_ack)
          {
            const char ack = static_cast<char>(1);
            asio::error_code ack_ec;
            asio::write(socket_data, asio::buffer(&ack, 1), ack_ec);
          }
          close_socket();
          return;
        }

        if (append_mode_str == "XUE_UPDATE" && azure_like)
        {
          int local_parity_cluster = -1;
          if (infer_class1_local_parity_cluster(*placement_copy, m_sys_config->k, m_sys_config->r,
                                                &local_parity_cluster) &&
              local_parity_cluster >= 0)
          {
            if (m_self_cluster_id == local_parity_cluster)
            {
              const XueParityWriteStats local_write =
                  applyXueLocalParityFromDataDeltas(*placement_copy, slices, tcp_slice_count);
              if (local_write.ranges > 0)
              {
                log_xue_parity_write_applied(proxy_xfer_timestamp(),
                                             "XUE_COMPUTE_LOCAL_PARITY applied", local_parity_cluster,
                                             placement_copy->key(), local_write, "route=xue_update");
              }
            }
            else if (!placement_copy->xue_strict_schedule() && tcp_slice_count > 0 &&
                     needsClass3MergedLocalParityForward(*placement_copy, tcp_slice_count))
            {
              const bool fwd_ok = forwardMergedLocalParityDelta(local_parity_cluster, *placement_copy,
                                                                slices, tcp_slice_count);
              if (!fwd_ok)
              {
                std::cerr << "[Proxy] class3 merged local parity delta forward failed append_key="
                          << placement_copy->key() << std::endl;
              }
            }
            else if (!placement_copy->xue_strict_schedule() && tcp_slice_count > 0)
            {
              forwardXueDataDeltaSync(local_parity_cluster, "XUE_COMPUTE_LOCAL_PARITY", *placement_copy,
                                      append_buf.data(), cluster_append_size);
            }
          }
          if (!placement_copy->xue_strict_schedule())
          {
            handleXueClass2Or3LocalParityOnDataCluster(*placement_copy, slices, tcp_slice_count,
                                                       append_buf.data(), cluster_append_size);
          }
        }

        // Global parity 与 local parity 独立：此前 else-if 会在进入 local 分支后跳过 global 转发/落盘。
        if (!placement_copy->xue_strict_schedule() && append_mode_str == "XUE_UPDATE" && azure_like &&
            placement_copy->xue_global_parity_cluster_id() >= 0 &&
            placement_copy->xue_global_parity_cluster_id() != m_self_cluster_id && tcp_slice_count > 0)
        {
          const int global_cluster = placement_copy->xue_global_parity_cluster_id();
          const bool global_fwd =
              forwardXueDataDeltaSync(global_cluster, "XUE_DELTA_TO_GLOBAL", *placement_copy,
                                    append_buf.data(), cluster_append_size);
          if (!global_fwd)
          {
            log_xfert_line(proxy_xfer_timestamp(),
                           "xue_update_global_forward_failed proxy_cluster=" +
                               std::to_string(m_self_cluster_id) + " -> " +
                               std::to_string(global_cluster) + " append_key=" +
                               placement_copy->key());
          }
        }
        else if (!placement_copy->xue_strict_schedule() && append_mode_str == "XUE_UPDATE" && azure_like &&
                 placement_copy->xue_compute_global_parity())
        {
          const XueParityWriteStats global_write =
              applyXueGlobalParityFromDataDeltas(*placement_copy, slices, tcp_slice_count);
          if (global_write.ranges > 0)
          {
            log_xue_parity_write_applied(proxy_xfer_timestamp(), "XUE_UPDATE global_parity applied",
                                         m_self_cluster_id, placement_copy->key(), global_write,
                                         "route=class2_ingress_data_global");
          }
          else
          {
            log_xfert_line(proxy_xfer_timestamp(),
                           "xue_update_global_parity_skipped proxy_cluster=" +
                               std::to_string(m_self_cluster_id) + " append_key=" +
                               placement_copy->key() +
                               " reason=apply_returned_zero meta_blocks_present");
          }
          int local_parity_cluster_after_global = -1;
          if (infer_class1_local_parity_cluster(*placement_copy, m_sys_config->k, m_sys_config->r,
                                                &local_parity_cluster_after_global) &&
              local_parity_cluster_after_global >= 0 &&
              needsClass3MergedLocalParityForward(*placement_copy, tcp_slice_count))
          {
            forwardMergedLocalParityDelta(local_parity_cluster_after_global, *placement_copy, slices,
                                        tcp_slice_count);
          }
          else if (!global_parity_indices.empty())
          {
            for (int idx : global_parity_indices)
            {
              append_to_datanode(placement_copy->blockkeys(idx).c_str(), placement_copy->blockids(idx),
                                 placement_copy->sizes(idx), slices[idx], placement_copy->offsets(idx),
                                 placement_copy->datanodeip(idx).c_str(), placement_copy->datanodeport(idx),
                                 is_serialized);
            }
            log_xfert_line(proxy_xfer_timestamp(),
                           "datanode_write_fallback proxy_cluster=" + std::to_string(m_self_cluster_id) +
                               " global_parity_blocks=" + std::to_string(global_parity_indices.size()));
          }
        }
        else if (!placement_copy->xue_strict_schedule() && append_mode_str == "XUE_UPDATE" && azure_like &&
                 placement_copy->xue_global_parity_cluster_id() < 0)
        {
          log_xfert_line(proxy_xfer_timestamp(),
                         "xue_update_global_parity_skipped proxy_cluster=" +
                             std::to_string(m_self_cluster_id) + " append_key=" +
                             placement_copy->key() + " reason=no_global_parity_cluster_in_plan");
        }

        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Append371]"
                    << "Finish appending to Stripe " << stripe_id << std::endl;
        }

        if (placement_copy->is_merge_parity())
        {
          if (!is_layout_stripe_append_mode(append_mode_str))
          {
            log_xfert_line(proxy_xfer_timestamp(),
                           "merge_parity_begin proxy_cluster=" + std::to_string(m_self_cluster_id) +
                               " stripe_id=" + std::to_string(stripe_id) + " append_key=" +
                               placement_copy->key());
          }
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
        grpc::Status status;
        status = m_coordinator_ptr->reportCommitAbort(&context, commit_abort_key, &result);
        if (!is_layout_stripe_append_mode(append_mode_str))
        {
          log_append_commit_report(proxy_xfer_timestamp(), m_self_cluster_id, placement_copy->key(),
                                   stripe_id, status.ok());
        }
        else if (!status.ok())
        {
          std::cerr << "[Proxy][XFERT] layout_commit_failed proxy_cluster=" << m_self_cluster_id
                    << " stripe_id=" << stripe_id << " key=" << placement_copy->key() << std::endl;
        }
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
      {
        std::lock_guard<std::mutex> lk(m_client_append_queue_mutex);
        m_client_append_tasks.push_back(std::move(append_and_save));
      }
      m_client_append_queue_cv.notify_one();
      ensure_client_append_worker();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  void ProxyImpl::ensure_client_append_worker()
  {
    bool expected = false;
    if (m_client_append_worker_started.compare_exchange_strong(expected, true))
    {
      std::thread([this]() { client_append_worker_loop(); }).detach();
    }
  }

  void ProxyImpl::client_append_worker_loop()
  {
    for (;;)
    {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lk(m_client_append_queue_mutex);
        m_client_append_queue_cv.wait(lk, [this]() { return !m_client_append_tasks.empty(); });
        task = std::move(m_client_append_tasks.front());
        m_client_append_tasks.pop_front();
      }
      if (task)
      {
        task();
      }
    }
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
        log_layout_set_done(m_self_cluster_id, key, static_cast<int>(senders.size()), block_size,
                            status.ok());
        if (status.ok() && IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "[SET] report to coordinator success" << std::endl;
        }
        else if (!status.ok())
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

      asio::write(sock_data, asio::buffer(key, key.size()), error);
      if(error)
      {
        std::cout << "error in write key" << std::endl;
      }
      asio::write(sock_data, asio::buffer(value, value_size_bytes), error);
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
        else if (code_type == "AzureLRC")
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
        else if (code_type == "AzureLRC")
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
      else if (code_type == "AzureLRC")
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
      else if (code_type == "AzureLRC")
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
      else if (code_type == "AzureLRC")
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
        else if (code_type == "AzureLRC")
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
        else if (code_type == "AzureLRC")
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
      asio::write(socket_data, asio::buffer(&block_id, sizeof(u_int32_t)));
      asio::write(socket_data, asio::buffer(blocks + i * static_cast<size_t>(BlockSize), BlockSize));
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