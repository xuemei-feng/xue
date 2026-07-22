#ifndef CLIENT_H
#define CLIENT_H

#ifdef BAZEL_BUILD
#include "src/proto/coordinator.grpc.pb.h"
#else
#include "coordinator.grpc.pb.h"
#endif

#include "meta_definition.h"
#include <grpcpp/grpcpp.h>
#include <asio.hpp>
#include "config.h"
#include "toolbox.h"
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
namespace ECProject
{
  /** CoRD 单次 cord_update 各阶段耗时（秒），由 cord_update 填充。 */
  struct CordUpdateTiming
  {
    double wall_sec = 0.0;
    double plan_sec = 0.0;         // uploadCordUpdate（coordinator 规划）
    double payload_prep_sec = 0.0; // 随机负载生成等
    double upload_sec = 0.0;         // TCP 上传各 cluster slice + checkCommitAbort
    double xfer_begin_sec = 0.0;     // 保留字段；auto-start 模式下恒为 0
    double xfer_wait_sec = 0.0;      // cordPlanWaitTransferComplete 总 wall time
    double xfer_pure_sec = 0.0;      // 跨 cluster 真实传输（proxy 上报 wall span）
    double xfer_grpc_sec = 0.0;      // xfer_wait 中非 pure 部分（gRPC + 编排 + Client↔Coordinator RTT）
  };

  /** upload 完成后、xfer wait 之前的状态；用于流水线 batch（defer xfer wait）。 */
  struct CordUpdatePending
  {
    int stripe_id = -1;
    std::string transfer_plan_key;
    std::chrono::steady_clock::time_point wall_t0{};
    double plan_sec = 0.0;
    double payload_prep_sec = 0.0;
    double upload_sec = 0.0;
  };

  class Client
  {
  public:
    Client(std::string ClientIP, int ClientPort, std::string CoordinatorIpPort) : m_coordinatorIpPort(CoordinatorIpPort),
                                                                                  m_clientIPForGet(ClientIP),
                                                                                  m_clientPortForGet(ClientPort),
                                                                                  acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address_v4::any(), m_clientPortForGet))
    {
      auto channel = grpc::CreateChannel(m_coordinatorIpPort, grpc::InsecureChannelCredentials());
      m_coordinator_ptr = coordinator_proto::coordinatorService::NewStub(channel);
      m_clientID = ClientIP + ":" + std::to_string(ClientPort);
    }

    Client(std::string ClientIP, int ClientPort, std::string CoordinatorIpPort, std::string config_path) : m_coordinatorIpPort(CoordinatorIpPort),
                                                                                                           m_clientIPForGet(ClientIP),
                                                                                                           m_clientPortForGet(ClientPort),
                                                                                                           acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address_v4::any(), m_clientPortForGet))
    {
      auto channel = grpc::CreateChannel(m_coordinatorIpPort, grpc::InsecureChannelCredentials());
      m_coordinator_ptr = coordinator_proto::coordinatorService::NewStub(channel);
      m_clientID = ClientIP + ":" + std::to_string(ClientPort);
      m_sys_config = ECProject::Config::getInstance(config_path);
      m_toolbox = ECProject::ToolBox::getInstance();
      m_pre_allocated_buffer = new char[static_cast<size_t> (m_sys_config->BlockSize) * static_cast<size_t> (m_sys_config->n)];
      memset(m_pre_allocated_buffer, 0xaa, (m_sys_config->BlockSize) * static_cast<size_t> (m_sys_config->n));
      if (m_sys_config->AppendMode == "CACHED_MODE")
      {
        m_cached_buffer = new char *[m_sys_config->r + m_sys_config->z];
        for (int i = 0; i < m_sys_config->r + m_sys_config->z; i++)
        {
          m_cached_buffer[i] = new char[m_sys_config->BlockSize];
          memset(m_cached_buffer[i], 0, m_sys_config->BlockSize);
        }
      }
    }

    ~Client()
    {
      delete[] m_pre_allocated_buffer;
      if (m_sys_config->AppendMode == "CACHED_MODE")
      {
        for (int i = 0; i < m_sys_config->r + m_sys_config->z; i++)
        {
          delete[] m_cached_buffer[i];
        }
        delete[] m_cached_buffer;
      }
    }

    std::string sayHelloToCoordinatorByGrpc(std::string hello);
    bool append(int append_size);
    bool sub_append(int append_size);
    bool sub_append_in_rep_mode(int append_size);
    bool set();
    bool sub_set(int block_num);
    /** 同一条带内多个不连续逻辑区间 [start, end] */
    bool xue_update(int stripe_id, const std::vector<std::pair<int, int>> &logical_ranges);
    /** CoRD：半开区间列表；全局校验由 uploadCordUpdate 下发的传输计划在 proxy 侧完成，
     * 本地校验由随后的 uploadCordLocalParityApply 完成（无需二选一）。
     * interval_count 由客户端按区间条数自动填充。
     * 若 update_payload==nullptr 且 update_payload_bytes==0，则在 coordinator 返回长度后用 0xBB 填充负载。 */
    bool cord_update(int stripe_id, const std::vector<std::pair<int, int>> &logical_ranges,
                     const char *update_payload, size_t update_payload_bytes,
                     CordUpdateTiming *out_timing = nullptr);
    /** plan + upload；若有跨 cluster 传输则写入 pending->transfer_plan_key，不阻塞 wait。 */
    bool cord_update_start(int stripe_id, const std::vector<std::pair<int, int>> &logical_ranges,
                           const char *update_payload, size_t update_payload_bytes,
                           CordUpdatePending *pending, CordUpdateTiming *partial_timing = nullptr);
    /** 等待 pending 中 transfer plan 完成并填充 xfer_* / wall_sec。plan_key 为空则 no-op。 */
    bool cord_update_wait_xfer(CordUpdatePending *pending, CordUpdateTiming *out_timing = nullptr);
    std::shared_ptr<char[]> get_degraded_read_block(int stripe_id, int failed_block_id);
    std::shared_ptr<char[]> get_degraded_read_block_breakdown(int stripe_id, int failed_block_id, double &total_time, double &disk_io_time, double &network_time, double &encode_time);
    bool recovery_breakdown(int stripe_id, int failed_block_id, double &disk_read_time, double &network_time, double &decode_time, double &disk_write_time);
    bool recovery(int stripe_id, int failed_block_id);
    bool multi_block_recovery(int stripe_id, std::vector<int> block_ids);
    int recovery_full_node(int node_id);
    std::vector<int> get_data_block_num_per_group(int k, int r, int z, std::string code_type);
    std::vector<int> get_global_parity_block_num_per_group(int k, int r, int z, std::string code_type);
    std::vector<int> get_local_parity_block_num_per_group(int k, int r, int z, std::string code_type);
    bool set(std::string key, std::string value);
    bool SetParameterByGrpc(ECSchema input_ecschema);
    std::shared_ptr<char[]> get(std::string key, size_t &data_size);
    std::shared_ptr<char[]> get_blocks(int start_block_id, int end_block_id);
    std::shared_ptr<char[]> get_degraded_read_blocks(int start_block_id, int end_block_id);
    bool get(std::string key, std::string &value);
    bool delete_key(std::string key);
    bool delete_stripe(int stripe_id);
    bool delete_all_stripes();
    int get_append_slice_plans(std::string append_mode, int curr_logical_offset, int append_size, std::vector<std::vector<int>> *node_slice_sizes_per_cluster, std::vector<int> *modified_data_block_nums_per_cluster, std::vector<int> *data_ptr_size_array, int &parity_slice_size, int &parity_slice_offset);
    void split_for_append_data_and_parity(const coordinator_proto::ReplyProxyIPsPorts *reply_proxy_ips_ports, const std::vector<char *> &cluster_slice_data, const std::vector<std::vector<int>> &node_slice_sizes_per_cluster, const std::vector<int> &modified_data_block_nums_per_cluster, std::vector<char *> &data_ptr_array, std::vector<char *> &global_parity_ptr_array, std::vector<char *> &local_parity_ptr_array);
    void split_for_set_data_and_parity(const coordinator_proto::ReplyProxyIPsPorts *reply_proxy_ips_ports, const std::vector<char *> &cluster_slice_data, const std::vector<int> &data_block_num_per_group, const std::vector<int> &global_parity_block_num_per_group, const std::vector<int> &local_parity_block_num_per_group, std::vector<char *> &data_ptr_array, std::vector<char *> &global_parity_ptr_array, std::vector<char *> &local_parity_ptr_array);
    void async_append_to_proxies(char *cluster_slice_data, std::string append_key, int cluster_slice_size, std::string proxy_ip, int proxy_port, int index, bool *if_commit_arr);
    // 真正的异步版本（Asio 多路复用）
    void async_append_to_proxies_async(asio::io_context &io_context,
                                       char *cluster_slice_data,
                                       std::string append_key,
                                       int cluster_slice_size,
                                       std::string proxy_ip,
                                       int proxy_port,
                                       int index,
                                       bool *if_commit_arr,
                                       std::shared_ptr<std::atomic<int>> pending_counter);
    void async_cord_update_to_proxies(char *cluster_slice_data, std::string cord_key, int cluster_slice_size, std::string proxy_ip, int proxy_port, int index, bool *if_commit_arr);
    void get_cached_parity_slices(std::vector<char *> &global_parity_ptr_array, std::vector<char *> &local_parity_ptr_array, const int parity_slice_size, const int parity_slice_offset);
    void cache_latest_parity_slices(std::vector<char *> &global_parity_ptr_array, std::vector<char *> &local_parity_ptr_array, const int parity_slice_size, const int parity_slice_offset);
    std::vector<int> get_parameters();
    bool decode_test(int stripe_id, int failed_block_id, std::string client_ip, int client_port, double &decode_time);

  private:
    std::unique_ptr<coordinator_proto::coordinatorService::Stub> m_coordinator_ptr;
    std::string m_coordinatorIpPort;
    std::string m_clientIPForGet;
    int m_clientPortForGet;
    std::string m_clientID;
    asio::io_context io_context;
    asio::ip::tcp::acceptor acceptor;

    int m_append_logical_offset = 0;
    ECProject::Config *m_sys_config;
    ECProject::ToolBox *m_toolbox;
    char *m_pre_allocated_buffer = nullptr;
    char **m_cached_buffer = nullptr;
    /** 串行化发往各 proxy 数据口的 TCP，避免与 coordinator 并行 notify 导致的 accept/期望长度错配。 */
    std::mutex m_proxy_tcp_mu;
  };

} // namespace ECProject

#endif // CLIENT_H
