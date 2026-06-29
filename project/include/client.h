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
#include <utility>
#include <vector>
#include <mutex>
#include <map>
#include <string>
namespace ECProject
{
  struct XueAppendNetworkTiming
  {
    double tcp_resolve_connect_write_shutdown_s = 0;
    double coordinator_checkCommitAbort_s = 0;
  };

  struct XueClientTimingSummary
  {
    double coordinator_uploadXueUpdate_s = 0;
    double prepare_parse_pack_encode_s = 0;
    double sum_tcp_to_proxy_s = 0;
    double sum_coordinator_checkCommitAbort_s = 0;
    double wait_all_ingress_ready_s = 0;
    double sum_release_schedule_wave_s = 0;
    double ingress_parallel_wave_tcp_wall_s = 0;
    double total_client_xue_s = 0;
  };

  /** XUE batch 输出用分阶段耗时（秒），字段命名与 CoRD batch 对齐便于对比。 */
  struct XueUpdateBatchTiming
  {
    double wall_sec = 0.0;
    double plan_sec = 0.0;         // uploadXueUpdate（coordinator 规划/调度）
    double payload_prep_sec = 0.0; // 本地 buffer、对齐 padding、TCP payload 打包
    double upload_sec = 0.0;         // Client -> Proxy TCP 上传（并行时为 wave wall）
    double xfer_begin_sec = 0.0;     // releaseXueScheduleWave（触发后续传输波次）
    double xfer_wait_sec = 0.0;      // wait ingress / commit 等待跨 cluster 执行完成
  };

  class Client
  {
  public:
    Client(std::string ClientIP, int ClientPort, std::string CoordinatorIpPort) : m_coordinatorIpPort(CoordinatorIpPort),
                                                                                  m_clientIPForGet(ClientIP),
                                                                                  m_clientPortForGet(ClientPort),
                                                                                  acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(ClientIP.c_str()), m_clientPortForGet))
    {
      auto channel = grpc::CreateChannel(m_coordinatorIpPort, grpc::InsecureChannelCredentials());
      m_coordinator_ptr = coordinator_proto::coordinatorService::NewStub(channel);
      m_clientID = ClientIP + ":" + std::to_string(ClientPort);
    }

    Client(std::string ClientIP, int ClientPort, std::string CoordinatorIpPort, std::string config_path) : m_coordinatorIpPort(CoordinatorIpPort),
                                                                                                           m_clientIPForGet(ClientIP),
                                                                                                           m_clientPortForGet(ClientPort),
                                                                                                           acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(ClientIP.c_str()), m_clientPortForGet))
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
    bool xue_update(int stripe_id, const std::vector<std::pair<int, int>> &logical_ranges,
                    XueUpdateBatchTiming *out_timing = nullptr);
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
    void async_append_to_proxies(char *cluster_slice_data, std::string append_key, int cluster_slice_size,
                                 std::string proxy_ip, int proxy_port, int index, bool *if_commit_arr,
                                 bool poll_commit_after_send = true,
                                 XueAppendNetworkTiming *out_timing = nullptr,
                                 uint64_t tcp_accept_token = 0);
    void launch_append_to_proxies_serial_per_endpoint(
        const coordinator_proto::ReplyProxyIPsPorts &reply, const char *send_buf,
        bool *if_commit_arr);
    void launch_append_subset_serial_per_endpoint(
        const coordinator_proto::ReplyProxyIPsPorts &reply, const char *send_buf,
        bool *if_commit_arr, const std::vector<int> &indices, bool poll_commit_after_send,
        XueClientTimingSummary *timing = nullptr);
    bool poll_append_commit(const std::string &append_key);
    bool wait_xue_all_commits_ready(int stripe_id, XueClientTimingSummary *timing);
    void wait_append_keys_commit_parallel(const std::vector<std::string> &keys,
                                          const std::map<std::string, int> &key_to_index,
                                          bool *if_commit_arr, XueClientTimingSummary *timing);
    bool xue_update_strict_schedule(const coordinator_proto::ReplyProxyIPsPorts &reply,
                                    const char *send_buf, bool *if_commit_arr,
                                    XueClientTimingSummary *timing);
    bool xue_update_follow_schedule(const coordinator_proto::ReplyProxyIPsPorts &reply,
                                    const char *send_buf, bool *if_commit_arr,
                                    XueClientTimingSummary *timing);
    void log_xue_client_timing_summary(int stripe_id, uint64_t xue_xfer_plan_id,
                                       const XueClientTimingSummary &timing) const;
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
  };

} // namespace ECProject

#endif // CLIENT_H