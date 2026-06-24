#ifndef CLIENT_H
#define CLIENT_H

#ifdef BAZEL_BUILD
#include "src/proto/coordinator.grpc.pb.h"
#include "src/proto/proxy.grpc.pb.h"
#include "src/proto/datanode.grpc.pb.h"
#else
#include "coordinator.grpc.pb.h"
#include "proxy.grpc.pb.h"
#include "datanode.grpc.pb.h"
#endif

#include "meta_definition.h"
#include <grpcpp/grpcpp.h>
#include <asio.hpp>
#include "config.h"
#include "toolbox.h"
#include <utility>
#include <vector>
namespace ECProject
{
  class Client
  {
  public:
    Client(std::string ClientIP, int ClientPort, std::string CoordinatorIpPort) : m_coordinatorIpPort(CoordinatorIpPort),
                                                                                  m_clientIPForGet(ClientIP),
                                                                                  m_clientPortForGet(ClientPort),
                                                                                  acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(ClientIP.c_str()), m_clientPortForGet))
    {
      grpc::ChannelArguments args;
      // Avoid ENHANCE_YOUR_CALM / too_many_pings when several main_client processes run in parallel.
      args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 120000);
      args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 20000);
      args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 0);
      auto channel = grpc::CreateCustomChannel(m_coordinatorIpPort, grpc::InsecureChannelCredentials(), args);
      m_coordinator_ptr = coordinator_proto::coordinatorService::NewStub(channel);
      m_clientID = ClientIP + ":" + std::to_string(ClientPort);
    }

    Client(std::string ClientIP, int ClientPort, std::string CoordinatorIpPort, std::string config_path) : m_coordinatorIpPort(CoordinatorIpPort),
                                                                                                           m_clientIPForGet(ClientIP),
                                                                                                           m_clientPortForGet(ClientPort),
                                                                                                           acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(ClientIP.c_str()), m_clientPortForGet))
    {
      grpc::ChannelArguments args;
      // Avoid ENHANCE_YOUR_CALM / too_many_pings when several main_client processes run in parallel.
      args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 120000);
      args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 20000);
      args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 0);
      auto channel = grpc::CreateCustomChannel(m_coordinatorIpPort, grpc::InsecureChannelCredentials(), args);
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
    void get_cached_parity_slices(std::vector<char *> &global_parity_ptr_array, std::vector<char *> &local_parity_ptr_array, const int parity_slice_size, const int parity_slice_offset);
    void cache_latest_parity_slices(std::vector<char *> &global_parity_ptr_array, std::vector<char *> &local_parity_ptr_array, const int parity_slice_size, const int parity_slice_offset);
    std::vector<int> get_parameters();
    bool decode_test(int stripe_id, int failed_block_id, std::string client_ip, int client_port, double &decode_time);

    /** Parix：在条带逻辑字节区间 [logical_offset_start, logical_offset_end_exclusive) 写入 new_span_bytes，并提交批次。 */
    bool parix_partial_update(int stripe_id, int logical_offset_start, int logical_offset_end_exclusive, const char *new_span_bytes);
    /**
     * Parix：同一条带多段互不重叠的逻辑区间 [start, end_exclusive)，按 ranges 顺序将各段新字节串联为 packed_new_bytes，
     * 一次 plan / 同一 batch_id 提交（与单段 API 相同的 journal / 写盘 / commit 流程）。
     */
    bool parix_partial_update_ranges(int stripe_id, const std::vector<std::pair<int, int>> &ranges, const char *packed_new_bytes);
    /**
     * Parix 全条带：调用方提供 k 个数据块的完整新内容（按块顺序拼接为 k*BlockSize 字节），
     * 在 Primary 侧用现有编码器从 Di_new 计算全局/局部校验块新值并分发覆盖写入；不读取盘上旧数据。
     */
    bool parix_full_stripe_rewrite(int stripe_id, const char *new_stripe_data);
    /** 若各区间为半开 [lo,hi) 且落在 [0, k*BlockSize)，并与全部 k 个数据块各自的逻辑区间都有交集，则走 full rewrite（不要求并集铺满条带）。 */
    bool parix_ranges_cover_full_stripe_data(const std::vector<std::pair<int, int>> &ranges) const;

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