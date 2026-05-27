#ifndef PROXY_H
#define PROXY_H
#include "coordinator.grpc.pb.h"
#include "proxy.grpc.pb.h"
#include "datanode.grpc.pb.h"
#include "devcommon.h"
#include "meta_definition.h"
#include "lrc.h"
#include "bw_limit.h"
#include <asio.hpp>
#include <grpc++/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <semaphore.h>
#include <config.h>
#include <toolbox.h>
#include <queue>
#include <unordered_map>
#include <chrono>
#include <mutex>
// #define IF_DEBUG true
#define IF_DEBUG false
namespace ECProject
{
  class ProxyImpl final
      : public proxy_proto::proxyService::Service,
        public std::enable_shared_from_this<ECProject::ProxyImpl>
  {

  public:
    ProxyImpl(std::string proxy_ip_port, std::string config_path, std::string coordinator_address) : config_path(config_path), proxy_ip_port(proxy_ip_port), acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(proxy_ip_port.substr(0, proxy_ip_port.find(':')).c_str()), ECProject::PROXY_PORT_SHIFT + std::stoi(proxy_ip_port.substr(proxy_ip_port.find(':') + 1, proxy_ip_port.size())))), m_coordinator_address(coordinator_address)
    {
      init_coordinator();
      init_datanodes(config_path);
      init_ip_to_cluster_map(config_path);
      init_bandwidth_matrix(config_path);
      m_ip = proxy_ip_port.substr(0, proxy_ip_port.find(':'));
      m_port = std::stoi(proxy_ip_port.substr(proxy_ip_port.find(':') + 1, proxy_ip_port.size()));
      std::cout << "Cluster id:" << m_self_cluster_id << std::endl;
    }
    ~ProxyImpl() {};
    grpc::Status checkalive(
        grpc::ServerContext *context,
        const proxy_proto::CheckaliveCMD *request,
        proxy_proto::RequestResult *response) override;
    // encode and set
    grpc::Status encodeAndSetObject(
        grpc::ServerContext *context,
        const proxy_proto::ObjectAndPlacement *object_and_placement,
        proxy_proto::SetReply *response) override;
    // append
    grpc::Status scheduleAppend2Datanode(
        grpc::ServerContext *context,
        const proxy_proto::AppendStripeDataPlacement *append_stripe_data_placement,
        proxy_proto::SetReply *response) override;
    grpc::Status rackCuPullXferTiming(
        grpc::ServerContext *context,
        const proxy_proto::RackCuXferTimingPullRequest *request,
        proxy_proto::RackCuXferTimingReply *response) override;
    grpc::Status fetchRackCuHomeDeltaStaging(
        grpc::ServerContext *context,
        const proxy_proto::RackCuHomeDeltaFetchRequest *request,
        proxy_proto::RackCuHomeDeltaFetchReply *response) override;
    grpc::Status deleteRackCuHomeDeltaStaging(
        grpc::ServerContext *context,
        const proxy_proto::RackCuHomeDeltaDeleteRequest *request,
        proxy_proto::RackCuHomeDeltaDeleteReply *response) override;
    // decode and get
    grpc::Status decodeAndGetObject(
        grpc::ServerContext *context,
        const proxy_proto::ObjectAndPlacement *object_and_placement,
        proxy_proto::GetReply *response) override;
    // degraded read
    grpc::Status degradedRead(
        grpc::ServerContext *context,
        const proxy_proto::DegradedReadRequest *degraded_read_request,
        proxy_proto::DegradedReadReply *response) override;
    grpc::Status degradedRead2Client(
      grpc::ServerContext *context,
      const proxy_proto::RecoveryRequest *recovery_request,
      proxy_proto::DegradedReadReply *response) override;
    grpc::Status degradedReadBreakdown(
      grpc::ServerContext *context,
      const proxy_proto::DegradedReadRequest *degraded_read_request,
      proxy_proto::DegradedReadReply *response) override;
    grpc::Status degradedRead2ClientBreakdown(
      grpc::ServerContext *context,
      const proxy_proto::RecoveryRequest *recovery_request,
      proxy_proto::DegradedReadReply *response) override;
    grpc::Status degradedReadWithBlockStripeID(
        grpc::ServerContext *context,
        const proxy_proto::DegradedReadRequest *degraded_read_request,
        proxy_proto::GetReply *response) override;
    grpc::Status partialDecoding(
        grpc::ServerContext *context,
        const proxy_proto::PartialDecodingRequest *partial_decoding_request,
        proxy_proto::DegradedReadReply *response) override;
    // recovery
    grpc::Status recovery(
        grpc::ServerContext *context,
        const proxy_proto::RecoveryRequest *recovery_request,
        proxy_proto::RecoveryReply *response) override;
    grpc::Status recoveryBreakdown(
        grpc::ServerContext *context,
        const proxy_proto::RecoveryRequest *recovery_request,
        proxy_proto::RecoveryReply *response) override;

    grpc::Status multipleRecovery(
        grpc::ServerContext *context,
        const proxy_proto::MultipleRecoveryRequest *multiple_recovery_request,
        proxy_proto::GetReply *response) override;
    // delete
    grpc::Status deleteBlock(
        grpc::ServerContext *context,
        const proxy_proto::NodeAndBlock *node_and_block,
        proxy_proto::DelReply *response) override;
    // get stripe
    grpc::Status getBlocks(
        grpc::ServerContext *context,
        const proxy_proto::StripeAndBlockIDs *request,
        proxy_proto::GetReply *response) override;

    bool SetToDatanode(const char *key, size_t key_length, const char *value, size_t value_length, const char *ip, int port, int offset);
    bool GetFromDatanode(const char *key, size_t key_length, char *value, size_t value_length, const char *ip, int port, int offset);
    bool DelInDatanode(std::string key, std::string node_ip_port);

    ECProject::Config *m_sys_config;
    ECProject::ToolBox *m_toolbox;
    std::queue<std::shared_ptr<char[]>> m_pre_allocated_buffer_queue;
    bool AppendToDatanode(const char *block_key, int block_id, size_t append_size, const char *append_buf, int append_offset, const char *ip, int port, bool is_serialized);
    bool MergeParityOnDatanode(const char *block_key, int block_id, const char *ip, int port, const std::string &append_mode);
    void printAppendStripeDataPlacement(const proxy_proto::AppendStripeDataPlacement *append_stripe_data_placement);
    std::vector<unsigned char *> convertToUnsignedCharArray(std::vector<char*> &input);
    bool GetFromDatanode(const std::string &key, char *value, const size_t value_length, const char *ip, const int port);
    bool GetFromDatanode(const std::string &key, char *value, const size_t value_length, const char *ip, const int port, 
      double *disk_io_start_time, double *disk_io_end_time, double *network_start_time, double *network_end_time, double *grpc_notify_time, double *grpc_start_time);
    bool RecoveryToDatanode(const char *block_key, int block_id, const char *buf, const char *ip, int port);
    bool RecoveryToDatanodeBreakdown(const char *block_key, int block_id, const char *buf, const char *ip, int port, double *network_time, double *disk_io_time);
    void get_from_node(const std::string &block_key, char *block_value, const size_t block_size, const char *datanode_ip, const int datanode_port, bool *status, int index);
    void get_from_node_breakdown(const std::string &block_key, char *block_value, const size_t block_size, const char *datanode_ip, const int datanode_port, bool *status, int index, 
      double *disk_io_start_time, double *disk_io_end_time, double *network_start_time, double *network_end_time, double *grpc_notify_time, double *grpc_start_time);

  private:
    bool init_ip_to_cluster_map(std::string cluster_xml_path);
    bool init_bandwidth_matrix(std::string cluster_xml_path);
    int cluster_for_datapath_ip(const char *ip) const;
    // Linux: SO_MAX_PACING_RATE (fq) + SO_RCVBUF 收紧窗口；非 Linux 无操作。
    void apply_kernel_bandwidth_to_peer_socket(asio::ip::tcp::socket &sock, const char *peer_ip) const;
    bool fetch_rack_cu_home_staging_blob(const proxy_proto::RackCuHomeDeltaStagingRef &ref, std::string *out_blob,
                                         int rack_cu_xfer_stripe_id = 0, std::uint64_t rack_cu_xfer_plan_id = 0);
    bool delete_rack_cu_staging_on_datanode(const std::string &key, const std::string &dn_ip, int dn_port);
    std::mutex m_mutex;
    std::condition_variable cv;
    bool init_coordinator();
    bool init_datanodes(std::string datanodeinfo_path);
    std::unique_ptr<coordinator_proto::coordinatorService::Stub> m_coordinator_ptr;
    std::map<std::string, std::unique_ptr<datanode_proto::datanodeService::Stub>> m_datanode_ptrs;
    std::string config_path;
    std::string proxy_ip_port;
    std::string m_ip;
    int m_port;
    int m_self_cluster_id;
    asio::io_context io_context;
    asio::ip::tcp::acceptor acceptor;
    sem_t sem;
    std::string m_coordinator_address;
    BWLimit m_bw_limit;
    bool m_bw_matrix_enabled = false;
    std::unordered_map<std::string, int> m_ip_to_cluster_id;
    std::unordered_map<std::string, int> m_proxy_cluster_by_endpoint;
    // RackCU：同一 staging_key 在一次更新内可能被多条计划重复拉取；短期缓存减少重复 Get / 跨 proxy fetch
    std::mutex m_rackcu_home_staging_cache_mu;
    std::unordered_map<std::string, std::pair<std::string, std::chrono::steady_clock::time_point>>
        m_rackcu_home_staging_blob_cache;
    /** RackCU: append_key -> plan registered by scheduleAppend; TCP carries key to disambiguate accept. */
    std::mutex m_rackcu_pending_appends_mu;
    std::unordered_map<std::string, std::shared_ptr<proxy_proto::AppendStripeDataPlacement>> m_rackcu_pending_appends;
    /** Serialize accept()+keyed header+payload on this proxy (avoids concurrent accept races). */
    std::mutex m_rackcu_accept_io_mu;
  };

  class Proxy
  {
  public:
    Proxy(std::string proxy_ip_port, std::string config_path, std::string coordinator_address) : proxy_ip_port(proxy_ip_port), m_proxyImpl_ptr(proxy_ip_port, config_path, coordinator_address)
    {
    }
    Proxy(std::string proxy_ip_port, std::string config_path, std::string coordinator_address, std::string sys_config_path) : proxy_ip_port(proxy_ip_port), m_proxyImpl_ptr(proxy_ip_port, config_path, coordinator_address)
    {
      m_proxyImpl_ptr.m_sys_config = ECProject::Config::getInstance(sys_config_path);
      m_proxyImpl_ptr.m_toolbox = ECProject::ToolBox::getInstance();
    }
    void Run()
    {
      grpc::EnableDefaultHealthCheckService(true);
      grpc::reflection::InitProtoReflectionServerBuilderPlugin();
      grpc::ServerBuilder builder;
      // RackCU fetchRackCuHomeDeltaStaging / Parix journal reply may carry multi‑MB blobs (default gRPC cap is 4MB).
      constexpr int k_grpc_max_msg = 128 * 1024 * 1024;
      builder.SetMaxSendMessageSize(k_grpc_max_msg);
      builder.SetMaxReceiveMessageSize(k_grpc_max_msg);
      std::cout << "proxy_ip_port:" << proxy_ip_port << std::endl;
      builder.AddListeningPort(proxy_ip_port, grpc::InsecureServerCredentials());
      builder.RegisterService(&m_proxyImpl_ptr);
      std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
      server->Wait();
    }

  private:
    std::string proxy_ip_port;
    ECProject::ProxyImpl m_proxyImpl_ptr;
  };
} // namespace ECProject
#endif