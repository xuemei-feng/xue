#ifndef PROXY_H
#define PROXY_H
#include "coordinator.grpc.pb.h"
#include "proxy.grpc.pb.h"
#include "datanode.grpc.pb.h"
#include "devcommon.h"
#include "meta_definition.h"
#include "lrc.h"
#include <asio.hpp>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <semaphore.h>
#include <config.h>
#include <toolbox.h>
#include "tcp_conn_pool.h"
#include <queue>
#include <set>
#include <deque>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <chrono>
#include <memory>
// #define IF_DEBUG true
#define IF_DEBUG false
namespace ECProject
{
  /** Successful parity range writes from XUE apply paths (ranges + total bytes XOR/Write). */
  struct XueParityWriteStats
  {
    int ranges = 0;
    size_t bytes = 0;
    explicit operator bool() const { return ranges > 0; }
  };

  class ProxyImpl final : public proxy_proto::proxyService::Service,
                          public std::enable_shared_from_this<ProxyImpl>
  {

  public:
    ProxyImpl(std::string proxy_ip_port, std::string config_path, std::string coordinator_address) : config_path(config_path), proxy_ip_port(proxy_ip_port), acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(proxy_ip_port.substr(0, proxy_ip_port.find(':')).c_str()), ECProject::PROXY_PORT_SHIFT + std::stoi(proxy_ip_port.substr(proxy_ip_port.find(':') + 1, proxy_ip_port.size())))), m_coordinator_address(coordinator_address)
    {
      init_coordinator();
      init_datanodes(config_path);
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
    grpc::Status xuePullXferTiming(
        grpc::ServerContext *context,
        const proxy_proto::XueXferTimingPull *request,
        proxy_proto::XueXferTimingProxyReply *response) override;
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
    bool ReadRangeFromDatanode(const char *block_key, int block_id, int range_offset, int range_size, char *out_buf, const char *ip, int port);
    bool WriteRangeToDatanode(const char *block_key, int block_id, int range_offset, const char *data, int range_size, const char *ip, int port);
    bool XorWriteRangeToDatanode(const char *block_key, int block_id, int range_offset, const char *delta, int range_size, const char *ip, int port);
    bool forwardXueDataDeltaSync(int dest_cluster_id, const std::string &append_mode,
                                 const proxy_proto::AppendStripeDataPlacement &placement,
                                 const char *delta_buf, size_t delta_size, bool log_send = true,
                                 bool gate_strict_schedule_step = true);
    void waitXueScheduleHopBeforeForward(int stripe_id, const std::string &append_key,
                                         int dest_cluster_id);
    bool waitXueScheduleStepBeforeForward(int stripe_id, const std::string &append_key,
                                          int from_cluster, int to_cluster,
                                          uint64_t xue_xfer_plan_id);
    void reportXueScheduleStepDoneAfterForward(int stripe_id, const std::string &append_key,
                                               int from_cluster, int to_cluster, bool success,
                                               uint64_t xue_xfer_plan_id);
    void reportXueIngressReadyToCoordinator(int stripe_id, const std::string &append_key);
    void record_xue_xfer_sample(int stripe_id, uint64_t xue_xfer_plan_id,
                                const std::chrono::steady_clock::time_point &t0,
                                const std::chrono::steady_clock::time_point &t1,
                                int64_t wall_ms_start, int64_t wall_ms_end);
    void runXueStrictDeferredForwards(
        std::shared_ptr<proxy_proto::AppendStripeDataPlacement> placement,
        std::shared_ptr<std::vector<char>> append_buf, std::vector<char *> slices,
        int tcp_slice_count, bool report_commit = true);
    int applyXueDataBlocksNewValueToDataDelta(const proxy_proto::AppendStripeDataPlacement &placement,
                                              std::vector<char *> &slices, int tcp_slice_count);
    bool handleXueClass1RelayAtDataCluster(const proxy_proto::AppendStripeDataPlacement &placement,
                                           std::vector<char *> &slices, int tcp_slice_count,
                                           const char *delta_buf, size_t delta_size);
    XueParityWriteStats applyXueGlobalParityFromDataDeltas(
        const proxy_proto::AppendStripeDataPlacement &placement, const std::vector<char *> &slices,
        int tcp_slice_count);
    XueParityWriteStats applyXueLocalParityFromDataDeltas(
        const proxy_proto::AppendStripeDataPlacement &placement, const std::vector<char *> &slices,
        int tcp_slice_count);
    bool needsClass3MergedLocalParityForward(const proxy_proto::AppendStripeDataPlacement &placement,
                                             int tcp_slice_count) const;
    void handleXueClass2Or3LocalParityOnDataCluster(
        const proxy_proto::AppendStripeDataPlacement &placement, const std::vector<char *> &slices,
        int tcp_slice_count, const char *append_buf, size_t cluster_append_size);
    bool forwardMergedLocalParityDelta(int dest_local_cluster,
                                       const proxy_proto::AppendStripeDataPlacement &placement,
                                       const std::vector<char *> &data_delta_slices, int tcp_slice_count);
    XueParityWriteStats applyReceivedLocalParityDelta(
        const proxy_proto::AppendStripeDataPlacement &placement, const char *delta_buf,
        size_t delta_size);
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

    friend class Proxy;

  private:
    struct XueGlobalParityIngressBatch
    {
      std::set<std::string> expected_append_keys;
      std::set<std::string> staged_append_keys;
      struct RangeAccum
      {
        int block_id = -1;
        int offset = 0;
        int length = 0;
        std::vector<char> delta_xor;
        std::string block_key;
        std::string datanode_ip;
        int datanode_port = 0;
      };
      /** Per global parity block_id: non-overlapping intervals with XOR-merged deltas. */
      std::map<int, std::vector<RangeAccum>> merged_by_block;
    };

    void registerXueGlobalParityIngressExpected(int stripe_id, const std::string &append_key);
    static void mergeXueGlobalParityDeltaIntoBlock(
        std::vector<XueGlobalParityIngressBatch::RangeAccum> &intervals,
        XueGlobalParityIngressBatch::RangeAccum incoming);
    XueParityWriteStats applyXueGlobalParityIngressMerged(
        const proxy_proto::AppendStripeDataPlacement &placement,
        const std::vector<char *> &slices, int tcp_slice_count);
    XueParityWriteStats flushXueGlobalParityIngressBatch(int stripe_id);
    std::mutex m_xue_global_parity_ingress_mutex;
    std::map<int, XueGlobalParityIngressBatch> m_xue_global_parity_ingress_batches;

    struct XueXferBatchAccumulator
    {
      double pure_xfer_sec_sum = 0;
      int64_t wall_span_start_ms = 0;
      int64_t wall_span_end_ms = 0;
      bool has_wall = false;
    };
    std::mutex m_xue_xfer_timing_mutex;
    std::map<std::pair<int, uint64_t>, XueXferBatchAccumulator> m_xue_xfer_batches;

    void attach_self(std::shared_ptr<ProxyImpl> self);
    std::shared_ptr<ProxyImpl> lock_self() const;
    void warm_up_worker_pools();
    void ensure_persistent_client_ingress_acceptor();
    void persistent_client_ingress_accept_loop(std::shared_ptr<ProxyImpl> keepalive);
    /** Read token-framed ingress messages on one TCP connection (reuse when enabled). */
    void drainClientIngressSocket(asio::ip::tcp::socket socket_data);
    void ensure_client_append_workers();
    void client_append_worker_loop(std::shared_ptr<ProxyImpl> keepalive);
    void ensure_xue_deferred_workers();
    void xue_deferred_worker_loop(std::shared_ptr<ProxyImpl> keepalive);
    /** Bounded async queue for strict-schedule forwards (replaces unbounded detach threads). */
    bool enqueueXueDeferredTask(std::function<void()> task);

    static constexpr int kClientAppendWorkerCount = 8;
    static constexpr int kXueDeferredWorkerCount = 12;
    static constexpr size_t kXueDeferredQueueMax = 512;
    static constexpr int kPendingTcpTokenTtlSec = 120;
    static constexpr bool kXueTcpConnReuse = true;
    static constexpr int kTcpConnPoolMaxPerEndpoint = 8;
    static constexpr int kClientIngressConnIdleSec = 30;
    std::atomic<bool> m_persistent_client_ingress_started{false};
    std::mutex m_client_accept_mutex;  // serializes acceptor.accept() across workers
    struct PendingTcpEntry
    {
      std::shared_ptr<proxy_proto::AppendStripeDataPlacement> placement;
      std::chrono::steady_clock::time_point created_at;
    };
    std::mutex m_pending_tcp_mutex;
    std::atomic<uint64_t> m_next_tcp_accept_token{1};
    std::map<uint64_t, PendingTcpEntry> m_pending_tcp_by_token;
    uint64_t registerPendingTcpAppend(std::shared_ptr<proxy_proto::AppendStripeDataPlacement> placement);
    std::shared_ptr<proxy_proto::AppendStripeDataPlacement> consumePendingTcpAppend(uint64_t token);
    void releasePendingTcpAppend(uint64_t token);
    void cleanupExpiredPendingTcpTokensLocked();
    std::mutex m_client_append_queue_mutex;
    std::condition_variable m_client_append_queue_cv;
    std::deque<std::function<void()>> m_client_append_tasks;
    std::atomic<int> m_client_append_worker_count{0};
    std::mutex m_xue_deferred_queue_mutex;
    std::condition_variable m_xue_deferred_queue_cv;
    std::deque<std::function<void()>> m_xue_deferred_tasks;
    std::atomic<int> m_xue_deferred_worker_count{0};
    // Weak back-reference set by Proxy after make_shared; lock_self() never throws.
    std::weak_ptr<ProxyImpl> m_self_weak;

    std::mutex m_mutex;
    std::condition_variable cv;
    bool init_coordinator();
    bool init_datanodes(std::string datanodeinfo_path);
    int clusterIdForDatanodeEndpoint(const std::string &ip, int port) const;
    proxy_proto::proxyService::Stub *getProxyStubForCluster(int cluster_id);
    std::unique_ptr<coordinator_proto::coordinatorService::Stub> m_coordinator_ptr;
    std::map<std::string, std::unique_ptr<datanode_proto::datanodeService::Stub>> m_datanode_ptrs;
    std::map<std::string, int> m_datanode_endpoint_to_cluster;
    std::map<int, std::pair<std::string, int>> m_cluster_proxy_endpoints;
    std::mutex m_proxy_peer_stubs_mutex;
    std::map<int, std::unique_ptr<proxy_proto::proxyService::Stub>> m_proxy_peer_stubs;
    std::string config_path;
    std::string proxy_ip_port;
    std::string m_ip;
    int m_port;
    int m_self_cluster_id;
    asio::io_context io_context;
    asio::ip::tcp::acceptor acceptor;
    sem_t sem;
    std::string m_coordinator_address;
  };

  class Proxy
  {
  public:
    Proxy(std::string proxy_ip_port, std::string config_path, std::string coordinator_address)
        : proxy_ip_port(proxy_ip_port),
          m_impl(std::make_shared<ProxyImpl>(proxy_ip_port, config_path, coordinator_address))
    {
      m_impl->attach_self(m_impl);
    }
    Proxy(std::string proxy_ip_port, std::string config_path, std::string coordinator_address,
          std::string sys_config_path)
        : proxy_ip_port(proxy_ip_port),
          m_impl(std::make_shared<ProxyImpl>(proxy_ip_port, config_path, coordinator_address))
    {
      m_impl->attach_self(m_impl);
      m_impl->m_sys_config = ECProject::Config::getInstance(sys_config_path);
      m_impl->m_toolbox = ECProject::ToolBox::getInstance();
    }
    void Run()
    {
      // Health check / reflection plugins call shared_from_this on registered services.
      grpc::EnableDefaultHealthCheckService(false);
      grpc::ServerBuilder builder;
      std::cout << "proxy_ip_port:" << proxy_ip_port << std::endl;
      std::cout << "proxy_runtime=v6-peer-stub-mutex" << std::endl;
      builder.AddListeningPort(proxy_ip_port, grpc::InsecureServerCredentials());
      builder.RegisterService(m_impl.get());
      std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
      server->Wait();
    }

  private:
    std::string proxy_ip_port;
    // Keeps ProxyImpl alive for detached worker threads and gRPC service lifetime.
    std::shared_ptr<ProxyImpl> m_impl;
  };
} // namespace ECProject
#endif