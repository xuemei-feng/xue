#ifndef COORDINATOR_H
#define COORDINATOR_H
#include "coordinator.grpc.pb.h"
#include "proxy.grpc.pb.h"
#include <grpc++/create_channel.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <meta_definition.h>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <condition_variable>
#include <atomic>
#include <config.h>
#include <toolbox.h>
#include "unilrc_encoder.h"
// #define IF_DEBUG true
#define IF_DEBUG false
namespace ECProject
{
  class CoordinatorImpl final
      : public coordinator_proto::coordinatorService::Service
  {
  public:
    CoordinatorImpl()
    {
      m_cur_cluster_id = 0;
      m_cur_stripe_id = 0;
    }
    ~CoordinatorImpl() {};
    grpc::Status setParameter(
        grpc::ServerContext *context,
        const coordinator_proto::Parameter *parameter,
        coordinator_proto::RepIfSetParaSuccess *setParameterReply) override;
    grpc::Status sayHelloToCoordinator(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    grpc::Status checkalive(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    // set
    grpc::Status uploadOriginKeyValue(
        grpc::ServerContext *context,
        const coordinator_proto::RequestProxyIPPort *keyValueSize,
        coordinator_proto::ReplyProxyIPPort *proxyIPPort) override;
    grpc::Status reportCommitAbort(
        grpc::ServerContext *context,
        const coordinator_proto::CommitAbortKey *commit_abortkey,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    grpc::Status checkCommitAbort(
        grpc::ServerContext *context,
        const coordinator_proto::AskIfSuccess *key_opp,
        coordinator_proto::RepIfSuccess *reply) override;
    grpc::Status uploadSetValue(
        grpc::ServerContext *context,
        const coordinator_proto::RequestProxyIPPort *keyValueSize,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    grpc::Status uploadSubsetValue(
        grpc::ServerContext *context,
        const coordinator_proto::RequestProxyIPPort *keyValueSize,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    // append
    grpc::Status uploadAppendValue(
        grpc::ServerContext *context,
        const coordinator_proto::RequestProxyIPPort *keyValueSize,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    grpc::Status uploadXueUpdate(
        grpc::ServerContext *context,
        const coordinator_proto::XueUpdateRequest *request,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    grpc::Status releaseXueScheduleWave(
        grpc::ServerContext *context,
        const coordinator_proto::XueScheduleWaveRelease *request,
        coordinator_proto::ReplyFromCoordinator *reply) override;
    grpc::Status waitXueScheduleHop(
        grpc::ServerContext *context,
        const coordinator_proto::XueScheduleHopWait *request,
        coordinator_proto::ReplyFromCoordinator *reply) override;
    grpc::Status reportXueIngressReady(
        grpc::ServerContext *context,
        const coordinator_proto::XueIngressReadyReport *request,
        coordinator_proto::ReplyFromCoordinator *reply) override;
    grpc::Status waitXueAllIngressReady(
        grpc::ServerContext *context,
        const coordinator_proto::XueStripeScheduleId *request,
        coordinator_proto::ReplyFromCoordinator *reply) override;
    grpc::Status waitXueAllCommitsReady(
        grpc::ServerContext *context,
        const coordinator_proto::XueStripeScheduleId *request,
        coordinator_proto::ReplyFromCoordinator *reply) override;
    grpc::Status waitXueScheduleStep(
        grpc::ServerContext *context,
        const coordinator_proto::XueScheduleStepWait *request,
        coordinator_proto::ReplyFromCoordinator *reply) override;
    grpc::Status reportXueScheduleStepDone(
        grpc::ServerContext *context,
        const coordinator_proto::XueScheduleStepDone *request,
        coordinator_proto::ReplyFromCoordinator *reply) override;
    grpc::Status pullXueXferTiming(
        grpc::ServerContext *context,
        const coordinator_proto::XueXferTimingPull *request,
        coordinator_proto::XueXferTimingSummary *response) override;
    // get
    grpc::Status getValue(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::RepIfGetSuccess *getReplyClient) override;
    // get stripe
    grpc::Status getStripe(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    // get blocks
    grpc::Status getBlocks(
        grpc::ServerContext *context,
        const coordinator_proto::BlockIDsAndClientIP *blockIDsClient,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    grpc::Status getDegradedReadBlocks(
        grpc::ServerContext *context,
        const coordinator_proto::BlockIDsAndClientIP *blockIDsClient,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    // degraded read
    grpc::Status getDegradedReadBlock(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::DegradedReadReply *degradedReadReply) override;
    grpc::Status getDegradedReadBlockBreakdown(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::DegradedReadReply *degradedReadReply) override;
    // recovery
    grpc::Status getRecovery(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::RecoveryReply *replyClient) override;
    grpc::Status getRecoveryBreakdown(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::RecoveryReply *replyClient) override;
    grpc::Status decodeTest(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::DegradedReadReply *degradedReadReply) override;
    grpc::Status fullNodeRecovery(
      grpc::ServerContext *context,
      const coordinator_proto::NodeIdFromClient *request,
      coordinator_proto::RepBlockNum* response) override;
    grpc::Status multiBlockRecovery(
      grpc::ServerContext *context,
      const coordinator_proto::StripeIdAndBlockIDsFromClient *request,
      coordinator_proto::RecoveryReply *replyClient) override;
    // delete
    grpc::Status delByKey(
        grpc::ServerContext *context,
        const coordinator_proto::KeyFromClient *del_key,
        coordinator_proto::RepIfDeling *delReplyClient) override;
    grpc::Status delByStripe(
        grpc::ServerContext *context,
        const coordinator_proto::StripeIdFromClient *stripeid,
        coordinator_proto::RepIfDeling *delReplyClient) override;
    // other
    grpc::Status listStripes(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *req,
        coordinator_proto::RepStripeIds *listReplyClient) override;

    bool init_clusterinfo(std::string m_clusterinfo_path);
    bool init_proxyinfo();
    void update_stripe_info_in_node(bool add_or_sub, int t_node_id, int stripe_id);
    int randomly_select_a_cluster(int stripe_id);
    int randomly_select_a_node(int cluster_id, int stripe_id);
    int generate_placement(int stripe_id, int block_size);
    void blocks_in_cluster(std::map<char, std::vector<ECProject::Block *>> &block_info, int cluster_id, int stripe_id);
    void find_max_group(int &max_group_id, int &max_group_num, int cluster_id, int stripe_id);
    int count_block_num(char type, int cluster_id, int stripe_id, int group_id);
    bool find_block(char type, int cluster_id, int stripe_id);

    void initialize_unilrc_and_azurelrc_stripe_placement(Stripe *stripe);
    void initialize_optimal_lrc_stripe_placement(Stripe *stripe);
    void initialize_uniform_lrc_stripe_placement(Stripe *stripe);
    void initialize_xue_tripe_placement(Stripe *stripe);
    void add_to_map(std::map<int, std::vector<int>> &map, int key, int value);
    std::vector<proxy_proto::AppendStripeDataPlacement> generate_add_plans(Stripe *stripe);
    std::vector<proxy_proto::AppendStripeDataPlacement> generate_sub_add_plans(Stripe *stripe, size_t subset_size);
    std::vector<proxy_proto::AppendStripeDataPlacement> generateAppendPlan(Stripe *stripe, int curr_logical_offset, int append_size);
    /** 与 generateAppendPlan 相同：由条带逻辑偏移区间得到各块 (size, 块内offset) 及校验条带尺寸 */
    bool build_slice_plan_for_logical_range(Stripe *stripe, int logical_offset_start, int append_size,
                                            std::map<int, std::pair<int, int>> *out_block_to_slice_sizes,
                                            int *out_parity_slice_size, int *out_parity_slice_offset,
                                            bool *out_is_merge_parity, std::string *err_msg);
    void update_stripe_info_in_node(int t_node_id, int stripe_id, int index);
    int getClusterAppendSize(Stripe *stripe, const std::map<int, std::pair<int, int>> &block_to_slice_sizes, int curr_group_id, int parity_slice_size);
    void notify_proxies_ready(const proxy_proto::AppendStripeDataPlacement &plan);
    void note_xue_strict_append_commit(int stripe_id, const std::string &append_key, bool committed);
    std::vector<int> get_recovery_group_ids(std::string code_type, int k, int r, int z, int failed_block_id);
    void init_recovery_group_lookup_table();
    void print_stripe_data_placement(Stripe &stripe);
    int get_cluster_id_by_group_id(Stripe &stripe, int group_id);
    void getStripeFromProxy(std::string client_ip, int client_port, std::string proxy_ip, int proxy_port, int stripe_id, int group_id, std::vector<int> block_ids);
    bool recovery_one_block(int stripe_id, int failed_block_id);
    bool recovery_one_block_breakdown(int stripe_id, int failed_block_id, 
      std::vector<double> &disk_io_start_time, std::vector<double> &disk_io_end_time, std::vector<double> &decode_start_time, std::vector<double> &decode_end_time,
      std::vector<double> &network_start_time, std::vector<double> &network_end_time, double &cross_rack_network_time, double &cross_rack_xor_time,
      std::vector<double> &grpc_notify_time, std::vector<double> &grpc_start_time, std::vector<double> &data_node_grpc_notify_time, std::vector<double> &data_node_grpc_start_time,
      double &dest_data_node_network_time, double &dest_data_node_disk_io_time);
    bool degraded_read_one_block_breakdown(int stripe_id, int failed_block_id, std::string client_ip, int client_port, 
      std::vector<double> &disk_io_start_time, std::vector<double> &disk_io_end_time, std::vector<double> &decode_start_time, std::vector<double> &decode_end_time,
      std::vector<double> &network_start_time, std::vector<double> &network_end_time, double &cross_rack_network_time, double &cross_rack_xor_time,
      std::vector<double> &grpc_notify_time, std::vector<double> &grpc_start_time, std::vector<double> &data_node_grpc_notify_time, std::vector<double> &data_node_grpc_start_time);
    bool degraded_read_one_block(int stripe_id, int failed_block_id, std::string client_ip, int client_port);
    bool degraded_read_one_block_for_workload(int stripe_id, int failed_block_id, std::string client_ip, int client_port, int block_id_to_send);
    ECProject::Config *m_sys_config;
    ECProject::ToolBox *m_toolbox;
    int m_cur_cluster_id = 0;
    int m_cur_stripe_id = 0;
    std::unordered_map<std::string, ObjectInfo> m_object_commit_table;
    std::unordered_map<std::string, ObjectInfo> m_object_updating_table;
    std::map<int, Cluster> m_cluster_table;
    std::map<int, Node> m_node_table;
    std::map<int, Stripe> m_stripe_table;
    std::map<std::string, StripeOffset> m_cur_offset_table;
    std::map<int, std::vector<int>> m_recovery_group_lookup_table;
    
    std::vector<int> get_data_block_num_per_group(int k, int r, int z, std::string code_type);

    enum class XueStepRuntimeState
    {
      PENDING = 0,
      READY = 1,
      RUNNING = 2,
      DONE = 3
    };

    struct XueStrictScheduleSession
    {
      int stripe_id = -1;
      bool ingress_complete = false;
      bool commits_complete = false;
      bool commits_failed = false;
      std::set<std::string> required_ingress_keys;
      std::set<std::string> ingress_ready_keys;
      std::set<std::string> required_commit_keys;
      std::set<std::string> committed_keys;
      std::vector<coordinator_proto::XueTransferStepInfo> steps;
      std::vector<XueStepRuntimeState> step_states;
      int num_parallel_groups = 0;
      std::mutex mutex;
      std::condition_variable cv;
      void try_advance_ready_steps();
      void try_advance_commits();
      int find_step_index_by_no(int step_no) const;
      int find_step_index_by_hop(const std::string &append_key, int from_cluster,
                                 int to_cluster) const;
    };

    struct XueWaveScheduleState
    {
      int released_wave = -1;
      std::map<std::string, int> hop_wave;
      std::mutex mutex;
      std::condition_variable cv;
    };

    std::mutex m_xue_schedule_mutex;
    std::map<int, std::shared_ptr<XueStrictScheduleSession>> m_xue_strict_by_stripe;
    std::map<int, XueWaveScheduleState> m_xue_wave_by_stripe;
    std::atomic<uint64_t> m_next_xue_xfer_plan_id{1};

  private:
    std::mutex m_mutex;
    std::condition_variable cv;
    std::map<std::string, std::unique_ptr<proxy_proto::proxyService::Stub>>
        m_proxy_ptrs;
    ECSchema m_encode_parameters;
    std::vector<int> m_stripe_deleting_table;
    int m_num_of_Clusters;
    // merge groups, for DIS and OPT, the stripes from the same group object to the selected placement scheme
    std::vector<std::vector<int>> m_merge_groups;
    std::vector<int> m_free_clusters;
    int m_agg_start_cid = 0;
  };

  class Coordinator
  {
  public:
    Coordinator(
        std::string m_coordinator_ip_port,
        std::string m_clusterinfo_path)
        : m_coordinator_ip_port{m_coordinator_ip_port},
          m_clusterinfo_path{m_clusterinfo_path}
    {
      m_coordinatorImpl.init_clusterinfo(m_clusterinfo_path);
      m_coordinatorImpl.init_proxyinfo();
    };
    Coordinator(
        std::string m_coordinator_ip_port,
        std::string m_clusterinfo_path, std::string sys_config_path)
        : m_coordinator_ip_port{m_coordinator_ip_port},
          m_clusterinfo_path{m_clusterinfo_path}
    {
      m_coordinatorImpl.init_clusterinfo(m_clusterinfo_path);
      m_coordinatorImpl.init_proxyinfo();
      m_coordinatorImpl.m_sys_config = ECProject::Config::getInstance(sys_config_path);
      m_coordinatorImpl.m_toolbox = ECProject::ToolBox::getInstance();

      // initializing
      m_coordinatorImpl.m_cur_cluster_id = 0;
      m_coordinatorImpl.m_cur_stripe_id = 0;
      m_coordinatorImpl.m_object_commit_table.clear();
      m_coordinatorImpl.m_object_updating_table.clear();
      for (auto it = m_coordinatorImpl.m_cluster_table.begin(); it != m_coordinatorImpl.m_cluster_table.end(); it++)
      {
        Cluster &t_cluster = it->second;
        t_cluster.blocks.clear();
        t_cluster.stripes.clear();
      }
      for (auto it = m_coordinatorImpl.m_node_table.begin(); it != m_coordinatorImpl.m_node_table.end(); it++)
      {
        Node &t_node = it->second;
        t_node.stripes.clear();
      }
      m_coordinatorImpl.m_stripe_table.clear();
      m_coordinatorImpl.m_cur_offset_table.clear();
    };
    // Coordinator
    void Run()
    {
      grpc::EnableDefaultHealthCheckService(true);
      grpc::reflection::InitProtoReflectionServerBuilderPlugin();
      grpc::ServerBuilder builder;
      std::string server_address(m_coordinator_ip_port);
      builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
      builder.RegisterService(&m_coordinatorImpl);
      std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
      std::cout << "Server listening on " << server_address << std::endl;
      server->Wait();
    }


  private:
    std::string m_coordinator_ip_port;
    std::string m_clusterinfo_path;
    ECProject::CoordinatorImpl m_coordinatorImpl;
  };
} // namespace ECProject

#endif