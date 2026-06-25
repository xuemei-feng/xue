#ifndef DATANODE_H
#define DATANODE_H

#include "datanode.grpc.pb.h"
#include <grpc++/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <asio.hpp>
#include <string>
#include <vector>
#include <queue>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include "meta_definition.h"
#include "config.h"
// #define IF_DEBUG true
#define IF_DEBUG false
namespace ECProject
{
    class DatanodeImpl final
        : public datanode_proto::datanodeService::Service
    {
    public:
        DatanodeImpl(std::string datanode_ip_port) : datanode_ip_port(datanode_ip_port), acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(datanode_ip_port.substr(0, datanode_ip_port.find(':')).c_str()), ECProject::DATANODE_PORT_SHIFT + std::stoi(datanode_ip_port.substr(datanode_ip_port.find(':') + 1, datanode_ip_port.size()))))
        {
            m_ip = datanode_ip_port.substr(0, datanode_ip_port.find(':'));
            m_port = std::stoi(datanode_ip_port.substr(datanode_ip_port.find(':') + 1, datanode_ip_port.size()));
            m_download_port = m_port + ECProject::DATANODE_PORT_SHIFT;
        }
        ~DatanodeImpl() {};
        grpc::Status checkalive(
            grpc::ServerContext *context,
            const datanode_proto::CheckaliveCMD *request,
            datanode_proto::RequestResult *response) override;
        // set
        grpc::Status handleSet(
            grpc::ServerContext *context,
            const datanode_proto::SetInfo *set_info,
            datanode_proto::RequestResult *response) override;
        // append
        grpc::Status handleAppend(
            grpc::ServerContext *context,
            const datanode_proto::AppendInfo *append_info,
            datanode_proto::RequestResult *response) override;
        // merge parity
        grpc::Status handleMergeParity(
            grpc::ServerContext *context,
            const datanode_proto::MergeParityInfo *merge_parity_info,
            datanode_proto::RequestResult *response) override;
        // merge parity with rep
        /*grpc::Status handleMergeParityWithRep(
            grpc::ServerContext *context,
            const datanode_proto::MergeParityInfo *merge_parity_info,
            datanode_proto::RequestResult *response) override;*/
        // recovery
        grpc::Status handleRecovery(
            grpc::ServerContext *context,
            const datanode_proto::MergeParityInfo *recovery_info,
            datanode_proto::RequestResult *response) override;
        grpc::Status handleRecoveryBreakdown(
            grpc::ServerContext *context,
            const datanode_proto::MergeParityInfo *recovery_info,
            datanode_proto::RequestResult *response) override;
        // get
        grpc::Status handleGet(
            grpc::ServerContext *context,
            const datanode_proto::GetInfo *get_info,
            datanode_proto::RequestResult *response) override;
        grpc::Status handleGetBreakdown(
            grpc::ServerContext *context,
            const datanode_proto::GetInfo *get_info,
            datanode_proto::RequestResult *response) override;
        // delete
        grpc::Status handleDelete(
            grpc::ServerContext *context,
            const datanode_proto::DelInfo *del_info,
            datanode_proto::RequestResult *response) override;

        void serialize(const std::string &filename, const ParitySlice &slice);
        std::vector<ParitySlice> deserialize(const std::string &filename);
        void deserialize(const std::string &filename, char *buf);
        bool createDirectories(const std::string &path);

        /// Start the single-threaded acceptor loop (must be called before Run).
        void start_acceptor();
        /// Stop the acceptor loop cleanly.
        void stop_acceptor();

        ECProject::Config *m_sys_config;

    private:
        std::string datanode_ip_port;
        std::string m_ip;
        int m_port;
        int m_block_size;
        int m_download_port;
        asio::io_context io_context;
        asio::ip::tcp::acceptor acceptor;

        // Tag-based connection matching: acceptor reads a tag header from each
        // incoming TCP connection, then signals the specific gRPC handler thread
        // that is waiting for that tag (keyed by block_key).  This eliminates the
        // cross-connection race inherent in the previous shared FIFO queue.
        struct PendingConnection {
            std::unique_ptr<asio::ip::tcp::socket> socket;
            bool ready = false;
        };
        std::mutex pending_mutex_;
        std::condition_variable pending_cv_;
        std::unordered_map<std::string, PendingConnection> pending_connections_;
        std::thread acceptor_thread_;
        std::atomic<bool> acceptor_running_{false};

        /// Block until a socket with the given tag is available from the acceptor.
        /// The tag is the block_key, which is unique per in-flight request.
        /// Must be called after register_pending_tag().
        asio::ip::tcp::socket wait_for_tagged_socket(const std::string &tag);

        /// Pre-register a tag synchronously (call BEFORE detaching the handler thread).
        /// This guarantees the tag is in the map before the gRPC response is sent,
        /// eliminating the race between the handler thread and the proxy's TCP connect.
        void register_pending_tag(const std::string &tag);

        /// Release a pre-registered tag (call on handler error/early-exit to prevent leaks).
        void release_pending_tag(const std::string &tag);
    };

    class DataNode
    {
    public:
        DataNode(std::string datanode_ip_port) : datanode_ip_port(datanode_ip_port), m_datanodeImpl_ptr(datanode_ip_port) {}
        DataNode(std::string datanode_ip_port, std::string sys_config_path) : datanode_ip_port(datanode_ip_port), m_datanodeImpl_ptr(datanode_ip_port)
        {
            m_datanodeImpl_ptr.m_sys_config = ECProject::Config::getInstance(sys_config_path);
        }

        void Run()
        {
            // Start the single-threaded acceptor before gRPC to avoid concurrent accept() races.
            m_datanodeImpl_ptr.start_acceptor();

            grpc::EnableDefaultHealthCheckService(true);
            grpc::reflection::InitProtoReflectionServerBuilderPlugin();
            grpc::ServerBuilder builder;
            builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::NUM_CQS, 8);
            builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MIN_POLLERS, 8);
            builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, 48);
            std::cout << "datanode_ip_port:" << datanode_ip_port << std::endl;
            builder.AddListeningPort(datanode_ip_port, grpc::InsecureServerCredentials());
            builder.RegisterService(&m_datanodeImpl_ptr);
            std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
            server->Wait();
        }

    private:
        std::string datanode_ip_port;
        ECProject::DatanodeImpl m_datanodeImpl_ptr;
    };
}

#endif