#include "datanode.h"
#include "toolbox.h"
#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <chrono>
#include <atomic>
#include <map>
#include <mutex>
#include <vector>
#include <arpa/inet.h>
#include <cerrno>
#include <limits>
#include <memory>
#include <stdexcept>

namespace
{
  std::atomic<uint64_t> g_cord_dn_next_xfer_tag{1};
  std::mutex g_cord_dn_pending_mu;
  std::mutex g_cord_dn_accept_mu;
  std::mutex g_cord_dn_writepath_mu;
  std::map<std::string, std::shared_ptr<std::mutex>> g_cord_dn_writepath_mutexes;

  static std::shared_ptr<std::mutex> cord_dn_mutex_for_writepath(const std::string &writepath)
  {
    std::lock_guard<std::mutex> lk(g_cord_dn_writepath_mu);
    auto &entry = g_cord_dn_writepath_mutexes[writepath];
    if (!entry)
      entry = std::make_shared<std::mutex>();
    return entry;
  }

  struct CordDnPendingRead {
    std::vector<char> data;
    int range_length = 0;
  };
  struct CordDnPendingWrite {
    std::string writepath;
    int range_offset = 0;
    int range_length = 0;
    uint64_t logical_block_size = 0;
  };
  struct CordDnPendingBlob {
    std::string writepath;
    int byte_length = 0;
  };

  std::map<uint64_t, CordDnPendingRead> g_cord_dn_pending_reads;
  std::map<uint64_t, CordDnPendingWrite> g_cord_dn_pending_writes;
  std::map<uint64_t, CordDnPendingBlob> g_cord_dn_pending_blobs;

  static uint64_t cord_dn_alloc_xfer_tag()
  {
    return g_cord_dn_next_xfer_tag.fetch_add(1, std::memory_order_relaxed);
  }

  static uint64_t cord_dn_read_u64_be(asio::ip::tcp::socket &sock)
  {
    uint8_t b[8];
    asio::read(sock, asio::buffer(b, 8));
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
      v = (v << 8) | static_cast<uint64_t>(b[i]);
    return v;
  }

  static void cord_dn_write_u64_be(asio::ip::tcp::socket &sock, uint64_t v)
  {
    uint8_t b[8];
    for (int i = 0; i < 8; ++i)
      b[7 - i] = static_cast<uint8_t>((v >> (8 * i)) & 0xffu);
    asio::write(sock, asio::buffer(b, 8));
  }
}

namespace ECProject
{
    grpc::Status DatanodeImpl::checkalive(
        grpc::ServerContext *context,
        const datanode_proto::CheckaliveCMD *request,
        datanode_proto::RequestResult *response)
    {
        // std::cout << "[Datanode] checkalive " << request->name() << std::endl;
        response->set_message(true);
        return grpc::Status::OK;
    }

    void DatanodeImpl::serialize(const std::string &filename, const ParitySlice &slice)
    {
        std::ofstream outFile(filename, std::ios::out | std::ios::binary | std::ios::app);
        if (outFile.is_open())
        {
            // Serialize a single struct
            outFile.write(reinterpret_cast<const char *>(&slice.offset), sizeof(slice.offset));
            outFile.write(reinterpret_cast<const char *>(&slice.size), sizeof(slice.size));
            outFile.write(slice.slice_ptr, slice.size);
            outFile.flush();
            outFile.close();
        }
        else
        {
            std::cerr << "Unable to open file for writing." << std::endl;
        }
    }

    std::vector<ParitySlice> DatanodeImpl::deserialize(const std::string &filename)
    {
        std::vector<ParitySlice> slices;
        std::ifstream inFile(filename, std::ios::in | std::ios::binary);
        if (inFile.is_open())
        {
            // Read until end of file
            while (inFile.peek() != EOF)
            {
                ParitySlice slice;

                // Read basic data types
                inFile.read(reinterpret_cast<char *>(&slice.offset), sizeof(slice.offset));
                inFile.read(reinterpret_cast<char *>(&slice.size), sizeof(slice.size));

                // for output, append a \0 at the end
                // slice.slice_ptr = new char[slice.size + 1];
                // inFile.read(slice.slice_ptr, slice.size);
                // slice.slice_ptr[slice.size] = '\0';

                // for no output
                slice.slice_ptr = new char[slice.size];
                inFile.read(slice.slice_ptr, slice.size);

                slices.push_back(std::move(slice));
            }
            inFile.close();
        }
        else
        {
            std::cerr << "Unable to open file for reading." << std::endl;
        }
        return slices;
    }

    void DatanodeImpl::deserialize(const std::string &filename, char *buf)
    {
        std::ifstream inFile(filename, std::ios::in | std::ios::binary);
        if (inFile.is_open())
        {
            int accumulated_offset = 0;
            // read until file end
            while (inFile.peek() != EOF)
            {
                int dummy_offset, size;

                // read basic data types
                inFile.read(reinterpret_cast<char *>(&dummy_offset), sizeof(dummy_offset));
                inFile.read(reinterpret_cast<char *>(&size), sizeof(size));

                // read data to buf
                inFile.read(buf + accumulated_offset, size);
                accumulated_offset += size;
            }
            inFile.close();
        }
        else
        {
            std::cerr << "Unable to open file for reading." << std::endl;
        }
    }

    // create directories for the given path
    bool DatanodeImpl::createDirectories(const std::string &path)
    {
        size_t pos = 0;
        std::string dir;
        while ((pos = path.find('/', pos)) != std::string::npos)
        {
            dir = path.substr(0, pos++);
            if (dir.empty())
                continue;
            if (access(dir.c_str(), 0) == -1)
            {
                if (mkdir(dir.c_str(), S_IRWXU) == -1)
                {
                    return false;
                }
            }
        }

        // create the last directory if it does not exist
        if (!path.empty() && access(path.c_str(), 0) == -1)
        {
            return mkdir(path.c_str(), S_IRWXU) != -1;
        }
        return true;
    }

    grpc::Status DatanodeImpl::handleAppend(
        grpc::ServerContext *context,
        const datanode_proto::AppendInfo *append_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = append_info->block_key();
        int block_id = append_info->block_id();
        int append_size = append_info->append_size();
        int append_offset = append_info->append_offset();
        bool is_serialized = append_info->is_serialized();

        // append_offset must be the physical offset of the block
        auto dataBlockHandler = [this](std::string block_key, int append_size, int append_offset) mutable
        {
            try
            {
                std::vector<char> buf(append_size);
                // only send data
                asio::error_code ec;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);
                asio::read(socket, asio::buffer(buf.data(), append_size), ec);

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;

                // std::cout << "[Datanode" << m_port << "][Append101] writepath: " << writepath << " append_offset: " << append_offset << " append_size: " << append_size << std::endl;

                if (access(targetdir.c_str(), 0) == -1)
                {
                    createDirectories(targetdir);
                }

                if (append_offset == 0)
                {
                    // append_offset==0 允许重建同名块文件（例如重复更新同一 block_key）。
                    // 这里统一使用 trunc，避免因文件已存在触发断言导致进程崩溃。
                    std::ofstream create_file(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                    create_file.close();
                }

                // Open file in append mode
                // write the data to the disk using pagecache
                std::ofstream append_file(writepath, std::ios::binary | std::ios::out | std::ios::app);
                // Append data from buffer to end of file
                append_file.write(buf.data(), append_size);
                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Append120] successfully append data block " << block_key << " with " << append_size << " bytes" << std::endl;
                }
                append_file.flush();
                append_file.close();
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        // append_offset must be the physical offset of the block
        auto ParityBlockHandler = [this](std::string block_key, int append_size, int append_offset, bool is_serialized) mutable
        {
            try
            {
                char *buf = new char[append_size];
                // only send data
                asio::error_code ec;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);
                asio::read(socket, asio::buffer(buf, append_size), ec);

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;

                // std::cout << "[Datanode" << m_port << "][Append101] writepath: " << writepath << " append_offset: " << append_offset << " append_size: " << append_size << std::endl;

                if (access(targetdir.c_str(), 0) == -1)
                {
                    createDirectories(targetdir);
                }

                if (append_offset == 0 && access(writepath.c_str(), 0) == -1)
                {
                    // std::cout << "create parity block file with path: " << writepath << std::endl;
                    // Create new file if append_offset is 0 and file does not exist
                    std::ofstream create_file(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                    create_file.close();
                }

                // serialize and append to file
                if (is_serialized)
                {
                    serialize(writepath, ParitySlice(append_offset, append_size, buf));
                }
                else
                {
                    std::ofstream append_file(writepath, std::ios::binary | std::ios::out | std::ios::app);
                    append_file.write(buf, append_size);
                    append_file.flush();
                    append_file.close();
                }

                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Append167] successfully append parity block " << block_key << " with " << append_size << " bytes" << std::endl;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            if (IF_DEBUG)
            {
                // std::cout << "[Datanode" << m_port << "][Append109] block_key: " << block_key << ", block_id: " << block_id << ", append_size: " << append_size << ", append_offset: " << append_offset << " is_serialized: " << is_serialized << std::endl;
            }
            if (block_id < m_sys_config->k)
            {
                std::thread my_thread(dataBlockHandler, block_key, append_size, append_offset);
                my_thread.detach();
            }
            else
            {
                std::thread my_thread(ParityBlockHandler, block_key, append_size, append_offset, is_serialized);
                my_thread.detach();
            }
            response->set_message(true);
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }

        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleRecovery(
        grpc::ServerContext *context,
        const datanode_proto::MergeParityInfo *recovery_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = recovery_info->block_key();
        int block_id = recovery_info->block_id();

        auto handler = [this, &response](std::string block_key, int block_id) mutable
        {
            try
            {
                std::vector<char> buf(m_sys_config->BlockSize);
                // only send data
                asio::error_code ec;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);
                asio::read(socket, asio::buffer(buf.data(), m_sys_config->BlockSize), ec);

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;
                if(access(targetdir.c_str(), 0) == -1)
                {
                    mkdir(targetdir.c_str(), S_IRWXU);
                }

                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                if (!ofs.is_open())
                {
                    std::cerr << "[Recovery] Failed to open file: " << writepath << std::endl;
                    exit(-1);
                }
                ofs.write(buf.data(), m_sys_config->BlockSize);
                ofs.flush();
                ofs.close();

                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Recovery] successfully recovery block " << block_key << " with " << m_sys_config->BlockSize << " bytes" << std::endl;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            std::thread my_thread(handler, block_key, block_id);
            my_thread.join();
            response->set_message(true);
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }

        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleRecoveryBreakdown(
        grpc::ServerContext *context,
        const datanode_proto::MergeParityInfo *recovery_info,
        datanode_proto::RequestResult *response)
    {
        std::chrono::high_resolution_clock::time_point grpc_start_time = std::chrono::high_resolution_clock::now();
        response->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(grpc_start_time.time_since_epoch()).count());

        std::string block_key = recovery_info->block_key();
        int block_id = recovery_info->block_id();

        auto handler = [this, &response](std::string block_key, int block_id) mutable
        {
            try
            {
                std::vector<char> buf(m_sys_config->BlockSize);
                // only send data
                asio::error_code ec;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);
                asio::read(socket, asio::buffer(buf.data(), m_sys_config->BlockSize), ec);

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;
                if(access(targetdir.c_str(), 0) == -1)
                {
                    mkdir(targetdir.c_str(), S_IRWXU);
                }

                std::chrono::high_resolution_clock::time_point begin = std::chrono::high_resolution_clock::now(); // start time for disk io
                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                if (!ofs.is_open())
                {
                    std::cerr << "[Recovery] Failed to open file: " << writepath << std::endl;
                    exit(-1);
                }
                ofs.write(buf.data(), m_sys_config->BlockSize);
                ofs.flush();
                ofs.close();
                std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now(); // end time for disk io
                response->set_disk_io_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(begin.time_since_epoch()).count());
                response->set_disk_io_end_time(std::chrono::duration_cast<std::chrono::duration<double>>(end.time_since_epoch()).count());

                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Recovery] successfully recovery block " << block_key << " with " << m_sys_config->BlockSize << " bytes" << std::endl;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            std::thread my_thread(handler, block_key, block_id);
            my_thread.join();
            response->set_message(true);
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }

        return grpc::Status::OK;
    }


    grpc::Status DatanodeImpl::handleMergeParity(
        grpc::ServerContext *context,
        const datanode_proto::MergeParityInfo *merge_parity_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = merge_parity_info->block_key();
        int block_id = merge_parity_info->block_id();
        auto handler = [this](std::string block_key, int block_id) mutable
        {
            try
            {
                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string readpath = targetdir + block_key;

                // std::cout << "[Datanode" << m_port << "][Merge Parity Slices] readpath: " << readpath << std::endl;

                if (access(readpath.c_str(), 0) == -1)
                {
                    std::cerr << "[Datanode" << m_port << "][Merge Parity Slices] file does not exist!" << readpath << std::endl;
                    exit(-1);
                }
                std::vector<ParitySlice> slices = deserialize(readpath);
                std::string writepath = targetdir + block_key;
                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                std::unique_ptr<char[]> mergedBuf(new char[m_sys_config->BlockSize]);
                memset(mergedBuf.get(), 0, m_sys_config->BlockSize);
                for (const auto &slice : slices)
                {
                    for (int i = 0; i < slice.size; i++)
                    {
                        assert(slice.offset + i < m_sys_config->BlockSize && "Parity slice.offset + i >= m_sys_config->BlockSize!");
                        mergedBuf[slice.offset + i] ^= slice.slice_ptr[i];
                    }
                }
                ofs.write(mergedBuf.get(), m_sys_config->BlockSize);
                ofs.flush();
                ofs.close();
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            std::thread my_thread(handler, block_key, block_id);
            my_thread.detach();
            response->set_message(true);
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }

        return grpc::Status::OK;
    }

    /*grpc::Status DatanodeImpl::handleMergeParityWithRep(
        grpc::ServerContext *context,
        const datanode_proto::MergeParityInfo *merge_parity_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = merge_parity_info->block_key();
        int block_id = merge_parity_info->block_id();
        auto handler = [this](std::string block_key, int block_id) mutable
        {
            try
            {
                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string readpath = targetdir + block_key;
                if (access(readpath.c_str(), 0) == -1)
                {
                    std::cout << "[Datanode" << m_port << "][Merge Parity Slices] file does not exist!" << readpath << std::endl;
                    exit(-1);
                }

                std::string writepath = targetdir + block_key;
                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                std::unique_ptr<char[]> dataBuf(new char[m_sys_config->BlockSize * m_sys_config->k]);
                memset(dataBuf.get(), 0, m_sys_config->BlockSize * m_sys_config->k);
                std::unique_ptr<char[]> mergedBuf(new char[m_sys_config->BlockSize]);
                memset(mergedBuf.get(), 0, m_sys_config->BlockSize);

                deserialize(readpath, dataBuf.get());
                ECProject::encode_unilrc_w_rep_mode(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char *>(dataBuf.get()), reinterpret_cast<unsigned char *>(mergedBuf.get()), m_sys_config->BlockSize, m_sys_config->UnitSize, block_id);

                ofs.write(mergedBuf.get(), m_sys_config->BlockSize);
                ofs.flush();
                ofs.close();
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            std::thread my_thread(handler, block_key, block_id);
            my_thread.detach();
            response->set_message(true);
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }

        return grpc::Status::OK;
    }*/

    grpc::Status DatanodeImpl::handleSet(
        grpc::ServerContext *context,
        const datanode_proto::SetInfo *set_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = set_info->block_key();
        int block_size = set_info->block_size();
        std::string proxy_ip = set_info->proxy_ip();
        int proxy_port = set_info->proxy_port();
        bool ispull = set_info->ispull();
        auto handler1 = [this](std::string block_key, int block_size) mutable
        {
            try
            {
                // char *buf = new char[block_size];
                std::vector<char> buf(block_size);
                // only send data
                asio::error_code ec;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);
                asio::read(socket, asio::buffer(buf.data(), block_size), ec);

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;
                if (access(targetdir.c_str(), 0) == -1)
                {
                    mkdir(targetdir.c_str(), S_IRWXU);
                }

                // write the data to the disk using pagecache
                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                ofs.write(buf.data(), block_size);
                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Write] successfully write " << block_key << " with " << ofs.tellp() << "bytes" << std::endl;
                }
                ofs.flush();
                ofs.close();
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };
        auto handler2 = [this, proxy_ip, proxy_port](std::string block_key, int block_size) mutable
        {
            try
            {
                std::vector<char> buf(block_size);

                asio::ip::tcp::socket socket(io_context);
                asio::ip::tcp::resolver resolver(io_context);
                asio::error_code con_error;
                asio::connect(socket, resolver.resolve({std::string(proxy_ip), std::to_string(proxy_port)}), con_error);
                asio::error_code ec;
                if (!con_error && IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "] Connect to " << proxy_ip << ":" << proxy_port << " success!" << std::endl;
                }

                asio::read(socket, asio::buffer(buf.data(), block_size), ec);

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;
                if (access(targetdir.c_str(), 0) == -1)
                {
                    mkdir(targetdir.c_str(), S_IRWXU);
                }

                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                ofs.write(buf.data(), block_size);
                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Write] successfully write " << block_key << " with " << ofs.tellp() << "bytes" << std::endl;
                }
                ofs.flush();
                ofs.close();
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };
        try
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][SET] ready to handle set!" << std::endl;
            }
            if (ispull)
            {
                std::thread my_thread(handler2, block_key, block_size);
                my_thread.join();
            }
            else
            {
                std::thread my_thread(handler1, block_key, block_size);
                my_thread.detach();
            }
            response->set_message(true);
        }
        catch (std::exception &e)
        {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleGetBreakdown(
        grpc::ServerContext *context,
        const datanode_proto::GetInfo *get_info,
        datanode_proto::RequestResult *response)
    {
        std::chrono::high_resolution_clock::time_point grpc_start = std::chrono::high_resolution_clock::now(); 
        response->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(grpc_start.time_since_epoch()).count());

        std::string block_key = get_info->block_key();
        int block_size = get_info->block_size();
        std::string proxy_ip = get_info->proxy_ip();
        int proxy_port = get_info->proxy_port();
        std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
        std::string readpath = targetdir + block_key;
        char *buf = new char[block_size];
        std::chrono::high_resolution_clock::time_point begin = std::chrono::high_resolution_clock::now(); // start time for disk io
        if (access(readpath.c_str(), 0) == -1)
        {
            std::cout << "[Datanode" << m_port << "][Read] file does not exist!" << readpath << std::endl;
        }
        else
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][GET] read from the disk and write to socket with port " << m_port + ECProject::DATANODE_PORT_SHIFT << std::endl;
            }
            std::ifstream ifs(readpath);
            ifs.read(buf, block_size);
            ifs.close();
        }
        std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now(); // end time for disk io
        double disk_io_start_time = std::chrono::duration_cast<std::chrono::duration<double>>(begin.time_since_epoch()).count();
        double disk_io_end_time = std::chrono::duration_cast<std::chrono::duration<double>>(end.time_since_epoch()).count();
        response->set_disk_io_start_time(disk_io_start_time);
        response->set_disk_io_end_time(disk_io_end_time);
        auto handler = [this](std::string block_key, int block_size, std::string proxy_ip, int proxy_port, char* buf) mutable
        {
            asio::error_code error;
            asio::ip::tcp::socket socket(io_context);
            acceptor.accept(socket);
            asio::write(socket, asio::buffer(buf, block_size), error);
            asio::error_code ignore_ec;
            socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket.close(ignore_ec);
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][GET] write to socket!" << std::endl;
            }
            delete buf;
        };
        try
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][GET] ready to handle get!" << std::endl;
            }
            std::thread my_thread(handler, block_key, block_size, proxy_ip, proxy_port, buf);
            my_thread.detach();
            response->set_message(true);
        }
        catch (std::exception &e)
        {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleGet(
        grpc::ServerContext *context,
        const datanode_proto::GetInfo *get_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = get_info->block_key();
        int block_size = get_info->block_size();
        std::string proxy_ip = get_info->proxy_ip();
        int proxy_port = get_info->proxy_port();
        std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
        std::string readpath = targetdir + block_key;
        char *buf = new char[block_size];
        if (access(readpath.c_str(), 0) == -1)
        {
            std::cout << "[Datanode" << m_port << "][Read] file does not exist!" << readpath << std::endl;
        }
        else
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][GET] read from the disk and write to socket with port " << m_port + ECProject::DATANODE_PORT_SHIFT << std::endl;
            }
            std::ifstream ifs(readpath);
            ifs.read(buf, block_size);
            ifs.close();
        }
        auto handler = [this](std::string block_key, int block_size, std::string proxy_ip, int proxy_port, char* buf) mutable
        {
            asio::error_code error;
            asio::ip::tcp::socket socket(io_context);
            acceptor.accept(socket);
            asio::write(socket, asio::buffer(buf, block_size), error);
            asio::error_code ignore_ec;
            socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket.close(ignore_ec);
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][GET] write to socket!" << std::endl;
            }
            delete buf;
        };
        try
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][GET] ready to handle get!" << std::endl;
            }
            std::thread my_thread(handler, block_key, block_size, proxy_ip, proxy_port, buf);
            my_thread.detach();
            response->set_message(true);
        }
        catch (std::exception &e)
        {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        return grpc::Status::OK;
    }



  grpc::Status DatanodeImpl::handleCordRangeRead(
      grpc::ServerContext *context,
      const datanode_proto::CordRangeRWInfo *info,
      datanode_proto::RequestResult *response)
  {
    (void)context;
    std::string block_key = info->block_key();
    int range_offset = info->range_offset();
    int range_length = info->range_length();
    std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
    std::string readpath = targetdir + block_key;
    const uint64_t xfer_tag = cord_dn_alloc_xfer_tag();
    std::vector<char> data(static_cast<size_t>(range_length), 0);
    if (access(readpath.c_str(), 0) != -1)
    {
      std::ifstream ifs(readpath, std::ios::binary);
      if (ifs)
      {
        ifs.seekg(range_offset);
        ifs.read(data.data(), range_length);
      }
    }
    {
      std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
      g_cord_dn_pending_reads[xfer_tag] = CordDnPendingRead{std::move(data), range_length};
    }
    auto handler = [this]() mutable
    {
      asio::ip::tcp::socket socket(io_context);
      try
      {
        uint64_t wire_tag = 0;
        {
          std::lock_guard<std::mutex> accept_lk(g_cord_dn_accept_mu);
          acceptor.accept(socket);
          wire_tag = cord_dn_read_u64_be(socket);
        }
        std::vector<char> payload;
        {
          std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
          auto it = g_cord_dn_pending_reads.find(wire_tag);
          if (it == g_cord_dn_pending_reads.end())
          {
            std::cout << "[Datanode] cord range read unknown tag=" << wire_tag << std::endl;
            asio::error_code ignore_ec;
            socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket.close(ignore_ec);
            return;
          }
          payload = std::move(it->second.data);
          g_cord_dn_pending_reads.erase(it);
        }
        if (payload.empty())
        {
          std::cout << "[Datanode] cord range read empty payload tag=" << wire_tag << std::endl;
          asio::error_code ignore_ec;
          socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
          socket.close(ignore_ec);
          return;
        }
        asio::error_code error;
        asio::write(socket, asio::buffer(payload.data(), payload.size()), error);
        asio::error_code ignore_ec;
        socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
        socket.close(ignore_ec);
      }
      catch (std::exception &e)
      {
        std::cout << "handleCordRangeRead tcp exception: " << e.what() << std::endl;
        asio::error_code ignore_ec;
        socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
        socket.close(ignore_ec);
      }
    };
    try
    {
      std::thread my_thread(handler);
      my_thread.detach();
      response->set_message(true);
      response->set_cord_tcp_xfer_tag(xfer_tag);
    }
    catch (std::exception &e)
    {
      std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
      g_cord_dn_pending_reads.erase(xfer_tag);
      std::cout << "handleCordRangeRead exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  grpc::Status DatanodeImpl::handleCordRangeWrite(
      grpc::ServerContext *context,
      const datanode_proto::CordRangeRWInfo *info,
      datanode_proto::RequestResult *response)
  {
    (void)context;
    const std::string block_key = info->block_key();
    const int range_offset = info->range_offset();
    const int range_length = info->range_length();
    const uint64_t logical_block_size = info->logical_block_size();
    if (range_offset < 0 || range_length < 0)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "negative range offset or length");
    const uint64_t range_end = static_cast<uint64_t>(range_offset) + static_cast<uint64_t>(range_length);
    if (logical_block_size > 0 && range_end > logical_block_size)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "range exceeds logical block size");
    if (logical_block_size > static_cast<uint64_t>(std::numeric_limits<off_t>::max()))
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "logical block size exceeds filesystem limits");

    const std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
    const std::string writepath = targetdir + block_key;
    if (access(targetdir.c_str(), F_OK) == -1 && !createDirectories(targetdir))
      return grpc::Status(grpc::StatusCode::INTERNAL, "failed to create storage directory");

    const uint64_t xfer_tag = cord_dn_alloc_xfer_tag();
    {
      std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
      g_cord_dn_pending_writes[xfer_tag] =
          CordDnPendingWrite{writepath, range_offset, range_length, logical_block_size};
    }

    auto handler = [this]() mutable
    {
      asio::ip::tcp::socket socket(io_context);
      uint8_t ack = 0;
      try
      {
        uint64_t wire_tag = 0;
        {
          std::lock_guard<std::mutex> accept_lk(g_cord_dn_accept_mu);
          acceptor.accept(socket);
          wire_tag = cord_dn_read_u64_be(socket);
        }
        CordDnPendingWrite pending;
        {
          std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
          auto it = g_cord_dn_pending_writes.find(wire_tag);
          if (it == g_cord_dn_pending_writes.end())
            throw std::runtime_error("unknown cord range write tag");
          pending = std::move(it->second);
          g_cord_dn_pending_writes.erase(it);
        }

        std::vector<char> payload(static_cast<size_t>(pending.range_length));
        asio::error_code read_ec;
        const size_t received = asio::read(socket, asio::buffer(payload.data(), payload.size()), read_ec);
        if (read_ec || received != payload.size())
          throw std::runtime_error("incomplete cord range write payload");

        const auto writepath_mu = cord_dn_mutex_for_writepath(pending.writepath);
        std::lock_guard<std::mutex> write_lk(*writepath_mu);
        const int fd = ::open(pending.writepath.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd < 0)
          throw std::runtime_error("open failed: " + std::string(std::strerror(errno)));

        bool file_ok = true;
        size_t written = 0;
        while (written < payload.size())
        {
          const ssize_t n = ::pwrite(fd, payload.data() + written, payload.size() - written,
                                     static_cast<off_t>(pending.range_offset) + static_cast<off_t>(written));
          if (n < 0)
          {
            if (errno == EINTR)
              continue;
            file_ok = false;
            break;
          }
          if (n == 0)
          {
            file_ok = false;
            break;
          }
          written += static_cast<size_t>(n);
        }
        if (file_ok && pending.logical_block_size > 0 &&
            ::ftruncate(fd, static_cast<off_t>(pending.logical_block_size)) != 0)
          file_ok = false;
        if (file_ok && ::fsync(fd) != 0)
          file_ok = false;
        if (::close(fd) != 0)
          file_ok = false;
        if (!file_ok)
          throw std::runtime_error("failed to persist cord range write");
        ack = 1;
      }
      catch (const std::exception &e)
      {
        std::cout << "handleCordRangeWrite tcp exception: " << e.what() << std::endl;
      }

      if (socket.is_open())
      {
        asio::error_code write_ec;
        const size_t sent = asio::write(socket, asio::buffer(&ack, 1), write_ec);
        if (write_ec || sent != 1)
          std::cout << "handleCordRangeWrite ACK failed: " << write_ec.message() << std::endl;
        asio::error_code ignore_ec;
        socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
        socket.close(ignore_ec);
      }
    };
    try
    {
      std::thread(handler).detach();
      response->set_message(true);
      response->set_cord_tcp_xfer_tag(xfer_tag);
    }
    catch (const std::exception &e)
    {
      std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
      g_cord_dn_pending_writes.erase(xfer_tag);
      return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
    return grpc::Status::OK;
  }

  grpc::Status DatanodeImpl::handleCordDeltaBlob(
      grpc::ServerContext *context,
      const datanode_proto::CordDeltaBlobInfo *info,
      datanode_proto::RequestResult *response)
  {
    (void)context;
    std::string blob_key = info->blob_key();
    int byte_length = info->byte_length();
    std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
    std::string writepath = targetdir + blob_key;
    if (access(targetdir.c_str(), 0) == -1)
      createDirectories(targetdir);

    const uint64_t xfer_tag = cord_dn_alloc_xfer_tag();
    {
      std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
      g_cord_dn_pending_blobs[xfer_tag] = CordDnPendingBlob{writepath, byte_length};
    }

    auto handler = [this]() mutable
    {
      try
      {
        asio::ip::tcp::socket socket(io_context);
        uint64_t wire_tag = 0;
        {
          std::lock_guard<std::mutex> accept_lk(g_cord_dn_accept_mu);
          acceptor.accept(socket);
          wire_tag = cord_dn_read_u64_be(socket);
        }
        CordDnPendingBlob pending;
        {
          std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
          auto it = g_cord_dn_pending_blobs.find(wire_tag);
          if (it == g_cord_dn_pending_blobs.end())
          {
            std::cout << "[Datanode] cord delta blob unknown tag=" << wire_tag << std::endl;
            return;
          }
          pending = std::move(it->second);
          g_cord_dn_pending_blobs.erase(it);
        }
        const int byte_length = pending.byte_length;
        std::vector<char> payload(static_cast<size_t>(byte_length));
        asio::error_code ec;
        asio::read(socket, asio::buffer(payload.data(), static_cast<size_t>(byte_length)), ec);
        asio::error_code ignore_ec;
        socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
        socket.close(ignore_ec);
        if (ec)
          return;
        std::ofstream ofs(pending.writepath, std::ios::binary | std::ios::out | std::ios::trunc);
        ofs.write(payload.data(), byte_length);
        ofs.flush();
        ofs.close();
      }
      catch (std::exception &e)
      {
        std::cout << "handleCordDeltaBlob tcp exception: " << e.what() << std::endl;
      }
    };
    try
    {
      std::thread my_thread(handler);
      my_thread.detach();
      response->set_message(true);
      response->set_cord_tcp_xfer_tag(xfer_tag);
    }
    catch (std::exception &e)
    {
      std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
      g_cord_dn_pending_blobs.erase(xfer_tag);
      std::cout << "handleCordDeltaBlob exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

    grpc::Status DatanodeImpl::handleDelete(
        grpc::ServerContext *context,
        const datanode_proto::DelInfo *del_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = del_info->block_key();
        std::string file_path = "./storage/" + std::to_string(m_port) + "/" + block_key;
        if (IF_DEBUG)
        {
            std::cout << "[Datanode" << m_port << "] File path:" << file_path << std::endl;
        }
        if (remove(file_path.c_str()))
        {
            std::cout << "[DEL] delete error!" << std::endl;
        }
        response->set_message(true);
        return grpc::Status::OK;
    }
}