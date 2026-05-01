#include "datanode.h"
#include "toolbox.h"
#include <fstream>
#include <unistd.h>
#include <sys/stat.h>
#include <chrono>
#include <iomanip>
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
            // Read parity slices in a robust way: stop on short/corrupted records.
            while (true)
            {
                ParitySlice slice;

                if (!inFile.read(reinterpret_cast<char *>(&slice.offset), sizeof(slice.offset)))
                {
                    break;
                }
                if (!inFile.read(reinterpret_cast<char *>(&slice.size), sizeof(slice.size)))
                {
                    std::cerr << "[Datanode" << m_port << "][Deserialize] truncated parity slice header in file: " << filename << std::endl;
                    break;
                }
                if (slice.offset < 0 || slice.size < 0 ||
                    slice.offset >= static_cast<int>(m_sys_config->BlockSize) ||
                    slice.size > static_cast<int>(m_sys_config->BlockSize) ||
                    slice.offset + slice.size > static_cast<int>(m_sys_config->BlockSize))
                {
                    std::cerr << "[Datanode" << m_port << "][Deserialize] invalid parity slice header in file: " << filename
                              << " offset=" << slice.offset << " size=" << slice.size
                              << " block_size=" << m_sys_config->BlockSize << std::endl;
                    break;
                }

                slice.slice_ptr = new char[slice.size];
                if (!inFile.read(slice.slice_ptr, slice.size))
                {
                    std::cerr << "[Datanode" << m_port << "][Deserialize] truncated parity slice payload in file: " << filename
                              << " offset=" << slice.offset << " size=" << slice.size << std::endl;
                    delete[] slice.slice_ptr;
                    slice.slice_ptr = nullptr;
                    break;
                }

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
                std::string delta_log_path = writepath + ".delta";

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
                    // For parity merge, keep serialized deltas in a dedicated temp log.
                    // The base parity block file (writepath) stores raw block bytes.
                    serialize(delta_log_path, ParitySlice(append_offset, append_size, buf));
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
                std::string writepath = targetdir + block_key;
                std::string delta_log_path = writepath + ".delta";
                std::unique_ptr<char[]> mergedBuf(new char[m_sys_config->BlockSize]);
                memset(mergedBuf.get(), 0, m_sys_config->BlockSize);

                // Load old parity block as base; if missing, treat as zero block.
                if (access(writepath.c_str(), 0) != -1)
                {
                    std::ifstream base_in(writepath, std::ios::in | std::ios::binary);
                    if (base_in.is_open())
                    {
                        base_in.read(mergedBuf.get(), m_sys_config->BlockSize);
                        base_in.close();
                    }
                }

                if (access(delta_log_path.c_str(), 0) == -1)
                {
                    std::cout << "[Datanode" << m_port << "][MergeParity] no delta log for block_key=" << block_key << std::endl;
                    return;
                }
                std::vector<ParitySlice> slices = deserialize(delta_log_path);
                for (const auto &slice : slices)
                {
                    if (slice.offset < 0 || slice.size < 0 ||
                        slice.offset + slice.size > static_cast<int>(m_sys_config->BlockSize))
                    {
                        std::cerr << "[Datanode" << m_port << "][MergeParity] skip invalid slice"
                                  << " offset=" << slice.offset
                                  << " size=" << slice.size
                                  << " block_size=" << m_sys_config->BlockSize << std::endl;
                        continue;
                    }
                    for (int i = 0; i < slice.size; i++)
                    {
                        mergedBuf[slice.offset + i] ^= slice.slice_ptr[i];
                    }
                }
                {
                    constexpr int kPrev = 16;
                    const int n = std::min(kPrev, static_cast<int>(m_sys_config->BlockSize));
                    std::cout << "[Datanode" << m_port << "][MergeParity] block_key=" << block_key
                              << " block_id=" << block_id << " merged_block first " << n << " byte(s) hex:";
                    for (int i = 0; i < n; i++)
                    {
                        std::cout << ' ' << std::hex << std::setfill('0') << std::setw(2)
                                  << static_cast<unsigned>(static_cast<unsigned char>(mergedBuf[i]));
                    }
                    std::cout << std::dec << std::endl;
                }
                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                ofs.write(mergedBuf.get(), m_sys_config->BlockSize);
                ofs.flush();
                ofs.close();
                if (std::remove(delta_log_path.c_str()) != 0)
                {
                    std::cerr << "[Datanode" << m_port << "][MergeParity] failed to remove delta log: " << delta_log_path << std::endl;
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