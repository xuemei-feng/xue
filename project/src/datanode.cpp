#include "datanode.h"
#include "toolbox.h"
#include "parity_log_store.h"
#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <chrono>
#include <atomic>
#include <map>
#include <mutex>
#include <thread>
#include <vector>
#include <arpa/inet.h>

namespace
{
  std::atomic<uint64_t> g_cord_dn_next_xfer_tag{1};
  std::mutex g_cord_dn_pending_mu;
  std::mutex g_cord_dn_accept_mu;

  struct CordDnPendingRead {
    std::vector<char> data;
    int range_length = 0;
  };
  struct CordDnPendingWrite {
    std::string writepath;
    int range_offset = 0;
    int range_length = 0;
  };
  struct CordDnPendingBlob {
    std::string writepath;
    int byte_length = 0;
  };
  struct CordDnPendingParityAppend {
    int stripe_id = 0;
    int parity_block_id = 0;
    std::string parity_block_key;
    int data_block_id = 0;
    int range_offset = 0;
    int range_length = 0;
    int k = 0;
    int r = 0;
    int z = 0;
    int block_size = 0;
    std::string code_type;
  };
  struct CordDnPendingParityD0 {
    int stripe_id = 0;
    int parity_block_id = 0;
    int data_block_id = 0;
    int range_offset = 0;
    int range_length = 0;
    int datanode_port = 0;
  };

  std::map<uint64_t, CordDnPendingRead> g_cord_dn_pending_reads;
  std::map<uint64_t, CordDnPendingWrite> g_cord_dn_pending_writes;
  std::map<uint64_t, CordDnPendingBlob> g_cord_dn_pending_blobs;
  std::map<uint64_t, CordDnPendingParityAppend> g_cord_dn_pending_parity_append;
  std::map<uint64_t, CordDnPendingParityD0> g_cord_dn_pending_parity_d0;

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

  static void cord_dn_close_socket(asio::ip::tcp::socket &socket)
  {
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
    socket.close(ignore_ec);
  }
}

namespace ECProject
{
    void DatanodeImpl::startCordTcpAcceptLoopIfNeeded()
    {
      if (m_cord_tcp_accept_loop_started.exchange(true))
        return;
      std::thread([this]() { cordTcpAcceptLoop(); }).detach();
    }

    void DatanodeImpl::cordTcpAcceptLoop()
    {
      for (;;)
      {
        try
        {
          asio::ip::tcp::socket socket(io_context);
          {
            std::lock_guard<std::mutex> accept_lk(g_cord_dn_accept_mu);
            acceptor.accept(socket);
          }
          const uint64_t wire_tag = cord_dn_read_u64_be(socket);
          std::thread([this, sock = std::move(socket), wire_tag]() mutable {
            dispatchCordTcpSocket(sock, wire_tag);
          }).detach();
        }
        catch (const std::exception &e)
        {
          std::cout << "[Datanode" << m_port << "] cordTcpAcceptLoop: " << e.what() << std::endl;
        }
      }
    }

    void DatanodeImpl::dispatchCordTcpSocket(asio::ip::tcp::socket &socket, uint64_t wire_tag)
    {
      // Range write
      {
        CordDnPendingWrite pending;
        {
          std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
          auto it = g_cord_dn_pending_writes.find(wire_tag);
          if (it != g_cord_dn_pending_writes.end())
          {
            pending = std::move(it->second);
            g_cord_dn_pending_writes.erase(it);
          }
          else
          {
            pending.writepath.clear();
          }
        }
        if (!pending.writepath.empty())
        {
          try
          {
            const int range_length = pending.range_length;
            std::vector<char> payload(static_cast<size_t>(range_length));
            asio::error_code ec;
            asio::read(socket, asio::buffer(payload.data(), static_cast<size_t>(range_length)), ec);
            cord_dn_close_socket(socket);
            if (!ec)
            {
              int fd = ::open(pending.writepath.c_str(), O_CREAT | O_RDWR, 0644);
              if (fd >= 0)
              {
                ssize_t w = ::pwrite(fd, payload.data(), range_length, pending.range_offset);
                ::fsync(fd);
                ::close(fd);
                (void)w;
              }
            }
          }
          catch (std::exception &e)
          {
            std::cout << "dispatchCordTcpSocket range write exception: " << e.what() << std::endl;
            cord_dn_close_socket(socket);
          }
          return;
        }
      }

      // Range read
      {
        std::vector<char> payload;
        bool found = false;
        {
          std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
          auto it = g_cord_dn_pending_reads.find(wire_tag);
          if (it != g_cord_dn_pending_reads.end())
          {
            payload = std::move(it->second.data);
            g_cord_dn_pending_reads.erase(it);
            found = true;
          }
        }
        if (found && !payload.empty())
        {
          try
          {
            asio::error_code error;
            asio::write(socket, asio::buffer(payload.data(), payload.size()), error);
            cord_dn_close_socket(socket);
          }
          catch (std::exception &e)
          {
            std::cout << "dispatchCordTcpSocket range read exception: " << e.what() << std::endl;
            cord_dn_close_socket(socket);
          }
          return;
        }
        if (found)
        {
          std::cout << "[Datanode" << m_port << "] cord range read empty payload tag=" << wire_tag << std::endl;
          cord_dn_close_socket(socket);
          return;
        }
      }

      // Delta blob
      {
        CordDnPendingBlob pending;
        {
          std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
          auto it = g_cord_dn_pending_blobs.find(wire_tag);
          if (it != g_cord_dn_pending_blobs.end())
          {
            pending = std::move(it->second);
            g_cord_dn_pending_blobs.erase(it);
          }
          else
          {
            pending.writepath.clear();
          }
        }
        if (!pending.writepath.empty())
        {
          try
          {
            const int byte_length = pending.byte_length;
            std::vector<char> payload(static_cast<size_t>(byte_length));
            asio::error_code ec;
            asio::read(socket, asio::buffer(payload.data(), static_cast<size_t>(byte_length)), ec);
            cord_dn_close_socket(socket);
            if (!ec)
            {
              std::ofstream ofs(pending.writepath, std::ios::binary | std::ios::out | std::ios::trunc);
              ofs.write(payload.data(), byte_length);
              ofs.flush();
              ofs.close();
            }
          }
          catch (std::exception &e)
          {
            std::cout << "dispatchCordTcpSocket delta blob exception: " << e.what() << std::endl;
            cord_dn_close_socket(socket);
          }
          return;
        }
      }

      // Parity log append
      {
        CordDnPendingParityAppend pending;
        bool found = false;
        {
          std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
          auto it = g_cord_dn_pending_parity_append.find(wire_tag);
          if (it != g_cord_dn_pending_parity_append.end())
          {
            pending = std::move(it->second);
            g_cord_dn_pending_parity_append.erase(it);
            found = true;
          }
        }
        if (found)
        {
          uint8_t ack[2] = {0, 0};
          try
          {
            std::vector<char> payload(static_cast<size_t>(pending.range_length));
            asio::error_code ec;
            asio::read(socket, asio::buffer(payload.data(), static_cast<size_t>(pending.range_length)), ec);
            if (!ec)
            {
              ParityLogAppendResult r = ParityLogStore::instance().append_new_data(
                  pending.stripe_id, pending.parity_block_id, pending.parity_block_key, pending.data_block_id,
                  pending.range_offset, pending.range_length, pending.k, pending.r, pending.z, pending.block_size,
                  pending.code_type, payload.data(), pending.range_length);
              ack[0] = r.ok ? 1 : 0;
              ack[1] = r.need_d0 ? 1 : 0;
            }
            asio::write(socket, asio::buffer(ack, 2));
          }
          catch (std::exception &e)
          {
            std::cout << "dispatchCordTcpSocket parity append exception: " << e.what() << std::endl;
            try
            {
              asio::write(socket, asio::buffer(ack, 2));
            }
            catch (...)
            {
            }
          }
          cord_dn_close_socket(socket);
          return;
        }
      }

      // Parity log store D0
      {
        CordDnPendingParityD0 pending;
        bool found = false;
        {
          std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
          auto it = g_cord_dn_pending_parity_d0.find(wire_tag);
          if (it != g_cord_dn_pending_parity_d0.end())
          {
            pending = std::move(it->second);
            g_cord_dn_pending_parity_d0.erase(it);
            found = true;
          }
        }
        if (found)
        {
          uint8_t ack = 0;
          try
          {
            std::vector<char> payload(static_cast<size_t>(pending.range_length));
            asio::error_code ec;
            asio::read(socket, asio::buffer(payload.data(), static_cast<size_t>(pending.range_length)), ec);
            if (!ec)
            {
              const bool stored = ParityLogStore::instance().store_d0(
                  pending.stripe_id, pending.parity_block_id, pending.data_block_id, pending.range_offset,
                  pending.range_length, payload.data(), pending.range_length);
              ack = stored ? 1 : 0;
              if (stored)
                (void)ParityLogStore::instance().merge_if_full_cached(pending.stripe_id, pending.parity_block_id,
                                                                      pending.datanode_port);
            }
            asio::write(socket, asio::buffer(&ack, 1));
          }
          catch (std::exception &e)
          {
            std::cout << "dispatchCordTcpSocket parity d0 exception: " << e.what() << std::endl;
            try
            {
              asio::write(socket, asio::buffer(&ack, 1));
            }
            catch (...)
            {
            }
          }
          cord_dn_close_socket(socket);
          return;
        }
      }

      std::cout << "[Datanode" << m_port << "] tcp unknown tag=" << wire_tag << std::endl;
      cord_dn_close_socket(socket);
    }

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
    startCordTcpAcceptLoopIfNeeded();
    response->set_message(true);
    response->set_cord_tcp_xfer_tag(xfer_tag);
    return grpc::Status::OK;
  }

  grpc::Status DatanodeImpl::handleCordRangeWrite(
      grpc::ServerContext *context,
      const datanode_proto::CordRangeRWInfo *info,
      datanode_proto::RequestResult *response)
  {
    (void)context;
    std::string block_key = info->block_key();
    int range_offset = info->range_offset();
    int range_length = info->range_length();
    std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
    std::string writepath = targetdir + block_key;
    if (access(targetdir.c_str(), 0) == -1)
      createDirectories(targetdir);

    const uint64_t xfer_tag = cord_dn_alloc_xfer_tag();
    {
      std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
      g_cord_dn_pending_writes[xfer_tag] = CordDnPendingWrite{writepath, range_offset, range_length};
    }
    startCordTcpAcceptLoopIfNeeded();
    response->set_message(true);
    response->set_cord_tcp_xfer_tag(xfer_tag);
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
    startCordTcpAcceptLoopIfNeeded();
    response->set_message(true);
    response->set_cord_tcp_xfer_tag(xfer_tag);
    return grpc::Status::OK;
  }

  grpc::Status DatanodeImpl::handleParityLogAppend(grpc::ServerContext *context,
                                                   const datanode_proto::ParityLogAppendInfo *info,
                                                   datanode_proto::ParityLogAppendReply *response)
  {
    (void)context;
    response->set_ok(false);
    response->set_need_d0(false);
    response->set_cord_tcp_xfer_tag(0);

    const int range_length = info->range_length();
    if (range_length <= 0)
      return grpc::Status::OK;

    if (!info->new_data().empty())
    {
      const std::string &new_data = info->new_data();
      if (static_cast<int>(new_data.size()) != range_length)
        return grpc::Status::OK;
      ParityLogAppendResult r = ParityLogStore::instance().append_new_data(
          info->stripe_id(), info->parity_block_id(), info->parity_block_key(), info->data_block_id(),
          info->range_offset(), range_length, info->k(), info->r(), info->z(), info->block_size(),
          info->code_type(), new_data.data(), static_cast<int>(new_data.size()));
      response->set_ok(r.ok);
      response->set_need_d0(r.need_d0);
      return grpc::Status::OK;
    }

    const uint64_t xfer_tag = cord_dn_alloc_xfer_tag();
    {
      std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
      CordDnPendingParityAppend pending;
      pending.stripe_id = info->stripe_id();
      pending.parity_block_id = info->parity_block_id();
      pending.parity_block_key = info->parity_block_key();
      pending.data_block_id = info->data_block_id();
      pending.range_offset = info->range_offset();
      pending.range_length = range_length;
      pending.k = info->k();
      pending.r = info->r();
      pending.z = info->z();
      pending.block_size = info->block_size();
      pending.code_type = info->code_type();
      g_cord_dn_pending_parity_append[xfer_tag] = std::move(pending);
    }
    startCordTcpAcceptLoopIfNeeded();
    response->set_cord_tcp_xfer_tag(xfer_tag);
    response->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status DatanodeImpl::handleParityLogStoreD0(grpc::ServerContext *context,
                                                    const datanode_proto::ParityLogStoreD0Info *info,
                                                    datanode_proto::RequestResult *response)
  {
    (void)context;
    response->set_message(false);
    response->set_cord_tcp_xfer_tag(0);

    const int range_length = info->range_length();
    if (range_length <= 0)
      return grpc::Status::OK;

    if (!info->d0().empty())
    {
      const std::string &d0 = info->d0();
      if (static_cast<int>(d0.size()) != range_length)
        return grpc::Status::OK;
      bool stored = ParityLogStore::instance().store_d0(info->stripe_id(), info->parity_block_id(), info->data_block_id(),
                                                   info->range_offset(), range_length, d0.data(),
                                                   static_cast<int>(d0.size()));
      if (stored)
        (void)ParityLogStore::instance().merge_if_full_cached(info->stripe_id(), info->parity_block_id(), m_port);
      response->set_message(stored);
      return grpc::Status::OK;
    }

    const uint64_t xfer_tag = cord_dn_alloc_xfer_tag();
    {
      std::lock_guard<std::mutex> lk(g_cord_dn_pending_mu);
      CordDnPendingParityD0 pending;
      pending.stripe_id = info->stripe_id();
      pending.parity_block_id = info->parity_block_id();
      pending.data_block_id = info->data_block_id();
      pending.range_offset = info->range_offset();
      pending.range_length = range_length;
      pending.datanode_port = m_port;
      g_cord_dn_pending_parity_d0[xfer_tag] = std::move(pending);
    }
    startCordTcpAcceptLoopIfNeeded();
    response->set_cord_tcp_xfer_tag(xfer_tag);
    response->set_message(true);
    return grpc::Status::OK;
  }

  grpc::Status DatanodeImpl::handleParityLogClearStripe(grpc::ServerContext *context,
                                                        const datanode_proto::ParityLogClearStripeInfo *info,
                                                        datanode_proto::RequestResult *response)
  {
    (void)context;
    response->set_message(
        ParityLogStore::instance().clear_stripe_logs(info->stripe_id(), info->parity_begin(), info->parity_end()));
    return grpc::Status::OK;
  }

  grpc::Status DatanodeImpl::handleParityLogMergeIfFull(grpc::ServerContext *context,
                                                        const datanode_proto::ParityLogMergeIfFullInfo *info,
                                                        datanode_proto::RequestResult *response)
  {
    (void)context;
    response->set_message(false);
    const std::string storage_path =
        "./storage/" + std::to_string(m_port) + "/" + info->parity_block_key();
    response->set_message(ParityLogStore::instance().merge_if_full(
        info->stripe_id(), info->parity_block_id(), storage_path, info->k(), info->r(), info->z(), info->block_size(),
        info->code_type()));
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