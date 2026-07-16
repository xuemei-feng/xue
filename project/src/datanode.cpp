#include "datanode.h"
#include "devcommon.h"
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
#include <cstring>
#include <arpa/inet.h>
#include <sys/select.h>

namespace
{
  std::atomic<uint64_t> g_cord_dn_next_xfer_tag{1};

  // 同一块文件的 read-xor-write 串行（XOR 可交换，仅需保证单次 RMW 原子）。
  std::mutex g_cord_dn_file_mu_map_mu;
  std::map<std::string, std::shared_ptr<std::mutex>> g_cord_dn_file_mu;
  static std::shared_ptr<std::mutex> cord_dn_file_mu_for(const std::string &path)
  {
    std::lock_guard<std::mutex> lk(g_cord_dn_file_mu_map_mu);
    auto &p = g_cord_dn_file_mu[path];
    if (!p)
      p = std::make_shared<std::mutex>();
    return p;
  }

  static uint64_t cord_dn_alloc_xfer_tag()
  {
    return g_cord_dn_next_xfer_tag.fetch_add(1, std::memory_order_relaxed);
  }

  static bool dn_is_serialized_parity_key(const std::string &block_key)
  {
    return block_key.find("_G") != std::string::npos || block_key.find("_L") != std::string::npos;
  }

  static void dn_read_block_into_buf(const std::string &readpath, const std::string &block_key,
                                     int block_size, char *buf, ECProject::DatanodeImpl *self)
  {
    memset(buf, 0, static_cast<size_t>(block_size));
    if (access(readpath.c_str(), 0) == -1)
      return;

    // After full-stripe merge / flat append, parity file size == BlockSize → raw read.
    // Intermediate CoRD/partial appends keep serialized (offset,size,payload) slices.
    long long file_size = -1;
    {
      std::ifstream szf(readpath, std::ios::binary | std::ios::ate);
      if (szf.is_open())
        file_size = static_cast<long long>(szf.tellg());
    }
    const bool look_like_flat = (file_size == static_cast<long long>(block_size));

    if (dn_is_serialized_parity_key(block_key) && !look_like_flat)
    {
      std::vector<ECProject::ParitySlice> slices = self->deserialize(readpath);
      for (const auto &slice : slices)
      {
        for (int i = 0; i < slice.size; i++)
        {
          if (slice.offset + i < block_size)
            buf[slice.offset + i] ^= slice.slice_ptr[i];
        }
        delete[] slice.slice_ptr;
      }
    }
    else
    {
      std::ifstream ifs(readpath, std::ios::binary);
      if (ifs.is_open())
      {
        ifs.read(buf, block_size);
        ifs.close();
      }
    }
  }

  static uint64_t cord_dn_parse_u64_be(const uint8_t b[8])
  {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
      v = (v << 8) | static_cast<uint64_t>(b[i]);
    return v;
  }

  static bool cord_dn_socket_has_readable_data(asio::ip::tcp::socket &socket, int timeout_ms)
  {
    const int fd = socket.native_handle();
    if (fd < 0)
      return false;
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    const int sel = ::select(fd + 1, &rfds, nullptr, nullptr, &tv);
    return sel > 0 && FD_ISSET(fd, &rfds);
  }
}

namespace ECProject
{
  DatanodeImpl::DatanodeImpl(std::string datanode_ip_port)
      : datanode_ip_port(std::move(datanode_ip_port)),
        acceptor(io_context,
                 asio::ip::tcp::endpoint(
                     asio::ip::address::from_string(this->datanode_ip_port.substr(0, this->datanode_ip_port.find(':')).c_str()),
                     ECProject::DATANODE_PORT_SHIFT +
                         std::stoi(this->datanode_ip_port.substr(this->datanode_ip_port.find(':') + 1, this->datanode_ip_port.size()))))
  {
    m_ip = this->datanode_ip_port.substr(0, this->datanode_ip_port.find(':'));
    m_port = std::stoi(this->datanode_ip_port.substr(this->datanode_ip_port.find(':') + 1, this->datanode_ip_port.size()));
    m_download_port = m_port + ECProject::DATANODE_PORT_SHIFT;
    dn_start_accept_loop();
  }

  DatanodeImpl::~DatanodeImpl()
  {
    m_dn_accept_running.store(false, std::memory_order_release);
    asio::error_code ignore_ec;
    acceptor.close(ignore_ec);
    if (m_dn_accept_thread.joinable())
      m_dn_accept_thread.join();
  }

  void DatanodeImpl::dn_start_accept_loop()
  {
    m_dn_accept_running.store(true, std::memory_order_release);
    m_dn_accept_thread = std::thread(&DatanodeImpl::dn_accept_dispatch_loop, this);
  }

  DatanodeImpl::DnDeliveredSocket DatanodeImpl::dn_wait_for_connection(DnConnWaitKind kind)
  {
    auto prom = std::make_shared<std::promise<DnDeliveredSocket>>();
    auto fut = prom->get_future();
    {
      std::lock_guard<std::mutex> lk(m_dn_conn_wait_mu);
      m_dn_conn_waiters.emplace_back(kind, prom);
    }
    return fut.get();
  }

  asio::error_code DatanodeImpl::dn_tcp_read_with_prefix(DnDeliveredSocket &delivered, char *out, size_t total)
  {
    size_t done = 0;
    if (delivered.prefix_len > 0)
    {
      const size_t copy_n = std::min(delivered.prefix_len, total);
      std::memcpy(out, delivered.prefix.data(), copy_n);
      done = copy_n;
    }
    if (done >= total)
      return asio::error_code();
    asio::error_code ec;
    asio::read(delivered.socket, asio::buffer(out + done, total - done), ec);
    return ec;
  }

  bool DatanodeImpl::dn_take_cord_pending(uint64_t wire_tag, CordDnDispatchJob &job)
  {
    std::lock_guard<std::mutex> lk(m_cord_pending_mu);
    if (auto it = m_cord_pending_reads.find(wire_tag); it != m_cord_pending_reads.end())
    {
      job.op = CordDnDispatchOp::Read;
      job.read = std::move(it->second);
      m_cord_pending_reads.erase(it);
      return true;
    }
    if (auto it = m_cord_pending_writes.find(wire_tag); it != m_cord_pending_writes.end())
    {
      job.op = CordDnDispatchOp::Write;
      job.write = std::move(it->second);
      m_cord_pending_writes.erase(it);
      return true;
    }
    if (auto it = m_cord_pending_xor_writes.find(wire_tag); it != m_cord_pending_xor_writes.end())
    {
      job.op = CordDnDispatchOp::XorWrite;
      job.write = std::move(it->second);
      m_cord_pending_xor_writes.erase(it);
      return true;
    }
    if (auto it = m_cord_pending_blobs.find(wire_tag); it != m_cord_pending_blobs.end())
    {
      job.op = CordDnDispatchOp::Blob;
      job.blob = std::move(it->second);
      m_cord_pending_blobs.erase(it);
      return true;
    }
    return false;
  }

  void DatanodeImpl::dn_run_cord_read_worker(asio::ip::tcp::socket socket, uint64_t wire_tag, CordDnPendingRead pending)
  {
    try
    {
      if (pending.data.empty())
      {
        std::cout << "[Datanode] cord range read empty payload tag=" << wire_tag << std::endl;
        asio::error_code ignore_ec;
        socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
        socket.close(ignore_ec);
        return;
      }
      asio::error_code error;
      asio::write(socket, asio::buffer(pending.data.data(), pending.data.size()), error);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
    }
    catch (std::exception &e)
    {
      std::cout << "handleCordRangeRead tcp exception: " << e.what() << std::endl;
    }
  }

  void DatanodeImpl::dn_run_cord_write_worker(asio::ip::tcp::socket socket, uint64_t wire_tag, CordDnPendingWrite pending)
  {
    (void)wire_tag;
    try
    {
      const int range_length = pending.range_length;
      std::vector<char> payload(static_cast<size_t>(range_length));
      asio::error_code ec;
      asio::read(socket, asio::buffer(payload.data(), static_cast<size_t>(range_length)), ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (ec)
        return;
      int fd = ::open(pending.writepath.c_str(), O_CREAT | O_RDWR, 0644);
      if (fd >= 0)
      {
        ssize_t w = ::pwrite(fd, payload.data(), range_length, pending.range_offset);
        ::fsync(fd);
        ::close(fd);
        (void)w;
      }
    }
    catch (std::exception &e)
    {
      std::cout << "handleCordRangeWrite tcp exception: " << e.what() << std::endl;
    }
  }

  void DatanodeImpl::dn_run_cord_xor_write_worker(asio::ip::tcp::socket socket, uint64_t wire_tag, CordDnPendingWrite pending)
  {
    (void)wire_tag;
    try
    {
      const int range_length = pending.range_length;
      std::vector<char> delta(static_cast<size_t>(range_length));
      asio::error_code ec;
      asio::read(socket, asio::buffer(delta.data(), static_cast<size_t>(range_length)), ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (ec)
        return;
      const auto file_mu = cord_dn_file_mu_for(pending.writepath);
      std::lock_guard<std::mutex> file_lk(*file_mu);
      int fd = ::open(pending.writepath.c_str(), O_CREAT | O_RDWR, 0644);
      if (fd < 0)
        return;
      std::vector<char> cur(static_cast<size_t>(range_length), 0);
      ssize_t r = ::pread(fd, cur.data(), range_length, pending.range_offset);
      (void)r;
      for (int u = 0; u < range_length; ++u)
        cur[static_cast<size_t>(u)] = static_cast<char>(
            static_cast<unsigned char>(cur[static_cast<size_t>(u)]) ^
            static_cast<unsigned char>(delta[static_cast<size_t>(u)]));
      ssize_t w = ::pwrite(fd, cur.data(), range_length, pending.range_offset);
      ::fsync(fd);
      ::close(fd);
      (void)w;
    }
    catch (std::exception &e)
    {
      std::cout << "handleCordRangeXorWrite tcp exception: " << e.what() << std::endl;
    }
  }

  void DatanodeImpl::dn_run_cord_blob_worker(asio::ip::tcp::socket socket, uint64_t wire_tag, CordDnPendingBlob pending)
  {
    (void)wire_tag;
    try
    {
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
  }

  void DatanodeImpl::dn_dispatch_cord_job(asio::ip::tcp::socket socket, uint64_t wire_tag, CordDnDispatchJob job)
  {
    switch (job.op)
    {
    case CordDnDispatchOp::Read:
      dn_run_cord_read_worker(std::move(socket), wire_tag, std::move(job.read));
      break;
    case CordDnDispatchOp::Write:
      dn_run_cord_write_worker(std::move(socket), wire_tag, std::move(job.write));
      break;
    case CordDnDispatchOp::XorWrite:
      dn_run_cord_xor_write_worker(std::move(socket), wire_tag, std::move(job.write));
      break;
    case CordDnDispatchOp::Blob:
      dn_run_cord_blob_worker(std::move(socket), wire_tag, std::move(job.blob));
      break;
    }
  }

  void DatanodeImpl::dn_accept_dispatch_loop()
  {
    while (m_dn_accept_running.load(std::memory_order_acquire))
    {
      try
      {
        DnDeliveredSocket delivered(io_context);
        acceptor.accept(delivered.socket);
        std::cout << "[Datanode" << m_port << "][Accept] got TCP connection" << std::endl;

        bool delivered_plain_write = false;
        {
          std::lock_guard<std::mutex> lk(m_dn_conn_wait_mu);
          if (!m_dn_conn_waiters.empty() && m_dn_conn_waiters.front().first == DnConnWaitKind::PlainWrite &&
              !cord_dn_socket_has_readable_data(delivered.socket, 100))
          {
            auto prom = std::move(m_dn_conn_waiters.front().second);
            m_dn_conn_waiters.pop_front();
            prom->set_value(std::move(delivered));
            delivered_plain_write = true;
          }
        }
        if (delivered_plain_write)
          continue;

        uint8_t tag_buf[8] = {0};
        asio::error_code read_ec;
        asio::read(delivered.socket, asio::buffer(tag_buf, 8), read_ec);
        if (read_ec)
        {
          asio::error_code ignore_ec;
          delivered.socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
          delivered.socket.close(ignore_ec);
          continue;
        }

        const uint64_t wire_tag = cord_dn_parse_u64_be(tag_buf);
        CordDnDispatchJob job;
        if (dn_take_cord_pending(wire_tag, job))
        {
          std::thread worker(&DatanodeImpl::dn_dispatch_cord_job, this, std::move(delivered.socket), wire_tag,
                             std::move(job));
          worker.detach();
          continue;
        }

        bool delivered_plain_read = false;
        {
          std::lock_guard<std::mutex> lk(m_dn_conn_wait_mu);
          if (!m_dn_conn_waiters.empty() && m_dn_conn_waiters.front().first == DnConnWaitKind::PlainRead)
          {
            delivered.prefix_len = 8;
            std::memcpy(delivered.prefix.data(), tag_buf, 8);
            auto prom = std::move(m_dn_conn_waiters.front().second);
            m_dn_conn_waiters.pop_front();
            prom->set_value(std::move(delivered));
            delivered_plain_read = true;
          }
        }
        if (delivered_plain_read)
          continue;

        std::cout << "[Datanode] cord tcp unknown tag=" << wire_tag << std::endl;
        asio::error_code ignore_ec;
        delivered.socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
        delivered.socket.close(ignore_ec);
      }
      catch (std::exception &e)
      {
        if (m_dn_accept_running.load(std::memory_order_acquire))
          std::cout << "[Datanode] accept dispatch exception: " << e.what() << std::endl;
      }
    }
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
        auto dataBlockHandler = [this](std::string block_key, int append_size, int append_offset,
                                        std::future<DnDeliveredSocket> fut) mutable
        {
            try
            {
                std::cout << "[Datanode" << m_port << "][Append] waiting for TCP data, block_key=" << block_key << " size=" << append_size << std::endl;
                std::vector<char> buf(append_size);
                DnDeliveredSocket delivered = fut.get();
                std::cout << "[Datanode" << m_port << "][Append] TCP connected, reading data..." << std::endl;
                asio::error_code ec = dn_tcp_read_with_prefix(delivered, buf.data(), static_cast<size_t>(append_size));

                asio::error_code ignore_ec;
                delivered.socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                delivered.socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;

                std::cout << "[Datanode" << m_port << "][Append] writing to: " << writepath << " size=" << append_size << std::endl;

                if (access(targetdir.c_str(), 0) == -1)
                {
                    createDirectories(targetdir);
                }

                if (append_offset == 0)
                {
                    std::ofstream create_file(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                    create_file.close();
                }

                std::ofstream append_file(writepath, std::ios::binary | std::ios::out | std::ios::app);
                append_file.write(buf.data(), append_size);
                std::cout << "[Datanode" << m_port << "][Append] wrote " << block_key << " size=" << append_size << " OK" << std::endl;
                append_file.flush();
                append_file.close();
            }
            catch (const std::exception &e)
            {
                std::cout << "[Datanode" << m_port << "][Append] EXCEPTION: " << e.what() << " block_key=" << block_key << std::endl;
            }
        };

        // append_offset must be the physical offset of the block
        auto ParityBlockHandler = [this](std::string block_key, int append_size, int append_offset, bool is_serialized,
                                         std::future<DnDeliveredSocket> fut) mutable
        {
            try
            {
                char *buf = new char[append_size];
                DnDeliveredSocket delivered = fut.get();
                asio::error_code ec = dn_tcp_read_with_prefix(delivered, buf, static_cast<size_t>(append_size));

                asio::error_code ignore_ec;
                delivered.socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                delivered.socket.close(ignore_ec);

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

                if (cord_trace_log(IF_DEBUG))
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
            // Register waiter BEFORE spawning thread, so the accept loop can find it
            // when TCP data arrives (fixes race between gRPC and TCP connect in proxy)
            auto prom = std::make_shared<std::promise<DnDeliveredSocket>>();
            std::future<DnDeliveredSocket> fut = prom->get_future();

            {
                std::lock_guard<std::mutex> lk(m_dn_conn_wait_mu);
                m_dn_conn_waiters.emplace_back(DnConnWaitKind::PlainRead, std::move(prom));
            }

            if (block_id < m_sys_config->k)
            {
                std::thread my_thread(dataBlockHandler, block_key, append_size, append_offset, std::move(fut));
                my_thread.detach();
            }
            else
            {
                std::thread my_thread(ParityBlockHandler, block_key, append_size, append_offset, is_serialized, std::move(fut));
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
        (void)block_id;

        auto handler = [this](std::string block_key, std::future<DnDeliveredSocket> fut) mutable
        {
            try
            {
                std::cout << "[Datanode" << m_port << "][Recovery] waiting for TCP data, block_key=" << block_key
                          << " size=" << m_sys_config->BlockSize << std::endl;
                std::vector<char> buf(m_sys_config->BlockSize);
                DnDeliveredSocket delivered = fut.get();
                asio::error_code ec = dn_tcp_read_with_prefix(delivered, buf.data(), static_cast<size_t>(m_sys_config->BlockSize));

                asio::error_code ignore_ec;
                delivered.socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                delivered.socket.close(ignore_ec);

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

                std::cout << "[Datanode" << m_port << "][Recovery] wrote " << block_key
                          << " size=" << m_sys_config->BlockSize << " OK" << std::endl;
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            // Register waiter BEFORE returning so proxy can TCP after sync gRPC
            auto prom = std::make_shared<std::promise<DnDeliveredSocket>>();
            std::future<DnDeliveredSocket> fut = prom->get_future();
            {
                std::lock_guard<std::mutex> lk(m_dn_conn_wait_mu);
                m_dn_conn_waiters.emplace_back(DnConnWaitKind::PlainRead, std::move(prom));
            }
            std::thread my_thread(handler, block_key, std::move(fut));
            my_thread.detach();
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
        (void)block_id;

        // Shared disk timing so proxy can still observe it after early gRPC return is not possible;
        // keep measuring locally for logs only (response must not be written after detach).
        auto handler = [this](std::string block_key, std::future<DnDeliveredSocket> fut) mutable
        {
            try
            {
                std::cout << "[Datanode" << m_port << "][Recovery] waiting for TCP data, block_key=" << block_key
                          << " size=" << m_sys_config->BlockSize << std::endl;
                std::vector<char> buf(m_sys_config->BlockSize);
                DnDeliveredSocket delivered = fut.get();
                asio::error_code ec = dn_tcp_read_with_prefix(delivered, buf.data(), static_cast<size_t>(m_sys_config->BlockSize));

                asio::error_code ignore_ec;
                delivered.socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                delivered.socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;
                if(access(targetdir.c_str(), 0) == -1)
                {
                    mkdir(targetdir.c_str(), S_IRWXU);
                }

                std::chrono::high_resolution_clock::time_point begin = std::chrono::high_resolution_clock::now();
                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                if (!ofs.is_open())
                {
                    std::cerr << "[Recovery] Failed to open file: " << writepath << std::endl;
                    exit(-1);
                }
                ofs.write(buf.data(), m_sys_config->BlockSize);
                ofs.flush();
                ofs.close();
                std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
                double disk_io_time = std::chrono::duration_cast<std::chrono::duration<double>>(end - begin).count();
                std::cout << "[Datanode" << m_port << "][Recovery] wrote " << block_key
                          << " size=" << m_sys_config->BlockSize << " OK disk_io=" << disk_io_time << "s" << std::endl;
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            auto prom = std::make_shared<std::promise<DnDeliveredSocket>>();
            std::future<DnDeliveredSocket> fut = prom->get_future();
            {
                std::lock_guard<std::mutex> lk(m_dn_conn_wait_mu);
                m_dn_conn_waiters.emplace_back(DnConnWaitKind::PlainRead, std::move(prom));
            }
            std::thread my_thread(handler, block_key, std::move(fut));
            my_thread.detach();
            response->set_message(true);
            // Disk IO completes asynchronously after TCP; leave times unset (0) for early return.
            response->set_disk_io_start_time(0);
            response->set_disk_io_end_time(0);
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
                DnDeliveredSocket delivered = dn_wait_for_connection(DnConnWaitKind::PlainRead);
                asio::error_code ec = dn_tcp_read_with_prefix(delivered, buf.data(), static_cast<size_t>(block_size));

                asio::error_code ignore_ec;
                delivered.socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                delivered.socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;
                if (access(targetdir.c_str(), 0) == -1)
                {
                    mkdir(targetdir.c_str(), S_IRWXU);
                }

                // write the data to the disk using pagecache
                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                ofs.write(buf.data(), block_size);
                if (cord_trace_log(IF_DEBUG))
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
                if (!con_error && cord_trace_log(IF_DEBUG))
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
                if (cord_trace_log(IF_DEBUG))
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
            if (cord_trace_log(IF_DEBUG))
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
            if (cord_trace_log(IF_DEBUG))
            {
                std::cout << "[Datanode" << m_port << "][GET] read from the disk and write to socket with port " << m_port + ECProject::DATANODE_PORT_SHIFT << std::endl;
            }
            dn_read_block_into_buf(readpath, block_key, block_size, buf, this);
        }
        std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now(); // end time for disk io
        double disk_io_start_time = std::chrono::duration_cast<std::chrono::duration<double>>(begin.time_since_epoch()).count();
        double disk_io_end_time = std::chrono::duration_cast<std::chrono::duration<double>>(end.time_since_epoch()).count();
        response->set_disk_io_start_time(disk_io_start_time);
        response->set_disk_io_end_time(disk_io_end_time);
        auto handler = [this](std::string block_key, int block_size, std::string proxy_ip, int proxy_port, char* buf) mutable
        {
            asio::error_code error;
            DnDeliveredSocket delivered = dn_wait_for_connection(DnConnWaitKind::PlainWrite);
            asio::write(delivered.socket, asio::buffer(buf, block_size), error);
            asio::error_code ignore_ec;
            delivered.socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            delivered.socket.close(ignore_ec);
            if (cord_trace_log(IF_DEBUG))
            {
                std::cout << "[Datanode" << m_port << "][GET] write to socket!" << std::endl;
            }
            delete buf;
        };
        try
        {
            if (cord_trace_log(IF_DEBUG))
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
            if (cord_trace_log(IF_DEBUG))
            {
                std::cout << "[Datanode" << m_port << "][GET] read from the disk and write to socket with port " << m_port + ECProject::DATANODE_PORT_SHIFT << std::endl;
            }
            dn_read_block_into_buf(readpath, block_key, block_size, buf, this);
        }
        auto handler = [this](std::string block_key, int block_size, std::string proxy_ip, int proxy_port, char* buf) mutable
        {
            asio::error_code error;
            DnDeliveredSocket delivered = dn_wait_for_connection(DnConnWaitKind::PlainWrite);
            asio::write(delivered.socket, asio::buffer(buf, block_size), error);
            asio::error_code ignore_ec;
            delivered.socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            delivered.socket.close(ignore_ec);
            if (cord_trace_log(IF_DEBUG))
            {
                std::cout << "[Datanode" << m_port << "][GET] write to socket!" << std::endl;
            }
            delete buf;
        };
        try
        {
            if (cord_trace_log(IF_DEBUG))
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
      std::lock_guard<std::mutex> lk(m_cord_pending_mu);
      m_cord_pending_reads[xfer_tag] = CordDnPendingRead{std::move(data), range_length};
    }
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
      std::lock_guard<std::mutex> lk(m_cord_pending_mu);
      m_cord_pending_writes[xfer_tag] = CordDnPendingWrite{writepath, range_offset, range_length};
    }
    response->set_message(true);
    response->set_cord_tcp_xfer_tag(xfer_tag);
    return grpc::Status::OK;
  }

  grpc::Status DatanodeImpl::handleCordRangeXorWrite(
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
      std::lock_guard<std::mutex> lk(m_cord_pending_mu);
      m_cord_pending_xor_writes[xfer_tag] = CordDnPendingWrite{writepath, range_offset, range_length};
    }
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
      std::lock_guard<std::mutex> lk(m_cord_pending_mu);
      m_cord_pending_blobs[xfer_tag] = CordDnPendingBlob{writepath, byte_length};
    }
    response->set_message(true);
    response->set_cord_tcp_xfer_tag(xfer_tag);
    return grpc::Status::OK;
  }

    grpc::Status DatanodeImpl::handleDelete(
        grpc::ServerContext *context,
        const datanode_proto::DelInfo *del_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = del_info->block_key();
        std::string file_path = "./storage/" + std::to_string(m_port) + "/" + block_key;
        if (cord_trace_log(IF_DEBUG))
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