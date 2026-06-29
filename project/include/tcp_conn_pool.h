#ifndef TCP_CONN_POOL_H
#define TCP_CONN_POOL_H

#include <asio.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <vector>

namespace ECProject
{

inline void tcp_set_recv_timeout(asio::ip::tcp::socket &socket, int sec)
{
  struct timeval tv;
  tv.tv_sec = sec;
  tv.tv_usec = 0;
  setsockopt(socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

inline void tcp_tune_socket(asio::ip::tcp::socket &socket)
{
  asio::error_code ec;
  socket.set_option(asio::ip::tcp::no_delay(true), ec);
  int one = 1;
  setsockopt(socket.native_handle(), SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
}

struct PooledTcpConnection
{
  std::shared_ptr<asio::io_context> io_context;
  asio::ip::tcp::socket socket;

  PooledTcpConnection()
      : io_context(std::make_shared<asio::io_context>()),
        socket(*io_context)
  {
  }
};

class TcpEndpointPool
{
public:
  explicit TcpEndpointPool(size_t max_idle = 8) : m_max_idle(max_idle) {}

  std::shared_ptr<PooledTcpConnection> acquire(const std::string &host, int port)
  {
    std::lock_guard<std::mutex> lk(m_mutex);
    while (!m_idle.empty())
    {
      auto conn = std::move(m_idle.back());
      m_idle.pop_back();
      if (conn != nullptr && conn->socket.is_open())
      {
        return conn;
      }
    }

    auto conn = std::make_shared<PooledTcpConnection>();
    asio::ip::tcp::resolver resolver(*conn->io_context);
    asio::error_code ec;
    asio::connect(conn->socket,
                  resolver.resolve(host, std::to_string(port)),
                  ec);
    if (ec)
    {
      return nullptr;
    }
    tcp_tune_socket(conn->socket);
    return conn;
  }

  void release(std::shared_ptr<PooledTcpConnection> conn, bool healthy)
  {
    if (conn == nullptr)
    {
      return;
    }
    if (!healthy || !conn->socket.is_open())
    {
      asio::error_code ec;
      conn->socket.close(ec);
      return;
    }
    std::lock_guard<std::mutex> lk(m_mutex);
    if (m_idle.size() < m_max_idle)
    {
      m_idle.push_back(std::move(conn));
      return;
    }
    asio::error_code ec;
    conn->socket.close(ec);
  }

private:
  size_t m_max_idle;
  std::mutex m_mutex;
  std::vector<std::shared_ptr<PooledTcpConnection>> m_idle;
};

class TcpEndpointPoolRegistry
{
public:
  static TcpEndpointPool &pool_for(const std::string &host, int port)
  {
    const std::string key = host + ":" + std::to_string(port);
    std::lock_guard<std::mutex> lk(map_mutex());
    auto &slot = pools()[key];
    if (slot == nullptr)
    {
      slot = std::make_unique<TcpEndpointPool>();
    }
    return *slot;
  }

private:
  static std::mutex &map_mutex()
  {
    static std::mutex m;
    return m;
  }

  static std::map<std::string, std::unique_ptr<TcpEndpointPool>> &pools()
  {
    static std::map<std::string, std::unique_ptr<TcpEndpointPool>> p;
    return p;
  }
};

/** conn_reusable: set true only when peer sends explicit ack=1 and keeps connection open. */
inline bool tcp_write_token_payload_read_ack(asio::ip::tcp::socket &socket,
                                            uint64_t token,
                                            const char *payload,
                                            size_t payload_size,
                                            int ack_timeout_sec,
                                            asio::error_code &ec,
                                            bool *conn_reusable = nullptr)
{
  ec.clear();
  if (conn_reusable != nullptr)
  {
    *conn_reusable = false;
  }
  if (token == 0)
  {
    ec = asio::error::invalid_argument;
    return false;
  }
  asio::write(socket, asio::buffer(&token, sizeof(token)), ec);
  if (ec)
  {
    return false;
  }
  asio::write(socket, asio::buffer(payload, payload_size), ec);
  if (ec)
  {
    return false;
  }
  tcp_set_recv_timeout(socket, ack_timeout_sec);
  char ack = 0;
  asio::read(socket, asio::buffer(&ack, 1), ec);
  if (ec == asio::error::eof)
  {
    ec.clear();
    return true;
  }
  const bool ok = !ec && ack == 1;
  if (ok && conn_reusable != nullptr)
  {
    *conn_reusable = true;
  }
  return ok;
}

} // namespace ECProject

#endif
