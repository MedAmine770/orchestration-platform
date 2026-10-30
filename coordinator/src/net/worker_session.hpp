#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

#include "orch/protocol.hpp"

namespace orch::net {

class Server;

// WebSocket close codes (proto/PROTOCOL.md "Closing").
inline constexpr std::uint16_t kClosePolicy = 1008;    // auth failure, hello missing/late
inline constexpr std::uint16_t kCloseProtocol = 1002;  // undecodable or out-of-place message
inline constexpr std::uint16_t kCloseReplaced = 4000;  // same node_id connected again

// One worker connection, from the upgrade request until the socket closes.
//
// Lifecycle: accept handshake -> wait for `hello` (hello_timeout) -> authenticated,
// reading heartbeats. Lifetime is shared_from_this(): pending async handlers
// own the session, and it is destroyed when the last one completes.
class WorkerSession : public std::enable_shared_from_this<WorkerSession> {
  public:
    WorkerSession(boost::asio::ip::tcp::socket&& socket, Server& server);

    void run(boost::beast::http::request<boost::beast::http::string_body> upgrade);

    // Queues a message. Beast allows one write in flight per stream, so
    // messages wait in outbox_ and are written strictly in order.
    void send(const protocol::Message& message);

    // Sends a close frame once queued messages are flushed. Idempotent.
    void close(std::uint16_t code, std::string_view reason);

    const std::string& node_id() const { return node_id_; }

  private:
    void on_accept(boost::beast::error_code ec);
    void read();
    void on_read(boost::beast::error_code ec);
    void handle(std::string_view frame);
    void handle_hello(const protocol::Hello& hello);
    void write_next();
    void on_write(boost::beast::error_code ec);
    void start_close();

    boost::beast::websocket::stream<boost::beast::tcp_stream> ws_;
    boost::beast::flat_buffer buffer_;
    boost::asio::steady_timer hello_timer_;
    Server& server_;

    std::string node_id_;  // set once authenticated
    bool authenticated_ = false;
    std::chrono::steady_clock::time_point last_heartbeat_{};  // read by failure detection (M6)

    std::deque<std::string> outbox_;
    std::optional<boost::beast::websocket::close_reason> pending_close_;
};

}  // namespace orch::net
