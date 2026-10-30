#include "net/worker_session.hpp"

#include <iostream>
#include <utility>
#include <variant>

#include "net/server.hpp"

namespace orch::net {

namespace beast = boost::beast;
namespace websocket = beast::websocket;

namespace {

constexpr std::size_t kMaxFrameBytes = 64 * 1024;  // proto/PROTOCOL.md "Transport"
constexpr auto kHandshakeTimeout = std::chrono::seconds(5);

template <typename... Fs>
struct Overloaded : Fs... {
    using Fs::operator()...;
};

}  // namespace

WorkerSession::WorkerSession(boost::asio::ip::tcp::socket&& socket, Server& server)
    : ws_(std::move(socket)), hello_timer_(ws_.get_executor()), server_(server) {}

void WorkerSession::run(beast::http::request<beast::http::string_body> upgrade) {
    // The HTTP read timeout no longer applies; the WebSocket has its own.
    beast::get_lowest_layer(ws_).expires_never();
    websocket::stream_base::timeout timeouts{};
    timeouts.handshake_timeout = kHandshakeTimeout;
    timeouts.idle_timeout = websocket::stream_base::none();  // liveness = heartbeats (M6)
    timeouts.keep_alive_pings = false;
    ws_.set_option(timeouts);
    ws_.read_message_max(kMaxFrameBytes);
    ws_.text(true);

    ws_.async_accept(upgrade,
                     [self = shared_from_this()](beast::error_code ec) { self->on_accept(ec); });
}

void WorkerSession::on_accept(beast::error_code ec) {
    if (ec) return;
    // Start the clock only after the handshake: a close frame can't be sent before it.
    hello_timer_.expires_after(server_.config().hello_timeout);
    hello_timer_.async_wait([self = shared_from_this()](beast::error_code timer_ec) {
        if (timer_ec || self->authenticated_) return;
        self->close(kClosePolicy, "hello timeout");
    });
    read();
}

void WorkerSession::read() {
    ws_.async_read(buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t) {
        self->on_read(ec);
    });
}

void WorkerSession::on_read(beast::error_code ec) {
    if (ec) {
        // Closed by either side, or the connection broke. This is the single
        // place a session ends, so the server is told exactly once.
        hello_timer_.cancel();
        if (authenticated_) server_.on_closed(*this);
        return;
    }
    const std::string frame = beast::buffers_to_string(buffer_.data());
    buffer_.consume(buffer_.size());
    if (!pending_close_) handle(frame);
    read();  // keep reading so the peer's close frame completes the handshake
}

void WorkerSession::handle(std::string_view frame) {
    protocol::Message message;
    try {
        message = protocol::decode(frame);
    } catch (const protocol::ProtocolError& e) {
        std::cerr << "node " << (node_id_.empty() ? "<unauthenticated>" : node_id_)
                  << ": protocol error: " << e.what() << '\n';
        // The close reason carries only the error code: it is ASCII, and the
        // frame's own text (which may contain arbitrary bytes) stays in our log.
        close(kCloseProtocol, protocol::to_string(e.code()));
        return;
    }

    if (!authenticated_) {
        if (const auto* hello = std::get_if<protocol::Hello>(&message)) {
            handle_hello(*hello);
        } else {
            close(kClosePolicy, "first message must be hello");
        }
        return;
    }

    std::visit(
        Overloaded{
            [&](const protocol::Heartbeat&) { last_heartbeat_ = std::chrono::steady_clock::now(); },
            [&](const protocol::Unknown& u) {
                std::cerr << "node " << node_id_ << ": ignoring unknown message type '" << u.type
                          << "'\n";
            },
            [&](const protocol::Hello&) { close(kCloseProtocol, "duplicate hello"); },
            [&](const protocol::Welcome&) { close(kCloseProtocol, "unexpected welcome"); },
            [&](const protocol::Assign&) { close(kCloseProtocol, "unexpected assign"); },
            [&](const protocol::Cancel&) { close(kCloseProtocol, "unexpected cancel"); },
            // ack / reject / started / finished: job handling arrives in M5.
            [&](const auto&) {},
        },
        message);
}

void WorkerSession::handle_hello(const protocol::Hello& hello) {
    if (!server_.token_matches(hello.token)) {
        server_.metrics().auth_failures.inc();
        std::cerr << "rejected node " << hello.node_id << ": bad cluster token\n";
        close(kClosePolicy, "authentication failed");
        return;
    }
    authenticated_ = true;
    hello_timer_.cancel();
    node_id_ = hello.node_id;
    last_heartbeat_ = std::chrono::steady_clock::now();
    server_.on_authenticated(shared_from_this());
    std::cerr << "node " << node_id_ << " connected (" << hello.capabilities.cpus << " cpus, "
              << hello.slots << " slots)\n";

    const auto interval = server_.config().heartbeat_interval.count();
    send(protocol::Welcome{static_cast<std::uint32_t>(interval)});
}

void WorkerSession::send(const protocol::Message& message) {
    if (pending_close_) return;
    outbox_.push_back(protocol::encode(message));
    if (outbox_.size() == 1) write_next();
}

void WorkerSession::write_next() {
    ws_.async_write(
        boost::asio::buffer(outbox_.front()),
        [self = shared_from_this()](beast::error_code ec, std::size_t) { self->on_write(ec); });
}

void WorkerSession::on_write(beast::error_code ec) {
    if (ec) return;  // the read side sees the same failure and ends the session
    outbox_.pop_front();
    if (!outbox_.empty()) {
        write_next();
    } else if (pending_close_) {
        start_close();
    }
}

void WorkerSession::close(std::uint16_t code, std::string_view reason) {
    if (pending_close_) return;
    pending_close_.emplace(static_cast<websocket::close_code>(code), reason);
    hello_timer_.cancel();
    // A close frame is a write too, so it must wait for any write in flight.
    if (outbox_.empty()) start_close();
}

void WorkerSession::start_close() {
    ws_.async_close(*pending_close_, [self = shared_from_this()](beast::error_code) {});
}

}  // namespace orch::net
