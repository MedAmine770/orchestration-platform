#include "net/http_session.hpp"

#include <chrono>
#include <string>
#include <string_view>
#include <utility>

#include <boost/beast/websocket.hpp>

#include "net/server.hpp"
#include "net/worker_session.hpp"
#include "orch/metrics.hpp"
#include "orch/version.hpp"

namespace orch::net {

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;

namespace {

constexpr auto kReadTimeout = std::chrono::seconds(30);
constexpr std::size_t kBodyLimit = 8 * 1024;  // no endpoint takes a body yet
constexpr std::string_view kWorkersPath = "/v1/workers";

std::string_view path_of(std::string_view target) { return target.substr(0, target.find('?')); }

}  // namespace

HttpSession::HttpSession(boost::asio::ip::tcp::socket&& socket, Server& server)
    : stream_(std::move(socket)), server_(server) {}

void HttpSession::run() { read(); }

void HttpSession::read() {
    parser_.emplace();
    parser_->body_limit(kBodyLimit);
    stream_.expires_after(kReadTimeout);
    http::async_read(
        stream_, buffer_, *parser_,
        [self = shared_from_this()](beast::error_code ec, std::size_t) { self->on_read(ec); });
}

void HttpSession::on_read(beast::error_code ec) {
    if (ec == http::error::end_of_stream) {
        stream_.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);
        return;
    }
    if (ec) return;  // timeout, reset or malformed request: just drop the connection

    Request req = parser_->release();
    if (websocket::is_upgrade(req) && path_of(req.target()) == kWorkersPath) {
        // The WebSocket session takes over the socket; this session ends here.
        std::make_shared<WorkerSession>(stream_.release_socket(), server_)->run(std::move(req));
        return;
    }
    write(route(req));
}

HttpSession::Response HttpSession::route(const Request& req) const {
    Response res{http::status::ok, req.version()};
    res.keep_alive(req.keep_alive());
    const std::string_view path = path_of(req.target());

    const auto reply = [&](http::status status, std::string_view type, std::string body) {
        res.result(status);
        res.set(http::field::content_type, type);
        res.body() = std::move(body);
        res.prepare_payload();
        return res;
    };

    const bool known = path == "/healthz" || path == "/metrics" || path == kWorkersPath;
    if (!known) return reply(http::status::not_found, "text/plain", "not found\n");
    if (req.method() != http::verb::get) {
        return reply(http::status::method_not_allowed, "text/plain", "method not allowed\n");
    }
    if (path == "/healthz") {
        return reply(http::status::ok, "application/json",
                     R"({"status":"ok","version":")" + std::string(version()) + "\"}\n");
    }
    if (path == "/metrics") {
        return reply(http::status::ok, metrics::kContentType, server_.registry().render());
    }
    return reply(http::status::upgrade_required, "text/plain",
                 "this endpoint only accepts WebSocket upgrades\n");
}

void HttpSession::write(Response res) {
    response_ = std::move(res);
    http::async_write(
        stream_, response_, [self = shared_from_this()](beast::error_code ec, std::size_t) {
            if (ec) return;
            if (!self->response_.keep_alive()) {
                self->stream_.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);
                return;
            }
            self->read();
        });
}

}  // namespace orch::net
