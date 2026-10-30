#pragma once

#include <memory>
#include <optional>

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

namespace orch::net {

class Server;

// One plain-HTTP connection: serves /healthz and /metrics with keep-alive, and
// hands `GET /v1/workers` upgrade requests to a WorkerSession.
class HttpSession : public std::enable_shared_from_this<HttpSession> {
  public:
    HttpSession(boost::asio::ip::tcp::socket&& socket, Server& server);
    void run();

  private:
    using Request = boost::beast::http::request<boost::beast::http::string_body>;
    using Response = boost::beast::http::response<boost::beast::http::string_body>;

    void read();
    void on_read(boost::beast::error_code ec);
    Response route(const Request& req) const;
    void write(Response res);

    boost::beast::tcp_stream stream_;
    boost::beast::flat_buffer buffer_;
    std::optional<boost::beast::http::request_parser<boost::beast::http::string_body>> parser_;
    Response response_;  // kept alive until async_write completes
    Server& server_;
};

}  // namespace orch::net
