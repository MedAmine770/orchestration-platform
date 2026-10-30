#include "net/server.hpp"

#include <iostream>
#include <utility>

#include <boost/asio/ip/address.hpp>

#include "net/http_session.hpp"
#include "net/worker_session.hpp"

namespace orch::net {

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

Server::Server(asio::io_context& ioc, ServerConfig config, const metrics::Registry& registry,
               CoordinatorMetrics& metrics)
    : ioc_(ioc),
      acceptor_(ioc, tcp::endpoint(asio::ip::make_address(config.bind_address), config.port)),
      config_(std::move(config)),
      registry_(registry),
      metrics_(metrics) {}

void Server::start() { accept(); }

unsigned short Server::port() const { return acceptor_.local_endpoint().port(); }

void Server::accept() {
    acceptor_.async_accept(ioc_, [this](boost::system::error_code ec, tcp::socket socket) {
        if (ec) {
            if (ec != asio::error::operation_aborted) {
                std::cerr << "accept failed: " << ec.message() << '\n';
            }
            return;
        }
        std::make_shared<HttpSession>(std::move(socket), *this)->run();
        accept();
    });
}

void Server::on_authenticated(const std::shared_ptr<WorkerSession>& session) {
    auto& slot = workers_[session->node_id()];
    if (const auto previous = slot.lock(); previous && previous != session) {
        std::cerr << "node " << session->node_id() << " reconnected; closing its old session\n";
        previous->close(kCloseReplaced, "replaced by a newer session");
    }
    slot = session;
    update_node_gauge();
}

void Server::on_closed(const WorkerSession& session) {
    const auto it = workers_.find(session.node_id());
    // Only remove the entry if it still points at this session: a replaced
    // session closing late must not evict its replacement.
    if (it != workers_.end() && it->second.lock().get() == &session) {
        workers_.erase(it);
        update_node_gauge();
    }
}

void Server::update_node_gauge() {
    // Until failure detection lands (M6) every connected worker counts as alive.
    metrics_.nodes("alive").set(static_cast<std::int64_t>(workers_.size()));
}

}  // namespace orch::net
