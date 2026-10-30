#include "net/server.hpp"

#include <iostream>
#include <utility>

#include <boost/asio/ip/address.hpp>
#include <boost/hash2/sha2.hpp>

#include "net/http_session.hpp"
#include "net/worker_session.hpp"

namespace orch::net {

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

namespace {

std::array<unsigned char, 32> sha256(std::string_view s) {
    boost::hash2::sha2_256 h;
    h.update(s.data(), s.size());
    const auto digest = h.result();
    std::array<unsigned char, 32> out{};
    for (std::size_t i = 0; i < out.size(); ++i) out[i] = digest[i];
    return out;
}

}  // namespace

Server::Server(asio::io_context& ioc, ServerConfig config, const metrics::Registry& registry,
               CoordinatorMetrics& metrics)
    : ioc_(ioc),
      acceptor_(ioc, tcp::endpoint(asio::ip::make_address(config.bind_address), config.port)),
      config_(std::move(config)),
      token_digest_(sha256(config_.cluster_token)),
      registry_(registry),
      metrics_(metrics) {}

bool Server::token_matches(std::string_view submitted) const {
    const auto candidate = sha256(submitted);
    unsigned char diff = 0;
    for (std::size_t i = 0; i < candidate.size(); ++i) {
        diff = static_cast<unsigned char>(diff | (candidate[i] ^ token_digest_[i]));
    }
    return diff == 0;
}

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
