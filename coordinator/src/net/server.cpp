#include "net/server.hpp"

#include <iostream>
#include <tuple>
#include <utility>
#include <variant>

#include <boost/asio/ip/address.hpp>
#include <boost/hash2/sha2.hpp>

#include "net/http_session.hpp"
#include "net/overloaded.hpp"
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
      metrics_(metrics),
      nodes_(metrics),
      jobs_(config_.jobs, nodes_, policy_, metrics),
      rng_(std::random_device{}()),
      api_(jobs_, nodes_, [this] { return uuid_v4(rng_); }),
      deadline_timer_(ioc) {}

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

void Server::on_authenticated(const std::shared_ptr<WorkerSession>& session,
                              const protocol::Hello& hello) {
    auto& slot = workers_[session->node_id()];
    if (const auto previous = slot.lock(); previous && previous != session) {
        std::cerr << "node " << session->node_id() << " reconnected; closing its old session\n";
        previous->close(kCloseReplaced, "replaced by a newer session");
    }
    slot = session;
    jobs_.node_joined(hello.node_id, hello.capabilities, hello.slots, Clock::now());
    flush();
}

bool Server::is_current(const WorkerSession& session) const {
    const auto it = workers_.find(session.node_id());
    return it != workers_.end() && it->second.lock().get() == &session;
}

void Server::on_closed(const WorkerSession& session) {
    // A replaced session closing late must neither evict its replacement nor
    // fail the node's jobs: the node is still connected.
    if (!is_current(session)) return;
    workers_.erase(session.node_id());
    std::cerr << "node " << session.node_id() << " disconnected\n";
    jobs_.node_lost(session.node_id(), Clock::now());
    flush();
}

void Server::on_heartbeat(const WorkerSession& session) {
    if (is_current(session)) nodes_.heartbeat(session.node_id(), Clock::now());
}

void Server::on_job_message(const WorkerSession& session, const protocol::Message& message) {
    if (!is_current(session)) return;
    const NodeId& node = session.node_id();
    const auto now = Clock::now();
    const auto [result, type, ref] =
        std::visit(Overloaded{
                       [&](const protocol::Ack& m) {
                           return std::tuple{jobs_.on_ack(node, m, now), "ack", m.job};
                       },
                       [&](const protocol::Reject& m) {
                           return std::tuple{jobs_.on_reject(node, m, now), "reject", m.job};
                       },
                       [&](const protocol::Started& m) {
                           return std::tuple{jobs_.on_started(node, m, now), "started", m.job};
                       },
                       [&](const protocol::Finished& m) {
                           return std::tuple{jobs_.on_finished(node, m, now), "finished", m.job};
                       },
                       [](const auto&) {
                           return std::tuple{MessageResult::ignored, "other", protocol::JobRef{}};
                       },
                   },
                   message);
    if (result != MessageResult::accepted) {
        std::cerr << "node " << node << ": "
                  << (result == MessageResult::stale ? "stale" : "out-of-order") << ' ' << type
                  << " for job " << ref.job_id << " attempt " << ref.attempt << '\n';
    }
    flush();
}

std::optional<ApiResponse> Server::handle_api(std::string_view method, std::string_view target,
                                              std::string_view body) {
    auto response = api_.handle(method, target, body, Clock::now());
    flush();
    return response;
}

void Server::flush() {
    for (const Effect& effect : jobs_.take_effects()) {
        std::visit(
            [&](const auto& e) {
                // No live session: the node just went away. Dropping the
                // message is safe; its timer or node_lost requeues the job.
                const auto it = workers_.find(e.node);
                if (const auto session = it == workers_.end() ? nullptr : it->second.lock()) {
                    session->send(e.message);
                }
            },
            effect);
    }

    // One timer for every job deadline (DESIGN.md §5.2). Re-arming cancels the
    // previous wait, whose handler then sees operation_aborted.
    if (const auto deadline = jobs_.next_deadline()) {
        deadline_timer_.expires_at(*deadline);
        deadline_timer_.async_wait([this](boost::system::error_code ec) {
            if (ec) return;
            jobs_.tick(Clock::now());
            flush();
        });
    } else {
        deadline_timer_.cancel();
    }
}

}  // namespace orch::net
