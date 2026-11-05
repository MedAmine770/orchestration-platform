#include "net/server.hpp"

#include <iostream>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

#include <boost/asio/ip/address.hpp>
#include <boost/hash2/sha2.hpp>

#include "net/http_session.hpp"
#include "net/overloaded.hpp"
#include "net/worker_session.hpp"
#include "orch/job_record.hpp"

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
               CoordinatorMetrics& metrics, Store* store)
    : ioc_(ioc),
      acceptor_(ioc, tcp::endpoint(asio::ip::make_address(config.bind_address), config.port)),
      config_(std::move(config)),
      token_digest_(sha256(config_.cluster_token)),
      registry_(registry),
      metrics_(metrics),
      store_(store),
      nodes_(metrics),
      jobs_(config_.jobs, nodes_, policy_, metrics),
      rng_(std::random_device{}()),
      api_(jobs_, nodes_, [this] { return uuid_v4(rng_); }),
      deadline_timer_(ioc) {
    if (config_.admin_port) {
        admin_acceptor_.emplace(
            ioc, tcp::endpoint(asio::ip::make_address(config_.bind_address), *config_.admin_port));
    }
}

bool Server::token_matches(std::string_view submitted) const {
    const auto candidate = sha256(submitted);
    unsigned char diff = 0;
    for (std::size_t i = 0; i < candidate.size(); ++i) {
        diff = static_cast<unsigned char>(diff | (candidate[i] ^ token_digest_[i]));
    }
    return diff == 0;
}

void Server::start() {
    accept(acceptor_, admin_acceptor_ ? Listener::workers : Listener::combined);
    if (admin_acceptor_) accept(*admin_acceptor_, Listener::admin);
}

unsigned short Server::port() const { return acceptor_.local_endpoint().port(); }

std::optional<unsigned short> Server::admin_port() const {
    if (!admin_acceptor_) return std::nullopt;
    return admin_acceptor_->local_endpoint().port();
}

void Server::accept(tcp::acceptor& acceptor, Listener listener) {
    const auto on_accept = [this, &acceptor, listener](boost::system::error_code ec,
                                                       tcp::socket socket) {
        if (ec) {
            if (ec != asio::error::operation_aborted) {
                std::cerr << "accept failed: " << ec.message() << '\n';
            }
            return;
        }
        std::make_shared<HttpSession>(std::move(socket), *this, listener)->run();
        accept(acceptor, listener);
    };
    acceptor.async_accept(ioc_, on_accept);
}

void Server::on_authenticated(const std::shared_ptr<WorkerSession>& session,
                              const protocol::Hello& hello) {
    auto& slot = workers_[session->node_id()];
    if (const auto previous = slot.lock(); previous && previous != session) {
        std::cerr << "node " << session->node_id() << " reconnected; closing its old session\n";
        previous->close(kCloseReplaced, "replaced by a newer session");
    }
    slot = session;
    jobs_.node_joined(hello.node_id, hello.capabilities, hello.slots, Clock::now(), hello.running);
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
    if (!is_current(session)) return;
    jobs_.heartbeat(session.node_id(), Clock::now());
    flush();  // a recovered node may take jobs; and the heartbeat deadline moved
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
                  << (result == MessageResult::stale ? "stale" : "duplicate or out-of-order") << ' '
                  << type << " for job " << ref.job_id << " attempt " << ref.attempt << '\n';
    }
    flush();
}

void Server::restore(std::vector<Job> jobs) { jobs_.restore(std::move(jobs), Clock::now()); }

void Server::handle_api(std::string_view method, std::string_view target, std::string_view body,
                        ApiDone done) {
    auto response = api_.handle(method, target, body, Clock::now());
    if (!response) return done(std::nullopt);
    // Replies wait like messages do: a 201 means the job is on disk.
    flush([done = std::move(done), r = std::move(*response)]() mutable { done(std::move(r)); });
}

void Server::flush(std::function<void()> reply) {
    Outgoing out{jobs_.take_effects(), std::move(reply), false};
    if (auto changes = jobs_.take_changes(); store_ != nullptr && !changes.empty()) {
        Batch batch;
        for (const Job* job : changes.jobs) {
            batch.jobs.push_back(JobRow{job->id, job->seq, std::string(to_string(job->state)),
                                        job->idempotency_key, encode_job(*job)});
        }
        const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
        for (Transition& t : changes.transitions) {
            batch.events.push_back(EventRow{std::move(t.job_id), now_ms,
                                            std::string(to_string(t.state)), t.attempt,
                                            std::move(t.node), std::move(t.reason)});
        }
        out.waiting_for_commit = true;
        store_->submit(std::move(batch), [this] {
            for (Outgoing& o : outbox_) {
                if (o.waiting_for_commit) {
                    o.waiting_for_commit = false;
                    break;
                }
            }
            deliver();
        });
    }
    outbox_.push_back(std::move(out));
    deliver();

    // One timer for every deadline (DESIGN.md §5.2, §8.3, §10). Re-arming cancels the
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

void Server::deliver() {
    while (!outbox_.empty() && !outbox_.front().waiting_for_commit) {
        Outgoing out = std::move(outbox_.front());
        outbox_.pop_front();
        for (const Effect& effect : out.effects) send(effect);
        if (out.reply) out.reply();
    }
}

void Server::send(const Effect& effect) {
    std::visit(
        [&](const auto& e) {
            // No live session: the node just went away. Dropping the message is
            // safe; its timer, node_lost or reconciliation takes care of the job.
            const auto it = workers_.find(e.node);
            const auto session = it == workers_.end() ? nullptr : it->second.lock();
            if (!session) return;
            if constexpr (std::is_same_v<std::decay_t<decltype(e)>, Disconnect>) {
                std::cerr << "node " << e.node << " missed its heartbeats; declared dead\n";
                session->close(kCloseDead, "missed heartbeats");
            } else {
                session->send(e.message);
            }
        },
        effect);
}

}  // namespace orch::net
