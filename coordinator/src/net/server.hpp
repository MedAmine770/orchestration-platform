#pragma once

#include <array>
#include <chrono>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>

#include "orch/api.hpp"
#include "orch/coordinator_metrics.hpp"
#include "orch/jobs.hpp"
#include "orch/metrics.hpp"
#include "orch/protocol.hpp"
#include "orch/registry.hpp"
#include "orch/scheduler.hpp"

namespace orch::net {

struct ServerConfig {
    std::string bind_address = "0.0.0.0";
    unsigned short port = 8080;  // 0 = let the OS pick (tests)
    std::string cluster_token;
    std::chrono::milliseconds hello_timeout{5000};
    std::chrono::milliseconds heartbeat_interval{1000};
    JobsConfig jobs;
};

class WorkerSession;

// Accepts connections, owns the table of authenticated workers, and is the
// imperative shell around the pure core (Registry, Jobs, Api): it feeds them
// events with the current time, then delivers the messages they queued and
// re-arms the one timer that drives T_ack / T_start.
//
// Threading (DESIGN.md §9): the Server, every session, the core and the
// metrics they update live on the single io thread, so none of it needs a lock.
class Server {
  public:
    // Binds immediately; throws boost::system::system_error if the port is taken.
    Server(boost::asio::io_context& ioc, ServerConfig config, const metrics::Registry& registry,
           CoordinatorMetrics& metrics);

    void start();
    unsigned short port() const;  // the bound port, useful when configured as 0

    const ServerConfig& config() const { return config_; }
    const metrics::Registry& registry() const { return registry_; }
    CoordinatorMetrics& metrics() { return metrics_; }

    // Checks a worker's token without leaking the configured token through
    // timing: both sides are SHA-256 digests (the configured one computed once
    // at startup), compared byte-for-byte in constant time. Hashing time
    // depends only on the submitted token, which the caller already knows.
    bool token_matches(std::string_view submitted) const;

    // Called by WorkerSession. A second session for the same node_id replaces
    // the first, which is closed with code 4000 (DESIGN.md §7.3).
    void on_authenticated(const std::shared_ptr<WorkerSession>& session,
                          const protocol::Hello& hello);
    void on_closed(const WorkerSession& session);
    void on_heartbeat(const WorkerSession& session);
    // ack, reject, started or finished.
    void on_job_message(const WorkerSession& session, const protocol::Message& message);

    // REST requests under /v1/jobs and /v1/nodes; nullopt for other paths.
    std::optional<ApiResponse> handle_api(std::string_view method, std::string_view target,
                                          std::string_view body);

  private:
    void accept();
    bool is_current(const WorkerSession& session) const;
    // Sends the messages the core queued, then re-arms the deadline timer.
    void flush();

    boost::asio::io_context& ioc_;
    boost::asio::ip::tcp::acceptor acceptor_;
    ServerConfig config_;
    std::array<unsigned char, 32> token_digest_{};
    const metrics::Registry& registry_;
    CoordinatorMetrics& metrics_;

    Registry nodes_;
    LeastLoaded policy_;
    Jobs jobs_;
    std::mt19937_64 rng_;  // job ids only; not a security boundary
    Api api_;
    boost::asio::steady_timer deadline_timer_;

    // weak_ptr: the table observes sessions, it doesn't keep them alive.
    std::unordered_map<std::string, std::weak_ptr<WorkerSession>> workers_;
};

}  // namespace orch::net
