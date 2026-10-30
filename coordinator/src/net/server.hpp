#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "orch/coordinator_metrics.hpp"
#include "orch/metrics.hpp"

namespace orch::net {

struct ServerConfig {
    std::string bind_address = "0.0.0.0";
    unsigned short port = 8080;  // 0 = let the OS pick (tests)
    std::string cluster_token;
    std::chrono::milliseconds hello_timeout{5000};
    std::chrono::milliseconds heartbeat_interval{1000};
};

class WorkerSession;

// Accepts connections and owns the table of authenticated workers.
//
// Threading (DESIGN.md §9): the Server, every session and the metrics they
// update live on the single io thread, so none of this state needs a lock.
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

    // Called by WorkerSession. A second session for the same node_id replaces
    // the first, which is closed with code 4000 (DESIGN.md §7.3).
    void on_authenticated(const std::shared_ptr<WorkerSession>& session);
    void on_closed(const WorkerSession& session);

  private:
    void accept();
    void update_node_gauge();

    boost::asio::io_context& ioc_;
    boost::asio::ip::tcp::acceptor acceptor_;
    ServerConfig config_;
    const metrics::Registry& registry_;
    CoordinatorMetrics& metrics_;
    // weak_ptr: the table observes sessions, it doesn't keep them alive.
    std::unordered_map<std::string, std::weak_ptr<WorkerSession>> workers_;
};

}  // namespace orch::net
