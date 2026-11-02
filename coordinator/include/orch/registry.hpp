#pragma once

// Worker nodes known to the coordinator (DESIGN.md §4.1): capabilities, slots,
// the jobs placed on each, and health. Pure state, touched only on the io
// thread; no I/O and no clock reads (time is a parameter).

#include <chrono>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <string_view>

#include "orch/coordinator_metrics.hpp"
#include "orch/protocol.hpp"

namespace orch {

using Clock = std::chrono::steady_clock;
using NodeId = std::string;
using JobId = std::string;

enum class Health { alive, suspect, dead };
std::string_view to_string(Health health) noexcept;

struct Node {
    NodeId id;
    protocol::Capabilities capabilities;
    std::uint32_t slots = 0;
    std::set<JobId> jobs;  // placed here and not yet finished: ASSIGNED, STARTING, RUNNING
    Health health = Health::alive;
    Clock::time_point last_heartbeat;

    std::uint32_t free_slots() const noexcept {
        return jobs.size() >= slots ? 0 : slots - static_cast<std::uint32_t>(jobs.size());
    }
};

class Registry {
  public:
    explicit Registry(CoordinatorMetrics& metrics) : metrics_(metrics) {}

    // On an authenticated hello. A known id (reconnect, or a session replacing a
    // half-open one) becomes alive again with the new capabilities and slots,
    // and keeps the jobs still placed on it.
    Node& join(const NodeId& id, protocol::Capabilities capabilities, std::uint32_t slots,
               Clock::time_point now);

    // The node stays listed (for GET /v1/nodes and the health gauge) until it
    // rejoins. Placed jobs are the caller's to requeue and release.
    void mark_dead(const NodeId& id);

    void heartbeat(const NodeId& id, Clock::time_point now);

    // Bookkeeping for jobs placed on a node; the node must exist.
    void place(const NodeId& node, const JobId& job);
    void release(const NodeId& node, const JobId& job);

    Node* find(const NodeId& id);
    const Node* find(const NodeId& id) const;
    // Ordered by id, which gives the scheduler a deterministic tie-break.
    const std::map<NodeId, Node>& nodes() const noexcept { return nodes_; }

  private:
    void set_health(Node& node, Health health);

    std::map<NodeId, Node> nodes_;
    CoordinatorMetrics& metrics_;
};

}  // namespace orch
