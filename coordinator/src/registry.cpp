#include "orch/registry.hpp"

#include <utility>

namespace orch {

std::string_view to_string(Health health) noexcept {
    switch (health) {
        case Health::alive:
            return "alive";
        case Health::suspect:
            return "suspect";
        case Health::dead:
            return "dead";
    }
    return "unknown";
}

Node& Registry::join(const NodeId& id, protocol::Capabilities capabilities, std::uint32_t slots,
                     Clock::time_point now) {
    auto [it, inserted] = nodes_.try_emplace(id);
    Node& node = it->second;
    if (inserted) {
        node.id = id;
        metrics_.nodes(to_string(Health::alive)).inc();
    } else {
        set_health(node, Health::alive);
    }
    node.capabilities = std::move(capabilities);
    node.slots = slots;
    node.last_heartbeat = now;
    return node;
}

void Registry::mark_dead(const NodeId& id) {
    if (Node* node = find(id)) set_health(*node, Health::dead);
}

bool Registry::heartbeat(const NodeId& id, Clock::time_point now) {
    Node* node = find(id);
    if (node == nullptr || node->health == Health::dead) return false;
    node->last_heartbeat = now;
    if (node->health != Health::suspect) return false;
    set_health(*node, Health::alive);
    return true;
}

void Registry::mark_suspect(const NodeId& id) {
    if (Node* node = find(id)) set_health(*node, Health::suspect);
}

void Registry::place(const NodeId& node, const JobId& job) { nodes_.at(node).jobs.insert(job); }

void Registry::release(const NodeId& node, const JobId& job) {
    // The node may be unknown: a job restored after a restart that its node
    // never reclaimed was never placed on it in this process.
    if (Node* n = find(node)) n->jobs.erase(job);
}

Node* Registry::find(const NodeId& id) {
    auto it = nodes_.find(id);
    return it == nodes_.end() ? nullptr : &it->second;
}

const Node* Registry::find(const NodeId& id) const {
    auto it = nodes_.find(id);
    return it == nodes_.end() ? nullptr : &it->second;
}

// The only place a node's health changes, so the gauge can't drift from it.
void Registry::set_health(Node& node, Health health) {
    if (node.health == health) return;
    metrics_.nodes(to_string(node.health)).dec();
    metrics_.nodes(to_string(health)).inc();
    node.health = health;
}

}  // namespace orch
