#include "orch/scheduler.hpp"

#include <cstdint>

namespace orch {

bool capable(const protocol::Requirements& requirements,
             const protocol::Capabilities& capabilities) noexcept {
    if (requirements.accelerator != protocol::Accelerator::cpu &&
        requirements.accelerator != capabilities.accelerator) {
        return false;
    }
    return capabilities.gpu_mem_mb.value_or(0) >= requirements.min_gpu_mem_mb;
}

bool eligible(const protocol::JobSpec& spec, const std::set<NodeId>& rejected_by,
              const Node& node) noexcept {
    return node.health != Health::dead && capable(spec.requirements, node.capabilities) &&
           !rejected_by.contains(node.id);
}

std::optional<NodeId> LeastLoaded::place(const protocol::JobSpec& spec,
                                         const std::set<NodeId>& rejected_by,
                                         const std::map<NodeId, Node>& nodes) const {
    const Node* best = nullptr;
    for (const auto& [id, node] : nodes) {
        if (node.health != Health::alive || node.free_slots() == 0 ||
            !eligible(spec, rejected_by, node)) {
            continue;
        }
        // Compare running/slots without floating point: a/b < c/d  <=>  a*d < c*b.
        // Strictly less, so the first (lowest) id wins ties.
        if (best == nullptr || std::uint64_t{node.jobs.size()} * best->slots <
                                   std::uint64_t{best->jobs.size()} * node.slots) {
            best = &node;
        }
    }
    if (best == nullptr) return std::nullopt;
    return best->id;
}

}  // namespace orch
