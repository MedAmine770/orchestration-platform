#pragma once

// Placement (DESIGN.md §4.3): which node, if any, a job should go to now.
// Pure functions of the job and the registry's nodes.

#include <map>
#include <optional>
#include <set>

#include "orch/protocol.hpp"
#include "orch/registry.hpp"

namespace orch {

// The node has what the job needs: cpu jobs run anywhere, cuda/mps jobs only on
// a matching accelerator, and the node's GPU memory covers min_gpu_mem_mb.
bool capable(const protocol::Requirements& requirements,
             const protocol::Capabilities& capabilities) noexcept;

// Could ever run the job: connected (not dead), capable, and hasn't rejected
// it. Decides QUEUED (some eligible node exists) vs UNSCHEDULABLE.
bool eligible(const protocol::JobSpec& spec, const std::set<NodeId>& rejected_by,
              const Node& node) noexcept;

class PlacementPolicy {
  public:
    virtual ~PlacementPolicy() = default;
    // The chosen node, or nullopt if no node can take the job right now.
    virtual std::optional<NodeId> place(const protocol::JobSpec& spec,
                                        const std::set<NodeId>& rejected_by,
                                        const std::map<NodeId, Node>& nodes) const = 0;
};

// Feasible = eligible, alive (not suspect) and with a free slot. Picks the
// lowest running/slots; ties go to the lowest node id, so tests are stable.
class LeastLoaded final : public PlacementPolicy {
  public:
    std::optional<NodeId> place(const protocol::JobSpec& spec, const std::set<NodeId>& rejected_by,
                                const std::map<NodeId, Node>& nodes) const override;
};

}  // namespace orch
