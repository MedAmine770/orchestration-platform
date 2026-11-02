#include "orch/scheduler.hpp"

#include <chrono>
#include <optional>
#include <set>
#include <string>

#include <gtest/gtest.h>

#include "orch/coordinator_metrics.hpp"
#include "orch/metrics.hpp"
#include "orch/registry.hpp"

using orch::Clock;
using orch::Health;
using orch::LeastLoaded;
using orch::NodeId;
using orch::protocol::Accelerator;
using orch::protocol::Capabilities;
using orch::protocol::JobSpec;

namespace {

Capabilities caps(Accelerator accelerator = Accelerator::cpu, std::uint64_t gpu_mem_mb = 0) {
    Capabilities c;
    c.cpus = 4;
    c.mem_mb = 8192;
    c.accelerator = accelerator;
    if (gpu_mem_mb > 0) c.gpu_mem_mb = gpu_mem_mb;
    c.runtimes = {orch::protocol::Runtime::docker};
    return c;
}

JobSpec job(Accelerator accelerator = Accelerator::cpu, std::uint64_t min_gpu_mem_mb = 0) {
    JobSpec s;
    s.image = "alpine";
    s.command = {"true"};
    s.requirements = {accelerator, min_gpu_mem_mb};
    s.max_runtime_s = 60;
    return s;
}

class SchedulerTest : public ::testing::Test {
  protected:
    orch::metrics::Registry metrics_registry;
    orch::CoordinatorMetrics metrics{metrics_registry};
    orch::Registry nodes{metrics};
    LeastLoaded policy;
    Clock::time_point now{};

    std::optional<NodeId> place(const JobSpec& spec, const std::set<NodeId>& rejected_by = {}) {
        return policy.place(spec, rejected_by, nodes.nodes());
    }
};

}  // namespace

TEST_F(SchedulerTest, NoNodesMeansNoPlacement) { EXPECT_EQ(place(job()), std::nullopt); }

TEST_F(SchedulerTest, CpuJobsRunAnywhereGpuJobsOnlyOnMatchingAccelerator) {
    nodes.join("mps-1", caps(Accelerator::mps), 1, now);
    EXPECT_EQ(place(job(Accelerator::cpu)), "mps-1");
    EXPECT_EQ(place(job(Accelerator::cuda)), std::nullopt);

    nodes.join("cuda-1", caps(Accelerator::cuda, 24576), 1, now);
    EXPECT_EQ(place(job(Accelerator::cuda)), "cuda-1");
}

TEST_F(SchedulerTest, GpuMemoryMustCoverTheRequirement) {
    nodes.join("cuda-small", caps(Accelerator::cuda, 8192), 1, now);
    EXPECT_EQ(place(job(Accelerator::cuda, 16384)), std::nullopt);
    nodes.join("cuda-big", caps(Accelerator::cuda, 24576), 1, now);
    EXPECT_EQ(place(job(Accelerator::cuda, 16384)), "cuda-big");
}

TEST_F(SchedulerTest, PicksLowestRunningOverSlotsThenLowestId) {
    nodes.join("a", caps(), 2, now);
    nodes.join("b", caps(), 4, now);
    EXPECT_EQ(place(job()), "a");  // both empty: tie goes to the lowest id

    nodes.place("a", "j1");        // a: 1/2
    EXPECT_EQ(place(job()), "b");  // b: 0/4

    nodes.place("b", "j2");  // b: 1/4 < a: 1/2
    EXPECT_EQ(place(job()), "b");

    nodes.place("b", "j3");  // b: 2/4 == a: 1/2, tie goes to a
    EXPECT_EQ(place(job()), "a");
}

TEST_F(SchedulerTest, FullNodesAreSkipped) {
    nodes.join("a", caps(), 1, now);
    nodes.place("a", "j1");
    EXPECT_EQ(place(job()), std::nullopt);
    nodes.release("a", "j1");
    EXPECT_EQ(place(job()), "a");
}

TEST_F(SchedulerTest, RejectingNodesAreNeverChosen) {
    nodes.join("a", caps(), 1, now);
    nodes.join("b", caps(), 1, now);
    EXPECT_EQ(place(job(), {"a"}), "b");
    EXPECT_EQ(place(job(), {"a", "b"}), std::nullopt);
}

TEST_F(SchedulerTest, DeadNodesAreIneligibleUntilTheyRejoin) {
    nodes.join("a", caps(), 1, now);
    nodes.mark_dead("a");
    EXPECT_EQ(place(job()), std::nullopt);
    EXPECT_FALSE(orch::eligible(job(), {}, *nodes.find("a")));

    nodes.join("a", caps(), 1, now);
    EXPECT_EQ(place(job()), "a");
}

TEST_F(SchedulerTest, SuspectNodesAreEligibleButGetNoNewPlacements) {
    // Suspect = missed heartbeats but not yet declared dead (DESIGN.md §10).
    // Health changes come from failure detection (M6); set it directly here.
    nodes.join("a", caps(), 1, now);
    nodes.find("a")->health = Health::suspect;
    EXPECT_TRUE(orch::eligible(job(), {}, *nodes.find("a")));
    EXPECT_EQ(place(job()), std::nullopt);
}

TEST_F(SchedulerTest, HealthGaugeFollowsJoinDeathAndRejoin) {
    auto gauge = [&](Health h) { return metrics.nodes(orch::to_string(h)).value(); };
    nodes.join("a", caps(), 1, now);
    nodes.join("b", caps(), 1, now);
    EXPECT_EQ(gauge(Health::alive), 2);

    nodes.mark_dead("a");
    EXPECT_EQ(gauge(Health::alive), 1);
    EXPECT_EQ(gauge(Health::dead), 1);

    nodes.join("a", caps(), 1, now);  // rejoin, not a third node
    EXPECT_EQ(gauge(Health::alive), 2);
    EXPECT_EQ(gauge(Health::dead), 0);
}
