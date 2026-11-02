#include "orch/jobs.hpp"

#include <array>
#include <chrono>
#include <map>
#include <ostream>
#include <string>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include "orch/coordinator_metrics.hpp"
#include "orch/metrics.hpp"
#include "orch/registry.hpp"
#include "orch/scheduler.hpp"

using namespace std::chrono_literals;
using orch::CancelResult;
using orch::Clock;
using orch::Effect;
using orch::Job;
using orch::JobState;
using orch::MessageResult;
using orch::SendAssign;
using orch::SendCancel;
using orch::protocol::Accelerator;
using orch::protocol::JobRef;

namespace orch {
// Readable gtest failures ("queued", not "4-byte object <00-00 00-00>").
void PrintTo(JobState state, std::ostream* os) { *os << to_string(state); }
}  // namespace orch

namespace {

orch::protocol::Capabilities caps(Accelerator accelerator = Accelerator::cpu) {
    orch::protocol::Capabilities c;
    c.cpus = 4;
    c.mem_mb = 8192;
    c.accelerator = accelerator;
    if (accelerator != Accelerator::cpu) c.gpu_mem_mb = 24576;
    c.runtimes = {orch::protocol::Runtime::docker};
    return c;
}

orch::protocol::JobSpec spec(Accelerator accelerator = Accelerator::cpu) {
    orch::protocol::JobSpec s;
    s.image = "alpine:3.20";
    s.command = {"echo", "hi"};
    s.requirements.accelerator = accelerator;
    s.max_runtime_s = 60;
    return s;
}

class JobsTest : public ::testing::Test {
  protected:
    orch::metrics::Registry metrics_registry;
    orch::CoordinatorMetrics metrics{metrics_registry};
    orch::Registry registry{metrics};
    orch::LeastLoaded policy;
    orch::Jobs jobs{orch::JobsConfig{}, registry, policy, metrics};
    Clock::time_point t0{};  // tests advance time explicitly from here

    // Every test ends with the orch_jobs gauge agreeing with the job table.
    void TearDown() override {
        std::map<JobState, std::int64_t> counts;
        for (const auto& [id, job] : jobs.jobs()) ++counts[job.state];
        for (JobState s : {JobState::queued, JobState::unschedulable, JobState::assigned,
                           JobState::starting, JobState::running}) {
            EXPECT_EQ(metrics.jobs(orch::to_string(s)).value(), counts[s]) << orch::to_string(s);
        }
    }

    const Job& submit(const std::string& id, Accelerator accelerator = Accelerator::cpu,
                      Clock::time_point at = {}) {
        return jobs.submit({.id = id, .spec = spec(accelerator)}, at).job;
    }
    const Job& job(const std::string& id) { return *jobs.find(id); }

    std::vector<Effect> effects() { return jobs.take_effects(); }
    static SendAssign assign_to(const std::string& node, const std::string& id,
                                std::uint32_t attempt) {
        return {node, {{id, attempt}, spec()}};
    }
    static SendCancel cancel_to(const std::string& node, const std::string& id,
                                std::uint32_t attempt) {
        return {node, {{id, attempt}}};
    }

    MessageResult ack(const std::string& node, const std::string& id, std::uint32_t attempt,
                      Clock::time_point at) {
        return jobs.on_ack(node, {JobRef{id, attempt}}, at);
    }
    MessageResult started(const std::string& node, const std::string& id, std::uint32_t attempt,
                          Clock::time_point at, bool pulled = false) {
        return jobs.on_started(node, {JobRef{id, attempt}, pulled}, at);
    }
    MessageResult finished(const std::string& node, const std::string& id, std::uint32_t attempt,
                           std::int32_t exit_code, Clock::time_point at) {
        return jobs.on_finished(node, {JobRef{id, attempt}, exit_code, std::nullopt, 10}, at);
    }
    MessageResult reject(const std::string& node, const std::string& id, std::uint32_t attempt,
                         Clock::time_point at) {
        return jobs.on_reject(node, {JobRef{id, attempt}, "runtime unavailable"}, at);
    }

    std::uint64_t stale(std::string_view type) { return metrics.stale_messages(type).value(); }
    std::uint64_t requeues(std::string_view reason) { return metrics.requeues(reason).value(); }
    std::uint64_t finished_total(std::string_view outcome) {
        return metrics.jobs_finished(outcome).value();
    }
};

}  // namespace

// ---- Happy path, latencies, outcomes ----------------------------------------

TEST_F(JobsTest, HappyPathRecordsExactLatenciesAndFreesTheSlot) {
    jobs.node_joined("a", caps(), 1, t0);
    submit("j1", Accelerator::cpu, t0);
    EXPECT_EQ(job("j1").state, JobState::assigned);
    EXPECT_EQ(effects(), std::vector<Effect>{assign_to("a", "j1", 1)});
    EXPECT_EQ(jobs.next_deadline(), t0 + 2s);  // T_ack

    EXPECT_EQ(ack("a", "j1", 1, t0 + 3ms), MessageResult::accepted);
    EXPECT_EQ(job("j1").state, JobState::starting);
    EXPECT_EQ(jobs.next_deadline(), t0 + 3ms + 120s);  // T_start

    EXPECT_EQ(started("a", "j1", 1, t0 + 40ms, /*pulled=*/false), MessageResult::accepted);
    EXPECT_EQ(job("j1").state, JobState::running);
    EXPECT_EQ(jobs.next_deadline(), std::nullopt);  // the worker enforces max_runtime_s

    EXPECT_EQ(finished("a", "j1", 1, 0, t0 + 1s), MessageResult::accepted);
    EXPECT_EQ(job("j1").state, JobState::succeeded);
    EXPECT_EQ(job("j1").exit_code, 0);
    EXPECT_EQ(registry.find("a")->free_slots(), 1u);

    const orch::Attempt& a = job("j1").attempts.at(0);
    EXPECT_EQ(a.dispatch_latency, 3ms);
    EXPECT_EQ(a.start_latency, 40ms);
    EXPECT_EQ(a.image_pulled, false);
    EXPECT_EQ(a.outcome, "succeeded");
    EXPECT_EQ(metrics.dispatch_latency.snapshot().count, 1u);
    EXPECT_EQ(metrics.start_latency_cached.snapshot().count, 1u);
    EXPECT_EQ(metrics.start_latency_pulled.snapshot().count, 0u);
    EXPECT_EQ(finished_total("succeeded"), 1u);
}

TEST_F(JobsTest, NonZeroExitAndWorkerErrorsFailWithoutRetry) {
    jobs.node_joined("a", caps(), 2, t0);
    submit("exit3");
    submit("no-image");
    ack("a", "exit3", 1, t0);
    started("a", "exit3", 1, t0);
    ack("a", "no-image", 1, t0);

    finished("a", "exit3", 1, 3, t0);
    // An error can arrive before started, e.g. the image doesn't exist.
    jobs.on_finished("a", {JobRef{"no-image", 1}, std::nullopt, "image not found", 0}, t0);

    EXPECT_EQ(job("exit3").state, JobState::failed);
    EXPECT_EQ(job("exit3").reason, "exit code 3");
    EXPECT_EQ(job("no-image").state, JobState::failed);
    EXPECT_EQ(job("no-image").reason, "image not found");
    EXPECT_EQ(job("exit3").retries_used, 0u);
    EXPECT_EQ(finished_total("failed"), 2u);
}

TEST_F(JobsTest, IdempotencyKeyReplayReturnsTheOriginalJob) {
    auto first = jobs.submit({.id = "j1", .spec = spec(), .idempotency_key = "k"}, t0);
    auto replay = jobs.submit({.id = "j2", .spec = spec(), .idempotency_key = "k"}, t0);
    EXPECT_TRUE(first.created);
    EXPECT_FALSE(replay.created);
    EXPECT_EQ(replay.job.id, "j1");
    EXPECT_EQ(jobs.jobs().size(), 1u);
}

// ---- QUEUED vs UNSCHEDULABLE, FIFO with skipping ------------------------------

TEST_F(JobsTest, UnschedulableUntilAnEligibleNodeJoins) {
    submit("gpu", Accelerator::cuda);
    EXPECT_EQ(job("gpu").state, JobState::unschedulable);

    jobs.node_joined("cpu-1", caps(Accelerator::cpu), 1, t0);  // not eligible
    EXPECT_EQ(job("gpu").state, JobState::unschedulable);

    jobs.node_joined("cuda-1", caps(Accelerator::cuda), 1, t0 + 5s);
    EXPECT_EQ(job("gpu").state, JobState::assigned);
    EXPECT_EQ(job("gpu").node, "cuda-1");
    // Dispatch latency starts when the job became QUEUED, not at submit.
    ack("cuda-1", "gpu", 1, t0 + 5s + 2ms);
    EXPECT_EQ(job("gpu").attempts.at(0).dispatch_latency, 2ms);
}

TEST_F(JobsTest, QueuedWaitsForCapacityAndSkipsJobsThatCantBePlaced) {
    jobs.node_joined("cuda-1", caps(Accelerator::cuda), 1, t0);
    jobs.node_joined("cpu-1", caps(Accelerator::cpu), 1, t0);
    submit("cpu-a");                     // least loaded, lowest id: cpu-1
    submit("gpu", Accelerator::cuda);    // cuda-1
    submit("gpu-2", Accelerator::cuda);  // QUEUED: cuda-1 is busy
    submit("cpu-b");                     // QUEUED: both busy
    EXPECT_EQ(job("cpu-a").node, "cpu-1");
    EXPECT_EQ(job("gpu").node, "cuda-1");
    EXPECT_EQ(job("gpu-2").state, JobState::queued);
    EXPECT_EQ(job("cpu-b").state, JobState::queued);

    // cpu-1 frees up: gpu-2 is first in line but can't use it, so cpu-b
    // passes it instead of waiting behind it.
    ack("cpu-1", "cpu-a", 1, t0);
    started("cpu-1", "cpu-a", 1, t0);
    finished("cpu-1", "cpu-a", 1, 0, t0);
    EXPECT_EQ(job("gpu-2").state, JobState::queued);
    EXPECT_EQ(job("cpu-b").node, "cpu-1");
}

TEST_F(JobsTest, QueuedJobsBecomeUnschedulableWhenTheLastEligibleNodeDies) {
    jobs.node_joined("a", caps(), 1, t0);
    submit("j1");
    submit("j2");  // QUEUED behind j1
    jobs.node_lost("a", t0);
    EXPECT_EQ(job("j1").state, JobState::unschedulable);  // requeued, nowhere to go
    EXPECT_EQ(job("j2").state, JobState::unschedulable);
}

// ---- Reject rule ------------------------------------------------------------

TEST_F(JobsTest, RejectSpendsNoRetryAndTheNodeIsNeverOfferedTheJobAgain) {
    jobs.node_joined("a", caps(), 1, t0);
    jobs.node_joined("b", caps(), 1, t0);
    submit("j1");
    EXPECT_EQ(job("j1").node, "a");
    effects();

    EXPECT_EQ(reject("a", "j1", 1, t0), MessageResult::accepted);
    EXPECT_EQ(job("j1").node, "b");
    EXPECT_EQ(job("j1").attempt, 2u);  // attempts count placements, rejects included
    EXPECT_EQ(job("j1").retries_used, 0u);
    EXPECT_EQ(effects(), std::vector<Effect>{assign_to("b", "j1", 2)});  // no cancel to a

    reject("b", "j1", 2, t0);
    EXPECT_EQ(job("j1").state, JobState::unschedulable);
    EXPECT_EQ(job("j1").retries_used, 0u);

    // Reconnecting doesn't make a node eligible again.
    jobs.node_joined("a", caps(), 1, t0);
    EXPECT_EQ(job("j1").state, JobState::unschedulable);
    jobs.node_joined("c", caps(), 1, t0);
    EXPECT_EQ(job("j1").node, "c");
}

// ---- Infrastructure failures and the retry budget -----------------------------

TEST_F(JobsTest, AckTimeoutRequeuesSpendsARetryAndCancels) {
    jobs.node_joined("a", caps(), 1, t0);
    submit("j1");
    effects();

    jobs.tick(t0 + 2s - 1ns);
    EXPECT_EQ(job("j1").state, JobState::assigned);  // not yet

    jobs.tick(t0 + 2s);
    EXPECT_EQ(job("j1").retries_used, 1u);
    EXPECT_EQ(job("j1").attempts.at(0).outcome, "ack_timeout");
    EXPECT_EQ(requeues("ack_timeout"), 1u);
    // Cancel the lost attempt, then the same node (not a rejecter) gets attempt 2.
    EXPECT_EQ(effects(), (std::vector<Effect>{cancel_to("a", "j1", 1), assign_to("a", "j1", 2)}));
}

TEST_F(JobsTest, StartTimeoutRequeuesSpendsARetryAndCancels) {
    jobs.node_joined("a", caps(), 1, t0);
    submit("j1");
    ack("a", "j1", 1, t0 + 1ms);
    effects();

    jobs.tick(t0 + 1ms + 120s);
    EXPECT_EQ(job("j1").retries_used, 1u);
    EXPECT_EQ(job("j1").attempts.at(0).outcome, "start_timeout");
    EXPECT_EQ(requeues("start_timeout"), 1u);
    EXPECT_EQ(effects(), (std::vector<Effect>{cancel_to("a", "j1", 1), assign_to("a", "j1", 2)}));
}

TEST_F(JobsTest, NodeDeathRequeuesItsJobsElsewhereWithoutSendingToTheDeadNode) {
    jobs.node_joined("a", caps(), 1, t0);
    jobs.node_joined("b", caps(), 1, t0);
    submit("j1");
    submit("j2");
    ack("a", "j1", 1, t0);
    started("a", "j1", 1, t0);
    finished("b", "j2", 1, 0, t0);  // ignored: j2 isn't acked yet
    ack("b", "j2", 1, t0);
    started("b", "j2", 1, t0);
    finished("b", "j2", 1, 0, t0);  // b is free again
    effects();

    jobs.node_lost("a", t0 + 1s);
    EXPECT_EQ(job("j1").node, "b");
    EXPECT_EQ(job("j1").attempt, 2u);
    EXPECT_EQ(job("j1").retries_used, 1u);
    EXPECT_EQ(requeues("node_dead"), 1u);
    EXPECT_EQ(effects(), std::vector<Effect>{assign_to("b", "j1", 2)});
}

TEST_F(JobsTest, RetriesExhaustedMeansLost) {
    jobs.node_joined("a", caps(), 1, t0);
    submit("j1");  // max_retries = 2: three attempts, then LOST
    for (int i = 1; i <= 3; ++i) {
        ASSERT_EQ(job("j1").state, JobState::assigned) << "attempt " << i;
        jobs.tick(t0 + i * 2s);
    }
    EXPECT_EQ(job("j1").state, JobState::lost);
    EXPECT_EQ(job("j1").retries_used, 3u);
    EXPECT_EQ(job("j1").reason, "retries exhausted; last failure: ack_timeout");
    EXPECT_EQ(requeues("ack_timeout"), 2u);  // the third failure isn't a requeue
    EXPECT_EQ(finished_total("lost"), 1u);
    EXPECT_EQ(registry.find("a")->free_slots(), 1u);
}

// ---- Fencing ------------------------------------------------------------------

TEST_F(JobsTest, MessagesForAnOldAttemptAreStaleCountedAndCancelled) {
    jobs.node_joined("a", caps(), 1, t0);
    jobs.node_joined("b", caps(), 1, t0);
    submit("j1");
    ack("a", "j1", 1, t0);
    jobs.node_lost("a", t0);  // a was only partitioned, and keeps running attempt 1
    ack("b", "j1", 2, t0);
    effects();

    EXPECT_EQ(started("a", "j1", 1, t0), MessageResult::stale);
    EXPECT_EQ(effects(), std::vector<Effect>{cancel_to("a", "j1", 1)});
    EXPECT_EQ(finished("a", "j1", 1, 0, t0), MessageResult::stale);
    EXPECT_EQ(effects(), std::vector<Effect>{});  // the work already ended
    EXPECT_EQ(stale("started"), 1u);
    EXPECT_EQ(stale("finished"), 1u);

    // The stale result changed nothing; the current attempt still completes.
    EXPECT_EQ(job("j1").state, JobState::starting);
    started("b", "j1", 2, t0);
    EXPECT_EQ(finished("b", "j1", 2, 0, t0), MessageResult::accepted);
    EXPECT_EQ(job("j1").state, JobState::succeeded);
}

TEST_F(JobsTest, ALateMessageFromTheSameNodeForAnOlderAttemptIsStale) {
    // The node check alone can't catch this one: T_ack fires, the job goes back
    // to the same node as attempt 2, and then attempt 1's ack arrives.
    jobs.node_joined("a", caps(), 1, t0);
    submit("j1");
    jobs.tick(t0 + 2s);
    ASSERT_EQ(job("j1").node, "a");
    ASSERT_EQ(job("j1").attempt, 2u);
    effects();

    EXPECT_EQ(ack("a", "j1", 1, t0 + 2s + 1ms), MessageResult::stale);
    EXPECT_EQ(effects(), std::vector<Effect>{cancel_to("a", "j1", 1)});
    EXPECT_EQ(job("j1").state, JobState::assigned);  // still waiting for attempt 2's ack
    EXPECT_EQ(ack("a", "j1", 2, t0 + 2s + 2ms), MessageResult::accepted);
}

TEST_F(JobsTest, TheRightAttemptFromTheWrongNodeIsStale) {
    jobs.node_joined("a", caps(), 1, t0);
    jobs.node_joined("b", caps(), 1, t0);
    submit("j1");
    EXPECT_EQ(ack("b", "j1", 1, t0), MessageResult::stale);
    EXPECT_EQ(ack("a", "nope", 1, t0), MessageResult::stale);  // unknown job
    EXPECT_EQ(stale("ack"), 2u);
    EXPECT_EQ(job("j1").state, JobState::assigned);
}

TEST_F(JobsTest, DuplicateMessagesForTheCurrentAttemptAreIgnored) {
    jobs.node_joined("a", caps(), 1, t0);
    submit("j1");
    EXPECT_EQ(ack("a", "j1", 1, t0 + 1ms), MessageResult::accepted);
    EXPECT_EQ(ack("a", "j1", 1, t0 + 9ms), MessageResult::ignored);
    EXPECT_EQ(job("j1").attempts.at(0).dispatch_latency, 1ms);  // first ack wins
    EXPECT_EQ(metrics.dispatch_latency.snapshot().count, 1u);
}

TEST_F(JobsTest, ATerminalJobIgnoresLateResults) {
    jobs.node_joined("a", caps(), 1, t0);
    submit("j1");
    ack("a", "j1", 1, t0);
    started("a", "j1", 1, t0);
    jobs.cancel("j1", t0);
    EXPECT_EQ(finished("a", "j1", 1, 0, t0), MessageResult::stale);
    EXPECT_EQ(job("j1").state, JobState::cancelled);
    EXPECT_EQ(finished_total("succeeded"), 0u);
}

// ---- Cancel -------------------------------------------------------------------

TEST_F(JobsTest, CancelStopsRunningWorkFreesTheSlotAndIsIdempotent) {
    jobs.node_joined("a", caps(), 1, t0);
    submit("j1");
    submit("j2");  // QUEUED behind j1
    ack("a", "j1", 1, t0);
    started("a", "j1", 1, t0);
    effects();

    EXPECT_EQ(jobs.cancel("j1", t0), CancelResult::cancelled);
    EXPECT_EQ(effects(), (std::vector<Effect>{cancel_to("a", "j1", 1), assign_to("a", "j2", 1)}));
    EXPECT_EQ(jobs.cancel("j1", t0), CancelResult::already_cancelled);
    EXPECT_EQ(jobs.cancel("missing", t0), CancelResult::not_found);

    EXPECT_EQ(jobs.cancel("j2", t0), CancelResult::cancelled);  // from ASSIGNED
    EXPECT_EQ(finished_total("cancelled"), 2u);

    submit("j3");
    ack("a", "j3", 1, t0);
    started("a", "j3", 1, t0);
    finished("a", "j3", 1, 0, t0);
    EXPECT_EQ(jobs.cancel("j3", t0), CancelResult::already_terminal);
}

TEST_F(JobsTest, CancellingAQueuedJobSendsNothing) {
    submit("j1");  // UNSCHEDULABLE: no nodes
    EXPECT_EQ(jobs.cancel("j1", t0), CancelResult::cancelled);
    EXPECT_EQ(effects(), std::vector<Effect>{});
}
