#include "orch/jobs.hpp"

#include <array>
#include <chrono>
#include <map>
#include <optional>
#include <ostream>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include "orch/coordinator_metrics.hpp"
#include "orch/job_record.hpp"
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

// Assigned field by field: GCC warns on designated initializers that leave
// fields out (-Wmissing-field-initializers).
orch::Submission submission(std::string id, orch::protocol::JobSpec s,
                            std::optional<std::string> key = std::nullopt) {
    orch::Submission sub;
    sub.id = std::move(id);
    sub.spec = std::move(s);
    sub.idempotency_key = std::move(key);
    return sub;
}

class JobsTest : public ::testing::Test {
  protected:
    orch::metrics::Registry metrics_registry;
    orch::CoordinatorMetrics metrics{metrics_registry};
    orch::Registry registry{metrics};
    orch::LeastLoaded policy;
    // These tests are about job timers, not failure detection, so heartbeat
    // deadlines are pushed out of their way. HeartbeatTest uses the defaults.
    static orch::JobsConfig quiet_heartbeats() {
        orch::JobsConfig c;
        c.suspect_after = 1h;
        c.dead_after = 2h;
        return c;
    }
    orch::Jobs jobs{quiet_heartbeats(), registry, policy, metrics};
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
        return jobs.submit(submission(id, spec(accelerator)), at).job;
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
    // The worker enforces max_runtime_s; only the node's heartbeat deadline is left.
    EXPECT_EQ(jobs.next_deadline(), t0 + 1h);

    EXPECT_EQ(finished("a", "j1", 1, 0, t0 + 1s), MessageResult::accepted);
    EXPECT_EQ(job("j1").state, JobState::succeeded);
    EXPECT_EQ(job("j1").exit_code, 0);
    EXPECT_EQ(registry.find("a")->free_slots(), 1u);

    const orch::Attempt& a = job("j1").attempts.at(0);
    EXPECT_EQ(a.queue_wait, 0ns);  // placed the moment it was queued
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
    auto first = jobs.submit(submission("j1", spec(), "k"), t0);
    auto replay = jobs.submit(submission("j2", spec(), "k"), t0);
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
    finished("cpu-1", "cpu-a", 1, 0, t0 + 7ms);
    EXPECT_EQ(job("gpu-2").state, JobState::queued);
    EXPECT_EQ(job("cpu-b").node, "cpu-1");
    EXPECT_EQ(job("cpu-b").attempts.at(0).queue_wait, 7ms);  // waited for the slot
    EXPECT_EQ(metrics.queue_wait.snapshot().count, 3u);
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

// ---- Failure detection and reconciliation (DESIGN.md §10, §7.3) ---------------

namespace {

class HeartbeatTest : public ::testing::Test {
  protected:
    orch::metrics::Registry metrics_registry;
    orch::CoordinatorMetrics metrics{metrics_registry};
    orch::Registry registry{metrics};
    orch::LeastLoaded policy;
    orch::Jobs jobs{orch::JobsConfig{}, registry, policy, metrics};  // 3 s suspect, 5 s dead
    Clock::time_point t0{};

    orch::Health health(const std::string& node) { return registry.find(node)->health; }
    const Job& job(const std::string& id) { return *jobs.find(id); }
    void run_on(const std::string& node, const std::string& id, Clock::time_point at) {
        jobs.submit(submission(id, spec()), at);
        ASSERT_EQ(job(id).node, node);
        jobs.on_ack(node, {JobRef{id, 1}}, at);
        jobs.on_started(node, {JobRef{id, 1}, false}, at);
    }
};

}  // namespace

TEST_F(HeartbeatTest, SilentNodeIsSuspectThenDeadAndItsJobMovesWithADisconnect) {
    jobs.node_joined("a", caps(), 1, t0);
    run_on("a", "j1", t0);
    jobs.node_joined("b", caps(), 1, t0 + 1s);
    jobs.take_effects();
    EXPECT_EQ(jobs.next_deadline(), t0 + 3s);  // a's suspect deadline

    jobs.heartbeat("b", t0 + 2500ms);  // b stays healthy throughout
    jobs.tick(t0 + 3s - 1ns);
    EXPECT_EQ(health("a"), orch::Health::alive);
    jobs.tick(t0 + 3s);
    EXPECT_EQ(health("a"), orch::Health::suspect);
    EXPECT_EQ(metrics.heartbeats_missed.value(), 1u);
    EXPECT_EQ(job("j1").state, JobState::running);  // suspect alone moves nothing
    EXPECT_EQ(jobs.next_deadline(), t0 + 5s);       // a's dead deadline

    jobs.heartbeat("b", t0 + 4500ms);
    jobs.tick(t0 + 5s);
    EXPECT_EQ(health("a"), orch::Health::dead);
    EXPECT_EQ(job("j1").node, "b");
    EXPECT_EQ(job("j1").attempt, 2u);
    EXPECT_EQ(job("j1").attempts.at(0).outcome, "node_dead");
    // The socket may still be open (hung process): the coordinator closes it.
    EXPECT_EQ(jobs.take_effects(),
              (std::vector<Effect>{orch::Disconnect{"a"}, SendAssign{"b", {{"j1", 2}, spec()}}}));
}

TEST_F(HeartbeatTest, SuspectNodeGetsNoPlacementsUntilItsHeartbeatReturns) {
    jobs.node_joined("a", caps(), 1, t0);
    jobs.tick(t0 + 3s);
    ASSERT_EQ(health("a"), orch::Health::suspect);

    jobs.submit(submission("j1", spec()), t0 + 3s);
    EXPECT_EQ(job("j1").state, JobState::queued);  // eligible, but not placed

    jobs.heartbeat("a", t0 + 3500ms);
    EXPECT_EQ(health("a"), orch::Health::alive);
    EXPECT_EQ(job("j1").state, JobState::assigned);
    jobs.tick(t0 + 6s);  // the recovered heartbeat reset the clock
    EXPECT_EQ(health("a"), orch::Health::alive);
}

TEST_F(HeartbeatTest, ReconnectKeepsReportedWorkCancelsStaleWorkRequeuesMissingWork) {
    jobs.node_joined("a", caps(), 3, t0);
    run_on("a", "kept", t0);
    run_on("a", "missing", t0);
    jobs.take_effects();

    // A new session for "a" (e.g. after a half-open connection) reports
    // `kept` and an attempt the coordinator no longer considers current.
    jobs.node_joined("a", caps(), 3, t0 + 1s, {{"kept", 1}, {"old-job", 7}});
    EXPECT_EQ(job("kept").state, JobState::running);
    EXPECT_EQ(job("kept").attempt, 1u);
    EXPECT_EQ(job("missing").retries_used, 1u);  // lost with the previous process
    EXPECT_EQ(job("missing").attempt, 2u);       // requeued and placed again
    EXPECT_EQ(jobs.take_effects(),
              (std::vector<Effect>{SendCancel{"a", {{"old-job", 7}}},
                                   SendAssign{"a", {{"missing", 2}, spec()}}}));
}

TEST_F(HeartbeatTest, ANodeDeclaredDeadThatComesBackHasItsOldAttemptCancelled) {
    jobs.node_joined("a", caps(), 1, t0);
    run_on("a", "j1", t0);
    jobs.node_joined("b", caps(), 1, t0 + 4s);
    jobs.tick(t0 + 5s);  // a dead, j1 moves to b as attempt 2
    jobs.take_effects();

    // a was only frozen (SIGSTOP, GC pause...) and still runs attempt 1.
    jobs.node_joined("a", caps(), 1, t0 + 6s, {{"j1", 1}});
    EXPECT_EQ(health("a"), orch::Health::alive);
    EXPECT_EQ(jobs.take_effects(), (std::vector<Effect>{SendCancel{"a", {{"j1", 1}}}}));
    EXPECT_EQ(job("j1").node, "b");
}

// ---- Durability and restart recovery (DESIGN.md §8) ---------------------------

namespace {

// One coordinator's worth of core state. Recovery tests build a "before"
// world, then restore its jobs (through their durable form) into "after".
struct World {
    orch::metrics::Registry metrics_registry;
    orch::CoordinatorMetrics metrics{metrics_registry};
    orch::Registry registry{metrics};
    orch::LeastLoaded policy;
    orch::Jobs jobs{orch::JobsConfig{}, registry, policy, metrics};

    const Job& job(const std::string& id) { return *jobs.find(id); }
    void run(const std::string& node, const std::string& id, Clock::time_point at) {
        jobs.on_ack(node, {JobRef{id, 1}}, at);
        jobs.on_started(node, {JobRef{id, 1}, false}, at);
    }
};

// What a restarted coordinator sees: every job, encoded and decoded again.
void restart(World& before, World& after, Clock::time_point now) {
    std::vector<Job> restored;
    for (const auto& [id, job] : before.jobs.jobs()) {
        restored.push_back(orch::decode_job(orch::encode_job(job)));
    }
    after.jobs.restore(std::move(restored), now);
}

}  // namespace

TEST(JobRecord, RoundTripsEveryDurableField) {
    World w;
    w.jobs.node_joined("a", caps(), 1, Clock::time_point{});
    auto sub = submission("j1", spec(Accelerator::cuda), "key-1");
    sub.max_retries = 5;
    w.jobs.node_joined("gpu", caps(Accelerator::cuda), 1, Clock::time_point{});
    w.jobs.submit(std::move(sub), Clock::time_point{});
    w.jobs.on_reject("gpu", {JobRef{"j1", 1}, "no"}, Clock::time_point{});

    const Job& original = w.job("j1");
    const Job copy = orch::decode_job(orch::encode_job(original));
    EXPECT_EQ(copy.id, original.id);
    EXPECT_EQ(copy.seq, original.seq);
    EXPECT_EQ(copy.idempotency_key, "key-1");
    EXPECT_EQ(copy.spec, original.spec);
    EXPECT_EQ(copy.max_retries, 5u);
    EXPECT_EQ(copy.state, original.state);
    EXPECT_EQ(copy.reason, original.reason);
    EXPECT_EQ(copy.attempt, 1u);
    EXPECT_EQ(copy.rejected_by, std::set<orch::NodeId>{"gpu"});
    ASSERT_EQ(copy.attempts.size(), 1u);
    EXPECT_EQ(copy.attempts[0].outcome, "rejected");
    EXPECT_EQ(copy.attempts[0].queue_wait, original.attempts[0].queue_wait);
    EXPECT_THROW(orch::decode_job(R"({"v": 2})"), std::invalid_argument);
}

TEST(Recovery, RestoreRebuildsTheTableTheIndexesAndTheGauges) {
    const Clock::time_point t0{};
    World before;
    before.jobs.node_joined("a", caps(), 2, t0);
    before.jobs.submit(submission("done", spec(), "nightly"), t0);
    before.run("a", "done", t0);
    before.jobs.on_finished("a", {JobRef{"done", 1}, 0, std::nullopt, 1}, t0);
    before.jobs.submit(submission("running", spec()), t0);
    before.run("a", "running", t0);
    before.jobs.submit(submission("gpu", spec(Accelerator::cuda)), t0);  // unschedulable

    World after;
    restart(before, after, t0 + 1h);
    EXPECT_EQ(after.job("done").state, JobState::succeeded);
    EXPECT_EQ(after.job("running").state, JobState::running);  // waiting to be reclaimed
    EXPECT_EQ(after.job("gpu").state, JobState::unschedulable);
    EXPECT_EQ(after.metrics.jobs("running").value(), 1);
    EXPECT_EQ(after.metrics.jobs("unschedulable").value(), 1);
    EXPECT_EQ(after.metrics.jobs_finished("succeeded").value(), 0u);  // history, not news
    EXPECT_EQ(after.jobs.next_deadline(), t0 + 1h + 10s);             // the recovery window
    EXPECT_TRUE(after.jobs.take_changes().empty());                   // nothing new to persist

    // Idempotency and submission order survive.
    EXPECT_FALSE(after.jobs.submit(submission("again", spec(), "nightly"), t0 + 1h).created);
    const auto fresh = after.jobs.submit(submission("fresh", spec()), t0 + 1h);
    EXPECT_GT(fresh.job.seq, after.job("gpu").seq);
}

TEST(Recovery, AReclaimedJobKeepsItsAttemptAndRecordsNoLatencyAcrossTheOutage) {
    const Clock::time_point t0{};
    World before;
    before.jobs.node_joined("a", caps(), 2, t0);
    before.jobs.submit(submission("running", spec()), t0);
    before.run("a", "running", t0);
    before.jobs.submit(submission("acked-lost", spec()), t0);  // ASSIGNED: its ack was lost

    World after;
    const Clock::time_point t1 = t0 + 1h;
    restart(before, after, t1);
    after.jobs.node_joined("a", caps(), 2, t1 + 1s, {{"running", 1}, {"acked-lost", 1}});
    EXPECT_EQ(after.jobs.take_effects(), std::vector<Effect>{});  // nothing cancelled or moved
    EXPECT_EQ(after.job("running").state, JobState::running);
    EXPECT_EQ(after.job("running").attempt, 1u);
    EXPECT_EQ(after.job("running").retries_used, 0u);
    // Both are placed on a again: its 2 slots are taken, so nothing else lands there.
    EXPECT_EQ(after.registry.find("a")->free_slots(), 0u);
    // A worker only reports jobs it accepted, so a reclaimed ASSIGNED job is acked.
    EXPECT_EQ(after.job("acked-lost").state, JobState::starting);

    after.jobs.on_started("a", {JobRef{"acked-lost", 1}, false}, t1 + 2s);
    after.jobs.on_finished("a", {JobRef{"running", 1}, 0, std::nullopt, 1}, t1 + 2s);
    EXPECT_EQ(after.job("running").state, JobState::succeeded);
    EXPECT_EQ(after.job("acked-lost").attempts.at(0).start_latency, std::nullopt);
    EXPECT_EQ(after.metrics.start_latency_cached.snapshot().count, 0u);

    after.jobs.heartbeat("a", t1 + 1h - 1s);  // a stays healthy
    after.jobs.tick(t1 + 1h);                 // the window has long closed: nothing is requeued
    EXPECT_EQ(after.job("acked-lost").state, JobState::running);
}

TEST(Recovery, AnUnclaimedJobIsRequeuedWhenTheWindowCloses) {
    const Clock::time_point t0{};
    World before;
    before.jobs.node_joined("a", caps(), 1, t0);
    before.jobs.submit(submission("j1", spec()), t0);
    before.run("a", "j1", t0);

    World after;
    const Clock::time_point t1 = t0 + 1h;
    restart(before, after, t1);
    after.jobs.node_joined("b", caps(), 1, t1);  // a never comes back
    after.jobs.heartbeat("b", t1 + 9s);
    after.jobs.tick(t1 + 10s - 1ns);
    EXPECT_EQ(after.job("j1").state, JobState::running);

    after.jobs.tick(t1 + 10s);
    EXPECT_EQ(after.job("j1").node, "b");
    EXPECT_EQ(after.job("j1").attempt, 2u);
    EXPECT_EQ(after.job("j1").retries_used, 1u);
    EXPECT_EQ(after.job("j1").attempts.at(0).outcome, "node_dead");

    // a turns up late, still running attempt 1: told to cancel it.
    after.jobs.take_effects();
    after.jobs.node_joined("a", caps(), 1, t1 + 11s, {{"j1", 1}});
    EXPECT_EQ(after.jobs.take_effects(), (std::vector<Effect>{SendCancel{"a", {{"j1", 1}}}}));
}

TEST(Recovery, AResultReplayedAfterReconnectCompletesARecoveringJob) {
    const Clock::time_point t0{};
    World before;
    before.jobs.node_joined("a", caps(), 1, t0);
    before.jobs.submit(submission("j1", spec()), t0);
    before.run("a", "j1", t0);

    // j1 finished while the coordinator was down. The worker reconnects (no
    // longer running it) and resends the finished message it couldn't deliver.
    World after;
    const Clock::time_point t1 = t0 + 1h;
    restart(before, after, t1);
    after.jobs.node_joined("a", caps(), 1, t1, {});
    EXPECT_EQ(after.jobs.on_finished("a", {JobRef{"j1", 1}, 0, std::nullopt, 1}, t1),
              MessageResult::accepted);
    EXPECT_EQ(after.job("j1").state, JobState::succeeded);
    after.jobs.heartbeat("a", t1 + 9s);
    after.jobs.tick(t1 + 10s);  // no longer recovering: not requeued
    EXPECT_EQ(after.job("j1").retries_used, 0u);
}

TEST(Recovery, TakeChangesReportsChangedJobsAndTheirTransitions) {
    World w;
    w.jobs.submit(submission("j1", spec()), Clock::time_point{});
    w.jobs.node_joined("a", caps(), 1, Clock::time_point{});
    const auto changes = w.jobs.take_changes();
    ASSERT_EQ(changes.jobs.size(), 1u);
    EXPECT_EQ(changes.jobs[0]->id, "j1");
    ASSERT_EQ(changes.transitions.size(), 3u);
    EXPECT_EQ(changes.transitions[0].state, JobState::unschedulable);
    EXPECT_EQ(changes.transitions[1].state, JobState::queued);
    EXPECT_EQ(changes.transitions[2].state, JobState::assigned);
    EXPECT_EQ(changes.transitions[2].attempt, 1u);
    EXPECT_EQ(changes.transitions[2].node, "a");
    EXPECT_TRUE(w.jobs.take_changes().empty());
}
