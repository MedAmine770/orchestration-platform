#include "orch/jobs.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace orch {

std::string_view to_string(JobState state) noexcept {
    switch (state) {
        case JobState::queued:
            return "queued";
        case JobState::unschedulable:
            return "unschedulable";
        case JobState::assigned:
            return "assigned";
        case JobState::starting:
            return "starting";
        case JobState::running:
            return "running";
        case JobState::succeeded:
            return "succeeded";
        case JobState::failed:
            return "failed";
        case JobState::lost:
            return "lost";
        case JobState::cancelled:
            return "cancelled";
    }
    return "unknown";
}

bool is_terminal(JobState state) noexcept {
    return state == JobState::succeeded || state == JobState::failed || state == JobState::lost ||
           state == JobState::cancelled;
}

Jobs::Jobs(JobsConfig config, Registry& registry, const PlacementPolicy& policy,
           CoordinatorMetrics& metrics)
    : config_(config), registry_(registry), policy_(policy), metrics_(metrics) {}

// ---- External events --------------------------------------------------------

Jobs::Submitted Jobs::submit(Submission submission, Clock::time_point now) {
    if (submission.idempotency_key) {
        if (auto it = by_idempotency_key_.find(*submission.idempotency_key);
            it != by_idempotency_key_.end()) {
            return {jobs_.at(it->second), false};
        }
    }
    auto [it, inserted] = jobs_.try_emplace(submission.id);
    if (!inserted) throw std::invalid_argument("duplicate job id " + submission.id);

    Job& job = it->second;
    job.id = std::move(submission.id);
    job.idempotency_key = std::move(submission.idempotency_key);
    job.spec = std::move(submission.spec);
    job.max_retries = submission.max_retries.value_or(config_.default_max_retries);
    job.submitted_at = now;
    job.seq = next_seq_++;
    if (job.idempotency_key) by_idempotency_key_.emplace(*job.idempotency_key, job.id);

    enter(job, waiting_state(job, now));  // enter, not set_state: there is no previous state
    schedule(now);
    return {job, true};
}

CancelResult Jobs::cancel(const JobId& id, Clock::time_point now) {
    auto it = jobs_.find(id);
    if (it == jobs_.end()) return CancelResult::not_found;
    Job& job = it->second;
    if (job.state == JobState::cancelled) return CancelResult::already_cancelled;
    if (is_terminal(job.state)) return CancelResult::already_terminal;

    finish(job, JobState::cancelled, "cancelled by request");
    schedule(now);  // its slot may be free now
    return CancelResult::cancelled;
}

void Jobs::node_joined(const NodeId& id, protocol::Capabilities capabilities, std::uint32_t slots,
                       Clock::time_point now, const std::vector<protocol::JobRef>& running) {
    const Node& node = registry_.join(id, std::move(capabilities), slots, now);

    // Reconcile (DESIGN.md §7.3). A reported attempt that is still this
    // node's current assignment keeps running. Any other is stale (e.g. its job
    // was requeued while the node was gone): cancel it so it stops.
    std::set<JobId> reported;
    for (const protocol::JobRef& ref : running) {
        const auto it = jobs_.find(ref.job_id);
        if (it == jobs_.end() || it->second.node != id || it->second.attempt != ref.attempt) {
            effects_.push_back(SendCancel{id, protocol::Cancel{ref}});
            continue;
        }
        Job& job = it->second;
        reported.insert(job.id);
        if (recovering_.erase(job.id) > 0) {
            // Reclaimed after a coordinator restart (§8.3): placed here again.
            registry_.place(id, job.id);
            if (job.state == JobState::assigned) {
                // The worker only reports jobs it accepted: its ack was lost.
                job.deadline = now + config_.start_timeout;
                set_state(job, JobState::starting);
            } else if (job.state == JobState::starting) {
                job.deadline = now + config_.start_timeout;
            }
        }
    }
    // A job placed here that the worker doesn't report was lost with its
    // previous process (e.g. the worker restarted): requeue it.
    const std::set<JobId> placed = node.jobs;  // copy: requeueing releases them
    for (const JobId& job_id : placed) {
        if (!reported.contains(job_id)) {
            infrastructure_failure(jobs_.at(job_id), "node_dead", false, now);
        }
    }
    reevaluate_waiting(now);
    schedule(now);
}

void Jobs::node_lost(const NodeId& id, Clock::time_point now) {
    const Node* node = registry_.find(id);
    if (node == nullptr || node->health == Health::dead) return;
    fail_node(id, now);
    reevaluate_waiting(now);
    schedule(now);
}

void Jobs::heartbeat(const NodeId& id, Clock::time_point now) {
    // A SUSPECT node that recovers can take placements again.
    if (registry_.heartbeat(id, now)) schedule(now);
}

void Jobs::fail_node(const NodeId& id, Clock::time_point now) {
    // Dead first, so the requeued jobs' eligibility check already excludes it.
    registry_.mark_dead(id);
    const std::set<JobId> placed = registry_.find(id)->jobs;  // copy: requeueing releases them
    for (const JobId& job_id : placed) {
        // No cancel: the connection is gone or about to be closed. If the node
        // is only partitioned and still runs the job, fencing rejects its late
        // messages, and reconciliation cancels the work when it reconnects.
        infrastructure_failure(jobs_.at(job_id), "node_dead", false, now);
    }
}

// DESIGN.md §10: SUSPECT after suspect_after without a heartbeat, DEAD after
// dead_after. Socket close alone can't detect a hung process or a half-open
// connection, which keep the socket open.
void Jobs::check_heartbeats(Clock::time_point now) {
    std::vector<NodeId> dead;
    for (const auto& [id, node] : registry_.nodes()) {
        if (node.health == Health::dead) continue;
        const auto silence = now - node.last_heartbeat;
        if (silence >= config_.dead_after) {
            dead.push_back(id);
        } else if (silence >= config_.suspect_after && node.health == Health::alive) {
            registry_.mark_suspect(id);
            metrics_.heartbeats_missed.inc();
        }
    }
    for (const NodeId& id : dead) {
        fail_node(id, now);
        effects_.push_back(Disconnect{id});
    }
    if (!dead.empty()) reevaluate_waiting(now);
}

// ---- Worker messages --------------------------------------------------------

// Returns the job if (job, attempt, node) is the current assignment. Anything
// else is stale (DESIGN.md §6): counted, and if work may still be running on
// the sender, answered with cancel so the stale container is stopped.
Job* Jobs::fence(const NodeId& from, const protocol::JobRef& ref, std::string_view type,
                 bool cancel_if_stale) {
    if (auto it = jobs_.find(ref.job_id); it != jobs_.end()) {
        Job& job = it->second;
        if (job.attempt == ref.attempt && job.node == from) return &job;
    }
    metrics_.stale_messages(type).inc();
    if (cancel_if_stale) effects_.push_back(SendCancel{from, protocol::Cancel{ref}});
    return nullptr;
}

MessageResult Jobs::on_ack(const NodeId& from, const protocol::Ack& msg, Clock::time_point now) {
    Job* job = fence(from, msg.job, "ack", true);
    if (job == nullptr) return MessageResult::stale;
    if (job->state != JobState::assigned) return MessageResult::ignored;

    if (Attempt& attempt = job->attempts.back(); !attempt.spans_restart) {
        attempt.dispatch_latency = now - attempt.assigned_at;
        metrics_.dispatch_latency.observe(*attempt.dispatch_latency);
    }
    job->deadline = now + config_.start_timeout;
    set_state(*job, JobState::starting);
    return MessageResult::accepted;
}

MessageResult Jobs::on_reject(const NodeId& from, const protocol::Reject& msg,
                              Clock::time_point now) {
    // A reject means the worker did nothing, so a stale one needs no cancel.
    Job* job = fence(from, msg.job, "reject", false);
    if (job == nullptr) return MessageResult::stale;
    if (job->state != JobState::assigned) return MessageResult::ignored;

    // The reject rule (DESIGN.md §5.3): no retry is spent, and this node is
    // never offered the job again. Bounded: each node can reject a job once.
    job->rejected_by.insert(from);
    unplace(*job, "rejected", false);
    job->reason = "rejected by " + from + ": " + msg.reason;
    enqueue(*job, now);
    schedule(now);
    return MessageResult::accepted;
}

MessageResult Jobs::on_started(const NodeId& from, const protocol::Started& msg,
                               Clock::time_point now) {
    Job* job = fence(from, msg.job, "started", true);
    if (job == nullptr) return MessageResult::stale;
    if (job->state != JobState::starting) return MessageResult::ignored;

    Attempt& attempt = job->attempts.back();
    attempt.image_pulled = msg.image_pulled;
    if (!attempt.spans_restart) {
        attempt.start_latency = now - job->queued_at;
        (msg.image_pulled ? metrics_.start_latency_pulled : metrics_.start_latency_cached)
            .observe(*attempt.start_latency);
    }
    job->deadline.reset();
    set_state(*job, JobState::running);
    return MessageResult::accepted;
}

MessageResult Jobs::on_finished(const NodeId& from, const protocol::Finished& msg,
                                Clock::time_point now) {
    // The work is over either way, so a stale finished needs no cancel.
    Job* job = fence(from, msg.job, "finished", false);
    if (job == nullptr) return MessageResult::stale;
    // STARTING too: a job can fail before it starts (e.g. image not found).
    if (job->state != JobState::starting && job->state != JobState::running) {
        return MessageResult::ignored;
    }

    job->exit_code = msg.exit_code;
    if (msg.exit_code == 0) {
        finish(*job, JobState::succeeded, "");
    } else if (msg.exit_code) {
        finish(*job, JobState::failed, "exit code " + std::to_string(*msg.exit_code));
    } else {
        finish(*job, JobState::failed, msg.error.value_or("failed"));
    }
    schedule(now);
    return MessageResult::accepted;
}

// ---- Timers -----------------------------------------------------------------

void Jobs::tick(Clock::time_point now) {
    check_heartbeats(now);
    check_recovery(now);
    const std::set<JobId> timed = timed_;  // copy: requeueing edits timed_
    for (const JobId& id : timed) {
        Job& job = jobs_.at(id);
        if (!job.deadline || *job.deadline > now) continue;
        // The worker may still be working on it, so it gets a cancel.
        infrastructure_failure(
            job, job.state == JobState::assigned ? "ack_timeout" : "start_timeout", true, now);
    }
    schedule(now);
}

std::optional<Clock::time_point> Jobs::next_deadline() const {
    std::optional<Clock::time_point> next;
    const auto consider = [&](Clock::time_point d) {
        if (!next || d < *next) next = d;
    };
    for (const JobId& id : timed_) consider(*jobs_.at(id).deadline);
    for (const auto& [id, until] : recovering_) consider(until);
    for (const auto& [id, node] : registry_.nodes()) {
        if (node.health == Health::alive) consider(node.last_heartbeat + config_.suspect_after);
        if (node.health == Health::suspect) consider(node.last_heartbeat + config_.dead_after);
    }
    return next;
}

const Job* Jobs::find(const JobId& id) const {
    auto it = jobs_.find(id);
    return it == jobs_.end() ? nullptr : &it->second;
}

// ---- Transitions ------------------------------------------------------------

// enter() and leave() are the only code that touches the state indexes and the
// orch_jobs gauge, so neither can drift from job.state.
void Jobs::enter(Job& job, JobState state) {
    job.state = state;
    const auto& id = job.id;
    dirty_.insert(id);
    transitions_.push_back(Transition{id, state, job.attempt, job.node, job.reason});
    switch (state) {
        case JobState::queued:
            queue_.emplace(job.seq, id);
            break;
        case JobState::unschedulable:
            unschedulable_.insert(id);
            break;
        case JobState::assigned:
        case JobState::starting:
            timed_.insert(id);
            break;
        default:
            break;
    }
    if (is_terminal(state)) {
        metrics_.jobs_finished(to_string(state)).inc();
    } else {
        metrics_.jobs(to_string(state)).inc();
    }
}

void Jobs::leave(Job& job) {
    // One terminal transition (DESIGN.md §6): a finished job never moves again.
    if (is_terminal(job.state)) throw std::logic_error("job " + job.id + " is already terminal");
    queue_.erase({job.seq, job.id});
    unschedulable_.erase(job.id);
    timed_.erase(job.id);
    metrics_.jobs(to_string(job.state)).dec();
}

void Jobs::set_state(Job& job, JobState state) {
    leave(job);
    enter(job, state);
}

// QUEUED if some eligible node exists (latencies are measured from now), else
// UNSCHEDULABLE.
JobState Jobs::waiting_state(Job& job, Clock::time_point now) {
    if (!has_eligible_node(job)) return JobState::unschedulable;
    job.queued_at = now;
    return JobState::queued;
}

void Jobs::enqueue(Job& job, Clock::time_point now) { set_state(job, waiting_state(job, now)); }

void Jobs::place(Job& job, const NodeId& node, Clock::time_point now) {
    ++job.attempt;
    job.node = node;
    job.reason.clear();  // the previous attempt's reason stays in the history
    job.deadline = now + config_.ack_timeout;
    Attempt& attempt = job.attempts.emplace_back();
    attempt.number = job.attempt;
    attempt.node = node;
    attempt.assigned_at = now;
    attempt.queue_wait = now - job.queued_at;
    metrics_.queue_wait.observe(*attempt.queue_wait);
    registry_.place(node, job.id);
    set_state(job, JobState::assigned);
    effects_.push_back(SendAssign{node, protocol::Assign{{job.id, job.attempt}, job.spec}});
}

// Closes the current attempt and frees its slot.
void Jobs::unplace(Job& job, std::string_view outcome, bool send_cancel) {
    if (!job.node) return;
    if (send_cancel)
        effects_.push_back(SendCancel{*job.node, protocol::Cancel{{job.id, job.attempt}}});
    registry_.release(*job.node, job.id);
    recovering_.erase(job.id);
    job.attempts.back().outcome = outcome;
    job.node.reset();
    job.deadline.reset();
}

// Node death, T_ack or T_start (DESIGN.md §5.3): spends one retry, then
// requeues, or gives up as LOST once retries_used exceeds max_retries.
void Jobs::infrastructure_failure(Job& job, std::string_view reason, bool send_cancel,
                                  Clock::time_point now) {
    unplace(job, reason, send_cancel);
    ++job.retries_used;
    if (job.retries_used > job.max_retries) {
        job.reason = "retries exhausted; last failure: " + std::string(reason);
        set_state(job, JobState::lost);
        return;
    }
    job.reason = std::string(reason);
    metrics_.requeues(reason).inc();
    enqueue(job, now);
}

void Jobs::finish(Job& job, JobState terminal, std::string reason) {
    // A job still on a live node is told to stop (only cancel reaches here
    // with work possibly running; a finished message means it already ended).
    const Node* node = job.node ? registry_.find(*job.node) : nullptr;
    const bool on_live_node = node != nullptr && node->health != Health::dead;
    unplace(job, to_string(terminal), terminal == JobState::cancelled && on_live_node);
    job.reason = std::move(reason);
    set_state(job, terminal);
}

// After nodes join or leave: QUEUED jobs with no eligible node left become
// UNSCHEDULABLE, and UNSCHEDULABLE jobs with one become QUEUED.
void Jobs::reevaluate_waiting(Clock::time_point now) {
    std::vector<JobId> waiting(unschedulable_.begin(), unschedulable_.end());
    for (const auto& entry : queue_) waiting.push_back(entry.second);
    for (const JobId& id : waiting) {
        Job& job = jobs_.at(id);
        const bool queued = job.state == JobState::queued;
        if (queued != has_eligible_node(job)) enqueue(job, now);
    }
}

// FIFO scan that skips jobs that can't be placed right now (DESIGN.md §4.3),
// so a GPU job waiting for the busy GPU node doesn't block CPU jobs behind it.
void Jobs::schedule(Clock::time_point now) {
    for (auto it = queue_.begin(); it != queue_.end() && any_free_slot();) {
        Job& job = jobs_.at(it->second);
        ++it;  // place() removes this job from queue_
        if (auto node = policy_.place(job.spec, job.rejected_by, registry_.nodes())) {
            place(job, *node, now);
        }
    }
}

bool Jobs::has_eligible_node(const Job& job) const {
    for (const auto& [id, node] : registry_.nodes()) {
        if (eligible(job.spec, job.rejected_by, node)) return true;
    }
    return false;
}

bool Jobs::any_free_slot() const {
    for (const auto& [id, node] : registry_.nodes()) {
        if (node.health == Health::alive && node.free_slots() > 0) return true;
    }
    return false;
}

// ---- Durability and restart recovery (DESIGN.md §8) --------------------------

void Jobs::restore(std::vector<Job> restored, Clock::time_point now) {
    for (Job& r : restored) {
        next_seq_ = std::max(next_seq_, r.seq + 1);
        const JobId id = r.id;
        auto [it, inserted] = jobs_.try_emplace(id, std::move(r));
        if (!inserted) throw std::invalid_argument("duplicate job id " + id);
        Job& job = it->second;
        if (job.idempotency_key) by_idempotency_key_.emplace(*job.idempotency_key, id);
        job.submitted_at = now;
        if (is_terminal(job.state)) continue;  // history only: no gauge, no counters

        const bool in_flight = job.state == JobState::assigned || job.state == JobState::starting ||
                               job.state == JobState::running;
        if (!in_flight || job.attempts.empty()) {
            enter(job, waiting_state(job, now));  // nodes reconnect after this
            continue;
        }
        job.attempts.back().spans_restart = true;
        recovering_[id] = now + config_.recovery_window;
        if (job.state != JobState::running) job.deadline = now + config_.recovery_window;
        enter(job, job.state);
    }
    // Nothing durable changed: QUEUED vs UNSCHEDULABLE is recomputed on every start.
    dirty_.clear();
    transitions_.clear();
}

Jobs::Changes Jobs::take_changes() {
    Changes changes;
    for (const JobId& id : dirty_) changes.jobs.push_back(&jobs_.at(id));
    changes.transitions = std::exchange(transitions_, {});
    dirty_.clear();
    return changes;
}

void Jobs::check_recovery(Clock::time_point now) {
    std::vector<JobId> expired;
    for (const auto& [id, until] : recovering_) {
        if (until <= now) expired.push_back(id);
    }
    for (const JobId& id : expired) {
        // No worker reclaimed it: handled like a node death. A worker that
        // shows up later with this attempt is told to cancel it.
        infrastructure_failure(jobs_.at(id), "node_dead", false, now);
    }
}

}  // namespace orch
