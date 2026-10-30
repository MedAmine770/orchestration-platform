#include "orch/coordinator_metrics.hpp"

#include <array>
#include <string>

namespace orch {

namespace {

constexpr std::array<std::string_view, 5> kJobStates{"queued", "unschedulable", "assigned",
                                                     "starting", "running"};
constexpr std::array<std::string_view, 3> kHealth{"alive", "suspect", "dead"};
constexpr std::array<std::string_view, 4> kOutcomes{"succeeded", "failed", "lost", "cancelled"};
constexpr std::array<std::string_view, 3> kRequeueReasons{"node_dead", "ack_timeout",
                                                          "start_timeout"};
constexpr std::array<std::string_view, 3> kStaleTypes{"ack", "started", "finished"};

constexpr std::string_view kStartHelp =
    "Seconds from a job being committed as QUEUED until the worker reports started.";

template <typename T, std::size_t N, typename Register>
std::unordered_map<std::string_view, T*> family(const std::array<std::string_view, N>& values,
                                                Register register_one) {
    std::unordered_map<std::string_view, T*> out;
    for (const std::string_view v : values) out.emplace(v, &register_one(std::string(v)));
    return out;
}

}  // namespace

CoordinatorMetrics::CoordinatorMetrics(metrics::Registry& r)
    : dispatch_latency(
          r.histogram("orch_dispatch_latency_seconds",
                      "Seconds from a job being committed as QUEUED until the worker's ack.",
                      metrics::kLatencyBuckets)),
      start_latency_cached(r.histogram("orch_start_latency_seconds", kStartHelp,
                                       metrics::kLatencyBuckets, {{"image", "cached"}})),
      start_latency_pulled(r.histogram("orch_start_latency_seconds", kStartHelp,
                                       metrics::kLatencyBuckets, {{"image", "pulled"}})),
      heartbeats_missed(
          r.counter("orch_heartbeats_missed_total", "Heartbeat deadlines missed by workers.")),
      auth_failures(r.counter("orch_auth_failures_total",
                              "Worker connections rejected for a bad cluster token.")),
      jobs_(family<metrics::Gauge>(kJobStates,
                                   [&](std::string s) -> metrics::Gauge& {
                                       return r.gauge("orch_jobs", "Jobs by state.",
                                                      {{"state", std::move(s)}});
                                   })),
      nodes_(family<metrics::Gauge>(kHealth,
                                    [&](std::string s) -> metrics::Gauge& {
                                        return r.gauge("orch_nodes", "Worker nodes by health.",
                                                       {{"health", std::move(s)}});
                                    })),
      jobs_finished_(family<metrics::Counter>(
          kOutcomes,
          [&](std::string s) -> metrics::Counter& {
              return r.counter("orch_jobs_finished_total", "Jobs reaching a terminal state.",
                               {{"outcome", std::move(s)}});
          })),
      requeues_(family<metrics::Counter>(kRequeueReasons,
                                         [&](std::string s) -> metrics::Counter& {
                                             return r.counter(
                                                 "orch_requeues_total",
                                                 "Jobs requeued after an infrastructure failure.",
                                                 {{"reason", std::move(s)}});
                                         })),
      stale_messages_(
          family<metrics::Counter>(kStaleTypes, [&](std::string s) -> metrics::Counter& {
              return r.counter("orch_stale_messages_total",
                               "Worker messages rejected by attempt fencing.",
                               {{"type", std::move(s)}});
          })) {}

metrics::Gauge& CoordinatorMetrics::jobs(std::string_view state) const { return *jobs_.at(state); }
metrics::Gauge& CoordinatorMetrics::nodes(std::string_view health) const {
    return *nodes_.at(health);
}
metrics::Counter& CoordinatorMetrics::jobs_finished(std::string_view outcome) const {
    return *jobs_finished_.at(outcome);
}
metrics::Counter& CoordinatorMetrics::requeues(std::string_view reason) const {
    return *requeues_.at(reason);
}
metrics::Counter& CoordinatorMetrics::stale_messages(std::string_view type) const {
    return *stale_messages_.at(type);
}

}  // namespace orch
