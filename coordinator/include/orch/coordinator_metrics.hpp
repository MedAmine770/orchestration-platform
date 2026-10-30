#pragma once

// The coordinator's metric families (DESIGN.md §12.1), registered once at
// startup with their fixed label values, so every series exists (at zero) from
// the first scrape and dashboards never show gaps for "no data yet".

#include <string_view>
#include <unordered_map>

#include "orch/metrics.hpp"

namespace orch {

class CoordinatorMetrics {
  public:
    explicit CoordinatorMetrics(metrics::Registry& registry);

    metrics::Histogram& dispatch_latency;
    metrics::Histogram& start_latency_cached;
    metrics::Histogram& start_latency_pulled;
    metrics::Counter& heartbeats_missed;
    metrics::Counter& auth_failures;

    // Labelled families. Unknown label values throw std::out_of_range: that is
    // a programming error, and failing loudly beats inventing a new series.
    metrics::Gauge& jobs(
        std::string_view state) const;  // queued, unschedulable, assigned, starting, running
    metrics::Gauge& nodes(std::string_view health) const;  // alive, suspect, dead
    metrics::Counter& jobs_finished(
        std::string_view outcome) const;  // succeeded, failed, lost, cancelled
    metrics::Counter& requeues(
        std::string_view reason) const;  // node_dead, ack_timeout, start_timeout
    metrics::Counter& stale_messages(std::string_view type) const;  // ack, started, finished

  private:
    template <typename T>
    using ByLabel = std::unordered_map<std::string_view, T*>;

    ByLabel<metrics::Gauge> jobs_;
    ByLabel<metrics::Gauge> nodes_;
    ByLabel<metrics::Counter> jobs_finished_;
    ByLabel<metrics::Counter> requeues_;
    ByLabel<metrics::Counter> stale_messages_;
};

}  // namespace orch
