// Prints a representative /metrics page so CI can validate it with
// `promtool check metrics`. Covers every metric type, labels, escaping and
// the real latency buckets. Replaced by scraping the live endpoint in M3.

#include <chrono>
#include <iostream>

#include "orch/metrics.hpp"

int main() {
    using namespace std::chrono_literals;
    namespace m = orch::metrics;

    m::Registry r;
    auto& dispatch =
        r.histogram("orch_dispatch_latency_seconds",
                    "Job committed as QUEUED until the worker's ack.", m::kLatencyBuckets);
    auto& start_cached =
        r.histogram("orch_start_latency_seconds", "Job committed until the worker reports started.",
                    m::kLatencyBuckets, {{"image", "cached"}});
    r.histogram("orch_start_latency_seconds", "Job committed until the worker reports started.",
                m::kLatencyBuckets, {{"image", "pulled"}});
    r.gauge("orch_jobs", "Jobs by state.", {{"state", "queued"}}).set(4);
    r.gauge("orch_jobs", "Jobs by state.", {{"state", "running"}}).set(2);
    r.counter("orch_requeues_total", "Jobs requeued after an infrastructure failure.",
              {{"reason", "node_dead"}})
        .inc();
    r.gauge("orch_sample_escaping", "Help with a \\ backslash\nand a newline.",
            {{"value", "quote \" backslash \\ newline \n"}});

    dispatch.observe(4ms);
    dispatch.observe(12ms);
    start_cached.observe(850ms);

    std::cout << r.render();
    return 0;
}
