#pragma once

// Prometheus metrics: counters, gauges and latency histograms, rendered in the
// text exposition format (version 0.0.4). See DESIGN.md §12.
//
// Thread-safety: inc/set/observe and render() may run concurrently from any
// thread. Registration is not synchronised: register every metric at startup,
// before anything calls render().

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace orch::metrics {

inline constexpr std::string_view kContentType = "text/plain; version=0.0.4; charset=utf-8";

// Upper bounds in seconds for dispatch and start latency (DESIGN.md §12.1):
// 12 boundaries between 1 ms and 100 ms, and a tail up to a 120 s image pull.
inline constexpr std::array<double, 23> kLatencyBuckets{
    0.0005, 0.001, 0.002, 0.003, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.03, 0.05, 0.075,
    0.1,    0.15,  0.25,  0.5,   1,     2.5,    5,    10,    30,   60,   120};

using Labels = std::vector<std::pair<std::string, std::string>>;

// Monotonically increasing count. Relaxed atomics: a metric value never
// publishes other memory, so no ordering with surrounding code is needed.
class Counter {
  public:
    void inc(std::uint64_t n = 1) noexcept { value_.fetch_add(n, std::memory_order_relaxed); }
    std::uint64_t value() const noexcept { return value_.load(std::memory_order_relaxed); }

  private:
    std::atomic<std::uint64_t> value_{0};
};

// A value that goes up and down, e.g. jobs in a given state.
class Gauge {
  public:
    void set(std::int64_t v) noexcept { value_.store(v, std::memory_order_relaxed); }
    void inc(std::int64_t n = 1) noexcept { value_.fetch_add(n, std::memory_order_relaxed); }
    void dec(std::int64_t n = 1) noexcept { value_.fetch_sub(n, std::memory_order_relaxed); }
    std::int64_t value() const noexcept { return value_.load(std::memory_order_relaxed); }

  private:
    std::atomic<std::int64_t> value_{0};
};

// Fixed-bucket histogram of durations.
//
// Each observation increments exactly one bucket and adds to the sum; the
// cumulative counts and the total count are derived when a snapshot is taken,
// so `count` always equals the `+Inf` bucket.
//
// Consistency: the bucket counters and the sum are independent atomics, not
// one transaction. A scrape that races with observe() can therefore see an
// observation in the count but not yet in the sum, or the reverse; the two are
// at most a few in-flight observations apart and agree again on the next
// scrape. That is acceptable because Prometheus only uses these values for
// rates and approximate quantiles over many scrapes. The exact percentiles we
// publish come from raw per-job samples collected by the bench script
// (DESIGN.md §11), not from this histogram.
//
// The sum is kept as integer nanoseconds rather than in an atomic<double>
// (which C++20 does support via fetch_add). Integer addition is exact and
// order-independent: the same observations give the same sum no matter how
// threads interleave, whereas a floating-point sum rounds at every step and
// depends on addition order. A uint64_t of nanoseconds holds ~584 years of
// accumulated latency, so overflow is not a practical concern.
class Histogram {
  public:
    struct Snapshot {
        std::vector<std::uint64_t> cumulative;  // one per upper bound, then +Inf
        std::uint64_t count = 0;
        double sum_seconds = 0;
    };

    // Bounds in seconds; must be finite, positive and strictly increasing.
    // Throws std::invalid_argument otherwise.
    explicit Histogram(std::span<const double> upper_bounds_seconds);

    // Negative durations (impossible with a steady clock) are counted as zero.
    void observe(std::chrono::nanoseconds d) noexcept;

    Snapshot snapshot() const;
    std::span<const double> upper_bounds() const noexcept { return bounds_seconds_; }

  private:
    std::vector<double> bounds_seconds_;
    std::vector<std::int64_t> bounds_ns_;
    std::vector<std::atomic<std::uint64_t>> buckets_;  // bounds_ns_.size() + 1, last is +Inf
    std::atomic<std::uint64_t> sum_ns_{0};
};

// Owns all metrics and renders them. Metrics sharing a name form one family
// (one HELP/TYPE header) and differ only by label values. References returned
// by the registration functions stay valid for the registry's lifetime.
//
// Registration throws std::invalid_argument on: an invalid metric or label
// name, a counter not ending in `_total`, a histogram using the reserved
// label `le`, a name reused with a different type or help text, or the same
// name and labels registered twice.
class Registry {
  public:
    Counter& counter(std::string_view name, std::string_view help, Labels labels = {});
    Gauge& gauge(std::string_view name, std::string_view help, Labels labels = {});
    Histogram& histogram(std::string_view name, std::string_view help,
                         std::span<const double> upper_bounds_seconds, Labels labels = {});

    std::string render() const;

  private:
    enum class Type { counter, gauge, histogram };

    struct Child {
        Labels labels;
        std::variant<std::unique_ptr<Counter>, std::unique_ptr<Gauge>, std::unique_ptr<Histogram>>
            metric;
    };

    struct Family {
        std::string name;
        std::string help;
        Type type;
        std::vector<Child> children;
    };

    Family& family_for(std::string_view name, std::string_view help, Type type,
                       const Labels& labels);

    std::vector<Family> families_;  // in registration order, which is render order
};

}  // namespace orch::metrics
