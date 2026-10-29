#include "orch/metrics.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using namespace std::chrono_literals;
using orch::metrics::Counter;
using orch::metrics::Gauge;
using orch::metrics::Histogram;
using orch::metrics::kLatencyBuckets;
using orch::metrics::Registry;

namespace {

constexpr std::array<double, 3> kSmallBounds{0.001, 0.01, 0.1};  // 1 ms, 10 ms, 100 ms

}  // namespace

// ---- Counter / Gauge --------------------------------------------------------

TEST(Counter, StartsAtZeroAndIncrements) {
    Counter c;
    EXPECT_EQ(c.value(), 0u);
    c.inc();
    c.inc(41);
    EXPECT_EQ(c.value(), 42u);
}

TEST(Gauge, GoesUpAndDown) {
    Gauge g;
    g.inc(5);
    g.dec(7);
    EXPECT_EQ(g.value(), -2);
    g.set(10);
    EXPECT_EQ(g.value(), 10);
}

// ---- Histogram --------------------------------------------------------------

TEST(Histogram, BucketUpperBoundsAreInclusive) {
    Histogram h(kSmallBounds);
    h.observe(1ms);  // exactly on the first bound -> le="0.001"
    const auto s = h.snapshot();
    EXPECT_EQ(s.cumulative, (std::vector<std::uint64_t>{1, 1, 1, 1}));
}

TEST(Histogram, CumulativeCountsAndPlusInf) {
    Histogram h(kSmallBounds);
    h.observe(500us);  // <= 1 ms
    h.observe(5ms);    // <= 10 ms
    h.observe(10ms);   // <= 10 ms (inclusive)
    h.observe(2s);     // above every bound -> +Inf only
    const auto s = h.snapshot();
    EXPECT_EQ(s.cumulative, (std::vector<std::uint64_t>{1, 3, 3, 4}));
    EXPECT_EQ(s.count, 4u);
    EXPECT_DOUBLE_EQ(s.sum_seconds, 2.0155);
}

TEST(Histogram, NegativeDurationCountsAsZero) {
    Histogram h(kSmallBounds);
    h.observe(-5ms);
    const auto s = h.snapshot();
    EXPECT_EQ(s.cumulative.front(), 1u);
    EXPECT_EQ(s.sum_seconds, 0.0);
}

TEST(Histogram, RejectsInvalidBounds) {
    const std::array<double, 2> unsorted{0.1, 0.01};
    const std::array<double, 2> duplicate{0.1, 0.1};
    const std::array<double, 1> zero{0.0};
    EXPECT_THROW(Histogram{unsorted}, std::invalid_argument);
    EXPECT_THROW(Histogram{duplicate}, std::invalid_argument);
    EXPECT_THROW(Histogram{zero}, std::invalid_argument);
}

// Run under TSan in CI. Integer-nanosecond sums make the expected total exact.
TEST(Histogram, ConcurrentObservationsAreAllCounted) {
    Histogram h(kLatencyBuckets);
    constexpr int kThreads = 8;
    constexpr int kPerThread = 10'000;
    // std::thread rather than std::jthread: Apple's libc++ still gates jthread
    // behind -fexperimental-library.
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&h] {
            for (int i = 0; i < kPerThread; ++i) h.observe(3ms);
        });
    }
    threads.emplace_back([&h] {  // scrapes racing with observe() must be safe
        for (int i = 0; i < 100; ++i) (void)h.snapshot();
    });
    for (auto& t : threads) t.join();

    const auto s = h.snapshot();
    EXPECT_EQ(s.count, std::uint64_t{kThreads} * kPerThread);
    EXPECT_DOUBLE_EQ(s.sum_seconds, 0.003 * kThreads * kPerThread);
}

TEST(LatencyBuckets, TwelveBoundariesBetweenOneAndHundredMs) {
    EXPECT_TRUE(std::is_sorted(kLatencyBuckets.begin(), kLatencyBuckets.end()));
    const auto in_range = std::count_if(kLatencyBuckets.begin(), kLatencyBuckets.end(),
                                        [](double b) { return b >= 0.001 && b <= 0.1; });
    EXPECT_EQ(in_range, 12);
    EXPECT_EQ(kLatencyBuckets.back(), 120.0);  // covers T_start
}

// ---- Registry: exposition format --------------------------------------------

TEST(Registry, RendersCounterAndGaugeFamilies) {
    Registry r;
    r.counter("orch_requeues_total", "Jobs requeued.", {{"reason", "node_dead"}}).inc(2);
    r.counter("orch_requeues_total", "Jobs requeued.", {{"reason", "ack_timeout"}});
    r.gauge("orch_nodes", "Nodes by health.", {{"health", "alive"}}).set(3);

    EXPECT_EQ(r.render(),
              "# HELP orch_requeues_total Jobs requeued.\n"
              "# TYPE orch_requeues_total counter\n"
              "orch_requeues_total{reason=\"node_dead\"} 2\n"
              "orch_requeues_total{reason=\"ack_timeout\"} 0\n"
              "# HELP orch_nodes Nodes by health.\n"
              "# TYPE orch_nodes gauge\n"
              "orch_nodes{health=\"alive\"} 3\n");
}

TEST(Registry, RendersHistogram) {
    Registry r;
    auto& h = r.histogram("orch_dispatch_latency_seconds", "Dispatch latency.", kSmallBounds);
    h.observe(5ms);
    h.observe(250ms);

    EXPECT_EQ(r.render(),
              "# HELP orch_dispatch_latency_seconds Dispatch latency.\n"
              "# TYPE orch_dispatch_latency_seconds histogram\n"
              "orch_dispatch_latency_seconds_bucket{le=\"0.001\"} 0\n"
              "orch_dispatch_latency_seconds_bucket{le=\"0.01\"} 1\n"
              "orch_dispatch_latency_seconds_bucket{le=\"0.1\"} 1\n"
              "orch_dispatch_latency_seconds_bucket{le=\"+Inf\"} 2\n"
              "orch_dispatch_latency_seconds_sum 0.255\n"
              "orch_dispatch_latency_seconds_count 2\n");
}

TEST(Registry, BoundsRenderInFixedNotation) {
    Registry r;
    r.histogram("h_seconds", "h", kLatencyBuckets);
    const std::string out = r.render();
    EXPECT_NE(out.find("h_seconds_bucket{le=\"0.0005\"} 0\n"), std::string::npos) << out;
    EXPECT_NE(out.find("h_seconds_bucket{le=\"120\"} 0\n"), std::string::npos) << out;
    EXPECT_EQ(out.find("e-"), std::string::npos) << "no exponent notation expected:\n" << out;
    EXPECT_EQ(out.find("e+"), std::string::npos) << "no exponent notation expected:\n" << out;
}

TEST(Registry, HistogramLabelsPrecedeLe) {
    Registry r;
    r.histogram("orch_start_latency_seconds", "Start latency.", std::array<double, 1>{1.0},
                {{"image", "cached"}});
    const std::string out = r.render();
    EXPECT_NE(out.find("orch_start_latency_seconds_bucket{image=\"cached\",le=\"1\"} 0\n"),
              std::string::npos)
        << out;
    EXPECT_NE(out.find("orch_start_latency_seconds_count{image=\"cached\"} 0\n"), std::string::npos)
        << out;
}

TEST(Registry, EscapesHelpAndLabelValues) {
    Registry r;
    r.gauge("g", "line1\nback\\slash", {{"v", "a\"b\\c\nd"}});
    EXPECT_EQ(r.render(),
              "# HELP g line1\\nback\\\\slash\n"
              "# TYPE g gauge\n"
              "g{v=\"a\\\"b\\\\c\\nd\"} 0\n");
}

// ---- Registry: misuse is rejected at startup --------------------------------

TEST(Registry, RejectsInvalidRegistrations) {
    Registry r;
    EXPECT_THROW(r.counter("requeues", "no _total suffix"), std::invalid_argument);
    EXPECT_THROW(r.gauge("1bad", "starts with a digit"), std::invalid_argument);
    EXPECT_THROW(r.gauge("g", "h", {{"bad-label", "x"}}), std::invalid_argument);
    EXPECT_THROW(r.gauge("g", "h", {{"__reserved", "x"}}), std::invalid_argument);
    EXPECT_THROW(r.histogram("h_seconds", "h", kSmallBounds, {{"le", "1"}}), std::invalid_argument);
}

TEST(Registry, RejectsConflictingOrDuplicateRegistrations) {
    Registry r;
    r.gauge("orch_jobs", "Jobs by state.", {{"state", "queued"}});
    EXPECT_THROW(r.gauge("orch_jobs", "Jobs by state.", {{"state", "queued"}}),
                 std::invalid_argument);  // same labels twice
    EXPECT_THROW(r.gauge("orch_jobs", "Different help.", {{"state", "running"}}),
                 std::invalid_argument);
    EXPECT_THROW(r.histogram("orch_jobs", "Jobs by state.", kSmallBounds),
                 std::invalid_argument);  // different type
}

TEST(Registry, ReferencesStayValidAfterMoreRegistrations) {
    Registry r;
    Counter& first = r.counter("a_total", "a");
    for (int i = 0; i < 100; ++i) {
        r.counter("b_total", "b", {{"i", std::to_string(i)}});
    }
    first.inc();
    EXPECT_NE(r.render().find("a_total 1\n"), std::string::npos);
}
