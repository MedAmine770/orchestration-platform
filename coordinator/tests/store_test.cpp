#include "orch/store.hpp"

#include <chrono>
#include <filesystem>
#include <future>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using orch::Batch;
using orch::JobRow;
using orch::Store;

namespace {

// A fresh database path per test, removed (with its WAL files) afterwards.
class StoreTest : public ::testing::Test {
  protected:
    std::filesystem::path path =
        std::filesystem::temp_directory_path() /
        ("orch-store-test-" + std::to_string(std::random_device{}()) + ".db");

    void TearDown() override {
        for (const char* suffix : {"", "-wal", "-shm"}) {
            std::filesystem::remove(path.string() + suffix);
        }
    }

    static Batch job(const std::string& id, std::uint64_t seq, const std::string& doc) {
        Batch b;
        b.jobs.push_back(JobRow{id, seq, "queued", std::nullopt, doc});
        return b;
    }

    // Runs completions on the writer thread itself: fine for tests, which
    // only need to know that, and in which order, they ran.
    static Store::Post inline_post() {
        return [](Store::Task task) { task(); };
    }
};

}  // namespace

TEST_F(StoreTest, CommittedJobsSurviveReopeningInSubmissionOrder) {
    {
        Store store(path.string());
        store.start(inline_post(), [](std::string e) { FAIL() << e; });
        std::promise<void> done;
        store.submit(job("b", 2, "doc-b"), [] {});
        store.submit(job("a", 1, "doc-a"), [] {});
        store.submit(job("a", 1, "doc-a-v2"), [&] { done.set_value(); });  // upsert
        done.get_future().wait();
    }
    Store reopened(path.string());
    EXPECT_EQ(reopened.load_jobs(), (std::vector<std::string>{"doc-a-v2", "doc-b"}));
}

TEST_F(StoreTest, BatchesQueuedTogetherShareOneTransaction) {
    Store store(path.string());
    // Queued before the writer starts, so they are all waiting at its first look.
    std::vector<int> order;
    std::promise<void> done;
    for (int i = 0; i < 100; ++i) {
        store.submit(job("j" + std::to_string(i), static_cast<std::uint64_t>(i), "{}"), [&, i] {
            order.push_back(i);
            if (i == 99) done.set_value();
        });
    }
    store.start(inline_post(), [](std::string e) { FAIL() << e; });
    done.get_future().wait();

    EXPECT_EQ(store.transactions(), 1u);  // one fsync for all 100
    ASSERT_EQ(order.size(), 100u);
    for (int i = 0; i < 100; ++i) EXPECT_EQ(order[static_cast<std::size_t>(i)], i);
}

TEST_F(StoreTest, ConcurrentSubmittersAllGetDurableCompletions) {
    constexpr int kThreads = 4;
    constexpr int kEach = 50;
    {
        Store store(path.string());
        std::mutex m;
        int completed = 0;
        std::promise<void> all_done;
        store.start(inline_post(), [](std::string e) { FAIL() << e; });

        std::vector<std::thread> submitters;
        for (int t = 0; t < kThreads; ++t) {
            submitters.emplace_back([&, t] {
                for (int i = 0; i < kEach; ++i) {
                    const auto seq = static_cast<std::uint64_t>(t * kEach + i);
                    store.submit(job("j" + std::to_string(seq), seq, "{}"), [&] {
                        const std::lock_guard lock(m);
                        if (++completed == kThreads * kEach) all_done.set_value();
                    });
                }
            });
        }
        for (auto& s : submitters) s.join();
        all_done.get_future().wait();
        EXPECT_LE(store.transactions(), static_cast<std::uint64_t>(kThreads * kEach));
    }
    EXPECT_EQ(Store(path.string()).load_jobs().size(), static_cast<std::size_t>(kThreads * kEach));
}

TEST_F(StoreTest, DestructorCommitsWhatIsStillQueued) {
    {
        Store store(path.string());
        store.start(inline_post(), [](std::string e) { FAIL() << e; });
        for (int i = 0; i < 20; ++i) {
            store.submit(job("j" + std::to_string(i), static_cast<std::uint64_t>(i), "{}"), [] {});
        }
    }  // no waiting: shutdown must drain the queue
    EXPECT_EQ(Store(path.string()).load_jobs().size(), 20u);
}

TEST_F(StoreTest, AFailedCommitReportsAnErrorAndStopsAcceptingWork) {
    Store store(path.string());
    std::promise<std::string> error;
    store.start(inline_post(), [&](std::string e) { error.set_value(std::move(e)); });

    Batch conflicting = job("a", 1, "{}");
    conflicting.jobs.push_back(JobRow{"b", 1, "queued", std::nullopt, "{}"});  // seq is UNIQUE
    bool committed = false;
    store.submit(std::move(conflicting), [&] { committed = true; });

    auto future = error.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_NE(future.get().find("UNIQUE"), std::string::npos);
    EXPECT_FALSE(committed);
    store.submit(job("c", 3, "{}"), [&] { committed = true; });  // ignored from now on
    EXPECT_EQ(store.transactions(), 0u);
}

TEST_F(StoreTest, OpeningAnUnwritableLocationThrows) {
    EXPECT_THROW(Store("/nonexistent-dir/orch.db"), std::runtime_error);
}
