#pragma once

// Durable job state (DESIGN.md §8): SQLite in WAL mode with synchronous=FULL,
// owned by one writer thread. The io thread submits batches and gets a
// completion back, in submission order, once the batch is on disk; batches
// that queue up while a commit is in progress share the next transaction
// (group commit), so one fsync covers them all.
//
// The header is SQLite-free: callers see plain rows.

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

struct sqlite3;

namespace orch {

struct JobRow {
    std::string id;
    std::uint64_t seq = 0;
    std::string state;
    std::optional<std::string> idempotency_key;
    std::string doc;  // the whole job, JSON (job_record.hpp)
};

// One state transition, for the append-only history in job_events.
struct EventRow {
    std::string job_id;
    std::int64_t at_unix_ms = 0;
    std::string state;
    std::uint32_t attempt = 0;
    std::optional<std::string> node;
    std::string reason;
};

struct Batch {
    std::vector<JobRow> jobs;  // upserted
    std::vector<EventRow> events;
    bool empty() const noexcept { return jobs.empty() && events.empty(); }
};

class Store {
  public:
    using Task = std::function<void()>;
    using Post = std::function<void(Task)>;  // runs a task on the io thread

    // Opens or creates the database and its schema. Throws std::runtime_error.
    explicit Store(const std::string& path);
    // Commits anything still queued, then stops the writer thread.
    ~Store();
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    // Every job's doc, in submission order. Call before start(): until then the
    // calling thread owns the connection.
    std::vector<std::string> load_jobs() const;

    // Starts the writer thread. Completions run through `post`, in submission
    // order. If a commit fails, `on_error` is posted instead and the store stops
    // accepting work: the coordinator must fail-stop, not carry on with state it
    // can no longer persist.
    void start(Post post, std::function<void(std::string)> on_error);

    // Thread-safe. `done` runs (via post) once the batch is durable.
    void submit(Batch batch, Task done);

    std::uint64_t transactions() const noexcept { return transactions_.load(); }

  private:
    struct Pending {
        Batch batch;
        Task done;
    };
    struct CloseDb {
        void operator()(sqlite3* db) const noexcept;
    };

    void run();
    void commit(const std::vector<Pending>& group);
    void exec(const char* sql) const;

    std::unique_ptr<sqlite3, CloseDb> db_;
    Post post_;
    std::function<void(std::string)> on_error_;

    std::mutex mutex_;
    std::condition_variable wake_;
    std::deque<Pending> queue_;  // guarded by mutex_
    bool stopping_ = false;      // guarded by mutex_
    bool failed_ = false;        // guarded by mutex_
    std::atomic<std::uint64_t> transactions_{0};
    std::thread thread_;  // declared last: joined before the members above go away
};

}  // namespace orch
