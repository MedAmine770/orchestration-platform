#include "orch/store.hpp"

#include <iterator>
#include <stdexcept>
#include <utility>

#include <sqlite3.h>

namespace orch {

namespace {

constexpr const char* kSchemaVersion = "1";

constexpr const char* kSchema = R"sql(
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    seq             INTEGER NOT NULL UNIQUE,
    state           TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    doc             TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS job_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     TEXT NOT NULL,
    at_unix_ms INTEGER NOT NULL,
    state      TEXT NOT NULL,
    attempt    INTEGER NOT NULL,
    node       TEXT,
    reason     TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS job_events_by_job ON job_events (job_id);
INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1');
)sql";

struct Finalize {
    void operator()(sqlite3_stmt* s) const noexcept { sqlite3_finalize(s); }
};
using Statement = std::unique_ptr<sqlite3_stmt, Finalize>;

[[noreturn]] void fail(sqlite3* db, const std::string& what) {
    throw std::runtime_error("store: " + what + ": " + sqlite3_errmsg(db));
}

Statement prepare(sqlite3* db, const char* sql) {
    sqlite3_stmt* raw = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &raw, nullptr) != SQLITE_OK) fail(db, "prepare");
    return Statement(raw);
}

void bind(sqlite3* db, sqlite3_stmt* s, int i, const std::string& v) {
    if (sqlite3_bind_text(s, i, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT) !=
        SQLITE_OK) {
        fail(db, "bind");
    }
}

void bind(sqlite3* db, sqlite3_stmt* s, int i, const std::optional<std::string>& v) {
    if (v) return bind(db, s, i, *v);
    if (sqlite3_bind_null(s, i) != SQLITE_OK) fail(db, "bind");
}

void bind(sqlite3* db, sqlite3_stmt* s, int i, sqlite3_int64 v) {
    if (sqlite3_bind_int64(s, i, v) != SQLITE_OK) fail(db, "bind");
}

void step_and_reset(sqlite3* db, sqlite3_stmt* s) {
    if (sqlite3_step(s) != SQLITE_DONE) fail(db, "step");
    sqlite3_reset(s);
    sqlite3_clear_bindings(s);
}

}  // namespace

void Store::CloseDb::operator()(sqlite3* db) const noexcept { sqlite3_close_v2(db); }

Store::Store(const std::string& path) {
    sqlite3* raw = nullptr;
    const int rc =
        sqlite3_open_v2(path.c_str(), &raw, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
    db_.reset(raw);  // owned even if opening failed: the handle must still be closed
    if (rc != SQLITE_OK) fail(raw, "open " + path);

    exec("PRAGMA journal_mode = WAL");
    exec("PRAGMA synchronous = FULL");  // a commit survives power loss, not just a crash
    exec(kSchema);

    const Statement version =
        prepare(db_.get(), "SELECT value FROM meta WHERE key = 'schema_version'");
    if (sqlite3_step(version.get()) != SQLITE_ROW ||
        std::string(reinterpret_cast<const char*>(sqlite3_column_text(version.get(), 0))) !=
            kSchemaVersion) {
        throw std::runtime_error("store: " + path + " has an unsupported schema version");
    }
}

Store::~Store() {
    {
        const std::lock_guard lock(mutex_);
        stopping_ = true;
    }
    wake_.notify_all();
    if (thread_.joinable()) thread_.join();
}

void Store::exec(const char* sql) const {
    char* error = nullptr;
    if (sqlite3_exec(db_.get(), sql, nullptr, nullptr, &error) != SQLITE_OK) {
        const std::string message = error != nullptr ? error : "unknown error";
        sqlite3_free(error);
        throw std::runtime_error(std::string("store: ") + message);
    }
}

std::vector<std::string> Store::load_jobs() const {
    const Statement select = prepare(db_.get(), "SELECT doc FROM jobs ORDER BY seq");
    std::vector<std::string> docs;
    int rc = 0;
    while ((rc = sqlite3_step(select.get())) == SQLITE_ROW) {
        const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(select.get(), 0));
        const auto size = static_cast<std::size_t>(sqlite3_column_bytes(select.get(), 0));
        docs.emplace_back(text, size);
    }
    if (rc != SQLITE_DONE) fail(db_.get(), "load");
    return docs;
}

void Store::start(Post post, std::function<void(std::string)> on_error) {
    post_ = std::move(post);
    on_error_ = std::move(on_error);
    thread_ = std::thread([this] { run(); });
}

void Store::submit(Batch batch, Task done) {
    {
        const std::lock_guard lock(mutex_);
        if (failed_) return;  // on_error has been posted; nothing more is persisted
        queue_.push_back({std::move(batch), std::move(done)});
    }
    wake_.notify_one();
}

void Store::run() {
    for (;;) {
        std::vector<Pending> group;
        {
            std::unique_lock lock(mutex_);
            wake_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
            if (queue_.empty()) return;  // stopping, and everything is committed
            // Everything that queued up while the last commit ran goes into
            // this one transaction: one fsync for the whole group.
            group.assign(std::make_move_iterator(queue_.begin()),
                         std::make_move_iterator(queue_.end()));
            queue_.clear();
        }
        try {
            commit(group);
        } catch (const std::exception& e) {
            {
                const std::lock_guard lock(mutex_);
                failed_ = true;
                queue_.clear();
            }
            post_([on_error = on_error_, message = std::string(e.what())] { on_error(message); });
            return;
        }
        ++transactions_;
        for (Pending& p : group) post_(std::move(p.done));
    }
}

void Store::commit(const std::vector<Pending>& group) {
    sqlite3* db = db_.get();
    exec("BEGIN IMMEDIATE");
    try {
        const Statement upsert = prepare(db, R"sql(
            INSERT INTO jobs (id, seq, state, idempotency_key, doc) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT (id) DO UPDATE SET state = excluded.state, doc = excluded.doc)sql");
        const Statement event = prepare(db, R"sql(
            INSERT INTO job_events (job_id, at_unix_ms, state, attempt, node, reason)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6))sql");
        for (const Pending& p : group) {
            for (const JobRow& row : p.batch.jobs) {
                bind(db, upsert.get(), 1, row.id);
                bind(db, upsert.get(), 2, static_cast<sqlite3_int64>(row.seq));
                bind(db, upsert.get(), 3, row.state);
                bind(db, upsert.get(), 4, row.idempotency_key);
                bind(db, upsert.get(), 5, row.doc);
                step_and_reset(db, upsert.get());
            }
            for (const EventRow& row : p.batch.events) {
                bind(db, event.get(), 1, row.job_id);
                bind(db, event.get(), 2, sqlite3_int64{row.at_unix_ms});
                bind(db, event.get(), 3, row.state);
                bind(db, event.get(), 4, sqlite3_int64{row.attempt});
                bind(db, event.get(), 5, row.node);
                bind(db, event.get(), 6, row.reason);
                step_and_reset(db, event.get());
            }
        }
        exec("COMMIT");
    } catch (...) {
        sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
        throw;
    }
}

}  // namespace orch
