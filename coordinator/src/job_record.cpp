#include "orch/job_record.hpp"

#include <chrono>
#include <cstdint>
#include <optional>

#include <nlohmann/json.hpp>

namespace orch {

namespace {

using json = nlohmann::json;

constexpr int kVersion = 1;

template <typename T>
json optional_json(const std::optional<T>& v) {
    return v ? json(*v) : json(nullptr);
}

template <typename T>
std::optional<T> optional_from(const json& v) {
    if (v.is_null()) return std::nullopt;
    return v.get<T>();
}

json duration_json(const std::optional<Clock::duration>& d) {
    if (!d) return nullptr;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(*d).count();
}

std::optional<Clock::duration> duration_from(const json& v) {
    if (v.is_null()) return std::nullopt;
    return std::chrono::duration_cast<Clock::duration>(
        std::chrono::nanoseconds(v.get<std::int64_t>()));
}

JobState state_from(const std::string& name) {
    for (JobState s : {JobState::queued, JobState::unschedulable, JobState::assigned,
                       JobState::starting, JobState::running, JobState::succeeded, JobState::failed,
                       JobState::lost, JobState::cancelled}) {
        if (to_string(s) == name) return s;
    }
    throw std::invalid_argument("job record: unknown state '" + name + "'");
}

}  // namespace

std::string encode_job(const Job& job) {
    json attempts = json::array();
    for (const Attempt& a : job.attempts) {
        attempts.push_back({
            {"number", a.number},
            {"node", a.node},
            {"queue_wait_ns", duration_json(a.queue_wait)},
            {"dispatch_latency_ns", duration_json(a.dispatch_latency)},
            {"start_latency_ns", duration_json(a.start_latency)},
            {"image_pulled", optional_json(a.image_pulled)},
            {"spans_restart", a.spans_restart},
            {"outcome", a.outcome},
        });
    }
    return json{
        {"v", kVersion},
        {"id", job.id},
        {"seq", job.seq},
        {"idempotency_key", optional_json(job.idempotency_key)},
        {"image", job.spec.image},
        {"command", job.spec.command},
        {"accelerator", protocol::to_string(job.spec.requirements.accelerator)},
        {"min_gpu_mem_mb", job.spec.requirements.min_gpu_mem_mb},
        {"max_runtime_s", job.spec.max_runtime_s},
        {"max_retries", job.max_retries},
        {"state", to_string(job.state)},
        {"reason", job.reason},
        {"attempt", job.attempt},
        {"retries_used", job.retries_used},
        {"rejected_by", job.rejected_by},
        {"node", optional_json(job.node)},
        {"exit_code", optional_json(job.exit_code)},
        {"attempts", std::move(attempts)},
    }
        .dump();
}

Job decode_job(std::string_view doc) {
    try {
        const json j = json::parse(doc);
        if (j.at("v").get<int>() != kVersion) {
            throw std::invalid_argument("job record: unsupported version");
        }
        Job job;
        job.id = j.at("id").get<std::string>();
        job.seq = j.at("seq").get<std::uint64_t>();
        job.idempotency_key = optional_from<std::string>(j.at("idempotency_key"));
        job.spec.image = j.at("image").get<std::string>();
        job.spec.command = j.at("command").get<std::vector<std::string>>();
        const auto accelerator = protocol::accelerator_from(j.at("accelerator").get<std::string>());
        if (!accelerator) throw std::invalid_argument("job record: unknown accelerator");
        job.spec.requirements.accelerator = *accelerator;
        job.spec.requirements.min_gpu_mem_mb = j.at("min_gpu_mem_mb").get<std::uint64_t>();
        job.spec.max_runtime_s = j.at("max_runtime_s").get<std::uint32_t>();
        job.max_retries = j.at("max_retries").get<std::uint32_t>();
        job.state = state_from(j.at("state").get<std::string>());
        job.reason = j.at("reason").get<std::string>();
        job.attempt = j.at("attempt").get<std::uint32_t>();
        job.retries_used = j.at("retries_used").get<std::uint32_t>();
        job.rejected_by = j.at("rejected_by").get<std::set<NodeId>>();
        job.node = optional_from<NodeId>(j.at("node"));
        job.exit_code = optional_from<std::int32_t>(j.at("exit_code"));
        for (const json& a : j.at("attempts")) {
            Attempt& attempt = job.attempts.emplace_back();
            attempt.number = a.at("number").get<std::uint32_t>();
            attempt.node = a.at("node").get<NodeId>();
            attempt.queue_wait = duration_from(a.at("queue_wait_ns"));
            attempt.dispatch_latency = duration_from(a.at("dispatch_latency_ns"));
            attempt.start_latency = duration_from(a.at("start_latency_ns"));
            attempt.image_pulled = optional_from<bool>(a.at("image_pulled"));
            attempt.spans_restart = a.at("spans_restart").get<bool>();
            attempt.outcome = a.at("outcome").get<std::string>();
        }
        return job;
    } catch (const json::exception& e) {
        throw std::invalid_argument(std::string("job record: ") + e.what());
    }
}

}  // namespace orch
