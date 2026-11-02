#include "orch/api.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

#include <nlohmann/json.hpp>

namespace orch {

namespace {

using json = nlohmann::json;
using protocol::Accelerator;

constexpr std::uint32_t kDefaultMaxRuntimeS = 3600;
constexpr std::uint64_t kMaxRetriesLimit = 100;
constexpr std::string_view kJobsPath = "/v1/jobs";
constexpr std::string_view kNodesPath = "/v1/nodes";

struct BadRequest : std::runtime_error {
    using std::runtime_error::runtime_error;
};

std::string_view accelerator_name(Accelerator a) {
    switch (a) {
        case Accelerator::cpu:
            return "cpu";
        case Accelerator::cuda:
            return "cuda";
        case Accelerator::mps:
            return "mps";
    }
    return "unknown";
}

ApiResponse respond(unsigned status, const json& body) { return {status, body.dump() + "\n"}; }
ApiResponse error(unsigned status, std::string message) {
    return respond(status, json{{"error", std::move(message)}});
}
ApiResponse method_not_allowed() { return error(405, "method not allowed"); }

template <typename T>
json optional_json(const std::optional<T>& v) {
    return v ? json(*v) : json(nullptr);
}

std::optional<std::int64_t> ns(const std::optional<Clock::duration>& d) {
    if (!d) return std::nullopt;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(*d).count();
}

// ---- Request parsing ----------------------------------------------------------

// Integers must be JSON integers: nlohmann parses 5 as unsigned, -5 as signed
// and 5.0 as a float, and true is a boolean, so only the first passes.
std::uint64_t uint_field(const json& obj, const char* name, std::uint64_t fallback,
                         std::uint64_t min, std::uint64_t max) {
    const auto it = obj.find(name);
    if (it == obj.end()) return fallback;
    if (!it->is_number_unsigned()) {
        throw BadRequest(std::string(name) + " must be a non-negative integer");
    }
    const auto v = it->get<std::uint64_t>();
    if (v < min || v > max) {
        throw BadRequest(std::string(name) + " must be between " + std::to_string(min) + " and " +
                         std::to_string(max));
    }
    return v;
}

std::string non_empty_string(const json& v, const std::string& what) {
    if (!v.is_string() || v.get_ref<const std::string&>().empty()) {
        throw BadRequest(what + " must be a non-empty string");
    }
    return v.get<std::string>();
}

Submission parse_submission(std::string_view body) {
    const json req = json::parse(body, nullptr, /*allow_exceptions=*/false);
    if (req.is_discarded()) throw BadRequest("body is not valid JSON");
    if (!req.is_object()) throw BadRequest("body must be a JSON object");

    Submission s;
    const auto image = req.find("image");
    if (image == req.end()) throw BadRequest("image is required");
    s.spec.image = non_empty_string(*image, "image");

    const auto command = req.find("command");
    if (command == req.end() || !command->is_array() || command->empty()) {
        throw BadRequest("command must be a non-empty array of strings");
    }
    for (const json& arg : *command) {
        if (!arg.is_string()) throw BadRequest("command must be a non-empty array of strings");
        s.spec.command.push_back(arg.get<std::string>());
    }

    if (const auto reqs = req.find("requirements"); reqs != req.end()) {
        if (!reqs->is_object()) throw BadRequest("requirements must be an object");
        if (const auto acc = reqs->find("accelerator"); acc != reqs->end()) {
            const std::string name = acc->is_string() ? acc->get<std::string>() : "";
            if (name == "cpu") {
                s.spec.requirements.accelerator = Accelerator::cpu;
            } else if (name == "cuda") {
                s.spec.requirements.accelerator = Accelerator::cuda;
            } else if (name == "mps") {
                s.spec.requirements.accelerator = Accelerator::mps;
            } else {
                throw BadRequest("requirements.accelerator must be cpu, cuda or mps");
            }
        }
        s.spec.requirements.min_gpu_mem_mb =
            uint_field(*reqs, "min_gpu_mem_mb", 0, 0, std::numeric_limits<std::uint64_t>::max());
    }

    s.spec.max_runtime_s = static_cast<std::uint32_t>(uint_field(
        req, "max_runtime_s", kDefaultMaxRuntimeS, 1, std::numeric_limits<std::uint32_t>::max()));
    if (req.contains("max_retries")) {
        s.max_retries =
            static_cast<std::uint32_t>(uint_field(req, "max_retries", 0, 0, kMaxRetriesLimit));
    }
    if (const auto key = req.find("idempotency_key"); key != req.end()) {
        s.idempotency_key = non_empty_string(*key, "idempotency_key");
    }
    return s;
}

// ---- Serialisation ----------------------------------------------------------

json job_json(const Job& job) {
    json attempts = json::array();
    for (const Attempt& a : job.attempts) {
        attempts.push_back({
            {"attempt", a.number},
            {"node", a.node},
            {"outcome", a.outcome.empty() ? json(nullptr) : json(a.outcome)},
            // Integer nanoseconds: these are the bench's exact raw samples.
            {"queue_wait_ns", optional_json(ns(a.queue_wait))},
            {"dispatch_latency_ns", optional_json(ns(a.dispatch_latency))},
            {"start_latency_ns", optional_json(ns(a.start_latency))},
            {"image_pulled", optional_json(a.image_pulled)},
        });
    }
    return {
        {"job_id", job.id},
        {"state", to_string(job.state)},
        {"reason", job.reason},
        {"image", job.spec.image},
        {"command", job.spec.command},
        {"requirements",
         {{"accelerator", accelerator_name(job.spec.requirements.accelerator)},
          {"min_gpu_mem_mb", job.spec.requirements.min_gpu_mem_mb}}},
        {"max_runtime_s", job.spec.max_runtime_s},
        {"max_retries", job.max_retries},
        {"idempotency_key", optional_json(job.idempotency_key)},
        {"attempt", job.attempt},
        {"retries_used", job.retries_used},
        {"rejected_by", job.rejected_by},
        {"node", optional_json(job.node)},
        {"exit_code", optional_json(job.exit_code)},
        {"attempts", std::move(attempts)},
    };
}

json node_json(const Node& node) {
    const auto& c = node.capabilities;
    json runtimes = json::array();
    for (const auto r : c.runtimes) {
        runtimes.push_back(r == protocol::Runtime::docker ? "docker" : "process");
    }
    return {
        {"node_id", node.id},
        {"health", to_string(node.health)},
        {"slots", node.slots},
        {"running", node.jobs.size()},
        {"jobs", node.jobs},
        {"capabilities",
         {{"cpus", c.cpus},
          {"mem_mb", c.mem_mb},
          {"accelerator", accelerator_name(c.accelerator)},
          {"gpu_name", optional_json(c.gpu_name)},
          {"gpu_mem_mb", optional_json(c.gpu_mem_mb)},
          {"runtimes", std::move(runtimes)}}},
    };
}

std::optional<JobState> parse_state(std::string_view name) {
    for (JobState s : {JobState::queued, JobState::unschedulable, JobState::assigned,
                       JobState::starting, JobState::running, JobState::succeeded, JobState::failed,
                       JobState::lost, JobState::cancelled}) {
        if (to_string(s) == name) return s;
    }
    return std::nullopt;
}

}  // namespace

// ---- Routing ------------------------------------------------------------------

std::optional<ApiResponse> Api::handle(std::string_view method, std::string_view target,
                                       std::string_view body, Clock::time_point now) {
    const auto q = target.find('?');
    const std::string_view path = target.substr(0, q);
    const std::string_view query = q == std::string_view::npos ? "" : target.substr(q + 1);

    if (path == kNodesPath) return method == "GET" ? list_nodes() : method_not_allowed();
    if (path == kJobsPath) {
        if (method == "GET") return list_jobs(query);
        if (method == "POST") return submit(body, now);
        return method_not_allowed();
    }
    if (!path.starts_with(kJobsPath) || path[kJobsPath.size()] != '/') return std::nullopt;

    // /v1/jobs/{id} or /v1/jobs/{id}/cancel
    const std::string_view rest = path.substr(kJobsPath.size() + 1);
    const auto slash = rest.find('/');
    const JobId id(rest.substr(0, slash));
    if (slash == std::string_view::npos) {
        if (method != "GET") return method_not_allowed();
        const Job* job = jobs_.find(id);
        return job ? respond(200, job_json(*job)) : error(404, "no job " + id);
    }
    if (rest.substr(slash) != "/cancel") return error(404, "not found");
    return method == "POST" ? cancel(id, now) : method_not_allowed();
}

ApiResponse Api::submit(std::string_view body, Clock::time_point now) {
    Submission submission;
    try {
        submission = parse_submission(body);
    } catch (const BadRequest& e) {
        return error(400, e.what());
    }
    submission.id = new_id_();
    const auto [job, created] = jobs_.submit(std::move(submission), now);
    // 200 on an idempotency-key replay: the original job, not a new one.
    return respond(created ? 201 : 200, job_json(job));
}

ApiResponse Api::list_jobs(std::string_view query) const {
    std::optional<JobState> filter;
    if (query.starts_with("state=")) {
        filter = parse_state(query.substr(6));
        if (!filter) return error(400, "unknown state '" + std::string(query.substr(6)) + "'");
    }
    std::vector<const Job*> selected;
    for (const auto& [id, job] : jobs_.jobs()) {
        if (!filter || job.state == *filter) selected.push_back(&job);
    }
    std::ranges::sort(selected, {}, &Job::seq);  // submission order
    json out = json::array();
    for (const Job* job : selected) out.push_back(job_json(*job));
    return respond(200, json{{"jobs", std::move(out)}});
}

ApiResponse Api::cancel(const JobId& id, Clock::time_point now) {
    switch (jobs_.cancel(id, now)) {
        case CancelResult::not_found:
            return error(404, "no job " + id);
        case CancelResult::already_terminal:
            return respond(409, job_json(*jobs_.find(id)));
        case CancelResult::cancelled:
        case CancelResult::already_cancelled:
            break;
    }
    return respond(200, job_json(*jobs_.find(id)));  // idempotent
}

ApiResponse Api::list_nodes() const {
    json out = json::array();
    for (const auto& [id, node] : registry_.nodes()) out.push_back(node_json(node));
    return respond(200, json{{"nodes", std::move(out)}});
}

std::string uuid_v4(std::mt19937_64& rng) {
    std::array<unsigned char, 16> b{};
    for (std::size_t half = 0; half < 2; ++half) {
        const std::uint64_t r = rng();
        for (std::size_t i = 0; i < 8; ++i) {
            b[half * 8 + i] = static_cast<unsigned char>(r >> (8 * i));
        }
    }
    b[6] = static_cast<unsigned char>((b[6] & 0x0f) | 0x40);  // version 4
    b[8] = static_cast<unsigned char>((b[8] & 0x3f) | 0x80);  // RFC 4122 variant
    constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(36);
    for (std::size_t i = 0; i < b.size(); ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) out.push_back('-');
        out.push_back(kHex[b[i] >> 4]);
        out.push_back(kHex[b[i] & 0x0f]);
    }
    return out;
}

}  // namespace orch
