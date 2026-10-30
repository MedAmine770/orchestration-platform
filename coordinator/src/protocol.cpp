#include "orch/protocol.hpp"

#include <cmath>
#include <cstddef>
#include <limits>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

namespace orch::protocol {

namespace {

using json = nlohmann::json;

[[noreturn]] void fail(ErrorCode code, const std::string& detail) {
    throw ProtocolError(code, detail);
}

// ---- Typed field access -----------------------------------------------------
//
// Each rule in proto/PROTOCOL.md "Value rules" is implemented once here.
// `path` is the field's location for error messages, e.g. "body.spec.image".

template <typename T>
T as_uint(const json& v, const std::string& path, T min = 0) {
    // is_number_unsigned() is false for floats, negatives and booleans.
    if (!v.is_number_unsigned()) fail(ErrorCode::wrong_type, path + " must be an unsigned integer");
    const auto raw = v.get<std::uint64_t>();
    if (raw > std::numeric_limits<T>::max()) fail(ErrorCode::invalid_value, path + " out of range");
    if (raw < min) fail(ErrorCode::invalid_value, path + " must be >= " + std::to_string(min));
    return static_cast<T>(raw);
}

std::string as_string(const json& v, const std::string& path) {
    if (!v.is_string()) fail(ErrorCode::wrong_type, path + " must be a string");
    return v.get<std::string>();
}

Accelerator as_accelerator(const json& v, const std::string& path) {
    const std::string s = as_string(v, path);
    if (s == "cpu") return Accelerator::cpu;
    if (s == "cuda") return Accelerator::cuda;
    if (s == "mps") return Accelerator::mps;
    fail(ErrorCode::invalid_value, path + " has unknown accelerator '" + s + "'");
}

Runtime as_runtime(const json& v, const std::string& path) {
    const std::string s = as_string(v, path);
    if (s == "docker") return Runtime::docker;
    if (s == "process") return Runtime::process;
    fail(ErrorCode::invalid_value, path + " has unknown runtime '" + s + "'");
}

class Fields {
  public:
    Fields(const json& j, std::string path) : j_(j), path_(std::move(path)) {
        if (!j_.is_object()) {
            fail(ErrorCode::not_object,
                 (path_.empty() ? "envelope" : path_) + " must be an object");
        }
    }

    bool has(const char* key) const { return j_.contains(key); }

    const json& at(const char* key) const {
        const auto it = j_.find(key);
        if (it == j_.end()) fail(ErrorCode::missing_field, path(key) + " is required");
        return *it;
    }

    std::string path(const std::string& key) const {
        return path_.empty() ? key : path_ + "." + key;
    }

    std::string str(const char* key) const { return as_string(at(key), path(key)); }

    std::string id(const char* key) const {
        std::string s = str(key);
        if (s.empty()) fail(ErrorCode::invalid_value, path(key) + " must not be empty");
        return s;
    }

    template <typename T>
    T uint(const char* key, T min = 0) const {
        return as_uint<T>(at(key), path(key), min);
    }

    std::int32_t int32(const char* key) const {
        const json& v = at(key);
        if (!v.is_number_integer()) fail(ErrorCode::wrong_type, path(key) + " must be an integer");
        constexpr auto lo = std::numeric_limits<std::int32_t>::min();
        constexpr auto hi = std::numeric_limits<std::int32_t>::max();
        if (v.is_number_unsigned()) {
            if (v.get<std::uint64_t>() > static_cast<std::uint64_t>(hi)) {
                fail(ErrorCode::invalid_value, path(key) + " out of range");
            }
        } else if (v.get<std::int64_t>() < lo || v.get<std::int64_t>() > hi) {
            fail(ErrorCode::invalid_value, path(key) + " out of range");
        }
        return v.get<std::int32_t>();
    }

    double number(const char* key) const {
        const json& v = at(key);
        if (!v.is_number()) fail(ErrorCode::wrong_type, path(key) + " must be a number");
        const auto d = v.get<double>();
        if (!std::isfinite(d) || d < 0) fail(ErrorCode::invalid_value, path(key) + " must be >= 0");
        return d;
    }

    bool boolean(const char* key) const {
        const json& v = at(key);
        if (!v.is_boolean()) fail(ErrorCode::wrong_type, path(key) + " must be a boolean");
        return v.get<bool>();
    }

    Fields object(const char* key) const { return Fields(at(key), path(key)); }

    const json& array(const char* key, bool non_empty) const {
        const json& v = at(key);
        if (!v.is_array()) fail(ErrorCode::wrong_type, path(key) + " must be an array");
        if (non_empty && v.empty())
            fail(ErrorCode::invalid_value, path(key) + " must not be empty");
        return v;
    }

  private:
    const json& j_;
    std::string path_;
};

std::string element(const std::string& path, std::size_t i) {
    return path + "[" + std::to_string(i) + "]";
}

// ---- Decoding ---------------------------------------------------------------

JobRef decode_job(const Fields& f) {
    return JobRef{f.id("job_id"), f.uint<std::uint32_t>("attempt", 1)};
}

Capabilities decode_capabilities(const Fields& f) {
    Capabilities c;
    c.cpus = f.uint<std::uint32_t>("cpus", 1);
    c.mem_mb = f.uint<std::uint64_t>("mem_mb");
    c.accelerator = as_accelerator(f.at("accelerator"), f.path("accelerator"));
    if (f.has("gpu_name")) c.gpu_name = f.str("gpu_name");
    if (f.has("gpu_mem_mb")) c.gpu_mem_mb = f.uint<std::uint64_t>("gpu_mem_mb");
    const json& runtimes = f.array("runtimes", /*non_empty=*/true);
    for (std::size_t i = 0; i < runtimes.size(); ++i) {
        c.runtimes.push_back(as_runtime(runtimes[i], element(f.path("runtimes"), i)));
    }
    return c;
}

Hello decode_hello(const Fields& b) {
    Hello m;
    m.token = b.id("token");
    m.node_id = b.id("node_id");
    m.agent_version = b.str("agent_version");
    m.capabilities = decode_capabilities(b.object("capabilities"));
    m.slots = b.uint<std::uint32_t>("slots", 1);
    const json& running = b.array("running", /*non_empty=*/false);
    for (std::size_t i = 0; i < running.size(); ++i) {
        m.running.push_back(decode_job(Fields(running[i], element(b.path("running"), i))));
    }
    return m;
}

JobSpec decode_spec(const Fields& f) {
    JobSpec s;
    s.image = f.id("image");
    const json& command = f.array("command", /*non_empty=*/true);
    for (std::size_t i = 0; i < command.size(); ++i) {
        s.command.push_back(as_string(command[i], element(f.path("command"), i)));
    }
    const Fields req = f.object("requirements");
    s.requirements.accelerator = as_accelerator(req.at("accelerator"), req.path("accelerator"));
    s.requirements.min_gpu_mem_mb = req.uint<std::uint64_t>("min_gpu_mem_mb");
    s.max_runtime_s = f.uint<std::uint32_t>("max_runtime_s", 1);
    return s;
}

Finished decode_finished(const Fields& b) {
    Finished m;
    m.job = decode_job(b);
    if (b.has("exit_code") == b.has("error")) {
        fail(ErrorCode::invalid_value, "body must contain exactly one of exit_code, error");
    }
    if (b.has("exit_code")) {
        m.exit_code = b.int32("exit_code");
    } else {
        m.error = b.str("error");
    }
    m.run_ms = b.uint<std::uint64_t>("run_ms");
    return m;
}

Message decode_body(const std::string& type, const Fields& b) {
    if (type == "hello") return decode_hello(b);
    if (type == "welcome") return Welcome{b.uint<std::uint32_t>("heartbeat_interval_ms", 1)};
    if (type == "heartbeat") {
        return Heartbeat{b.uint<std::uint32_t>("running"), b.number("cpu_percent"),
                         b.uint<std::uint64_t>("mem_free_mb")};
    }
    if (type == "assign") return Assign{decode_job(b), decode_spec(b.object("spec"))};
    if (type == "ack") return Ack{decode_job(b)};
    if (type == "reject") return Reject{decode_job(b), b.str("reason")};
    if (type == "started") return Started{decode_job(b), b.boolean("image_pulled")};
    if (type == "finished") return decode_finished(b);
    if (type == "cancel") return Cancel{decode_job(b)};
    return Unknown{type};
}

// ---- Encoding ---------------------------------------------------------------

std::string_view name(Accelerator a) {
    switch (a) {
        case Accelerator::cpu:
            return "cpu";
        case Accelerator::cuda:
            return "cuda";
        case Accelerator::mps:
            return "mps";
    }
    return "cpu";  // unreachable; keeps -Wreturn-type quiet
}

std::string_view name(Runtime r) { return r == Runtime::docker ? "docker" : "process"; }

json job_fields(const JobRef& j) { return json{{"job_id", j.job_id}, {"attempt", j.attempt}}; }

// Each overload returns {type, body}.
struct BodyEncoder {
    std::pair<std::string_view, json> operator()(const Hello& m) const {
        json caps{{"cpus", m.capabilities.cpus},
                  {"mem_mb", m.capabilities.mem_mb},
                  {"accelerator", name(m.capabilities.accelerator)},
                  {"runtimes", json::array()}};
        if (m.capabilities.gpu_name) caps["gpu_name"] = *m.capabilities.gpu_name;
        if (m.capabilities.gpu_mem_mb) caps["gpu_mem_mb"] = *m.capabilities.gpu_mem_mb;
        for (const Runtime r : m.capabilities.runtimes) caps["runtimes"].push_back(name(r));
        json running = json::array();
        for (const JobRef& j : m.running) running.push_back(job_fields(j));
        return {"hello", json{{"token", m.token},
                              {"node_id", m.node_id},
                              {"agent_version", m.agent_version},
                              {"capabilities", std::move(caps)},
                              {"slots", m.slots},
                              {"running", std::move(running)}}};
    }
    std::pair<std::string_view, json> operator()(const Welcome& m) const {
        return {"welcome", json{{"heartbeat_interval_ms", m.heartbeat_interval_ms}}};
    }
    std::pair<std::string_view, json> operator()(const Heartbeat& m) const {
        return {"heartbeat", json{{"running", m.running},
                                  {"cpu_percent", m.cpu_percent},
                                  {"mem_free_mb", m.mem_free_mb}}};
    }
    std::pair<std::string_view, json> operator()(const Assign& m) const {
        json body = job_fields(m.job);
        body["spec"] = json{{"image", m.spec.image},
                            {"command", m.spec.command},
                            {"requirements",
                             {{"accelerator", name(m.spec.requirements.accelerator)},
                              {"min_gpu_mem_mb", m.spec.requirements.min_gpu_mem_mb}}},
                            {"max_runtime_s", m.spec.max_runtime_s}};
        return {"assign", std::move(body)};
    }
    std::pair<std::string_view, json> operator()(const Ack& m) const {
        return {"ack", job_fields(m.job)};
    }
    std::pair<std::string_view, json> operator()(const Reject& m) const {
        json body = job_fields(m.job);
        body["reason"] = m.reason;
        return {"reject", std::move(body)};
    }
    std::pair<std::string_view, json> operator()(const Started& m) const {
        json body = job_fields(m.job);
        body["image_pulled"] = m.image_pulled;
        return {"started", std::move(body)};
    }
    std::pair<std::string_view, json> operator()(const Finished& m) const {
        json body = job_fields(m.job);
        if (m.exit_code) body["exit_code"] = *m.exit_code;
        if (m.error) body["error"] = *m.error;
        body["run_ms"] = m.run_ms;
        return {"finished", std::move(body)};
    }
    std::pair<std::string_view, json> operator()(const Cancel& m) const {
        return {"cancel", job_fields(m.job)};
    }
    std::pair<std::string_view, json> operator()(const Unknown& m) const {
        throw std::invalid_argument("cannot encode unknown message type '" + m.type + "'");
    }
};

}  // namespace

std::string_view to_string(ErrorCode code) noexcept {
    switch (code) {
        case ErrorCode::malformed_json:
            return "malformed_json";
        case ErrorCode::not_object:
            return "not_object";
        case ErrorCode::missing_field:
            return "missing_field";
        case ErrorCode::wrong_type:
            return "wrong_type";
        case ErrorCode::invalid_value:
            return "invalid_value";
        case ErrorCode::unsupported_version:
            return "unsupported_version";
    }
    return "unknown";
}

Message decode(std::string_view text) {
    json j;
    try {
        j = json::parse(text.begin(), text.end());
    } catch (const json::parse_error& e) {
        fail(ErrorCode::malformed_json, e.what());
    }

    const Fields envelope(j, "");
    // Read `v` as uint64 so that e.g. 2^40 reports unsupported_version, not invalid_value.
    if (envelope.uint<std::uint64_t>("v") != kVersion) {
        fail(ErrorCode::unsupported_version, "only protocol version 1 is supported");
    }
    const std::string type = envelope.str("type");
    return decode_body(type, envelope.object("body"));
}

std::string encode(const Message& message) {
    auto [type, body] = std::visit(BodyEncoder{}, message);
    const json envelope{{"v", kVersion}, {"type", type}, {"body", std::move(body)}};
    // `replace` turns invalid UTF-8 into U+FFFD instead of throwing, so a bad
    // string can never make encode() fail halfway through a send.
    return envelope.dump(-1, ' ', false, json::error_handler_t::replace);
}

}  // namespace orch::protocol
