#include "orch/api.hpp"

#include <random>
#include <regex>
#include <string>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "orch/coordinator_metrics.hpp"
#include "orch/jobs.hpp"
#include "orch/metrics.hpp"
#include "orch/registry.hpp"
#include "orch/scheduler.hpp"

using json = nlohmann::json;
using orch::Clock;

namespace {

constexpr std::string_view kEcho = R"({"image": "alpine:3.20", "command": ["echo", "hi"]})";

class ApiTest : public ::testing::Test {
  protected:
    orch::metrics::Registry metrics_registry;
    orch::CoordinatorMetrics metrics{metrics_registry};
    orch::Registry registry{metrics};
    orch::LeastLoaded policy;
    orch::Jobs jobs{orch::JobsConfig{}, registry, policy, metrics};
    int next_id = 0;
    orch::Api api{jobs, registry, [this] { return "job-" + std::to_string(++next_id); }};

    orch::ApiResponse call(std::string_view method, std::string_view target,
                           std::string_view body = "") {
        auto response = api.handle(method, target, body, Clock::time_point{});
        EXPECT_TRUE(response.has_value()) << target;
        return response.value_or(orch::ApiResponse{0, "{}"});
    }
    json body(const orch::ApiResponse& r) { return json::parse(r.body); }
};

}  // namespace

TEST_F(ApiTest, SubmitAppliesDefaultsAndReturnsTheJob) {
    const auto r = call("POST", "/v1/jobs", kEcho);
    EXPECT_EQ(r.status, 201u);
    const json job = body(r);
    EXPECT_EQ(job["job_id"], "job-1");
    EXPECT_EQ(job["state"], "unschedulable");  // no workers connected
    EXPECT_EQ(job["requirements"]["accelerator"], "cpu");
    EXPECT_EQ(job["max_runtime_s"], 3600);
    EXPECT_EQ(job["max_retries"], 2);
    EXPECT_EQ(job["attempts"], json::array());
}

TEST_F(ApiTest, InvalidSpecsAre400WithTheReason) {
    const std::pair<std::string_view, std::string_view> cases[] = {
        {"not json", "body is not valid JSON"},
        {"[]", "body must be a JSON object"},
        {R"({"command": ["true"]})", "image is required"},
        {R"({"image": "", "command": ["true"]})", "image must be a non-empty string"},
        {R"({"image": "a", "command": []})", "command must be a non-empty array of strings"},
        {R"({"image": "a", "command": ["echo", 1]})",
         "command must be a non-empty array of strings"},
        {R"({"image": "a", "command": ["true"], "max_runtime_s": 0})",
         "max_runtime_s must be between 1 and 4294967295"},
        {R"({"image": "a", "command": ["true"], "max_runtime_s": 1.5})",
         "max_runtime_s must be a non-negative integer"},
        {R"({"image": "a", "command": ["true"], "max_runtime_s": true})",
         "max_runtime_s must be a non-negative integer"},
        {R"({"image": "a", "command": ["true"], "requirements": {"accelerator": "tpu"}})",
         "requirements.accelerator must be cpu, cuda or mps"},
        {R"({"image": "a", "command": ["true"], "max_retries": 101})",
         "max_retries must be between 0 and 100"},
    };
    for (const auto& [request, reason] : cases) {
        const auto r = call("POST", "/v1/jobs", request);
        EXPECT_EQ(r.status, 400u) << request;
        EXPECT_EQ(body(r)["error"], reason) << request;
    }
    EXPECT_TRUE(jobs.jobs().empty());
}

TEST_F(ApiTest, IdempotencyKeyReplayIs200WithTheOriginalJob) {
    const std::string req =
        R"({"image": "a", "command": ["true"], "idempotency_key": "nightly-42"})";
    EXPECT_EQ(call("POST", "/v1/jobs", req).status, 201u);
    const auto replay = call("POST", "/v1/jobs", req);
    EXPECT_EQ(replay.status, 200u);
    EXPECT_EQ(body(replay)["job_id"], "job-1");
}

TEST_F(ApiTest, GetListFilterAndNotFound) {
    call("POST", "/v1/jobs", kEcho);
    call("POST", "/v1/jobs", kEcho);
    call("POST", "/v1/jobs/job-2/cancel");

    EXPECT_EQ(body(call("GET", "/v1/jobs/job-1"))["job_id"], "job-1");
    EXPECT_EQ(call("GET", "/v1/jobs/nope").status, 404u);
    EXPECT_EQ(body(call("GET", "/v1/jobs"))["jobs"].size(), 2u);
    const json cancelled = body(call("GET", "/v1/jobs?state=cancelled"))["jobs"];
    ASSERT_EQ(cancelled.size(), 1u);
    EXPECT_EQ(cancelled[0]["job_id"], "job-2");
    EXPECT_EQ(call("GET", "/v1/jobs?state=bogus").status, 400u);
}

TEST_F(ApiTest, CancelIsIdempotentAnd409OnceOtherwiseTerminal) {
    registry.join("a", {}, 1, Clock::time_point{});
    call("POST", "/v1/jobs", kEcho);
    jobs.on_ack("a", {{"job-1", 1}}, Clock::time_point{});
    jobs.on_finished("a", {{"job-1", 1}, 0, std::nullopt, 1}, Clock::time_point{});
    EXPECT_EQ(call("POST", "/v1/jobs/job-1/cancel").status, 409u);

    call("POST", "/v1/jobs", kEcho);
    EXPECT_EQ(call("POST", "/v1/jobs/job-2/cancel").status, 200u);
    EXPECT_EQ(call("POST", "/v1/jobs/job-2/cancel").status, 200u);
    EXPECT_EQ(call("POST", "/v1/jobs/nope/cancel").status, 404u);
}

TEST_F(ApiTest, WrongMethodsAre405AndOtherPathsAreNotOurs) {
    EXPECT_EQ(call("DELETE", "/v1/jobs").status, 405u);
    EXPECT_EQ(call("POST", "/v1/nodes").status, 405u);
    EXPECT_EQ(call("GET", "/v1/jobs/job-1/cancel").status, 405u);
    EXPECT_FALSE(api.handle("GET", "/healthz", "", {}).has_value());
    EXPECT_FALSE(api.handle("GET", "/v1/jobsX", "", {}).has_value());
}

TEST_F(ApiTest, NodesListCapabilitiesHealthAndLoad) {
    orch::protocol::Capabilities caps;
    caps.cpus = 8;
    caps.accelerator = orch::protocol::Accelerator::cuda;
    caps.gpu_mem_mb = 24576;
    caps.runtimes = {orch::protocol::Runtime::docker};
    registry.join("gpu-1", caps, 2, Clock::time_point{});
    const json node = body(call("GET", "/v1/nodes"))["nodes"].at(0);
    EXPECT_EQ(node["node_id"], "gpu-1");
    EXPECT_EQ(node["health"], "alive");
    EXPECT_EQ(node["slots"], 2);
    EXPECT_EQ(node["running"], 0);
    EXPECT_EQ(node["capabilities"]["accelerator"], "cuda");
    EXPECT_EQ(node["capabilities"]["runtimes"], json::array({"docker"}));
}

TEST(Uuid, IsVersion4WithTheRfc4122Variant) {
    std::mt19937_64 rng(42);
    const std::regex v4("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");
    for (int i = 0; i < 100; ++i) EXPECT_TRUE(std::regex_match(orch::uuid_v4(rng), v4));
}
