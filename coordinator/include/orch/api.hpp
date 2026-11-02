#pragma once

// REST API (DESIGN.md §7.1). Thin: validate, call into Jobs, serialise. Pure:
// a request goes in and a response comes out, with the time and new job ids
// supplied by the caller, so it is unit-tested without sockets.

#include <functional>
#include <optional>
#include <random>
#include <string>
#include <string_view>

#include "orch/jobs.hpp"
#include "orch/registry.hpp"

namespace orch {

struct ApiResponse {
    unsigned status = 200;
    std::string body;  // JSON
};

class Api {
  public:
    using IdGenerator = std::function<JobId()>;

    Api(Jobs& jobs, const Registry& registry, IdGenerator new_id)
        : jobs_(jobs), registry_(registry), new_id_(std::move(new_id)) {}

    // nullopt if `target` is not under /v1/jobs or /v1/nodes.
    std::optional<ApiResponse> handle(std::string_view method, std::string_view target,
                                      std::string_view body, Clock::time_point now);

  private:
    ApiResponse submit(std::string_view body, Clock::time_point now);
    ApiResponse list_jobs(std::string_view query) const;
    ApiResponse cancel(const JobId& id, Clock::time_point now);
    ApiResponse list_nodes() const;

    Jobs& jobs_;
    const Registry& registry_;
    IdGenerator new_id_;
};

// A random (version 4) UUID, e.g. "0f8fad5b-d9cb-469f-a165-70867728950e".
std::string uuid_v4(std::mt19937_64& rng);

}  // namespace orch
