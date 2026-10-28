#pragma once

#include <string_view>

namespace orch {

// Reported by /healthz and checked against the worker's version in `hello`.
std::string_view version() noexcept;

}  // namespace orch
