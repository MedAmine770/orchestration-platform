#pragma once

// A job's durable form: the whole Job as one JSON document, stored in the
// jobs.doc column (DESIGN.md §8.1). Steady-clock times (submitted_at,
// queued_at, assigned_at, deadline) are not stored: they mean nothing to
// another process. Restore sets them from the restart time.

#include <stdexcept>
#include <string>
#include <string_view>

#include "orch/jobs.hpp"

namespace orch {

std::string encode_job(const Job& job);

// Throws std::invalid_argument if the document is not a job this version wrote.
Job decode_job(std::string_view doc);

}  // namespace orch
