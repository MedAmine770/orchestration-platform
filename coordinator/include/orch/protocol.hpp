#pragma once

// Worker protocol v1: message types and the JSON codec. The rules live in
// proto/PROTOCOL.md; proto/vectors/ holds the shared test vectors.
//
// The header is JSON-library-free on purpose: the rest of the coordinator
// sees plain structs, and nlohmann/json stays an implementation detail.

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace orch::protocol {

inline constexpr std::uint32_t kVersion = 1;

enum class Accelerator { cpu, cuda, mps };
enum class Runtime { docker, process };

// The fencing pair carried by every job-related message (DESIGN.md §6).
struct JobRef {
    std::string job_id;
    std::uint32_t attempt = 0;
    bool operator==(const JobRef&) const = default;
};

struct Capabilities {
    std::uint32_t cpus = 0;
    std::uint64_t mem_mb = 0;
    Accelerator accelerator = Accelerator::cpu;
    std::optional<std::string> gpu_name;
    std::optional<std::uint64_t> gpu_mem_mb;
    std::vector<Runtime> runtimes;
    bool operator==(const Capabilities&) const = default;
};

struct Requirements {
    Accelerator accelerator = Accelerator::cpu;
    std::uint64_t min_gpu_mem_mb = 0;
    bool operator==(const Requirements&) const = default;
};

struct JobSpec {
    std::string image;
    std::vector<std::string> command;
    Requirements requirements;
    std::uint32_t max_runtime_s = 0;
    bool operator==(const JobSpec&) const = default;
};

// ---- Messages (W→C: worker to coordinator, C→W: coordinator to worker) ------

struct Hello {  // W→C
    std::string token;
    std::string node_id;
    std::string agent_version;
    Capabilities capabilities;
    std::uint32_t slots = 0;
    std::vector<JobRef> running;
    bool operator==(const Hello&) const = default;
};

struct Welcome {  // C→W
    std::uint32_t heartbeat_interval_ms = 0;
    bool operator==(const Welcome&) const = default;
};

struct Heartbeat {  // W→C
    std::uint32_t running = 0;
    double cpu_percent = 0;
    std::uint64_t mem_free_mb = 0;
    bool operator==(const Heartbeat&) const = default;
};

struct Assign {  // C→W
    JobRef job;
    JobSpec spec;
    bool operator==(const Assign&) const = default;
};

struct Ack {  // W→C
    JobRef job;
    bool operator==(const Ack&) const = default;
};

struct Reject {  // W→C
    JobRef job;
    std::string reason;
    bool operator==(const Reject&) const = default;
};

struct Started {  // W→C
    JobRef job;
    bool image_pulled = false;
    bool operator==(const Started&) const = default;
};

struct Finished {  // W→C; exactly one of exit_code / error is set
    JobRef job;
    std::optional<std::int32_t> exit_code;
    std::optional<std::string> error;
    std::uint64_t run_ms = 0;
    bool operator==(const Finished&) const = default;
};

struct Cancel {  // C→W
    JobRef job;
    bool operator==(const Cancel&) const = default;
};

// A well-formed envelope with a type this version doesn't know. Not an error:
// receivers log and ignore it, so newer peers can add message types.
struct Unknown {
    std::string type;
    bool operator==(const Unknown&) const = default;
};

using Message = std::variant<Hello, Welcome, Heartbeat, Assign, Ack, Reject, Started, Finished,
                             Cancel, Unknown>;

// ---- Codec ------------------------------------------------------------------

enum class ErrorCode {
    malformed_json,
    not_object,
    missing_field,
    wrong_type,
    invalid_value,
    unsupported_version,
};

// The code's wire name, as used in proto/vectors/invalid.json.
std::string_view to_string(ErrorCode code) noexcept;

class ProtocolError : public std::runtime_error {
  public:
    ProtocolError(ErrorCode code, const std::string& detail)
        : std::runtime_error(std::string(to_string(code)) + ": " + detail), code_(code) {}
    ErrorCode code() const noexcept { return code_; }

  private:
    ErrorCode code_;
};

// Parses one text frame. Throws ProtocolError on any rule violation.
Message decode(std::string_view text);

// Serialises a message. Throws std::invalid_argument for Unknown, which has no
// body to send.
std::string encode(const Message& message);

}  // namespace orch::protocol
