#include "orch/protocol.hpp"

#include <fstream>
#include <set>
#include <stdexcept>
#include <string>
#include <variant>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace proto = orch::protocol;
using json = nlohmann::json;

namespace {

// Shared with the Python worker's tests; see proto/PROTOCOL.md.
json load_vectors(const std::string& file) {
    const std::string path = std::string(ORCH_PROTO_VECTORS_DIR) + "/" + file;
    std::ifstream in(path);
    if (!in) throw std::runtime_error("cannot open " + path);
    return json::parse(in);
}

std::string frame_of(const json& vector) {
    return vector.contains("raw") ? vector["raw"].get<std::string>() : vector["message"].dump();
}

}  // namespace

// ---- Shared vectors ---------------------------------------------------------

TEST(ProtocolVectors, ValidMessagesRoundTrip) {
    const json vectors = load_vectors("valid.json");
    ASSERT_FALSE(vectors.empty());
    for (const json& v : vectors) {
        SCOPED_TRACE(v["name"].get<std::string>());
        const proto::Message decoded = proto::decode(v["message"].dump());
        const json expected = v.contains("canonical") ? v["canonical"] : v["message"];
        // Compare as JSON values, so key order and whitespace don't matter.
        EXPECT_EQ(json::parse(proto::encode(decoded)), expected);
        // And decoding our own output gives back the same struct.
        EXPECT_EQ(proto::decode(proto::encode(decoded)), decoded);
    }
}

TEST(ProtocolVectors, InvalidMessagesFailWithTheExpectedCode) {
    const json vectors = load_vectors("invalid.json");
    ASSERT_FALSE(vectors.empty());
    for (const json& v : vectors) {
        SCOPED_TRACE(v["name"].get<std::string>());
        try {
            (void)proto::decode(frame_of(v));
            ADD_FAILURE() << "decoded successfully, expected " << v["error"];
        } catch (const proto::ProtocolError& e) {
            EXPECT_EQ(proto::to_string(e.code()), v["error"].get<std::string>()) << e.what();
        }
    }
}

TEST(ProtocolVectors, UnknownTypesDecodeAsUnknown) {
    const json vectors = load_vectors("unknown.json");
    ASSERT_FALSE(vectors.empty());
    for (const json& v : vectors) {
        SCOPED_TRACE(v["name"].get<std::string>());
        const proto::Message m = proto::decode(frame_of(v));
        ASSERT_TRUE(std::holds_alternative<proto::Unknown>(m));
        EXPECT_EQ(std::get<proto::Unknown>(m).type, v["type"].get<std::string>());
    }
}

// The vectors are only useful if they cover the whole protocol.
TEST(ProtocolVectors, CoverEveryMessageTypeAndErrorCode) {
    std::set<std::string> types;
    for (const json& v : load_vectors("valid.json")) types.insert(v["message"]["type"]);
    EXPECT_EQ(types, (std::set<std::string>{"hello", "welcome", "heartbeat", "assign", "ack",
                                            "reject", "started", "finished", "cancel"}));

    std::set<std::string> codes;
    for (const json& v : load_vectors("invalid.json")) codes.insert(v["error"]);
    EXPECT_EQ(codes, (std::set<std::string>{"malformed_json", "not_object", "missing_field",
                                            "wrong_type", "invalid_value", "unsupported_version"}));
}

// ---- Typed decoding ---------------------------------------------------------

TEST(Protocol, DecodesHelloIntoTypedFields) {
    const auto m = proto::decode(R"({"v":1,"type":"hello","body":{
        "token":"t","node_id":"sim-cuda-1","agent_version":"0.1.0",
        "capabilities":{"cpus":8,"mem_mb":16384,"accelerator":"cuda","gpu_mem_mb":24576,
                        "runtimes":["docker","process"]},
        "slots":2,"running":[{"job_id":"j1","attempt":3}]}})");
    const auto& hello = std::get<proto::Hello>(m);
    EXPECT_EQ(hello.node_id, "sim-cuda-1");
    EXPECT_EQ(hello.capabilities.accelerator, proto::Accelerator::cuda);
    EXPECT_EQ(hello.capabilities.gpu_mem_mb, 24576u);
    EXPECT_FALSE(hello.capabilities.gpu_name.has_value());
    EXPECT_EQ(hello.capabilities.runtimes,
              (std::vector<proto::Runtime>{proto::Runtime::docker, proto::Runtime::process}));
    EXPECT_EQ(hello.running, (std::vector<proto::JobRef>{{"j1", 3}}));
}

TEST(Protocol, DecodesNegativeExitCode) {
    const auto m = proto::decode(
        R"({"v":1,"type":"finished","body":{"job_id":"j","attempt":1,"exit_code":-9,"run_ms":5}})");
    const auto& f = std::get<proto::Finished>(m);
    EXPECT_EQ(f.exit_code, -9);
    EXPECT_FALSE(f.error.has_value());
}

TEST(Protocol, ErrorMessageNamesTheFieldPath) {
    try {
        (void)proto::decode(R"({"v":1,"type":"assign","body":{"job_id":"j","attempt":1,
            "spec":{"image":"i","command":["ok",3],"requirements":{"accelerator":"cpu",
            "min_gpu_mem_mb":0},"max_runtime_s":1}}})");
        FAIL() << "expected ProtocolError";
    } catch (const proto::ProtocolError& e) {
        EXPECT_EQ(e.code(), proto::ErrorCode::wrong_type);
        EXPECT_NE(std::string(e.what()).find("body.spec.command[1]"), std::string::npos)
            << e.what();
    }
}

TEST(Protocol, ExitCodeOutsideInt32IsInvalid) {
    EXPECT_THROW((void)proto::decode(R"({"v":1,"type":"finished","body":{"job_id":"j",
                     "attempt":1,"exit_code":2147483648,"run_ms":1}})"),
                 proto::ProtocolError);
}

TEST(Protocol, EncodingUnknownIsALogicError) {
    EXPECT_THROW((void)proto::encode(proto::Unknown{"drain"}), std::invalid_argument);
}
