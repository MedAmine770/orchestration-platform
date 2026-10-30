find_package(GTest CONFIG REQUIRED)
find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(orch_tests
    metrics_test.cpp
    protocol_test.cpp
    version_test.cpp)
target_link_libraries(orch_tests
    PRIVATE orch_core GTest::gtest_main Threads::Threads nlohmann_json::nlohmann_json)
target_compile_definitions(orch_tests
    PRIVATE ORCH_PROTO_VECTORS_DIR="${PROJECT_SOURCE_DIR}/../proto/vectors")

gtest_discover_tests(orch_tests)

# Prints a sample /metrics page; CI pipes it into `promtool check metrics`.
add_executable(metrics_sample metrics_sample.cpp)
target_link_libraries(metrics_sample PRIVATE orch_core)
