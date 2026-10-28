find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

add_executable(orch_tests
    version_test.cpp)
target_link_libraries(orch_tests PRIVATE orch_core GTest::gtest_main)

gtest_discover_tests(orch_tests)
