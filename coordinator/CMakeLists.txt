cmake_minimum_required(VERSION 3.25)

project(orch_coordinator VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ORCH_WERROR "Treat compiler warnings as errors" ON)
set(ORCH_SANITIZER "" CACHE STRING "Sanitizer build: '', 'address' (ASan+UBSan) or 'thread' (TSan)")
set_property(CACHE ORCH_SANITIZER PROPERTY STRINGS "" address thread)

# Compiler settings shared by every target we own. Third-party headers come in
# through imported targets, which CMake marks SYSTEM, so these warnings only
# apply to our code.
add_library(orch_options INTERFACE)
target_compile_options(orch_options INTERFACE
    -Wall -Wextra -Wpedantic
    -Wshadow -Wconversion -Wsign-conversion
    -Wnon-virtual-dtor -Wold-style-cast -Woverloaded-virtual
    $<$<BOOL:${ORCH_WERROR}>:-Werror>)

if(ORCH_SANITIZER STREQUAL "address")
    set(_san_flags -fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer)
elseif(ORCH_SANITIZER STREQUAL "thread")
    set(_san_flags -fsanitize=thread -fno-omit-frame-pointer)
elseif(NOT ORCH_SANITIZER STREQUAL "")
    message(FATAL_ERROR "Unknown ORCH_SANITIZER '${ORCH_SANITIZER}'")
endif()
if(_san_flags)
    target_compile_options(orch_options INTERFACE ${_san_flags})
    target_link_options(orch_options INTERFACE ${_san_flags})
endif()

find_package(nlohmann_json CONFIG REQUIRED)
find_package(boost_beast CONFIG REQUIRED)
find_package(Threads REQUIRED)

# Core library: everything except main(), so tests link the same code the binary runs.
add_library(orch_core
    src/coordinator_metrics.cpp
    src/metrics.cpp
    src/net/http_session.cpp
    src/net/server.cpp
    src/net/worker_session.cpp
    src/protocol.cpp
    src/version.cpp)
# include/ is the public API; src/ holds internal headers such as net/server.hpp.
target_include_directories(orch_core PUBLIC include PRIVATE src)
target_link_libraries(orch_core
    PUBLIC orch_options Boost::beast Threads::Threads
    PRIVATE nlohmann_json::nlohmann_json)  # JSON stays out of our public headers

add_executable(coordinator src/main.cpp)
target_include_directories(coordinator PRIVATE src)
target_link_libraries(coordinator PRIVATE orch_core)

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
