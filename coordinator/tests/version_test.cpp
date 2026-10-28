#include "orch/version.hpp"

#include <algorithm>

#include <gtest/gtest.h>

TEST(Version, IsSemver) {
    const auto v = orch::version();
    EXPECT_EQ(std::count(v.begin(), v.end(), '.'), 2) << v;
}
