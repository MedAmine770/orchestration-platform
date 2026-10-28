#include <iostream>

#include "orch/version.hpp"

int main() {
    std::cout << "orch coordinator " << orch::version() << '\n';
    return 0;
}
