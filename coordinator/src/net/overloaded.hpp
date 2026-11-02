#pragma once

namespace orch::net {

// Builds one visitor for std::visit out of several lambdas.
template <typename... Fs>
struct Overloaded : Fs... {
    using Fs::operator()...;
};

}  // namespace orch::net
