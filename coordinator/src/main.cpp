#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>

#include "net/server.hpp"
#include "orch/coordinator_metrics.hpp"
#include "orch/metrics.hpp"
#include "orch/version.hpp"

namespace {

constexpr std::string_view kUsage = R"(usage: coordinator [options]

  --bind ADDR               listen address (default 0.0.0.0)
  --port N                  listen port; 0 picks a free one (default 8080)
  --hello-timeout-ms N      time a worker has to send hello (default 5000)
  --heartbeat-interval-ms N heartbeat interval sent to workers (default 1000)
  --token-file PATH         read the cluster token from PATH

The cluster token comes from --token-file or the CLUSTER_TOKEN environment
variable. It is never accepted as a flag: command lines are visible to every
user on the host through ps.
)";

unsigned long parse_number(std::string_view flag, std::string_view text, unsigned long max) {
    unsigned long value = 0;
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || end != text.data() + text.size() || value > max) {
        throw std::invalid_argument(std::string(flag) + ": invalid value '" + std::string(text) +
                                    "'");
    }
    return value;
}

std::string read_token_file(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::invalid_argument("--token-file: cannot read " + path);
    std::stringstream ss;
    ss << in.rdbuf();
    std::string token = ss.str();
    while (!token.empty() &&
           (token.back() == '\n' || token.back() == '\r' || token.back() == ' ')) {
        token.pop_back();
    }
    return token;
}

orch::net::ServerConfig parse_args(int argc, char** argv) {
    orch::net::ServerConfig config;
    std::string token_file;
    for (int i = 1; i < argc; ++i) {
        const std::string_view flag = argv[i];
        if (flag == "--help" || flag == "-h") {
            std::cout << kUsage;
            std::exit(0);
        }
        if (i + 1 >= argc) throw std::invalid_argument(std::string(flag) + ": missing value");
        const std::string_view value = argv[++i];
        if (flag == "--bind") {
            config.bind_address = value;
        } else if (flag == "--port") {
            config.port = static_cast<unsigned short>(parse_number(flag, value, 65535));
        } else if (flag == "--hello-timeout-ms") {
            config.hello_timeout = std::chrono::milliseconds(parse_number(flag, value, 600'000));
        } else if (flag == "--heartbeat-interval-ms") {
            config.heartbeat_interval =
                std::chrono::milliseconds(parse_number(flag, value, 600'000));
        } else if (flag == "--token-file") {
            token_file = value;
        } else {
            throw std::invalid_argument("unknown option " + std::string(flag));
        }
    }

    if (!token_file.empty()) {
        config.cluster_token = read_token_file(token_file);
    } else if (const char* env = std::getenv("CLUSTER_TOKEN")) {
        config.cluster_token = env;
    }
    if (config.cluster_token.empty()) {
        throw std::invalid_argument("no cluster token: set CLUSTER_TOKEN or pass --token-file");
    }
    return config;
}

}  // namespace

int main(int argc, char** argv) {
    orch::net::ServerConfig config;
    try {
        config = parse_args(argc, argv);
    } catch (const std::invalid_argument& e) {
        std::cerr << "coordinator: " << e.what() << "\n\n" << kUsage;
        return 2;
    }

    try {
        // Declared first so it is destroyed last: pending handlers (which own
        // sessions) are destroyed with it, after the server they referred to.
        boost::asio::io_context ioc{1};  // concurrency hint: one io thread (DESIGN.md §9)

        orch::metrics::Registry registry;
        orch::CoordinatorMetrics metrics(registry);
        orch::net::Server server(ioc, config, registry, metrics);

        boost::asio::signal_set signals(ioc, SIGINT, SIGTERM);
        signals.async_wait([&ioc](const boost::system::error_code&, int) { ioc.stop(); });

        server.start();
        // Tests parse this line to learn the port when started with --port 0.
        std::cout << "listening on " << config.bind_address << ":" << server.port() << std::endl;
        std::cerr << "orch coordinator " << orch::version() << " started\n";

        ioc.run();
        std::cerr << "shutting down\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "coordinator: " << e.what() << '\n';
        return 1;
    }
}
