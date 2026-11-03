#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/signal_set.hpp>

#include "net/server.hpp"
#include "orch/coordinator_metrics.hpp"
#include "orch/job_record.hpp"
#include "orch/metrics.hpp"
#include "orch/store.hpp"
#include "orch/version.hpp"

namespace {

constexpr std::string_view kUsage = R"(usage: coordinator [options]

  --bind ADDR               listen address (default 0.0.0.0)
  --port N                  listen port; 0 picks a free one (default 8080)
  --admin-port N            serve REST and /metrics on this port instead, leaving
                            --port with only the worker WebSocket and /healthz;
                            0 picks a free one. REST has no auth, so keep this
                            port private
  --hello-timeout-ms N      time a worker has to send hello (default 5000)
  --heartbeat-interval-ms N heartbeat interval sent to workers (default 1000)
  --ack-timeout-ms N        T_ack: time a worker has to ack an assign (default 2000)
  --start-timeout-ms N      T_start: time from ack to started (default 120000)
  --suspect-after-ms N      no heartbeat for N ms: no new placements (default 3000)
  --dead-after-ms N         no heartbeat for N ms: node dead, jobs requeued (default 5000)
  --token-file PATH         read the cluster token from PATH
  --db PATH                 SQLite file for durable job state; without it nothing
                            survives a restart
  --recovery-window-ms N    after a restart, time in-flight jobs wait for their
                            worker to reclaim them (default 10000)

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

struct Options {
    orch::net::ServerConfig server;
    std::string db_path;
};

Options parse_args(int argc, char** argv) {
    Options options;
    orch::net::ServerConfig& config = options.server;
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
        } else if (flag == "--admin-port") {
            config.admin_port = static_cast<unsigned short>(parse_number(flag, value, 65535));
        } else if (flag == "--hello-timeout-ms") {
            config.hello_timeout = std::chrono::milliseconds(parse_number(flag, value, 600'000));
        } else if (flag == "--heartbeat-interval-ms") {
            config.heartbeat_interval =
                std::chrono::milliseconds(parse_number(flag, value, 600'000));
        } else if (flag == "--ack-timeout-ms") {
            config.jobs.ack_timeout = std::chrono::milliseconds(parse_number(flag, value, 600'000));
        } else if (flag == "--start-timeout-ms") {
            config.jobs.start_timeout =
                std::chrono::milliseconds(parse_number(flag, value, 3'600'000));
        } else if (flag == "--suspect-after-ms") {
            config.jobs.suspect_after =
                std::chrono::milliseconds(parse_number(flag, value, 600'000));
        } else if (flag == "--dead-after-ms") {
            config.jobs.dead_after = std::chrono::milliseconds(parse_number(flag, value, 600'000));
        } else if (flag == "--db") {
            options.db_path = value;
        } else if (flag == "--recovery-window-ms") {
            config.jobs.recovery_window =
                std::chrono::milliseconds(parse_number(flag, value, 3'600'000));
        } else if (flag == "--token-file") {
            token_file = value;
        } else {
            throw std::invalid_argument("unknown option " + std::string(flag));
        }
    }

    if (config.jobs.suspect_after >= config.jobs.dead_after) {
        throw std::invalid_argument("--suspect-after-ms must be less than --dead-after-ms");
    }
    if (!token_file.empty()) {
        config.cluster_token = read_token_file(token_file);
    } else if (const char* env = std::getenv("CLUSTER_TOKEN")) {
        config.cluster_token = env;
    }
    if (config.cluster_token.empty()) {
        throw std::invalid_argument("no cluster token: set CLUSTER_TOKEN or pass --token-file");
    }
    return options;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    try {
        options = parse_args(argc, argv);
    } catch (const std::invalid_argument& e) {
        std::cerr << "coordinator: " << e.what() << "\n\n" << kUsage;
        return 2;
    }

    try {
        // Declared first so it is destroyed last: pending handlers (which own
        // sessions) are destroyed with it, after the server they referred to.
        boost::asio::io_context ioc{1};  // concurrency hint: one io thread (DESIGN.md §9)

        const orch::net::ServerConfig& config = options.server;

        // Declared before the server so it outlives it: its destructor commits
        // what is still queued, and completions only ever reach a stopped ioc.
        std::optional<orch::Store> store;
        std::vector<orch::Job> restored;
        if (!options.db_path.empty()) {
            store.emplace(options.db_path);
            for (const std::string& doc : store->load_jobs()) {
                restored.push_back(orch::decode_job(doc));
            }
        } else {
            std::cerr << "warning: no --db, so job state will not survive a restart\n";
        }

        orch::metrics::Registry registry;
        orch::CoordinatorMetrics metrics(registry);
        orch::net::Server server(ioc, config, registry, metrics, store ? &*store : nullptr);
        if (!restored.empty()) {
            std::cerr << "restored " << restored.size() << " jobs from " << options.db_path << '\n';
            server.restore(std::move(restored));
        }
        if (store) {
            store->start(
                [&ioc](orch::Store::Task task) { boost::asio::post(ioc, std::move(task)); },
                // Fail-stop: the exception leaves ioc.run() and main exits 1,
                // rather than carrying on with state that is no longer persisted.
                [](std::string error) { throw std::runtime_error(error); });
        }

        boost::asio::signal_set signals(ioc, SIGINT, SIGTERM);
        signals.async_wait([&ioc](const boost::system::error_code&, int) { ioc.stop(); });

        server.start();
        // Tests parse this line to learn the port when started with --port 0.
        std::cout << "listening on " << config.bind_address << ":" << server.port() << std::endl;
        if (const auto admin = server.admin_port()) {
            std::cout << "admin listening on " << config.bind_address << ":" << *admin << std::endl;
        }
        std::cerr << "orch coordinator " << orch::version() << " started\n";

        ioc.run();
        std::cerr << "shutting down\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "coordinator: " << e.what() << '\n';
        return 1;
    }
}
