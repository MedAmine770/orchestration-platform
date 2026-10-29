#include "orch/metrics.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <stdexcept>
#include <system_error>
#include <type_traits>

namespace orch::metrics {

namespace {

bool is_valid_name(std::string_view s, bool allow_colon) {
    const auto first_ok = [&](char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' ||
               (allow_colon && c == ':');
    };
    const auto rest_ok = [&](char c) { return first_ok(c) || (c >= '0' && c <= '9'); };
    return !s.empty() && first_ok(s.front()) && std::all_of(s.begin() + 1, s.end(), rest_ok);
}

void validate_labels(const Labels& labels) {
    for (const auto& [name, value] : labels) {
        if (!is_valid_name(name, /*allow_colon=*/false) || name.starts_with("__")) {
            throw std::invalid_argument("invalid label name: " + name);
        }
    }
}

// Shortest decimal that round-trips, e.g. 0.0005 -> "0.0005", 120.0 -> "120".
// Floating-point values use fixed notation: plain to_chars picks whichever form
// is shorter and would print 0.0005 as "5e-04", which is valid but makes `le`
// labels hard to read. Values too large for the buffer fall back to scientific.
template <typename T>
void append_number(std::string& out, T v) {
    std::array<char, 64> buf{};
    std::to_chars_result r{};
    if constexpr (std::is_floating_point_v<T>) {
        r = std::to_chars(buf.data(), buf.data() + buf.size(), v, std::chars_format::fixed);
        if (r.ec == std::errc::value_too_large) {
            r = std::to_chars(buf.data(), buf.data() + buf.size(), v);
        }
    } else {
        r = std::to_chars(buf.data(), buf.data() + buf.size(), v);
    }
    if (r.ec != std::errc{}) {
        throw std::runtime_error("metrics: number formatting failed");
    }
    out.append(buf.data(), r.ptr);
}

void append_help(std::string& out, std::string_view help) {
    for (const char c : help) {
        if (c == '\\') {
            out += "\\\\";
        } else if (c == '\n') {
            out += "\\n";
        } else {
            out += c;
        }
    }
}

void append_label_value(std::string& out, std::string_view value) {
    for (const char c : value) {
        if (c == '\\') {
            out += "\\\\";
        } else if (c == '"') {
            out += "\\\"";
        } else if (c == '\n') {
            out += "\\n";
        } else {
            out += c;
        }
    }
}

// Writes `name{k="v",...,le="x"} ` (braces omitted when there are no labels).
void append_series(std::string& out, std::string_view name, const Labels& labels,
                   std::string_view le = {}) {
    out += name;
    if (!labels.empty() || !le.empty()) {
        out += '{';
        bool first = true;
        for (const auto& [k, v] : labels) {
            if (!first) out += ',';
            first = false;
            out += k;
            out += "=\"";
            append_label_value(out, v);
            out += '"';
        }
        if (!le.empty()) {
            if (!first) out += ',';
            out += "le=\"";
            out += le;
            out += '"';
        }
        out += '}';
    }
    out += ' ';
}

std::string format_bound(double seconds) {
    std::string s;
    append_number(s, seconds);
    return s;
}

}  // namespace

// ---- Histogram --------------------------------------------------------------

Histogram::Histogram(std::span<const double> upper_bounds_seconds)
    : bounds_seconds_(upper_bounds_seconds.begin(), upper_bounds_seconds.end()),
      buckets_(upper_bounds_seconds.size() + 1) {
    bounds_ns_.reserve(bounds_seconds_.size());
    for (const double b : bounds_seconds_) {
        if (!std::isfinite(b) || b <= 0) {
            throw std::invalid_argument("histogram bounds must be finite and positive");
        }
        const auto ns = std::llround(b * 1e9);
        if (!bounds_ns_.empty() && ns <= bounds_ns_.back()) {
            throw std::invalid_argument("histogram bounds must be strictly increasing");
        }
        bounds_ns_.push_back(ns);
    }
}

void Histogram::observe(std::chrono::nanoseconds d) noexcept {
    const std::int64_t ns = std::max<std::int64_t>(d.count(), 0);
    // Prometheus buckets are inclusive upper bounds (le = "less than or equal"),
    // so the target is the first bound >= ns, or +Inf if there is none.
    const auto it = std::lower_bound(bounds_ns_.begin(), bounds_ns_.end(), ns);
    const auto index = static_cast<std::size_t>(it - bounds_ns_.begin());
    buckets_[index].fetch_add(1, std::memory_order_relaxed);
    sum_ns_.fetch_add(static_cast<std::uint64_t>(ns), std::memory_order_relaxed);
}

Histogram::Snapshot Histogram::snapshot() const {
    Snapshot s;
    s.cumulative.reserve(buckets_.size());
    std::uint64_t running = 0;
    for (const auto& bucket : buckets_) {
        running += bucket.load(std::memory_order_relaxed);
        s.cumulative.push_back(running);
    }
    s.count = running;
    s.sum_seconds = static_cast<double>(sum_ns_.load(std::memory_order_relaxed)) / 1e9;
    return s;
}

// ---- Registry ---------------------------------------------------------------

Registry::Family& Registry::family_for(std::string_view name, std::string_view help, Type type,
                                       const Labels& labels) {
    if (!is_valid_name(name, /*allow_colon=*/true)) {
        throw std::invalid_argument("invalid metric name: " + std::string(name));
    }
    if (type == Type::counter && !name.ends_with("_total")) {
        throw std::invalid_argument("counter name must end in _total: " + std::string(name));
    }
    validate_labels(labels);
    if (type == Type::histogram) {
        for (const auto& [k, v] : labels) {
            if (k == "le") throw std::invalid_argument("label 'le' is reserved for histograms");
        }
    }

    const auto it = std::find_if(families_.begin(), families_.end(),
                                 [&](const Family& f) { return f.name == name; });
    if (it == families_.end()) {
        return families_.emplace_back(Family{std::string(name), std::string(help), type, {}});
    }
    if (it->type != type || it->help != help) {
        throw std::invalid_argument("metric re-registered with a different type or help: " +
                                    std::string(name));
    }
    const bool duplicate = std::any_of(it->children.begin(), it->children.end(),
                                       [&](const Child& c) { return c.labels == labels; });
    if (duplicate) {
        throw std::invalid_argument("metric registered twice with the same labels: " +
                                    std::string(name));
    }
    return *it;
}

Counter& Registry::counter(std::string_view name, std::string_view help, Labels labels) {
    Family& f = family_for(name, help, Type::counter, labels);
    auto metric = std::make_unique<Counter>();
    Counter& ref = *metric;
    f.children.push_back(Child{std::move(labels), std::move(metric)});
    return ref;
}

Gauge& Registry::gauge(std::string_view name, std::string_view help, Labels labels) {
    Family& f = family_for(name, help, Type::gauge, labels);
    auto metric = std::make_unique<Gauge>();
    Gauge& ref = *metric;
    f.children.push_back(Child{std::move(labels), std::move(metric)});
    return ref;
}

Histogram& Registry::histogram(std::string_view name, std::string_view help,
                               std::span<const double> upper_bounds_seconds, Labels labels) {
    Family& f = family_for(name, help, Type::histogram, labels);
    auto metric = std::make_unique<Histogram>(upper_bounds_seconds);
    Histogram& ref = *metric;
    f.children.push_back(Child{std::move(labels), std::move(metric)});
    return ref;
}

std::string Registry::render() const {
    std::string out;
    for (const Family& f : families_) {
        out += "# HELP ";
        out += f.name;
        out += ' ';
        append_help(out, f.help);
        out += "\n# TYPE ";
        out += f.name;
        switch (f.type) {
            case Type::counter:
                out += " counter\n";
                break;
            case Type::gauge:
                out += " gauge\n";
                break;
            case Type::histogram:
                out += " histogram\n";
                break;
        }

        for (const Child& c : f.children) {
            switch (f.type) {
                case Type::counter:
                    append_series(out, f.name, c.labels);
                    append_number(out, std::get<std::unique_ptr<Counter>>(c.metric)->value());
                    out += '\n';
                    break;
                case Type::gauge:
                    append_series(out, f.name, c.labels);
                    append_number(out, std::get<std::unique_ptr<Gauge>>(c.metric)->value());
                    out += '\n';
                    break;
                case Type::histogram: {
                    const Histogram& h = *std::get<std::unique_ptr<Histogram>>(c.metric);
                    const Histogram::Snapshot s = h.snapshot();
                    const std::string bucket_name = f.name + "_bucket";
                    const auto bounds = h.upper_bounds();
                    for (std::size_t i = 0; i < bounds.size(); ++i) {
                        append_series(out, bucket_name, c.labels, format_bound(bounds[i]));
                        append_number(out, s.cumulative[i]);
                        out += '\n';
                    }
                    append_series(out, bucket_name, c.labels, "+Inf");
                    append_number(out, s.count);
                    out += '\n';
                    append_series(out, f.name + "_sum", c.labels);
                    append_number(out, s.sum_seconds);
                    out += '\n';
                    append_series(out, f.name + "_count", c.labels);
                    append_number(out, s.count);
                    out += '\n';
                    break;
                }
            }
        }
    }
    return out;
}

}  // namespace orch::metrics
