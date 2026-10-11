#include "net.hpp"
#include "edgerelay/policy.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {
namespace net = edgerelay::net;
using Clock = std::chrono::steady_clock;
std::atomic_bool stopping{false};
static_assert(std::atomic_bool::is_always_lock_free, "Signal shutdown requires lock-free atomic bool");
void on_signal(int) { stopping.store(true, std::memory_order_relaxed); }
std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count();
}

struct Config {
    std::uint16_t listen_port = 8080, metrics_port = 9090;
    std::vector<std::uint16_t> ports{9001, 9002};
    int workers = 4, queue_capacity = 16, connect_timeout_ms = 300, idle_timeout_ms = 3000;
    int breaker_threshold = 2, breaker_cooldown_ms = 1000, rate = 100, burst = 100;
};
int number(const std::string& text, int minimum, int maximum) {
    if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
        throw std::invalid_argument("Expected a positive integer: " + text);
    std::size_t consumed = 0;
    const long long value = std::stoll(text, &consumed);
    if (consumed != text.size() || value < minimum || value > maximum)
        throw std::invalid_argument("Value outside permitted range: " + text);
    return static_cast<int>(value);
}
Config parse(int argc, char** argv) {
    Config config;
    for (int i = 1; i < argc; ++i) {
        const std::string option = argv[i];
        if (option == "--help") {
            std::cout << "EdgeRelay: loopback TCP load balancer\n"
                         "  --listen-port PORT          default 8080\n"
                         "  --upstreams HOST:PORT,...   default 127.0.0.1:9001,127.0.0.1:9002\n"
                         "  --metrics-port PORT         default 9090; GET /metrics and /health\n"
                         "  --workers N                 1..64, default 4\n"
                         "  --queue-capacity N          1..512, default 16\n"
                         "  --connect-timeout-ms N      1..60000, default 300 per attempted upstream\n"
                         "  --idle-timeout-ms N         1..600000, default 3000\n"
                         "  --breaker-threshold N       1..1000, default 2\n"
                         "  --breaker-cooldown-ms N     1..600000, default 1000\n"
                         "  --rate N                    connections/source/second, default 100\n"
                         "  --burst N                   connections/source burst, default 100\n"
                         "All addresses are IPv4 loopback. No TLS termination or HTTP routing.\n";
            std::exit(0);
        }
        if (i + 1 == argc) throw std::invalid_argument("Missing value for " + option);
        const std::string value = argv[++i];
        if (option == "--listen-port") config.listen_port = static_cast<std::uint16_t>(number(value, 1, 65535));
        else if (option == "--metrics-port") config.metrics_port = static_cast<std::uint16_t>(number(value, 1, 65535));
        else if (option == "--workers") config.workers = number(value, 1, 64);
        else if (option == "--queue-capacity") config.queue_capacity = number(value, 1, 512);
        else if (option == "--connect-timeout-ms") config.connect_timeout_ms = number(value, 1, 60000);
        else if (option == "--idle-timeout-ms") config.idle_timeout_ms = number(value, 1, 600000);
        else if (option == "--breaker-threshold") config.breaker_threshold = number(value, 1, 1000);
        else if (option == "--breaker-cooldown-ms") config.breaker_cooldown_ms = number(value, 1, 600000);
        else if (option == "--rate") config.rate = number(value, 1, 1000000);
        else if (option == "--burst") config.burst = number(value, 1, 1000000);
        else if (option == "--upstreams") {
            config.ports.clear();
            std::istringstream endpoints(value);
            std::string endpoint;
            if (value.empty() || value.back() == ',') throw std::invalid_argument("Empty upstream");
            while (std::getline(endpoints, endpoint, ',')) {
                constexpr auto prefix = "127.0.0.1:";
                if (endpoint.rfind(prefix, 0) != 0) throw std::invalid_argument("Upstreams must be 127.0.0.1:PORT");
                const auto port = static_cast<std::uint16_t>(number(endpoint.substr(std::strlen(prefix)), 1, 65535));
                if (std::find(config.ports.begin(), config.ports.end(), port) != config.ports.end())
                    throw std::invalid_argument("Duplicate upstream port");
                config.ports.push_back(port);
                if (config.ports.size() > 32) throw std::invalid_argument("At most 32 upstreams permitted");
            }
        } else throw std::invalid_argument("Unknown option: " + option);
    }
    if (config.listen_port == config.metrics_port) throw std::invalid_argument("Listener and metrics ports must differ");
    for (auto port : config.ports)
        if (port == config.listen_port || port == config.metrics_port)
            throw std::invalid_argument("Upstream port must differ from EdgeRelay listeners");
    return config;
}

class Queue {
public:
    explicit Queue(std::size_t capacity) : capacity_(capacity) {}
    bool try_push(net::Socket&& socket) {
        std::lock_guard lock(mutex_);
        if (closed_ || sockets_.size() >= capacity_) return false;
        sockets_.push_back(std::move(socket));
        available_.notify_one();
        return true;
    }
    std::optional<net::Socket> pop() {
        std::unique_lock lock(mutex_);
        available_.wait(lock, [this] { return closed_ || !sockets_.empty(); });
        if (closed_) return std::nullopt;
        auto socket = std::move(sockets_.front());
        sockets_.pop_front();
        return socket;
    }
    std::size_t size() const { std::lock_guard lock(mutex_); return sockets_.size(); }
    void close() {
        std::lock_guard lock(mutex_);
        closed_ = true;
        sockets_.clear();
        available_.notify_all();
    }
private:
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable available_;
    std::deque<net::Socket> sockets_;
    bool closed_ = false;
};

struct Counters {
    std::atomic<std::uint64_t> accepted{0}, rate_rejected{0}, queue_rejected{0}, no_upstream{0};
    std::atomic<std::uint64_t> active{0}, client_bytes{0}, upstream_bytes{0}, idle_timeouts{0};
};
struct Upstream {
    Upstream(std::uint16_t value, const Config& config)
        : port(value), breaker(config.breaker_threshold, config.breaker_cooldown_ms) {}
    std::uint16_t port;
    edgerelay::CircuitBreaker breaker;
    std::atomic<std::uint64_t> attempts{0}, failures{0};
};
using Upstreams = std::vector<std::unique_ptr<Upstream>>;

// Two fixed buffers provide backpressure. A full destination buffer disables
// reads from its source until some bytes have actually been sent onward.
struct Buffer {
    std::array<char, 65536> bytes{};
    std::size_t begin = 0, end = 0;
    std::size_t size() const { return end - begin; }
    std::size_t free() const { return bytes.size() - size(); }
    void compact() {
        if (begin) { const auto length = size(); std::memmove(bytes.data(), bytes.data() + begin, length); begin = 0; end = length; }
    }
    void consume(std::size_t count) { begin += count; if (begin == end) begin = end = 0; }
};
bool read_into(net::Handle source, Buffer& buffer, bool& eof, std::int64_t& last_activity) {
    buffer.compact();
    const int count = net::receive(source, buffer.bytes.data() + buffer.end, buffer.bytes.size() - buffer.end);
    if (count > 0) { buffer.end += static_cast<std::size_t>(count); last_activity = now_ms(); return true; }
    if (count == 0) { eof = true; return true; }
    return net::would_block();
}
bool write_from(net::Handle destination, Buffer& buffer, std::atomic<std::uint64_t>& total, std::int64_t& last_activity) {
    const int count = net::send(destination, buffer.bytes.data() + buffer.begin, buffer.size());
    if (count > 0) {
        buffer.consume(static_cast<std::size_t>(count));
        total.fetch_add(static_cast<std::uint64_t>(count), std::memory_order_relaxed);
        last_activity = now_ms();
        return true;
    }
    return count < 0 && net::would_block();
}
void relay(net::Handle client, net::Handle upstream, const Config& config, Counters& counters) {
    Buffer to_upstream, to_client;
    bool client_eof = false, upstream_eof = false, client_write_closed = false, upstream_write_closed = false;
    auto last_activity = now_ms();
    while (!stopping.load(std::memory_order_relaxed)) {
        if (client_eof && to_upstream.size() == 0 && !upstream_write_closed) { net::shutdown_write(upstream); upstream_write_closed = true; }
        if (upstream_eof && to_client.size() == 0 && !client_write_closed) { net::shutdown_write(client); client_write_closed = true; }
        if (client_eof && upstream_eof && !to_upstream.size() && !to_client.size()) return;
        const auto remaining = config.idle_timeout_ms - (now_ms() - last_activity);
        if (remaining <= 0) { ++counters.idle_timeouts; return; }
        const auto ready = net::wait(client, !client_eof && to_upstream.free(), to_client.size() > 0,
                                     upstream, !upstream_eof && to_client.free(), to_upstream.size() > 0,
                                     static_cast<int>(std::min<std::int64_t>(remaining, 100)));
        if (ready.error) return;
        if (ready.a_write && !write_from(client, to_client, counters.upstream_bytes, last_activity)) return;
        if (ready.b_write && !write_from(upstream, to_upstream, counters.client_bytes, last_activity)) return;
        if (ready.a_read && !read_into(client, to_upstream, client_eof, last_activity)) return;
        if (ready.b_read && !read_into(upstream, to_client, upstream_eof, last_activity)) return;
    }
}
void worker(Queue& queue, Upstreams& upstreams, const Config& config, Counters& counters, std::atomic<std::size_t>& next) {
    while (auto client = queue.pop()) {
        ++counters.active;
        struct ActiveGuard { Counters& counters; ~ActiveGuard() { --counters.active; } } guard{counters};
        net::Socket connection;
        const auto start = next.fetch_add(1, std::memory_order_relaxed) % upstreams.size();
        for (std::size_t offset = 0; offset < upstreams.size() && !stopping.load(std::memory_order_relaxed); ++offset) {
            auto& upstream = *upstreams[(start + offset) % upstreams.size()];
            auto permit = upstream.breaker.try_acquire(now_ms());
            if (!permit) continue;
            ++upstream.attempts;
            connection = net::connect_loopback(upstream.port, config.connect_timeout_ms);
            if (connection) { upstream.breaker.record_success(*permit); break; }
            ++upstream.failures;
            upstream.breaker.record_failure(*permit, now_ms());
        }
        if (connection) relay(client->get(), connection.get(), config, counters);
        else ++counters.no_upstream;
    }
}

std::string metrics(const Counters& counters, const Queue& queue, Upstreams& upstreams) {
    std::ostringstream out;
    out << "# TYPE edgerelay_connections_accepted_total counter\n"
        << "edgerelay_connections_accepted_total " << counters.accepted.load() << '\n'
        << "# TYPE edgerelay_connections_rejected_total counter\n"
        << "edgerelay_connections_rejected_total{reason=\"rate_limit\"} " << counters.rate_rejected.load() << '\n'
        << "edgerelay_connections_rejected_total{reason=\"queue_full\"} " << counters.queue_rejected.load() << '\n'
        << "# TYPE edgerelay_connections_no_upstream_total counter\n"
        << "edgerelay_connections_no_upstream_total " << counters.no_upstream.load() << '\n'
        << "# TYPE edgerelay_active_connections gauge\n"
        << "edgerelay_active_connections " << counters.active.load() << '\n'
        << "# TYPE edgerelay_queue_depth gauge\n"
        << "edgerelay_queue_depth " << queue.size() << '\n'
        << "# TYPE edgerelay_bytes_total counter\n"
        << "edgerelay_bytes_total{direction=\"client_to_upstream\"} " << counters.client_bytes.load() << '\n'
        << "edgerelay_bytes_total{direction=\"upstream_to_client\"} " << counters.upstream_bytes.load() << '\n'
        << "# TYPE edgerelay_idle_timeouts_total counter\n"
        << "edgerelay_idle_timeouts_total " << counters.idle_timeouts.load() << '\n'
        << "# TYPE edgerelay_upstream_connect_attempts_total counter\n";
    for (auto& item : upstreams)
        out << "edgerelay_upstream_connect_attempts_total{upstream=\"127.0.0.1:" << item->port << "\"} " << item->attempts.load() << '\n';
    out << "# TYPE edgerelay_upstream_connect_failures_total counter\n";
    for (auto& item : upstreams)
        out << "edgerelay_upstream_connect_failures_total{upstream=\"127.0.0.1:" << item->port << "\"} " << item->failures.load() << '\n';
    out << "# TYPE edgerelay_circuit_state gauge\n";
    for (auto& item : upstreams)
        out << "edgerelay_circuit_state{upstream=\"127.0.0.1:" << item->port << "\"} " << static_cast<int>(item->breaker.state(now_ms())) << '\n';
    return out.str();
}
void respond(net::Handle socket, const std::string& status, const std::string& body, bool prometheus = false) {
    const std::string response = "HTTP/1.1 " + status + "\r\nContent-Type: " +
        (prometheus ? "text/plain; version=0.0.4; charset=utf-8" : "text/plain; charset=utf-8") +
        "\r\nConnection: close\r\nContent-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body;
    std::size_t sent = 0;
    const auto deadline = now_ms() + 500;
    while (sent < response.size() && !stopping.load(std::memory_order_relaxed) && now_ms() < deadline) {
        const auto ready = net::wait(socket, false, true, net::invalid, false, false, 50);
        if (ready.error) return;
        if (!ready.a_write) continue;
        const int count = net::send(socket, response.data() + sent, response.size() - sent);
        if (count > 0) sent += static_cast<std::size_t>(count);
        else if (count == 0 || !net::would_block()) return;
    }
}
void serve_metrics(net::Handle listener, Counters& counters, Queue& queue, Upstreams& upstreams) {
    while (!stopping.load(std::memory_order_relaxed)) {
        const auto ready = net::wait(listener, true, false, net::invalid, false, false, 100);
        if (ready.error) throw std::runtime_error("Metrics listener wait failed");
        if (!ready.a_read) continue;
        std::string source;
        auto client = net::accept(listener, source);
        if (!client) continue;
        std::array<char, 1024> request{};
        std::size_t received = 0;
        const auto deadline = now_ms() + 500;
        bool complete = false;
        while (received < request.size() && now_ms() < deadline && !stopping.load(std::memory_order_relaxed)) {
            const auto input = net::wait(client.get(), true, false, net::invalid, false, false, 50);
            if (input.error) break;
            if (!input.a_read) continue;
            const int count = net::receive(client.get(), request.data() + received, request.size() - received);
            if (count > 0) {
                received += static_cast<std::size_t>(count);
                if (std::string_view(request.data(), received).find("\r\n\r\n") != std::string_view::npos) { complete = true; break; }
            } else if (count == 0 || !net::would_block()) break;
        }
        if (!complete) { respond(client.get(), "400 Bad Request", "Incomplete or oversized request\n"); continue; }
        const std::string_view raw(request.data(), received);
        const auto line = raw.substr(0, raw.find("\r\n"));
        if (line == "GET /metrics HTTP/1.1" || line == "GET /metrics HTTP/1.0")
            respond(client.get(), "200 OK", metrics(counters, queue, upstreams), true);
        else if (line == "GET /health HTTP/1.1" || line == "GET /health HTTP/1.0")
            respond(client.get(), "200 OK", "ok\n");
        else respond(client.get(), "404 Not Found", "Not found\n");
    }
}
int run(const Config& config) {
    auto listener = net::listen_loopback(config.listen_port);
    auto metrics_listener = net::listen_loopback(config.metrics_port);
    Queue queue(static_cast<std::size_t>(config.queue_capacity));
    Counters counters;
    Upstreams upstreams;
    for (auto port : config.ports) upstreams.push_back(std::make_unique<Upstream>(port, config));
    std::atomic<std::size_t> next{0};
    std::atomic_bool runtime_failed{false};
    edgerelay::TokenBucket limiter(config.rate, config.burst, 4096, 60000);
    std::vector<std::thread> workers;
    std::thread metrics_thread;
    auto cleanup = [&] {
        stopping.store(true, std::memory_order_relaxed);
        queue.close();
        for (auto& thread : workers) if (thread.joinable()) thread.join();
        if (metrics_thread.joinable()) metrics_thread.join();
    };
    try {
        for (int i = 0; i < config.workers; ++i)
            workers.emplace_back([&] {
                try { worker(queue, upstreams, config, counters, next); }
                catch (const std::exception& error) { std::cerr << "Worker failed: " << error.what() << '\n'; runtime_failed = true; stopping = true; }
            });
        metrics_thread = std::thread([&] {
            try { serve_metrics(metrics_listener.get(), counters, queue, upstreams); }
            catch (const std::exception& error) { std::cerr << "Metrics failed: " << error.what() << '\n'; runtime_failed = true; stopping = true; }
        });
        std::cout << "EdgeRelay listening on 127.0.0.1:" << config.listen_port
                  << "; metrics on 127.0.0.1:" << config.metrics_port
                  << "; workers=" << config.workers << "; queue=" << config.queue_capacity << std::endl;
        while (!stopping.load(std::memory_order_relaxed)) {
            const auto ready = net::wait(listener.get(), true, false, net::invalid, false, false, 100);
            if (ready.error) throw std::runtime_error("TCP listener wait failed");
            if (!ready.a_read) continue;
            std::string source;
            auto client = net::accept(listener.get(), source);
            if (!client) continue;
            ++counters.accepted;
            if (!limiter.allow(source, now_ms())) { ++counters.rate_rejected; continue; }
            if (!queue.try_push(std::move(client))) ++counters.queue_rejected;
        }
    } catch (...) { cleanup(); throw; }
    cleanup();
    return runtime_failed ? 1 : 0;
}
} // namespace

int main(int argc, char** argv) {
    try {
        const auto config = parse(argc, argv);
        net::initialize();
        struct NetworkGuard { ~NetworkGuard() { net::cleanup(); } } guard;
        std::signal(SIGINT, on_signal);
        std::signal(SIGTERM, on_signal);
        return run(config);
    } catch (const std::exception& error) {
        std::cerr << "EdgeRelay: " << error.what() << '\n';
        return 1;
    }
}
