#include "edgerelay/policy.hpp"

#include <atomic>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
int checks = 0;
void check(bool condition, const char* expression, int line) {
    ++checks;
    if (!condition) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) check(static_cast<bool>(expression), #expression, __LINE__)

template<class F> bool invalid_argument(F&& action) {
    try { action(); }
    catch (const std::invalid_argument&) { return true; }
    return false;
}

using edgerelay::CircuitBreaker;
using edgerelay::TokenBucket;
using State = CircuitBreaker::State;

void token_refill_and_isolation() {
    TokenBucket limiter(2, 2);
    CHECK(limiter.allow("alice", 0));
    CHECK(limiter.allow("alice", 0));
    CHECK(!limiter.allow("alice", 0));
    CHECK(limiter.allow("bob", 0));
    CHECK(!limiter.allow("alice", 499));
    CHECK(limiter.allow("alice", 500));
    CHECK(!limiter.allow("alice", 500));
    CHECK(limiter.allow("alice", 1000));
    CHECK(limiter.allow("alice", 100000));
    CHECK(limiter.allow("alice", 100000));
    CHECK(!limiter.allow("alice", 100000));
    CHECK(limiter.tracked_clients() == 2);
}

void token_capacity_and_idle_reclamation() {
    TokenBucket limiter(1, 1, 2, 1000);
    CHECK(limiter.allow("a", 0));
    CHECK(limiter.allow("b", 100));
    CHECK(!limiter.allow("c", 999));
    CHECK(limiter.size() == 2);
    CHECK(limiter.allow("c", 1000)); // a is idle and fully replenished
    CHECK(limiter.size() == 2);
    CHECK(!limiter.allow("a", 1000)); // cannot reset a quota through churn
    CHECK(limiter.allow("a", 1100)); // b is now reclaimable

    TokenBucket short_ttl(1, 2, 1, 1);
    CHECK(short_ttl.allow("a", 0, 2));
    CHECK(!short_ttl.allow("b", 1)); // an idle but depleted entry is retained
    CHECK(!short_ttl.allow("b", 1999));
    CHECK(short_ttl.allow("b", 2000, 2));

    TokenBucket lru(1, 1, 2, 1000);
    CHECK(lru.allow("a", 0));
    CHECK(lru.allow("b", 100));
    CHECK(!lru.allow("a", 500)); // denial also marks this source active
    CHECK(lru.allow("c", 1100)); // b, not recently touched a, is reclaimed
    CHECK(lru.allow("a", 1100));
    CHECK(lru.size() == 2);
}

void token_invalid_inputs_and_clock_regression() {
    const auto nan = std::numeric_limits<double>::quiet_NaN();
    const auto inf = std::numeric_limits<double>::infinity();
    CHECK(invalid_argument([] { TokenBucket bucket(0, 1); }));
    CHECK(invalid_argument([] { TokenBucket bucket(1, 0); }));
    CHECK(invalid_argument([] { TokenBucket bucket(1, 1, 0); }));
    CHECK(invalid_argument([] { TokenBucket bucket(1, 1, 1, 0); }));
    CHECK(invalid_argument([&] { TokenBucket bucket(nan, 1); }));
    CHECK(invalid_argument([&] { TokenBucket bucket(1, inf); }));
    TokenBucket limiter(2, 1);
    CHECK(invalid_argument([&] { limiter.allow("a", 0, nan); }));
    CHECK(invalid_argument([&] { limiter.allow("a", 0, 0); }));
    CHECK(invalid_argument([&] { limiter.allow("a", 0, -1); }));
    CHECK(!limiter.allow("a", 0, 2));
    CHECK(limiter.size() == 0);
    CHECK(limiter.allow("a", 1000));
    CHECK(!limiter.allow("a", 500));
    CHECK(!limiter.allow("a", 1000));
    CHECK(limiter.allow("a", 1500));

    TokenBucket fractional(1, 1);
    CHECK(fractional.allow("a", 0, 0.5));
    CHECK(fractional.allow("a", 0, 0.5));
    CHECK(!fractional.allow("a", 0, 0.5));
    CHECK(fractional.allow("a", 500, 0.5));

    TokenBucket extreme(1, 1);
    CHECK(extreme.allow("a", std::numeric_limits<std::int64_t>::min()));
    CHECK(extreme.allow("a", std::numeric_limits<std::int64_t>::max()));
}

void token_concurrent_admission() {
    TokenBucket limiter(1, 137);
    std::atomic<int> allowed = 0;
    std::vector<std::thread> threads;
    for (int i = 0; i < 32; ++i) {
        threads.emplace_back([&] {
            for (int attempt = 0; attempt < 100; ++attempt) {
                if (limiter.allow("shared-client", 0)) ++allowed;
            }
        });
    }
    for (auto& thread : threads) thread.join();
    CHECK(allowed == 137);
    CHECK(limiter.size() == 1);

    TokenBucket capped(1, 1, 16);
    allowed = 0;
    threads.clear();
    for (int i = 0; i < 64; ++i) {
        threads.emplace_back([&, i] {
            if (capped.allow("source-" + std::to_string(i), 0)) ++allowed;
        });
    }
    for (auto& thread : threads) thread.join();
    CHECK(allowed == 16);
    CHECK(capped.size() == 16);
}

void breaker_threshold_reset_and_recovery() {
    CircuitBreaker breaker(2, 100);
    CHECK(breaker.state(0) == State::Closed);
    auto a = breaker.try_acquire(0);
    CHECK(a);
    breaker.record_failure(*a, 0);
    CHECK(breaker.state(0) == State::Closed);
    auto b = breaker.try_acquire(1);
    CHECK(b);
    breaker.record_success(*b); // consecutive failures reset on a good connect
    auto c = breaker.try_acquire(2);
    CHECK(c);
    breaker.record_failure(*c, 2);
    CHECK(breaker.state(2) == State::Closed);
    auto d = breaker.try_acquire(3);
    CHECK(d);
    breaker.record_failure(*d, 3);
    CHECK(breaker.state(3) == State::Open);
    CHECK(!breaker.try_acquire(102));
    CHECK(breaker.state(103) == State::HalfOpen);
    CHECK(breaker.state(103) == State::HalfOpen); // observation does not consume
    auto probe = breaker.try_acquire(103);
    CHECK(probe);
    CHECK(!breaker.try_acquire(103));
    CHECK(!breaker.try_acquire(10000)); // in-flight probe remains exclusive
    breaker.record_success(*probe);
    CHECK(breaker.state(10000) == State::Closed);
    CHECK(breaker.try_acquire(10000));
}

void breaker_probe_failure_restarts_cooldown() {
    CircuitBreaker breaker(1, 100);
    auto initial = breaker.try_acquire(0);
    CHECK(initial);
    breaker.record_failure(*initial, 0);
    auto probe = breaker.try_acquire(100);
    CHECK(probe);
    breaker.record_failure(*probe, 110);
    CHECK(breaker.state(209) == State::Open);
    CHECK(!breaker.try_acquire(209));
    CHECK(breaker.state(210) == State::HalfOpen);
    auto retry = breaker.try_acquire(210);
    CHECK(retry);
    breaker.record_success(*retry);
    CHECK(breaker.state(210) == State::Closed);
}

void breaker_discards_stale_and_foreign_results() {
    CircuitBreaker breaker(2, 100);
    auto first = breaker.try_acquire(0);
    auto second = breaker.try_acquire(0);
    auto stale_success = breaker.try_acquire(0);
    auto stale_in_probe = breaker.try_acquire(0);
    auto stale_after_recovery = breaker.try_acquire(0);
    CHECK(first && second && stale_success && stale_in_probe && stale_after_recovery);
    breaker.record_failure(*first, 1);
    breaker.record_failure(*second, 2);
    breaker.record_success(*stale_success);
    CHECK(breaker.state(2) == State::Open);
    auto probe = breaker.try_acquire(102);
    CHECK(probe);
    breaker.record_failure(*stale_in_probe, 103);
    CHECK(breaker.state(103) == State::HalfOpen);
    CHECK(!breaker.try_acquire(103));
    breaker.record_success(*probe);
    breaker.record_failure(*stale_after_recovery, 104);
    CHECK(breaker.state(104) == State::Closed);
    // One new-generation failure must not reach a threshold of two.
    auto fresh = breaker.try_acquire(104);
    CHECK(fresh);
    breaker.record_failure(*fresh, 104);
    CHECK(breaker.state(104) == State::Closed);

    CircuitBreaker other(1, 100);
    auto foreign = other.try_acquire(104);
    CHECK(foreign);
    breaker.record_failure(*foreign, 104);
    CHECK(breaker.state(104) == State::Closed);
    other.record_success(*foreign);
}

void breaker_concurrent_single_probe() {
    CircuitBreaker breaker(1, 100);
    auto first = breaker.try_acquire(0);
    CHECK(first);
    breaker.record_failure(*first, 0);
    std::atomic<int> probes = 0;
    std::mutex winner_mutex;
    std::optional<CircuitBreaker::Permit> winner;
    std::vector<std::thread> threads;
    for (int i = 0; i < 32; ++i) {
        threads.emplace_back([&] {
            auto permit = breaker.try_acquire(100);
            if (permit) {
                ++probes;
                std::lock_guard lock(winner_mutex);
                winner = *permit;
            }
        });
    }
    for (auto& thread : threads) thread.join();
    CHECK(probes == 1);
    CHECK(winner);
    breaker.record_success(*winner);
    CHECK(breaker.state(100) == State::Closed);
}

void breaker_invalid_inputs_and_clock_regression() {
    CHECK(invalid_argument([] { CircuitBreaker breaker(0, 100); }));
    CHECK(invalid_argument([] { CircuitBreaker breaker(1, 0); }));
    CHECK(invalid_argument([] { CircuitBreaker breaker(1, -1); }));
    CircuitBreaker breaker(1, 100);
    auto first = breaker.try_acquire(1000);
    CHECK(first);
    breaker.record_failure(*first, 500); // clamp regressing completion time
    CHECK(breaker.state(1099) == State::Open);
    CHECK(!breaker.try_acquire(1099));
    CHECK(breaker.try_acquire(1100));

    CircuitBreaker extreme(1, 100);
    auto low = extreme.try_acquire(std::numeric_limits<std::int64_t>::min());
    CHECK(low);
    extreme.record_failure(*low, std::numeric_limits<std::int64_t>::min());
    CHECK(extreme.try_acquire(std::numeric_limits<std::int64_t>::max()));
}
} // namespace

int main() {
    const std::vector<std::pair<const char*, std::function<void()>>> tests = {
        {"token refill and identity isolation", token_refill_and_isolation},
        {"bounded identities, idle retention, and LRU", token_capacity_and_idle_reclamation},
        {"token inputs, fractional costs, and clock edges", token_invalid_inputs_and_clock_regression},
        {"concurrent token admission and identity cap", token_concurrent_admission},
        {"breaker failure threshold, reset, and recovery", breaker_threshold_reset_and_recovery},
        {"failed probe cooldown restart", breaker_probe_failure_restarts_cooldown},
        {"stale and foreign breaker completions", breaker_discards_stale_and_foreign_results},
        {"concurrent exclusive recovery probe", breaker_concurrent_single_probe},
        {"breaker invalid inputs and clock edges", breaker_invalid_inputs_and_clock_regression},
    };
    try {
        for (const auto& [name, run] : tests) {
            run();
            std::cout << "PASS: " << name << '\n';
        }
        std::cout << tests.size() << " test cases, " << checks << " checks passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL after " << checks << " checks: " << error.what() << '\n';
        return 1;
    }
}
