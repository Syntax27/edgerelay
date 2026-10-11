#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <list>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace edgerelay {

// Time arguments are monotonic milliseconds (normally steady_clock). Supplying
// them explicitly makes boundary behavior testable without sleeps. Regressions
// are clamped to the last observed time. All public operations are thread-safe.
class TokenBucket {
public:
    TokenBucket(double rate_per_second, double burst,
                std::size_t max_clients = 10000,
                std::int64_t idle_ttl_ms = 60000)
        : rate_(rate_per_second), burst_(burst), max_clients_(max_clients),
          idle_ttl_ms_(idle_ttl_ms) {
        if (!std::isfinite(rate_per_second) || rate_per_second <= 0 ||
            !std::isfinite(burst) || burst < 1 || max_clients == 0 ||
            idle_ttl_ms <= 0) {
            throw std::invalid_argument("invalid token bucket configuration");
        }
    }

    bool allow(const std::string& key, std::int64_t now_ms, double cost = 1) {
        if (!std::isfinite(cost) || cost <= 0) {
            throw std::invalid_argument("token cost must be finite and positive");
        }
        if (cost > burst_) return false;

        std::lock_guard lock(mutex_);
        now_ms = normalize_time(now_ms);
        auto found = clients_.find(key);
        if (found == clients_.end()) {
            // Reclaim only idle, fully refilled identities. Evicting a depleted
            // identity would let a reconnect reset its quota. LRU ordering and
            // the capacity bound avoid an unbounded scan for each new source.
            while (clients_.size() >= max_clients_ && !lru_.empty()) {
                const auto oldest = clients_.find(lru_.front());
                const auto idle_ms = elapsed(now_ms, oldest->second.last_seen);
                if (idle_ms < idle_ttl_ms_ || idle_ms * rate_ / 1000 < burst_) {
                    return false;
                }
                clients_.erase(oldest);
                lru_.pop_front();
            }
            lru_.push_back(key);
            try {
                found = clients_.emplace(
                    key, Entry{burst_, now_ms, now_ms, std::prev(lru_.end())}).first;
            } catch (...) {
                lru_.pop_back();
                throw;
            }
        }

        auto& entry = found->second;
        entry.tokens = std::min(burst_, entry.tokens +
            elapsed(now_ms, entry.last_refill) * rate_ / 1000);
        entry.last_refill = now_ms;
        entry.last_seen = now_ms;
        lru_.splice(lru_.end(), lru_, entry.position);
        if (entry.tokens < cost) return false;
        entry.tokens -= cost;
        return true;
    }

    std::size_t tracked_clients() const {
        std::lock_guard lock(mutex_);
        return clients_.size();
    }

    std::size_t size() const { return tracked_clients(); }

private:
    struct Entry {
        long double tokens;
        std::int64_t last_refill;
        std::int64_t last_seen;
        std::list<std::string>::iterator position;
    };

    static long double elapsed(std::int64_t now, std::int64_t before) {
        // Convert before subtracting so extreme injected timestamps cannot
        // overflow a signed integer.
        return static_cast<long double>(now) - static_cast<long double>(before);
    }

    std::int64_t normalize_time(std::int64_t now) {
        last_now_ = std::max(last_now_, now);
        return last_now_;
    }

    const long double rate_;
    const long double burst_;
    const std::size_t max_clients_;
    const std::int64_t idle_ttl_ms_;
    std::int64_t last_now_ = std::numeric_limits<std::int64_t>::min();
    mutable std::mutex mutex_;
    std::list<std::string> lru_;
    std::unordered_map<std::string, Entry> clients_;
};

class CircuitBreaker {
public:
    enum class State { Closed = 0, Open = 1, HalfOpen = 2 };

    // A permit belongs to exactly one breaker and generation. Finish every
    // acquired attempt once, with record_success or record_failure. A permit
    // from before a trip/recovery cannot change the newer breaker generation.
    class Permit {
        friend class CircuitBreaker;
        Permit(const CircuitBreaker* owner, std::uint64_t generation, bool probe)
            : owner_(owner), generation_(generation), probe_(probe) {}
        const CircuitBreaker* owner_;
        std::uint64_t generation_;
        bool probe_;
    };

    CircuitBreaker(std::size_t failure_threshold = 3,
                   std::int64_t cooldown_ms = 2000)
        : threshold_(failure_threshold), cooldown_ms_(cooldown_ms) {
        if (failure_threshold == 0 || cooldown_ms <= 0) {
            throw std::invalid_argument("invalid circuit breaker configuration");
        }
    }

    std::optional<Permit> try_acquire(std::int64_t now_ms) {
        std::lock_guard lock(mutex_);
        now_ms = normalize_time(now_ms);
        if (state_ == State::Closed) return Permit(this, generation_, false);
        if (state_ == State::Open && cooldown_elapsed(now_ms)) {
            state_ = State::HalfOpen;
            return Permit(this, generation_, true);
        }
        return std::nullopt;
    }

    void record_success(const Permit& permit) {
        std::lock_guard lock(mutex_);
        if (!current(permit)) return;
        if (state_ == State::Closed && !permit.probe_) {
            failures_ = 0;
        } else if (state_ == State::HalfOpen && permit.probe_) {
            state_ = State::Closed;
            failures_ = 0;
            ++generation_;
        }
    }

    void record_failure(const Permit& permit, std::int64_t now_ms) {
        std::lock_guard lock(mutex_);
        if (!current(permit)) return;
        now_ms = normalize_time(now_ms);
        if (state_ == State::Closed && !permit.probe_) {
            if (++failures_ >= threshold_) trip(now_ms);
        } else if (state_ == State::HalfOpen && permit.probe_) {
            trip(now_ms);
        }
    }

    // Observing the recovery window never acquires its one permitted probe.
    State state(std::int64_t now_ms) const {
        std::lock_guard lock(mutex_);
        now_ms = std::max(last_now_, now_ms);
        if (state_ == State::Open && cooldown_elapsed(now_ms)) {
            return State::HalfOpen;
        }
        return state_;
    }

private:
    bool current(const Permit& permit) const {
        return permit.owner_ == this && permit.generation_ == generation_;
    }

    std::int64_t normalize_time(std::int64_t now) {
        last_now_ = std::max(last_now_, now);
        return last_now_;
    }

    bool cooldown_elapsed(std::int64_t now) const {
        return static_cast<long double>(now) -
               static_cast<long double>(opened_at_) >= cooldown_ms_;
    }

    void trip(std::int64_t now) {
        state_ = State::Open;
        opened_at_ = now;
        ++generation_;
    }

    const std::size_t threshold_;
    const std::int64_t cooldown_ms_;
    mutable std::mutex mutex_;
    State state_ = State::Closed;
    std::size_t failures_ = 0;
    std::uint64_t generation_ = 0;
    std::int64_t opened_at_ = 0;
    std::int64_t last_now_ = std::numeric_limits<std::int64_t>::min();
};

} // namespace edgerelay
