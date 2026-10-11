#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#endif

namespace edgerelay::net {
#ifdef _WIN32
using Handle = SOCKET;
inline constexpr Handle invalid = INVALID_SOCKET;
#else
using Handle = int;
inline constexpr Handle invalid = -1;
#endif

void initialize();
void cleanup();
void close(Handle socket);
class Socket {
public:
    explicit Socket(Handle value = invalid) : value_(value) {}
    ~Socket() { close(value_); }
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    Socket(Socket&& other) noexcept : value_(other.release()) {}
    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) { close(value_); value_ = other.release(); }
        return *this;
    }
    Handle get() const { return value_; }
    explicit operator bool() const { return value_ != invalid; }
private:
    Handle release() { const auto result = value_; value_ = invalid; return result; }
    Handle value_;
};

struct Ready { bool a_read = false, a_write = false, b_read = false, b_write = false; bool error = false; };
Ready wait(Handle a, bool a_read, bool a_write, Handle b, bool b_read, bool b_write, int timeout_ms);
Socket listen_loopback(std::uint16_t port);
Socket accept(Handle listener, std::string& source);
Socket connect_loopback(std::uint16_t port, int timeout_ms);
int receive(Handle socket, char* buffer, std::size_t count);
int send(Handle socket, const char* buffer, std::size_t count);
bool would_block();
void shutdown_write(Handle socket);
} // namespace edgerelay::net
