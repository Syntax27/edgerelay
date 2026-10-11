#include "net.hpp"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <stdexcept>
#ifndef _WIN32
#include <fcntl.h>
#include <poll.h>
#endif

namespace edgerelay::net {
namespace {
bool nonblocking(Handle socket) {
#ifdef _WIN32
    u_long enabled = 1;
    return ioctlsocket(socket, FIONBIO, &enabled) == 0;
#else
    const int flags = fcntl(socket, F_GETFL, 0);
    return flags >= 0 && fcntl(socket, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
}
sockaddr_in address(std::uint16_t port) {
    sockaddr_in result{};
    result.sin_family = AF_INET;
    result.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    result.sin_port = htons(port);
    return result;
}
} // namespace

void initialize() {
#ifdef _WIN32
    WSADATA data{};
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) throw std::runtime_error("Winsock initialization failed");
#endif
}
void cleanup() {
#ifdef _WIN32
    WSACleanup();
#endif
}
void close(Handle socket) {
    if (socket == invalid) return;
#ifdef _WIN32
    closesocket(socket);
#else
    ::close(socket);
#endif
}
bool would_block() {
#ifdef _WIN32
    const int error = WSAGetLastError();
    return error == WSAEWOULDBLOCK || error == WSAEINPROGRESS || error == WSAEINTR;
#else
    return errno == EWOULDBLOCK || errno == EAGAIN || errno == EINPROGRESS || errno == EINTR;
#endif
}
Ready wait(Handle a, bool ar, bool aw, Handle b, bool br, bool bw, int timeout_ms) {
    Ready result;
#ifdef _WIN32
    fd_set readers, writers, errors;
    FD_ZERO(&readers); FD_ZERO(&writers); FD_ZERO(&errors);
    if (a != invalid) { if (ar) FD_SET(a, &readers); if (aw) FD_SET(a, &writers); FD_SET(a, &errors); }
    if (b != invalid) { if (br) FD_SET(b, &readers); if (bw) FD_SET(b, &writers); FD_SET(b, &errors); }
    timeval timeout{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    const int count = select(0, &readers, &writers, &errors, &timeout);
    if (count < 0) { result.error = !would_block(); return result; }
    if (count == 0) return result;
    result.a_read = a != invalid && FD_ISSET(a, &readers);
    result.a_write = a != invalid && FD_ISSET(a, &writers);
    result.b_read = b != invalid && FD_ISSET(b, &readers);
    result.b_write = b != invalid && FD_ISSET(b, &writers);
    result.error = (a != invalid && FD_ISSET(a, &errors)) || (b != invalid && FD_ISSET(b, &errors));
#else
    pollfd sockets[2]{{a, static_cast<short>((ar ? POLLIN : 0) | (aw ? POLLOUT : 0)), 0},
                      {b, static_cast<short>((br ? POLLIN : 0) | (bw ? POLLOUT : 0)), 0}};
    // Do not poll an endpoint after EOF with no pending write: POLLHUP would spin.
    if (!ar && !aw) sockets[0].fd = -1;
    if (!br && !bw) sockets[1].fd = -1;
    const int count = poll(sockets, 2, timeout_ms);
    if (count < 0) { result.error = errno != EINTR; return result; }
    if (count == 0) return result;
    result.a_read = ar && (sockets[0].revents & (POLLIN | POLLHUP));
    result.a_write = aw && (sockets[0].revents & POLLOUT);
    result.b_read = br && (sockets[1].revents & (POLLIN | POLLHUP));
    result.b_write = bw && (sockets[1].revents & POLLOUT);
    result.error = (sockets[0].revents & (POLLERR | POLLNVAL)) || (sockets[1].revents & (POLLERR | POLLNVAL));
#endif
    return result;
}
Socket listen_loopback(std::uint16_t port) {
    Socket socket(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    if (!socket) throw std::runtime_error("Cannot allocate listener");
    int enabled = 1;
#ifdef _WIN32
    const int option = SO_EXCLUSIVEADDRUSE;
#else
    const int option = SO_REUSEADDR;
#endif
    if (setsockopt(socket.get(), SOL_SOCKET, option, reinterpret_cast<const char*>(&enabled), sizeof(enabled)) != 0)
        throw std::runtime_error("Cannot configure listener");
    const auto endpoint = address(port);
    if (::bind(socket.get(), reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) != 0 ||
        ::listen(socket.get(), 128) != 0 || !nonblocking(socket.get()))
        throw std::runtime_error("Cannot bind 127.0.0.1:" + std::to_string(port));
    return socket;
}
Socket accept(Handle listener, std::string& source) {
    sockaddr_in peer{};
#ifdef _WIN32
    int length = sizeof(peer);
#else
    socklen_t length = sizeof(peer);
#endif
    Socket socket(::accept(listener, reinterpret_cast<sockaddr*>(&peer), &length));
    if (!socket) return Socket{};
    if (!nonblocking(socket.get())) return Socket{};
    char text[INET_ADDRSTRLEN]{};
    if (!inet_ntop(AF_INET, &peer.sin_addr, text, sizeof(text))) return Socket{};
    source = text;
    return socket;
}
Socket connect_loopback(std::uint16_t port, int timeout_ms) {
    Socket socket(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    if (!socket || !nonblocking(socket.get())) return Socket{};
    const auto endpoint = address(port);
    if (::connect(socket.get(), reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) != 0) {
        if (!would_block()) return Socket{};
        const auto ready = wait(socket.get(), false, true, invalid, false, false, timeout_ms);
        if (!ready.a_write || ready.error) return Socket{};
        int error = 0;
#ifdef _WIN32
        int length = sizeof(error);
#else
        socklen_t length = sizeof(error);
#endif
        if (getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &length) != 0 || error != 0)
            return Socket{};
    }
    return socket;
}
int receive(Handle socket, char* buffer, std::size_t count) {
    return static_cast<int>(::recv(socket, buffer, static_cast<int>(std::min(count, static_cast<std::size_t>(INT_MAX))), 0));
}
int send(Handle socket, const char* buffer, std::size_t count) {
#ifdef MSG_NOSIGNAL
    constexpr int flags = MSG_NOSIGNAL;
#else
    constexpr int flags = 0;
#endif
    return static_cast<int>(::send(socket, buffer, static_cast<int>(std::min(count, static_cast<std::size_t>(INT_MAX))), flags));
}
void shutdown_write(Handle socket) {
#ifdef _WIN32
    ::shutdown(socket, SD_SEND);
#else
    ::shutdown(socket, SHUT_WR);
#endif
}
} // namespace edgerelay::net
