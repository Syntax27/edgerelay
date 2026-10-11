"""Standard-library helpers shared by the integration tests and benchmark."""
from contextlib import contextmanager
import http.client
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class EchoServer:
    def __init__(self, port=0, reply_after_eof=False):
        self.socket = socket.socket()
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('127.0.0.1', port))
        self.port = self.socket.getsockname()[1]
        self.socket.listen(128)
        self.socket.settimeout(0.1)
        self.reply_after_eof = reply_after_eof
        self.stop_event = threading.Event()
        self.connections = 0
        self.clients = set()
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stop_event.is_set():
            try:
                client, _ = self.socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self.lock:
                self.clients.add(client)
                self.connections += 1
            threading.Thread(target=self.handle, args=(client,), daemon=True).start()

    def handle(self, client):
        collected = bytearray()
        try:
            client.settimeout(10)
            while True:
                data = client.recv(65536)
                if not data:
                    if self.reply_after_eof:
                        time.sleep(0.02)
                        client.sendall(bytes(collected)[::-1])
                    break
                if self.reply_after_eof:
                    collected.extend(data)
                else:
                    client.sendall(data)
        except OSError:
            pass
        finally:
            with self.lock:
                self.clients.discard(client)
            client.close()

    def close(self):
        self.stop_event.set()
        self.socket.close()
        with self.lock:
            clients = list(self.clients)
        for client in clients:
            try:
                client.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            client.close()
        self.thread.join(timeout=2)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def get_metrics(port):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
    try:
        connection.request('GET', '/metrics')
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(f'Metrics status {response.status}')
        return {line.rsplit(' ', 1)[0]: float(line.rsplit(' ', 1)[1])
                for line in response.read().decode().splitlines()
                if line and not line.startswith('#')}
    finally:
        connection.close()


def wait_until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, http.client.HTTPException):
            pass
        time.sleep(0.02)
    raise AssertionError('Condition was not met before timeout.')


@contextmanager
def running_relay(binary, upstream_ports, **options):
    listen_port, metrics_port = free_port(), free_port()
    while listen_port in upstream_ports:
        listen_port = free_port()
    while metrics_port == listen_port or metrics_port in upstream_ports:
        metrics_port = free_port()
    values = {'listen-port': listen_port, 'metrics-port': metrics_port,
              'upstreams': ','.join(f'127.0.0.1:{p}' for p in upstream_ports),
              'workers': 4, 'queue-capacity': 32,
              'connect-timeout-ms': 250, 'idle-timeout-ms': 3000,
              'breaker-threshold': 2, 'breaker-cooldown-ms': 300,
              'rate': 10000, 'burst': 10000}
    values.update({key.replace('_', '-'): value for key, value in options.items()})
    command = [str(Path(binary).resolve())]
    for key, value in values.items():
        command += ['--' + key, str(value)]
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        try:
            def ready():
                if process.poll() is not None:
                    log.seek(0)
                    raise RuntimeError('EdgeRelay failed to start: ' + log.read().decode(errors='replace'))
                return get_metrics(metrics_port)
            wait_until(ready)
            yield listen_port, metrics_port, process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def exchange(port, payload, expected=None):
    expected = payload if expected is None else expected
    with socket.create_connection(('127.0.0.1', port), timeout=5) as client:
        client.settimeout(5)
        # Send and receive concurrently so large payloads exercise real backpressure.
        failures = []
        def send():
            try:
                client.sendall(payload)
                client.shutdown(socket.SHUT_WR)
            except OSError as error:
                failures.append(error)
        sender = threading.Thread(target=send, daemon=True)
        sender.start()
        received = bytearray()
        while True:
            block = client.recv(65536)
            if not block:
                break
            received.extend(block)
        sender.join(timeout=6)
        if sender.is_alive() or failures:
            raise AssertionError(f'Client send failed: {failures}')
        if bytes(received) != expected:
            raise AssertionError(f'Payload mismatch: expected {len(expected)}, received {len(received)} bytes')
    return len(received)
