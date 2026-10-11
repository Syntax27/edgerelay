"""Exercise the compiled relay over real loopback TCP sockets."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import socket
import subprocess
import sys
import time
import unittest

from support import EchoServer, exchange, free_port, get_metrics, running_relay, wait_until

parser = argparse.ArgumentParser()
parser.add_argument('--binary', required=True)
args, remaining = parser.parse_known_args()
BINARY = args.binary


class RelayIntegration(unittest.TestCase):
    def test_binary_integrity_and_byte_counters(self):
        payload = bytes(range(256)) * 1024
        with EchoServer() as upstream, running_relay(BINARY, [upstream.port]) as (port, metrics_port, _):
            exchange(port, payload)
            metrics = wait_until(lambda: get_metrics(metrics_port) if get_metrics(metrics_port)['edgerelay_active_connections'] == 0 else None)
            self.assertEqual(metrics['edgerelay_bytes_total{direction="client_to_upstream"}'], len(payload))
            self.assertEqual(metrics['edgerelay_bytes_total{direction="upstream_to_client"}'], len(payload))

    def test_half_close_waits_for_delayed_reply(self):
        payload = b'half-close reply must survive\x00' * 2048
        with EchoServer(reply_after_eof=True) as upstream, running_relay(BINARY, [upstream.port]) as (port, _, _):
            exchange(port, payload, payload[::-1])

    def test_round_robin_distribution(self):
        with EchoServer() as first, EchoServer() as second, running_relay(BINARY, [first.port, second.port]) as (port, _, _):
            for _ in range(8):
                exchange(port, b'balanced')
            self.assertEqual(first.connections, 4)
            self.assertEqual(second.connections, 4)

    def test_concurrent_clients_keep_payloads_separate(self):
        with EchoServer() as upstream, running_relay(BINARY, [upstream.port], workers=4) as (port, _, _):
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(lambda i: exchange(port, bytes([i])*16384), range(16)))
            self.assertEqual(results, [16384]*16)

    def test_failed_upstream_is_skipped_then_recovers(self):
        unavailable = free_port()
        label = f'{{upstream="127.0.0.1:{unavailable}"}}'
        with EchoServer() as healthy, running_relay(BINARY, [unavailable, healthy.port], breaker_threshold=1, breaker_cooldown_ms=700) as (port, metric_port, _):
            exchange(port, b'failover')
            metrics = get_metrics(metric_port)
            self.assertEqual(metrics['edgerelay_circuit_state'+label], 1)
            failed_attempts = metrics['edgerelay_upstream_connect_attempts_total'+label]
            exchange(port, b'skip open circuit')
            exchange(port, b'start on open circuit and skip it')
            self.assertEqual(get_metrics(metric_port)['edgerelay_upstream_connect_attempts_total'+label], failed_attempts)
            with EchoServer(port=unavailable) as recovered:
                time.sleep(0.75)
                for _ in range(4):
                    exchange(port, b'recovered')
                self.assertGreater(recovered.connections, 0)
                self.assertEqual(get_metrics(metric_port)['edgerelay_circuit_state'+label], 0)

    def test_connection_rate_limit_and_refill(self):
        with EchoServer() as upstream, running_relay(BINARY, [upstream.port], rate=1, burst=2) as (port, metrics_port, _):
            exchange(port, b'one')
            exchange(port, b'two')
            with socket.create_connection(('127.0.0.1', port), timeout=2) as client:
                client.settimeout(2)
                try:
                    self.assertEqual(client.recv(1), b'')
                except ConnectionResetError:
                    pass
            metrics = get_metrics(metrics_port)
            self.assertEqual(metrics['edgerelay_connections_rejected_total{reason="rate_limit"}'], 1)
            time.sleep(1.1)
            exchange(port, b'after refill')

    def test_bounded_queue_rejects_excess_connections(self):
        with EchoServer() as upstream, running_relay(BINARY, [upstream.port], workers=1, queue_capacity=1, idle_timeout_ms=4000) as (port, metrics_port, _):
            with socket.create_connection(('127.0.0.1', port)) as active:
                wait_until(lambda: get_metrics(metrics_port)['edgerelay_active_connections'] == 1)
                with socket.create_connection(('127.0.0.1', port)) as queued:
                    wait_until(lambda: get_metrics(metrics_port)['edgerelay_queue_depth'] == 1)
                    with socket.create_connection(('127.0.0.1', port)) as rejected:
                        rejected.settimeout(2)
                        try:
                            self.assertEqual(rejected.recv(1), b'')
                        except ConnectionResetError:
                            pass
                    metrics = get_metrics(metrics_port)
                    self.assertEqual(metrics['edgerelay_connections_rejected_total{reason="queue_full"}'], 1)
                    self.assertEqual(metrics['edgerelay_active_connections'], 1)
                    self.assertEqual(metrics['edgerelay_queue_depth'], 1)

    def test_idle_connections_are_closed(self):
        with EchoServer() as upstream, running_relay(BINARY, [upstream.port], idle_timeout_ms=150) as (port, _, _):
            with socket.create_connection(('127.0.0.1', port)) as client:
                client.settimeout(2)
                self.assertEqual(client.recv(1), b'')

    def test_all_upstreams_down_closes_client(self):
        unavailable = free_port()
        with running_relay(BINARY, [unavailable], breaker_threshold=1) as (port, metrics_port, _):
            with socket.create_connection(('127.0.0.1', port)) as client:
                client.settimeout(2)
                self.assertEqual(client.recv(1), b'')
            wait_until(lambda: get_metrics(metrics_port)['edgerelay_connections_no_upstream_total'] == 1)

    def test_invalid_configuration_fails_before_serving(self):
        for values in [('--upstreams','10.0.0.1:9001'), ('--workers','0'),
                       ('--listen-port','8080','--metrics-port','8080'),
                       ('--upstreams','127.0.0.1:9001,127.0.0.1:9001')]:
            with self.subTest(values=values):
                result = subprocess.run([BINARY,*values],capture_output=True,text=True,timeout=2)
                self.assertNotEqual(result.returncode,0)

    def test_incomplete_metrics_request_has_bounded_response(self):
        with EchoServer() as upstream, running_relay(BINARY, [upstream.port]) as (_, metrics_port, _):
            with socket.create_connection(('127.0.0.1',metrics_port)) as client:
                client.settimeout(2)
                client.sendall(b'GET /metrics HTTP/1.1\r\n')
                received = bytearray()
                while True:
                    data=client.recv(1024)
                    if not data:
                        break
                    received.extend(data)
                self.assertIn(b'400 Bad Request',received)


if __name__ == '__main__':
    unittest.main(argv=[sys.argv[0]]+remaining, verbosity=2)
