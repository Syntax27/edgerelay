"""Reproducible closed-loop loopback benchmark; no production-scale inference."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import platform
import statistics
import time

from support import EchoServer, exchange, running_relay

parser = argparse.ArgumentParser()
parser.add_argument('--binary', required=True)
parser.add_argument('--connections', type=int, default=300)
parser.add_argument('--concurrency', type=int, default=8)
parser.add_argument('--payload-bytes', type=int, default=4096)
parser.add_argument('--output', default='benchmark-results.json')
args = parser.parse_args()
if min(args.connections, args.concurrency, args.payload_bytes) <= 0:
    parser.error('Workload sizes must be positive.')
payload = bytes(i % 256 for i in range(args.payload_bytes))

def run(port):
    def timed(_):
        started = time.perf_counter()
        exchange(port, payload)
        return (time.perf_counter()-started)*1000
    for _ in range(10):
        exchange(port, payload)
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        samples = list(pool.map(timed, range(args.connections)))
    elapsed = time.perf_counter()-started
    sorted_samples = sorted(samples)
    return {'connections': len(samples), 'elapsed_seconds': round(elapsed, 4),
        'connections_per_second': round(len(samples)/elapsed, 2),
        'median_roundtrip_ms': round(statistics.median(samples), 3),
        'p95_roundtrip_ms': round(sorted_samples[math.ceil(.95*len(samples))-1], 3),
        'roundtrip_ms_samples': [round(s,4) for s in samples]}

with EchoServer() as upstream:
    direct = run(upstream.port)
    with running_relay(args.binary, [upstream.port], workers=args.concurrency,
                       queue_capacity=max(args.concurrency*2,32), rate=1000000, burst=1000000) as (port, _, _):
        proxied = run(port)
result = {'environment': {'os': platform.system(), 'release': platform.release(),
    'machine': platform.machine(), 'processor': platform.processor(), 'python': platform.python_version()},
    'workload': {'concurrency': args.concurrency, 'payload_bytes_per_direction': args.payload_bytes,
        'warmup_connections_per_path': 10, 'mode':'closed-loop, one TCP connection per exchange'},
    'direct': direct, 'via_edgerelay': proxied,
    'limitations': ['Loopback includes Python client/server overhead and connection establishment.',
        'One sequential direct/relay run, no statistical performance claim.',
        'The benchmark excludes startup and warmup from timed measurements.',
        'No TLS, WAN latency, disk persistence, or production traffic.']}
destination = Path(args.output)
destination.parent.mkdir(parents=True,exist_ok=True)
destination.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({label: {k:v for k,v in result[label].items() if k!='roundtrip_ms_samples'} for label in ['direct','via_edgerelay']},indent=2))
