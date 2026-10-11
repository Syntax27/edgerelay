# Validation

Validated on 11 October 2026.

## Local build

- Windows 11, x86-64; GCC 15.2.0 from MinGW/MSYS2.
- CMake Release build with Ninja; C++20.
- Enabled compiler diagnostics: `-Wall -Wextra -Wpedantic -Wconversion -Wshadow`.
- Source compiled without warnings in this configuration.
- Python 3.12.14; standard-library-only integration and benchmark scripts.

## Tests

`ctest --test-dir build --output-on-failure` passed. CTest registers one policy executable containing **9 test cases and 101 checks**, including concurrent source admission and exclusive recovery probes.

`python scripts/integration_test.py --binary build/edgerelay.exe` passed **11 real-socket integration tests**:

1. Binary integrity across a 256 KiB exchange and exact forwarding counters.
2. Half-close propagation while retaining a delayed reverse-direction reply.
3. Round-robin distribution across two healthy upstreams.
4. Isolation of payloads across concurrent clients.
5. Failed-upstream bypass while open, followed by recovery after cooldown.
6. Connection burst limit and token refill.
7. Bounded waiting queue with excess connection rejection.
8. Idle connection expiry.
9. All-upstreams-down handling.
10. Rejection of invalid, duplicate, and self-conflicting configurations.
11. Bounded response to incomplete metrics requests.

The integration suite launches and stops its own synthetic servers; it does not contact public services.

## Recorded benchmark

File: `benchmarks/windows-loopback.json`.

Command:

```text
python scripts/benchmark.py --binary build/edgerelay.exe --connections 300 --concurrency 8 --payload-bytes 4096 --output benchmarks/windows-loopback.json
```

| Path | Completed connections | Connections/s | Median round-trip | p95 round-trip |
|---|---:|---:|---:|---:|
| Direct Python echo server | 300 | 1,874.32 | 4.134 ms | 5.232 ms |
| Through EdgeRelay | 300 | 742.32 | 11.967 ms | 25.458 ms |

These are observations from one closed-loop loopback run. Each measurement includes connection establishment, the Python harness, byte exchange, half-close, and completion. Ten warm-up exchanges per path and process startup are excluded. Raw samples and environment details are in the JSON. The direct path ran before the relay path, so host-load or ordering effects are not controlled by this experiment. This record does not establish a maximum throughput or isolated forwarding latency.

## Scope

The test matrix validates Windows and Linux on GitHub after publication. Review the workflow's actual status rather than inferring success from the presence of the workflow file. The local results above do not establish public-network security, production deployment, application-level health detection, TLS behavior, or performance at scale.
