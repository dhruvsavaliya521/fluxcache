# Failure Model

This document outlines how FluxCache handles various failure modes.

## Process Termination / Crashes
* **SIGINT/SIGTERM**: Caught by a global `broadcast` channel. The TCP and HTTP servers shut down, existing connections are closed, a final WAL sync and Snapshot are performed, and the process exits cleanly.
* **OOM Killer / Hard Crash**: If the process is abruptly killed, the in-memory state is lost. 
  * If persistence is OFF: Data is volatile, cache starts empty.
  * If persistence is ON: Recovery is initiated on restart. Any WAL records appended but not yet fsync'd (based on the `100ms` sync interval) are lost.

## Disk & IO Failures
* **Disk Full**: The WAL writer will return an `io::Error`. The `SET` operation will fail and return a `-ERR` to the client. The cache will continue to serve reads.
* **Corrupt WAL Tail**: Common during hard crashes where a block is only partially written. The reader verifies CRC32 checksums. If a checksum mismatches or a record is truncated, it logs a warning and assumes the valid WAL ends there, recovering all data prior.
* **Corrupt Snapshot**: Verified via CRC32. If corrupt, FluxCache will panic on startup, requiring manual intervention (e.g., deleting the corrupted snapshot and relying solely on an older snapshot or starting fresh).

## Network Failures
* **Client Disconnects**: Toko `read_buf` returns `0`. The connection task cleans up and terminates gracefully without affecting the server.
* **Slow Clients**: Bounded by connection limits and read timeouts to prevent resource exhaustion (Slowloris attacks).

## Cache Stampedes
Mitigated using **Singleflight / Request Coalescing**. Concurrent requests for the same missing key subscribe to a broadcast channel. Only one request proceeds to execute the loader function; all others block and share the eventual result.
