# Persistence Model

FluxCache optionally persists data to disk to survive restarts and crashes, providing durability guarantees.

## Write-Ahead Log (WAL)
Every mutating operation (`SET`, `DEL`, `EXPIRE`) is appended to a WAL file *before* being applied to the in-memory cache. 

* **Format**: Binary, length-prefixed, with a CRC32 checksum per record.
* **Durability**: The WAL is synced to disk periodically by a background task (e.g., every 100ms). This means at most 100ms of data is lost in a catastrophic power failure.
* **Corruption Handling**: During recovery, if a checksum fails or a record is truncated (common if a crash happens mid-write), the reader stops and assumes it has reached the end of the valid log.

## Snapshots
The WAL grows indefinitely. To bound recovery time, FluxCache periodically takes full memory snapshots.

* **Format**: A binary dump of all active key-value pairs and their remaining TTLs, prefixed by the `wal_sequence` number at the time of the snapshot.
* **Atomicity**: Snapshots are written to a `.tmp` file and atomically renamed to `.snap` using OS-level guarantees (`std::fs::rename`).
* **Recovery**: On startup, FluxCache loads the `.snap` file, then replays only WAL records with a sequence number *greater* than the snapshot's saved sequence.
