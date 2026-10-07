# LSM-Tree-Key-Value-Store
A small embedded key-value database with the core pieces of an LSM-tree storage engine: write-ahead log, memtable, sorted on-disk tables, Bloom filters, and compaction.

Demonstrates: storage engine design, crash recovery, binary file formats, probabilistic data structures, filesystem I/O.
# Architecture
put/del --> WAL (checksummed) --> memtable (std::map)

                                      |  flush when full

                                      v

                              sst_<id>.dat  (immutable, sorted)

                                      |  compact when 4 tables exist

                                      v

                              one merged sst

WAL: every write is appended with an FNV-1a checksum before it touches the memtable. On startup the log is replayed, and a torn or corrupt tail stops replay cleanly.
Memtable: in-memory ordered map. When it passes a size threshold it is written as a new SSTable.
SSTables: written to a temp file, fsynced, then atomically renamed. Each has a sparse index (one entry per 16 keys) and a Bloom filter (about 10 bits per key, 4 hashes).
Reads: check the memtable, then tables newest-first. Key-range and Bloom checks skip tables that cannot contain the key.
Deletes: written as tombstones so they shadow older values.
Recovery: unfinished .tmp files from a crash are removed on open.
# Build and run
g++ -std=c++20 -O2 -Wall -Wextra project10_lsm_kv.cpp -o lsm

./lsm

The demo writes 20,000 keys with a tiny memtable (to force many flushes and compactions), overwrites and deletes subsets, verifies every read, then reopens the database to prove recovery from disk and the WAL. It prints PASS lines. Data goes in ./lsm_demo, which is removed afterward.
# Known limitations
Compaction merges all tables in memory and keeps tombstones; a manifest file would make dropping them crash-safe.
Single writer, no block cache, no range scans.
The WAL is flushed per write but only fsynced if sync_wal is enabled (off by default), so recent writes can be lost on power failure.
