# Kafka-Inspired Distributed Banking Ledger (Core Java)

A Kafka-inspired, event-driven distributed banking ledger built using pure Core Java (no frameworks).

This project implements the core mechanics of a distributed log system from scratch, focusing on storage, partitioning, replication, and offset-based consumption.

---

## Architecture Overview

### Partitioned Append-Only Log
- Deterministic account-based sharding
- Multiple shard files (`transactions_0.log`, etc.)
- Strict ordering per shard
- Offset → byte index for fast reads
- Fully append-only storage

### Offset-Based Consumption
- Stateless server-side reads
- `READ,<shardId>,<offset>,<maxLines>` protocol
- Durable per-shard consumer offset persistence
- Independent consumer groups

### Synchronous Replication
- Leader–Replica model
- ACK=ALL semantics
- No success response unless replicas persist data
- Per-shard ordered replication

### Idempotent Replication
- Monotonic replication sequence per shard
- Duplicate-safe appends
- Strict ordering enforcement on replicas

---

## System Guarantees

- Deterministic partitioning
- Immutable append-only log
- Indexed fast reads
- Independent consumers
- Durable replication
- Duplicate-safe replication

---

## Tech Stack

- Core Java
- TCP sockets
- ExecutorService (thread pools)
- RandomAccessFile (log + index)
- No external frameworks

---

This project demonstrates how distributed log systems like Kafka work internally, implemented entirely from first principles.
