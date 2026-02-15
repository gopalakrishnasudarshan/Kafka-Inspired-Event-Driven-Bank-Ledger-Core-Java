A Kafka-inspired, event-driven banking ledger being built using pure Core Java.

The project explores low-level TCP networking, append-only log design, and event-driven system fundamentals without using frameworks.

 Current work done: Implement concurrent client handling and add offset-based ledger read support

- Introduced fixed thread pool in LedgerServer for multi-client handling
- Made LedgerWriter.append() thread-safe using synchronized
- Added graceful shutdown hook for ExecutorService
- Implemented READ,<offset>,<maxLines> protocol for ledger consumption
- Added basic offset-based log reading with bounded response and END marker
- Introduced deterministic account-based sharding using PartitionStrategy
- Implemented ShardedLedgerWriter to route writes to per-shard append-only log files
- Upgraded read protocol to shard-aware consumption READ,<shardId>,<offset>,<maxLines>
- Centralized shard routing and file resolution using ShardRouter
- Added META command to expose shard count dynamically and remove hardcoded shard assumptions from clients

Next expected steps

- Implement independent consumer services with per-shard offset tracking
- Add durable offset persistence and basic fault-tolerance mechanisms
