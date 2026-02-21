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
- Implemented reusable LedgerTcpClient abstraction to encapsulate META and shard-aware READ protocol handling with one request per connection 
- Designed and added durable per shard offset management using offsetStore and FileOffsetStore 
- Implemented independent consumer services with per-shard offset tracking
- Implemented leader–replica synchronous replication with ACK=ALL write guarantees 
- Introduced server roles (LEADER/REPLICA) with replication protocol using REPL_APPEND and ACK for durable log consistency

Next expected steps


- Idempotent replication (duplicate protection) on replicas (sequence/last-applied tracking per shard).
