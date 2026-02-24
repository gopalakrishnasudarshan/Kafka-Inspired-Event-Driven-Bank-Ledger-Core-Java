package com.bankledger.storage;

import com.bankledger.model.Transaction;

import java.io.IOException;

public class ShardedLedgerWriter {

    private final ShardRouter router;
    private final LedgerWriter[] writers;

    public ShardedLedgerWriter(ShardRouter router) {
        this.router = router;

        int n = router.shardCount();
        this.writers = new LedgerWriter[n];

        for (int shard = 0; shard < n; shard++) {
            this.writers[shard] = new LedgerWriter(router.shardPath(shard));
        }
    }


    public String append(Transaction transaction) throws IOException {
        int shard = router.shardForAccount(transaction.getAccountId());
        return writers[shard].append(transaction); // change LedgerWriter.append to return String
    }

    public String appendWithReplicationSeq(Transaction transaction, long replicationSeq) throws IOException {
        int shard = router.shardForAccount(transaction.getAccountId());

        String txLine =
                transaction.getTransactionId() + "," +
                        transaction.getAccountId() + "," +
                        transaction.getTransactionType() + "," +
                        transaction.getAmount() + "," +
                        transaction.getTimestamp() + "," +
                        replicationSeq;

        return writers[shard].appendLine(txLine);
    }
    public LedgerWriter.ReplicaAppendResult appendReplicaIdempotent(int shardId, String txLine) throws IOException {
        return writers[shardId].appendReplicaIdempotent(txLine);
    }


    public void appendReplica(int shardId, String txLine) throws IOException {
        writers[shardId].appendReplica(txLine);
    }
    public long lastReplicationSeq(int shardId) {
        return writers[shardId].getLastReplicationSeq();
    }
}
