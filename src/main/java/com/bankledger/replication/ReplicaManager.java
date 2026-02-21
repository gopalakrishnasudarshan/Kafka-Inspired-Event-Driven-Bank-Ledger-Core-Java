package com.bankledger.replication;

import java.util.ArrayList;
import java.util.List;

public class ReplicaManager {

    private final List<ReplicationConnection> replicas = new ArrayList<>();
    private final int timeoutMs;

    public ReplicaManager(List<String> replicaAddresses, int timeoutMs) throws Exception {
        this.timeoutMs = timeoutMs;

        for (String addr : replicaAddresses) {
            String[] parts = addr.split(":");
            ReplicationConnection conn =
                    new ReplicationConnection(parts[0], Integer.parseInt(parts[1]));
            conn.connect();
            replicas.add(conn);
        }
    }

    public boolean replicateToAll(int shardId, String txLine, long txId) {
        for (ReplicationConnection replica : replicas) {
            boolean ack = replica.replicate(shardId, txLine, txId, timeoutMs);
            if (!ack) {
                return false;
            }
        }
        return true;
    }

    public void close() {
        for (ReplicationConnection replica : replicas) {
            replica.close();
        }
    }
}
