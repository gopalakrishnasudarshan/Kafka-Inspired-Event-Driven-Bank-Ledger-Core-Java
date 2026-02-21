package com.bankledger.server;

import java.util.List;

public class LedgerServerConfig {

    private final ServerRole role;
    private final int port;
    private final int shardCount;
    private final List<String> replicaAddress;
    private final String leaderAddress;
    private final int replicationTimeoutMs;
    private final int workerThreads;



    public LedgerServerConfig(ServerRole role, int port, int shardCount, List<String> replicaAddress, String leaderAddress, int replicationTimeoutMs, int workerThreads) {

        this.role = role;
        this.port = port;
        this.shardCount = shardCount;
        this.replicaAddress = replicaAddress;
        this.leaderAddress = leaderAddress;
        this.replicationTimeoutMs = replicationTimeoutMs;
        this.workerThreads = workerThreads;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public ServerRole getRole() {
        return role;
    }

    public int getPort() {
        return port;
    }

    public int getShardCount() {
        return shardCount;
    }

    public List<String> getReplicaAddress() {
        return replicaAddress;
    }

    public String getLeaderAddress() {
        return leaderAddress;
    }

    public int getReplicationTimeoutMs() {
        return replicationTimeoutMs;
    }
}
