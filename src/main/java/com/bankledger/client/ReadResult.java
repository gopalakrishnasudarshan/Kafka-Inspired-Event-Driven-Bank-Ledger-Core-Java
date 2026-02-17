package com.bankledger.client;

import java.util.List;

public  final class ReadResult {

    private final int shardId;
    private final int nextOffset;
    private final List<String> lines;

    public ReadResult(int shardId, int nextOffset, List<String> lines) {
        this.shardId = shardId;
        this.nextOffset = nextOffset;
        this.lines = List.copyOf(lines);
    }

    public int getShardId() {
        return shardId;
    }

    public int getNextOffset() {
        return nextOffset;
    }

    public List<String> getLines() {
        return lines;
    }

    public boolean isEmpty(){
        return lines.isEmpty();
    }

    @Override
    public String toString() {
        return "ReadResult{" +
                "shardId=" + shardId +
                ", nextOffset=" + nextOffset +
                ", lines=" + lines +
                '}';
    }
}
