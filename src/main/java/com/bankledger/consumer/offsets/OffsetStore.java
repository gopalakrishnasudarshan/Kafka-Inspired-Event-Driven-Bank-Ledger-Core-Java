package com.bankledger.consumer.offsets;

import java.io.IOException;

public interface OffsetStore {
    long load(int shardId) throws IOException;
    void commit(int shardId, long nexttoffset) throws IOException;
}
