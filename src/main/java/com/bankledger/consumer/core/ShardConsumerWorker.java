package com.bankledger.consumer.core;

import com.bankledger.client.LedgerTcpClient;
import com.bankledger.client.ReadResult;
import com.bankledger.consumer.offsets.OffsetStore;
import com.bankledger.model.Transaction;

import java.util.concurrent.atomic.AtomicBoolean;

public class ShardConsumerWorker implements  Runnable{


    private final int shardId;
    private final LedgerTcpClient client;
    private final OffsetStore offsetStore;
    private final TransactionHandler handler;
    private final int maxLines;
    private final long pollIntervalMs;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public ShardConsumerWorker(
            int shardId,
            LedgerTcpClient client,
            OffsetStore offsetStore,
            TransactionHandler handler,
            int maxLines,
            long pollIntervalMs) {

        this.shardId = shardId;
        this.client = client;
        this.offsetStore = offsetStore;
        this.handler = handler;
        this.maxLines = maxLines;
        this.pollIntervalMs = pollIntervalMs;
    }


    @Override
    public void run() {
        try {
            long offset = offsetStore.load(shardId);

            while (running.get()) {

                ReadResult result = client.read(shardId, (int) offset, maxLines);

                if (result.isEmpty()) {
                    Thread.sleep(pollIntervalMs);
                    continue;
                }

                for (String line : result.getLines()) {
                    Transaction txn = TransactionParser.fromLine(line);
                    handler.handle(txn);
                }

                long nextOffset = result.getNextOffset();
                offsetStore.commit(shardId, nextOffset);
                offset = nextOffset;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public void shutdown() {
        running.set(false);
    }
}
