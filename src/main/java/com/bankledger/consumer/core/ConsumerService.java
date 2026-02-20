package com.bankledger.consumer.core;

import com.bankledger.client.LedgerTcpClient;
import com.bankledger.consumer.offsets.OffsetStore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerService {

    private final String name;
    private final LedgerTcpClient client;
    private final OffsetStore offsetStore;
    private final TransactionHandler handler;
    private final int maxLines;
    private final long pollIntervalMs;

    private final List<ShardConsumerWorker> workers = new ArrayList<>();
    private ExecutorService executor;

    public ConsumerService(
            String name,
            LedgerTcpClient client,
            OffsetStore offsetStore,
            TransactionHandler handler,
            int maxLines,
            long pollIntervalMs) {

        this.name = name;
        this.client = client;
        this.offsetStore = offsetStore;
        this.handler = handler;
        this.maxLines = maxLines;
        this.pollIntervalMs = pollIntervalMs;
    }

    public void start() throws Exception {

        int shardCount = client.meta();
        executor = Executors.newFixedThreadPool(shardCount);

        for (int shard = 0; shard < shardCount; shard++) {

            ShardConsumerWorker worker =
                    new ShardConsumerWorker(
                            shard,
                            client,
                            offsetStore,
                            handler,
                            maxLines,
                            pollIntervalMs
                    );

            workers.add(worker);
            executor.submit(worker);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        System.out.println("ConsumerService [" + name + "] started with "
                + shardCount + " shard workers.");
    }

    public void shutdown() {

        System.out.println("Shutting down consumer: " + name);

        for (ShardConsumerWorker worker : workers) {
            worker.shutdown();
        }

        if (executor != null) {
            executor.shutdown();
        }
    }
}
