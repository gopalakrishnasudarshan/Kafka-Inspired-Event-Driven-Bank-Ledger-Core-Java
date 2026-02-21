package com.bankledger.server;

import com.bankledger.model.Transaction;
import com.bankledger.model.TransactionType;
import com.bankledger.replication.ReplicaManager;
import com.bankledger.storage.ShardRouter;
import com.bankledger.storage.ShardedLedgerWriter;
import com.bankledger.storage.partition.HashPartitionStrategy;
import com.bankledger.storage.partition.PartitionStrategy;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class LedgerServer {

    private static final AtomicLong TXN_ID_SEQ = new AtomicLong(1);
    private static final Path BASE_DIR = Path.of(".");
    private static final String FILE_PREFIX = "transactions";

    private final LedgerServerConfig config;
    private final ExecutorService pool;

    private final ShardRouter router;
    private final ShardedLedgerWriter ledgerWriter;

    private final ReplicaManager replicaManager;

    public LedgerServer(LedgerServerConfig config) throws Exception {
        this.config = config;

        this.pool = Executors.newFixedThreadPool(config.getWorkerThreads());

        PartitionStrategy partitionStrategy = new HashPartitionStrategy(config.getShardCount());
        this.router = new ShardRouter(BASE_DIR, FILE_PREFIX, partitionStrategy);
        this.ledgerWriter = new ShardedLedgerWriter(router);

        if (config.getRole() == ServerRole.LEADER) {
            this.replicaManager = new ReplicaManager(
                    config.getReplicaAddress(),
                    config.getReplicationTimeoutMs()
            );
        } else {
            this.replicaManager = null;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pool.shutdown();
            if (replicaManager != null) replicaManager.close();
        }));
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(config.getPort())) {
            System.out.println("LedgerServer (" + config.getRole() + ") listening on port " + config.getPort());

            while (true) {
                Socket client = serverSocket.accept();
                pool.submit(() -> handleClient(client));
            }
        }
    }

    private void handleClient(Socket client) {
        try (client;
             BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
             PrintWriter out = new PrintWriter(client.getOutputStream(), true)) {

            String line = in.readLine();
            if (line == null || line.isBlank()) {
                out.println("ERROR,EMPTY_REQUEST");
                return;
            }

            // META
            if (line.equals("META")) {
                out.println("OK,META," + router.shardCount());
                return;
            }

            // READ
            if (line.startsWith("READ,")) {
                handleRead(line, out);
                return;
            }

            // REPL_APPEND (replica-only)
            if (line.startsWith("REPL_APPEND,")) {
                if (config.getRole() != ServerRole.REPLICA) {
                    out.println("ERROR,NOT_REPLICA");
                    return;
                }
                handleReplicaAppend(line, out);
                return;
            }

            // Client writes (leader-only)
            if (config.getRole() != ServerRole.LEADER) {
                out.println("ERROR,NOT_LEADER");
                return;
            }

            try {
                Transaction txn = parseToTransaction(line);

                // returns the exact txLine appended (no trailing newline)
                String txLine = ledgerWriter.append(txn);

                int shardId = router.shardForAccount(txn.getAccountId());
                long txId = txn.getTransactionId();

                boolean replicated = true;
                if (replicaManager != null) {
                    replicated = replicaManager.replicateToAll(shardId, txLine, txId);
                }

                if (replicated) {
                    out.println("OK," + txId + "," + shardId);
                } else {
                    out.println("ERR,REPLICATION_FAILED," + txId);
                }

            } catch (IllegalArgumentException e) {
                out.println("ERROR,BAD_REQUEST");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleReplicaAppend(String line, PrintWriter out) {
        // Format: REPL_APPEND,<shardId>,<txLine>
        String[] parts = line.split(",", 3);
        if (parts.length < 3) {
            out.println("ERROR,BAD_REQUEST");
            return;
        }

        int shardId;
        try {
            shardId = Integer.parseInt(parts[1].trim());
        } catch (NumberFormatException e) {
            out.println("ERROR,BAD_REQUEST");
            return;
        }

        String txLine = parts[2];

        long txId;
        try {
            txId = Long.parseLong(txLine.split(",", 2)[0]);
        } catch (Exception e) {
            out.println("ERROR,BAD_REQUEST");
            return;
        }

        try {
            ledgerWriter.appendReplica(shardId, txLine);
            out.println("ACK," + txId);
        } catch (Exception e) {
            out.println("ERROR,REPL_APPEND_FAILED");
        }
    }

    private Transaction parseToTransaction(String line) {
        String[] parts = line.split(",");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid request format. Expected: accountId,type,amount");
        }

        String accountId = parts[0].trim();
        TransactionType type = TransactionType.valueOf(parts[1].trim().toUpperCase());
        long amount = Long.parseLong(parts[2].trim());

        long txnId = TXN_ID_SEQ.getAndIncrement();
        long timestamp = System.currentTimeMillis();

        return new Transaction(txnId, accountId, type, amount, timestamp);
    }

    private void handleRead(String line, PrintWriter out) {
        String[] parts = line.split(",");
        if (parts.length != 4) {
            out.println("ERROR,BAD_READ_REQUEST");
            return;
        }

        int shardId;
        int offset;
        int maxLines;

        try {
            shardId = Integer.parseInt(parts[1].trim());
            offset = Integer.parseInt(parts[2].trim());
            maxLines = Integer.parseInt(parts[3].trim());
        } catch (NumberFormatException e) {
            out.println("ERROR,BAD_READ_REQUEST");
            return;
        }

        if (offset < 0 || maxLines < 0) {
            out.println("ERROR,BAD_READ_REQUEST");
            return;
        }

        Path logPath = router.shardPath(shardId);
        Path indexPath = Path.of(logPath.toString() + ".index");

        try (RandomAccessFile indexFile = new RandomAccessFile(indexPath.toFile(), "r");
             RandomAccessFile logFile = new RandomAccessFile(logPath.toFile(), "r")) {

            long bytePosition = -1;
            int currentLine = 0;
            String indexLine;

            while ((indexLine = indexFile.readLine()) != null) {
                if (currentLine == offset) {
                    String[] idxParts = indexLine.split(",");
                    bytePosition = Long.parseLong(idxParts[1]);
                    break;
                }
                currentLine++;
            }

            if (bytePosition == -1) {
                out.println("OK,READ," + shardId + "," + offset);
                out.println("END");
                return;
            }

            logFile.seek(bytePosition);

            List<String> buffer = new ArrayList<>();
            String txnLine;

            while (buffer.size() < maxLines && (txnLine = logFile.readLine()) != null) {
                buffer.add(txnLine);
            }

            int nextOffset = offset + buffer.size();

            out.println("OK,READ," + shardId + "," + nextOffset);
            for (String l : buffer) out.println(l);
            out.println("END");

        } catch (FileNotFoundException e) {
            out.println("OK,READ," + shardId + "," + offset);
            out.println("END");
        } catch (IOException e) {
            out.println("ERROR,READ_FAILED");
        }
    }

    public static void main(String[] args) throws Exception {
        // Supported args (any order):
        // --role=LEADER|REPLICA
        // --port=9090
        // --shards=3
        // --threads=3
        // --replicas=localhost:9091,localhost:9092   (leader only)
        // --leader=localhost:9090                     (replica optional)
        // --timeoutMs=2000

        Map<String, String> m = new HashMap<>();
        for (String a : args) {
            if (a.startsWith("--") && a.contains("=")) {
                String[] p = a.substring(2).split("=", 2);
                m.put(p[0].trim(), p[1].trim());
            }
        }

        ServerRole role = ServerRole.valueOf(m.getOrDefault("role", "LEADER").toUpperCase());
        int port = Integer.parseInt(m.getOrDefault("port", "9090"));
        int shards = Integer.parseInt(m.getOrDefault("shards", "3"));
        int threads = Integer.parseInt(m.getOrDefault("threads", "3"));
        int timeoutMs = Integer.parseInt(m.getOrDefault("timeoutMs", "2000"));

        List<String> replicas = List.of();
        String repStr = m.getOrDefault("replicas", "");
        if (!repStr.isBlank()) {
            replicas = Arrays.stream(repStr.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isBlank())
                    .toList();
        }

        String leaderAddr = m.getOrDefault("leader", null);


        LedgerServerConfig config = new LedgerServerConfig(
                role,
                port,
                shards,
                replicas,
                leaderAddr,
                timeoutMs,
                threads
        );

        new LedgerServer(config).start();
    }
}
