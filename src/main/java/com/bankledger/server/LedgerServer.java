package com.bankledger.server;

import com.bankledger.model.Transaction;
import com.bankledger.model.TransactionType;
import com.bankledger.storage.LedgerWriter;
import com.bankledger.storage.ShardRouter;
import com.bankledger.storage.ShardedLedgerWriter;
import com.bankledger.storage.partition.HashPartitionStrategy;
import com.bankledger.storage.partition.PartitionStrategy;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class LedgerServer {

    private static final int PORT = 9090;
    private static final Path LOG_PATH = Path.of("transactions.log");
    private static final AtomicLong TXN_ID_SEQ = new AtomicLong(1);
    private static final ExecutorService pool = Executors.newFixedThreadPool(3);
    private static final int SHARD_COUNT = 3;
    private static final Path BASE_DIR = Path.of(".");
    private static final String FILE_PREFIX = "transactions";

    public static void main(String[] args) throws IOException {

        PartitionStrategy partitionStrategy = new HashPartitionStrategy(SHARD_COUNT);
        ShardRouter router = new ShardRouter(BASE_DIR,FILE_PREFIX,partitionStrategy);
        ShardedLedgerWriter shardedLedgerWriter = new ShardedLedgerWriter(router);

        // When the application is terminated , this makes sure the thread pool is shut down gracefully.
        //To make sure that the worker threads are not left running and ensures server exits cleanly.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pool.shutdown();
        }));

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Ledgerserver is listening on port " + PORT);



            while (true) {
                Socket client = serverSocket.accept();
                pool.submit(() ->

                        handleClient(client, shardedLedgerWriter, router));
            }
        }


    }

    private static void handleClient(Socket client, ShardedLedgerWriter ledgerWriter, ShardRouter router) {
        try (client;

             BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
             PrintWriter out = new PrintWriter(client.getOutputStream(), true)) {

            String line = in.readLine();
            if (line == null || line.isBlank()) {
                out.println("ERROR,EMPTY_REQUEST");
                return;
            }
            if(line.startsWith("READ,"))
            {
                handleRead(line,out, router);
                return;
            }
            if (line.equals("META")) {
                handleMeta(out, router);
                return;
            }


            try {

                Transaction txn = parseToTransaction(line);
                ledgerWriter.append(txn);

                int shardId = router.shardForAccount(txn.getAccountId());
                out.println("OK," + txn.getTransactionId() + "," + shardId);
                System.out.println("Transaction Saved: " + txn);
            } catch (IllegalArgumentException e) {
                out.println("ERROR,invalid BAD_REQUEST");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handleMeta(PrintWriter out, ShardRouter router) {
        out.println("OK,META," + router.shardCount());
    }

    private static Transaction parseToTransaction(String line) {
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

    private static void handleRead(String line, PrintWriter out, ShardRouter router) {
        String [] parts = line.split(",");
        if(parts.length != 4)
        {
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
        }catch (NumberFormatException e)
        {
            out.println("ERROR,BAD_READ_REQUEST");
            return;
        }

        if(shardId < 0 || shardId >= SHARD_COUNT || offset < 0 || maxLines < 0)
        {
            out.println("ERROR,BAD_READ_REQUEST");
            return;
        }

        Path shardPath = router.shardPath(shardId);
        String [] buffer = new String[maxLines];
        int sent = 0 ;

        try(BufferedReader reader = new BufferedReader(new FileReader(shardPath.toFile()))) {

            for (int i = 0 ; i < offset; i++)
            {
                if(reader.readLine() == null)
                {
                    out.println("OK,READ," + shardId + "," + offset);
                    out.println("END");
                    return;
                }
            }

            String txnLine;
            while(sent < maxLines &&(txnLine = reader.readLine()) != null){
               buffer[sent++] = txnLine;
            }

            int nextOffset = offset + sent;
            out.println("OK,READ," +shardId+ "," +nextOffset);
            for (int i = 0; i < sent; i++) {
                out.println(buffer[i]);
            }
            out.println("END");


        }catch (FileNotFoundException e) {
            out.println("OK,READ," + shardId + "," + offset);
            out.println("END");
        }catch (IOException e) {
            out.println("ERROR,READ_FAILED");
        }

    }


}
