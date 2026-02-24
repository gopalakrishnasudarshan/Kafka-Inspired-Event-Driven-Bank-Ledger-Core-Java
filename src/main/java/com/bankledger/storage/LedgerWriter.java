package com.bankledger.storage;

import com.bankledger.model.Transaction;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class LedgerWriter {

    private final Path logPath;
    private final Path indexPath;

    private final RandomAccessFile logFile;
    private final RandomAccessFile indexFile;

    private long nextOffset = 0;

    private long lastReplicationSeq = 0;

    public LedgerWriter(Path logPath) {
        try {
            this.logPath = logPath;
            this.indexPath = Path.of(logPath.toString() + ".index");

            Files.createDirectories(logPath.getParent() == null ? Path.of(".") : logPath.getParent());

            this.logFile = new RandomAccessFile(logPath.toFile(), "rw");
            this.indexFile = new RandomAccessFile(indexPath.toFile(), "rw");

            rebuildIndexAndOffset();

            // Move both files to end for appending
            logFile.seek(logFile.length());
            indexFile.seek(indexFile.length());

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize LedgerWriter", e);
        }
    }

    private void rebuildIndexAndOffset() throws IOException {
        nextOffset = 0;
        lastReplicationSeq = 0;
        indexFile.setLength(0); // rebuild fresh

        logFile.seek(0);

        long bytePosition = 0;
        String line;

        while ((line = logFile.readLine()) != null) {
            // Record offset -> byte position
            String indexEntry = nextOffset + "," + bytePosition + "\n";
            indexFile.write(indexEntry.getBytes(StandardCharsets.UTF_8));

            long seq = tryParseReplicationSeq(line);
            if(seq > lastReplicationSeq){
                lastReplicationSeq = seq;
            }

            bytePosition = logFile.getFilePointer();
            nextOffset++;
        }
    }

    /**
     * Leader/client write path.
     * Appends transaction and returns the exact line appended (without trailing newline).
     */
    public synchronized String append(Transaction transaction) throws IOException {

        long bytePosition = logFile.length();
        logFile.seek(bytePosition);

        String txLine = toLine(transaction);
        byte[] bytes = (txLine + "\n").getBytes(StandardCharsets.UTF_8);

        logFile.write(bytes);

        // Write index entry (offset -> bytePosition)
        String indexEntry = nextOffset + "," + bytePosition + "\n";
        indexFile.write(indexEntry.getBytes(StandardCharsets.UTF_8));

        nextOffset++;

        return txLine;
    }


    public synchronized String appendReplica(String txLine) throws IOException {

        long bytePosition = logFile.length();
        logFile.seek(bytePosition);

        byte[] bytes = (txLine + "\n").getBytes(StandardCharsets.UTF_8);
        logFile.write(bytes);

        String indexEntry = nextOffset + "," + bytePosition + "\n";
        indexFile.write(indexEntry.getBytes(StandardCharsets.UTF_8));

        nextOffset++;

        long seq = tryParseReplicationSeq(txLine);
        if(seq > lastReplicationSeq) {
            lastReplicationSeq = seq;
        }
        return txLine;
    }

    public synchronized String appendLine(String txLine) throws IOException {
        long bytePosition = logFile.length();
        logFile.seek(bytePosition);

        byte[] bytes = (txLine + "\n").getBytes(StandardCharsets.UTF_8);
        logFile.write(bytes);

        String indexEntry = nextOffset + "," + bytePosition + "\n";
        indexFile.write(indexEntry.getBytes(StandardCharsets.UTF_8));

        nextOffset++;

        long seq = tryParseReplicationSeq(txLine);
        if (seq > lastReplicationSeq) {
            lastReplicationSeq = seq;
        }

        return txLine;
    }


    public synchronized ReplicaAppendResult appendReplicaIdempotent(String txLine) throws IOException {
        long seq = parseReplicationSeqRequired(txLine);

        if (seq <= lastReplicationSeq) {
            return ReplicaAppendResult.DUPLICATE_IGNORED;
        }

        if (seq != lastReplicationSeq + 1) {
            return ReplicaAppendResult.GAP_DETECTED;
        }

        // Expected next
        appendReplica(txLine);
        // appendReplica updates lastReplicationSeq already, but keep explicit correctness:
        lastReplicationSeq = seq;

        return ReplicaAppendResult.APPENDED;
    }

    public synchronized long getLastReplicationSeq() {
        return lastReplicationSeq;
    }

    private long tryParseReplicationSeq(String txLine) {
        try {
            String[] parts = txLine.split(",");
            if (parts.length >= 6) {
                return Long.parseLong(parts[5].trim());
            }
            return 0;
        } catch (Exception ignored) {
            return 0;
        }
    }

    private long parseReplicationSeqRequired(String txLine) {
        try {
            String[] parts = txLine.split(",");
            if (parts.length < 6) {
                throw new IllegalArgumentException("Missing replicationSeq field: " + txLine);
            }
            return Long.parseLong(parts[5].trim());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid replicationSeq field: " + txLine, e);
        }
    }
    private String toLine(Transaction transaction) {
        return transaction.getTransactionId() + "," +
                transaction.getAccountId() + "," +
                transaction.getTransactionType() + "," +
                transaction.getAmount() + "," +
                transaction.getTimestamp();
    }

    public enum ReplicaAppendResult {
        APPENDED,
        DUPLICATE_IGNORED,
        GAP_DETECTED
    }
}
