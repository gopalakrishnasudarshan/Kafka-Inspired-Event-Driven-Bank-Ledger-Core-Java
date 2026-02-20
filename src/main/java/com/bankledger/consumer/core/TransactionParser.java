package com.bankledger.consumer.core;

import com.bankledger.model.Transaction;
import com.bankledger.model.TransactionType;

public class TransactionParser {

    private TransactionParser() {
    }

    public static Transaction fromLine(String line) {
        if (line == null || line.isBlank()) {
            throw new IllegalArgumentException("Empty transaction line");
        }

        String[] parts = line.split(",");
        if (parts.length != 5) {
            throw new IllegalArgumentException("Invalid transaction log format: " + line);
        }

        try {
            long txnId = Long.parseLong(parts[0].trim());
            String accountId = parts[1].trim();
            TransactionType type = TransactionType.valueOf(parts[2].trim());
            long amount = Long.parseLong(parts[3].trim());
            long timestamp = Long.parseLong(parts[4].trim());

            return new Transaction(txnId, accountId, type, amount, timestamp);

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse transaction line: " + line, e);
        }
    }

}
