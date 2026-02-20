package com.bankledger.consumer.handlers;

import com.bankledger.consumer.core.TransactionHandler;
import com.bankledger.model.Transaction;

public class FraudHandler implements TransactionHandler {
    @Override
    public void handle(Transaction transaction) throws Exception {
        System.out.println("[FRAUD ALERT] High value txn: " + transaction);
    }

    @Override
    public String name() {
        return "Fraud";
    }
}
