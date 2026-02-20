package com.bankledger.consumer.handlers;

import com.bankledger.consumer.core.TransactionHandler;
import com.bankledger.model.Transaction;

public class AuditHandler implements TransactionHandler {
    @Override
    public void handle(Transaction transaction) throws Exception {
        System.out.println("[AUDIT] " + transaction);
    }

    @Override
    public String name() {
        return "Audit";
    }
}
