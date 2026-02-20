package com.bankledger.consumer.handlers;

import com.bankledger.consumer.core.TransactionHandler;
import com.bankledger.model.Transaction;

public class AccountingHnadler implements TransactionHandler {
    @Override
    public void handle(Transaction transaction) throws Exception {
        System.out.println("[ACCOUNTING] " + transaction);
    }

    @Override
    public String name() {
        return "Accounting";
    }
}
