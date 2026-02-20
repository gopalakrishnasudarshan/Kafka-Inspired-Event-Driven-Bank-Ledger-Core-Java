package com.bankledger.consumer.core;

import com.bankledger.model.Transaction;

public interface TransactionHandler {

    void handle(Transaction transaction) throws Exception;
    String name();
}
