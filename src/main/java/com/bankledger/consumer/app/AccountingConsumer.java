package com.bankledger.consumer.app;

import com.bankledger.client.LedgerTcpClient;
import com.bankledger.consumer.core.ConsumerService;
import com.bankledger.consumer.handlers.AccountingHnadler;
import com.bankledger.consumer.offsets.FileOffsetStore;
import com.bankledger.consumer.offsets.OffsetStore;

import java.nio.file.Path;

public class AccountingConsumer {

    public static void main(String[] args) throws Exception {

        LedgerTcpClient client = new LedgerTcpClient("localhost", 9090);

        OffsetStore offsetStore = new FileOffsetStore(Path.of("offsets/accounting"));

        ConsumerService service =
                new ConsumerService(
                        "Accounting",
                        client,
                        offsetStore,
                        new AccountingHnadler(),
                        10,
                        1000
                );

        service.start();
    }
}
