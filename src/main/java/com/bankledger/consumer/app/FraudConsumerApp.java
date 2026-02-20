package com.bankledger.consumer.app;

import com.bankledger.client.LedgerTcpClient;
import com.bankledger.consumer.core.ConsumerService;
import com.bankledger.consumer.handlers.FraudHandler;
import com.bankledger.consumer.offsets.OffsetStore;
import com.bankledger.consumer.offsets.FileOffsetStore;
import java.nio.file.Path;

public class FraudConsumerApp {
    public static void main(String[] args) throws Exception {

        LedgerTcpClient client = new LedgerTcpClient("localhost", 9090);

        OffsetStore offsetStore =
                new FileOffsetStore(Path.of("offsets/fraud"));

        ConsumerService service =
                new ConsumerService(
                        "Fraud",
                        client,
                        offsetStore,
                        new FraudHandler(),
                        10,
                        1000
                );

        service.start();
    }
}
