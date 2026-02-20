package com.bankledger.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;

public class ProducerClient {

    private static final String HOST = "localhost";
    private static final int PORT = 9090;

    public static void main(String[] args) throws Exception {

        int threads = 3;
        int messagesPerThread = 20;
        int maxAmount = 5000;

        for (int i = 0; i < threads; i++) {
            int producerId = i + 1;
            new Thread(() -> produce(producerId, messagesPerThread, maxAmount)).start();
        }
    }

    private static void produce(int producerId, int count, int maxAmount) {

        for (int i = 0; i < count; i++) {

            String accountId = "ACC" + String.format("%02d", ThreadLocalRandom.current().nextInt(1, 11));
            String type = ThreadLocalRandom.current().nextBoolean() ? "DEPOSIT" : "WITHDRAW";
            int amount = ThreadLocalRandom.current().nextInt(1, maxAmount + 1);

            String request = accountId + "," + type + "," + amount;

            try (Socket socket = new Socket(HOST, PORT);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                out.println(request);

                String response = in.readLine();
                System.out.println("[PRODUCER-" + producerId + "] sent: " + request + " | got: " + response);

            } catch (IOException e) {
                System.err.println("[PRODUCER-" + producerId + "] error: " + e.getMessage());
            }


            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(20, 120));
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
