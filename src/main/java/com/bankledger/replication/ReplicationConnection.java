package com.bankledger.replication;

import java.io.*;
import java.net.Socket;

public class ReplicationConnection {

    private final String host;
    private final int port;

    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public ReplicationConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public synchronized void connect() throws IOException {
        socket = new Socket(host, port);
        writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }

    public synchronized boolean replicate(int shardId, String txLine, long txId, int timeoutMs) {
        try {
            if (socket == null || socket.isClosed()) return false;

            socket.setSoTimeout(timeoutMs);

            writer.write("REPL_APPEND," + shardId + "," + txLine);
            writer.newLine();
            writer.flush();

            String response = reader.readLine();
            return response != null && response.equals("ACK," + txId);

        } catch (Exception e) {
            return false;
        }
    }

    public synchronized void close() {
        try {
            if (socket != null) socket.close();
        } catch (Exception ignored) {
        }
    }
}
