package com.bankledger.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class LedgerTcpClient {

    private final String host;
    private final int port;

    public LedgerTcpClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public int meta() throws IOException {
        String response = sendSingleLineRequest("META");
        if(response == null){
            throw new IOException("Server closed connection(no META response)");
        }
        if(response.startsWith("ERROR")){
            throw new IOException("META failed: "+ response);
        }

        String [] parts = response.split(",");
        if(parts.length != 3 || !"OK".equals(parts[0]) || "META".equals(parts[1])){
            throw new IOException("Invalid META response: "+ response);
        }
        try {
            return Integer.parseInt(parts[2].trim());
        }catch(NumberFormatException e){
            throw new IOException("Invalid shardCount in META response: "+ response, e);
        }
    }

    public ReadResult read(int shardId, int offset, int maxLines) throws IOException {
        String request = "READ" + shardId +","+offset+","+maxLines;

        try(Socket socket = new Socket(host,port);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))){
            out.println(request);

            String header = in.readLine();
            if(header == null){
                throw new IOException("Server closed connection(no READ header)");
            }
            if(header.startsWith("ERROR")){
                throw new IOException("READ failed: "+ header);
            }

            String [] parts = header.split(",");
            if(parts.length != 4 || !"OK".equals(parts[0]) || "READ".equals(parts[1])){
                throw new IOException("INVALID READ header"+ header);
            }
            int respShardId;
            int nextOffset;
            try{
                respShardId = Integer.parseInt(parts[2].trim());
                nextOffset = Integer.parseInt(parts[3].trim());
            }catch(NumberFormatException e){
                throw new IOException("Invalid shardCount in READ header: "+ header, e);
            }
            List<String> lines = new ArrayList<>();
            String line;
            while((line = in.readLine())!= null){
                if("END".equals(line)) break;
                lines.add(line);
            }
            if(line == null){
                throw new IOException("READ response ended without END marker");
            }
            return new ReadResult(respShardId,nextOffset,lines);
        }
    }

    private String sendSingleLineRequest(String request) throws IOException {

        try(Socket socket = new Socket(host,port);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))){
            out.println(request);
            return in.readLine();

        }

    }

}
