package com.bankledger.consumer.offsets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;

public class FileOffsetStore implements OffsetStore {


    private final Path baseDir;
    private final ConcurrentHashMap<Integer,Object> shardLocks = new ConcurrentHashMap<>();

    public FileOffsetStore(Path baseDir) throws  IOException{
        this.baseDir = Objects.requireNonNull(baseDir,"baseDir");
        Files.createDirectories(baseDir);
    }

    @Override
    public long load(int shardId) throws IOException {
        Path path = offsetPath(shardId);
        if(!Files.exists(path)){
            return 0L;
        }
        String text = Files.readString(path, StandardCharsets.UTF_8).trim();
        if(text.isEmpty()){
            return 0L;
        }
        try {
            long v = Long.parseLong(text);
            return Math.max(v, 0L);

        }catch(NumberFormatException e){

            throw new IOException("Corrupted offset file: " + path + " contents='" + text + "'", e);
        }
    }

    private Path offsetPath(int shardId) {
        return baseDir.resolve("shard_" + shardId + ".offset");
    }

    @Override
    public void commit(int shardId, long nexttoffset) throws IOException {

        if(nexttoffset < 0){
            throw new IllegalArgumentException("nextoffset must be >=0");

        }
        Object lock = shardLocks.computeIfAbsent(shardId, k -> new Object());
        synchronized (lock) {
            Files.createDirectories(baseDir);

            Path finalPath = offsetPath(shardId);
            Path tmpPath = tmpOffsetPath(shardId);

            byte[] bytes = (Long.toString(nexttoffset) + "\n").getBytes(StandardCharsets.UTF_8);
            try(FileChannel ch = FileChannel.open(tmpPath,CREATE, TRUNCATE_EXISTING,WRITE)){
                ch.write(ByteBuffer.wrap(bytes));
                ch.force(true);
            }

            try {
                Files.move(tmpPath, finalPath,ATOMIC_MOVE,REPLACE_EXISTING);
            }catch(AtomicMoveNotSupportedException e){
                Files.move(tmpPath, finalPath, REPLACE_EXISTING);
            }

        }

    }

    private Path tmpOffsetPath(int shardId) {
        return baseDir.resolve("shard_" + shardId + ".offset.tmp");
    }
}
