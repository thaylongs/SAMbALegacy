package br.uff.spark.vfs;

import jnr.ffi.Pointer;
import ru.serce.jnrfuse.struct.FileStat;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Based from: https://github.com/SerCeMan/jnr-fuse/blob/master/src/main/java/ru/serce/jnrfuse/examples/MemoryFS.java
 */
public class MemoryFile extends MemoryPath {

    protected long size;
    protected boolean modified = false;
    protected ByteBuffer contents = ByteBuffer.allocate(0);

    protected MemoryFile(MemoryFS memoryFS, String name, byte[] data, MemoryDirectory parent) {
        super(memoryFS, name, parent);
        size = data.length;
        contents = ByteBuffer.wrap(data);
    }

    @Override
    protected void getattr(FileStat stat) {
        stat.st_mode.set(FileStat.S_IFREG | 0777);
        stat.st_size.set(size);
        stat.st_uid.set(memoryFS.getContext().uid.get());
        stat.st_gid.set(memoryFS.getContext().pid.get());
    }

    protected int read(Pointer buffer, long size, long offset) {
        int bytesToRead = (int) Math.min(contents.capacity() - offset, size);
        byte[] bytesRead = new byte[bytesToRead];
        synchronized (this) {
            contents.position((int) offset);
            contents.get(bytesRead, 0, bytesToRead);
            buffer.put(0, bytesRead, 0, bytesToRead);
            contents.position(0); // Rewind
        }
        return bytesToRead;
    }

    protected synchronized void truncate(long size) {
        if (size < contents.capacity()) {
            // Need to create a new, smaller buffer
            ByteBuffer newContents = ByteBuffer.allocate((int) size);
            byte[] bytesRead = new byte[(int) size];
            contents.get(bytesRead);
            newContents.put(bytesRead);
            contents = newContents;
        }
    }

    protected int write(Pointer buffer, long bufSize, long writeOffset) {
        size = Math.max(size, writeOffset + bufSize);
        int maxWriteIndex = (int) (writeOffset + bufSize);
        byte[] bytesToWrite = new byte[(int) bufSize];
        synchronized (this) {
            if (maxWriteIndex > contents.capacity()) {
                // Need to create a new, larger buffer
                ByteBuffer newContents = ByteBuffer.allocate(maxWriteIndex + 1024 * 512);
                newContents.put(contents);
                contents = newContents;
            }
            buffer.get(0, bytesToWrite, 0, (int) bufSize);
            contents.position((int) writeOffset);
            contents.put(bytesToWrite);
            contents.position(0); // Rewind
        }
        return (int) bufSize;
    }

    public void trim() {
        truncate(size);
    }
}