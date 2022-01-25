package wal.internal;

import java.io.IOException;
import java.io.RandomAccessFile;

public class DiskStore implements Store {
    private final RandomAccessFile file;
    private long size;

    public DiskStore(RandomAccessFile file, long size) {
        this.file = file;
        this.size = size;
    }

    @Override
    public AppendResult append(byte[] content) {
        long contentSize = content.length;
        long position = size;
        try {
            file.writeLong(contentSize);
            file.write(content);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long written = contentSize + lenWidth;
        size += written;

        return new AppendResult(written, position);
    }

    @Override
    public ReadResult read(long position) {
        try {
            file.seek(position);
            long contentSize = file.readLong();

            byte[] content = new byte[(int) contentSize];

            file.seek(position + lenWidth);
            file.readFully(content);

            return new ReadResult(content);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
