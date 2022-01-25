package wal.internal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface Store {
    static DiskStore make(String filename) {
        Path path = Paths.get(filename);
        RandomAccessFile file;

        try {
            if (Files.exists(path)) {
                file = new RandomAccessFile(path.toString(), "rw");
            } else {
                Files.createFile(path);
                file = new RandomAccessFile(path.toString(), "rw");
            }
            System.out.println("File size is " + file.length());
            return new DiskStore(file, file.length());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    long lenWidth = 8;

    AppendResult append(byte[] content);

    ReadResult read(long position);
}
