package simpledb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

// Should create only one instance per SimpleDB
public class FileManager {
    private File dbDirectory;
    private int blockSize;
    private boolean isNew;
    private final Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileManager(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        isNew = !dbDirectory.exists();

        // Create the directory if the database is new
        if (isNew) {
            dbDirectory.mkdirs();
        }

        // Remove any leftover temporary tables
        Arrays.stream(dbDirectory.list()).forEach(filename -> {
            if (filename.startsWith("temp")) {
                new File(dbDirectory, filename).delete();
            }
        });
    }

    public synchronized void read(BlockId blockId, Page page) {
        try {
            final RandomAccessFile file = getFile(blockId.filename());
            file.seek(blockId.blockNumber() * blockSize);
            file.getChannel().read(page.contents());
        } catch (final IOException e) {
            throw new RuntimeException("Cannot read block " + blockId);
        }
    }

    public synchronized void write(BlockId blockId, Page page) {
        try {
            final RandomAccessFile file = getFile(blockId.filename());
            file.seek(blockId.blockNumber() * blockSize);
            file.getChannel().write(page.contents());
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot write block" + blockId);
        }
    }

    /**
     * Seeks to the end of the file and writes an empty array of bytes to it,
     * which causes the OS to automatically extend the file.
     */
    public synchronized BlockId append(String filename) {
        int newBlockNumber = length(filename);
        final BlockId blockId = new BlockId(filename, newBlockNumber);
        byte[] b = new byte[blockSize];

        try {
            final RandomAccessFile file = getFile(blockId.filename());
            file.seek(blockId.blockNumber() * blockSize);
            file.write(b);
        }
        catch (final IOException e) {
            throw new RuntimeException("Cannot append block" + blockId);
        }

        return blockId;
    }

    public int length(String filename) {
        try {
            final RandomAccessFile file = getFile(filename);
            return (int)(file.length() / blockSize);
        }
        catch (final IOException e) {
            throw new RuntimeException("Cannot access " + filename);
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int blockSize() {
        return blockSize;
    }

    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile file = openFiles.get(filename);
        if (file == null) {
            final File dbTable = new File(dbDirectory, filename);
            file = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, file);
        }
        return file;
    }
}
