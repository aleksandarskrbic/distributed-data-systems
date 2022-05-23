package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileManager;
import simpledb.file.Page;

import java.util.Iterator;

public class LogManager {
    private final FileManager fileManager;
    private final String logFile;
    private final Page logPage;
    private BlockId currentBlock;

    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    public LogManager(FileManager fileManager, String logFile) {
        this.fileManager = fileManager;
        this.logFile = logFile;
        byte[] bytes = new byte[fileManager.blockSize()];
        logPage = new Page(bytes);
        int logSize = fileManager.length(logFile);

        if (logSize == 0) {
            currentBlock = appendNewBlock();
        } else {
            currentBlock = new BlockId(logFile, logSize - 1);
            fileManager.read(currentBlock, logPage);
        }
    }

    /**
     * Appends a log record to the log buffer.
     * The record consists of an arbitrary array of bytes.
     * Log records are written right to left in the buffer.
     * The size of the record is written before the bytes.
     * The beginning of the buffer contains the location
     * of the last-written record (the "boundary").
     * Storing the records backwards makes it easy to read
     * them in reverse order.
     * @param record a byte array containing the bytes.
     * @return the LSN of the final value
     */
    public int append(byte[] record) {
        int boundary = logPage.getInt(0);
        int recordSize = record.length;

        int bytesNeeded = recordSize + Integer.BYTES;

        if (boundary - bytesNeeded < Integer.BYTES) { // record doesn't fit
            flush(); // move to new block
            currentBlock = appendNewBlock();
            boundary = logPage.getInt(0);
        }

        int recordPos = boundary - bytesNeeded;

        logPage.setBytes(recordPos, record);
        logPage.setInt(0, recordPos); // new boundary
        latestLSN += 1;

        return latestLSN;
    }

    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) {
            flush();
        }
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fileManager, currentBlock);
    }

    /**
     * Initialize the bytebuffer and append it to the log file.
     */
    private BlockId appendNewBlock() {
        final BlockId blockId = fileManager.append(logFile);
        logPage.setInt(0, fileManager.blockSize());
        fileManager.write(blockId, logPage);
        return blockId;
    }

    /**
     * Write the buffer to the log file.
     */
    private void flush() {
        fileManager.write(currentBlock, logPage);
        lastSavedLSN = latestLSN;
    }
}
