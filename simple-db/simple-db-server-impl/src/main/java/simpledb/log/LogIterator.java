package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileManager;
import simpledb.file.Page;

import java.util.Iterator;

public class LogIterator implements Iterator<byte[]> {
    private FileManager fileManager;
    private BlockId blockId;
    private Page page;
    private int currentPos;
    private int boundary;

    /**
     * Creates an iterator for the records in the log file,
     * positioned after the last log record.
     */
    public LogIterator(FileManager fileManager, BlockId blockId) {
        this.fileManager = fileManager;
        this.blockId = blockId;
        byte[] bytes = new byte[fileManager.blockSize()];
        page = new Page(bytes);
        moveToBlock(blockId);
    }

    /**
     * Determines if the current log record
     * is the earliest record in the log file.
     * @return true if there is an earlier record
     */
    public boolean hasNext() {
        return currentPos < fileManager.blockSize() || blockId.blockNumber() > 0;
    }

    /**
     * Moves to the next log record in the block.
     * If there are no more log records in the block,
     * then move to the previous block
     * and return the log record from there.
     * @return the next earliest log record
     */
    public byte[] next() {
        if (currentPos == fileManager.blockSize()) {
            blockId = new BlockId(blockId.filename(), blockId.blockNumber() - 1);
            moveToBlock(blockId);
        }
        byte[] rec = page.getBytes(currentPos);
        currentPos += Integer.BYTES + rec.length;
        return rec;
    }

    /**
     * Moves to the specified log block
     * and positions it at the first record in that block
     * (i.e., the most recent one).
     */
    private void moveToBlock(BlockId blockId) {
        fileManager.read(blockId, page);
        boundary = page.getInt(0);
        currentPos = boundary;
    }
}
