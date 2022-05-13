package simpledb.file;

public class BlockId {
    private final String filename;
    private final int blockNumber;

    public BlockId(String filename, int blockNumber) {
        this.filename = filename;
        this.blockNumber = blockNumber;
    }

    public String filename() {
        return filename;
    }

    public int blockNumber() {
        return blockNumber;
    }

    @Override
    public boolean equals(Object object) {
        final BlockId blockId = (BlockId) object;
        return filename.equals(blockId.filename) && blockNumber == blockId.blockNumber;
    }

    @Override
    public String toString() {
        return "[file " + filename + ", block " + blockNumber + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
