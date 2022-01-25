package wal.internal;

public class ReadResult {
    final byte[] content;

    public ReadResult(byte[] content) {
        this.content = content;
    }
}
