package wal.internal;

public class AppendResult {
    final long written;
    final long position;

    public AppendResult(long written, long position) {
        this.written = written;
        this.position = position;
    }
}
