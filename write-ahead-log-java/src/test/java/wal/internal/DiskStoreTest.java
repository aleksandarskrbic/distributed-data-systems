package wal.internal;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DiskStoreTest {

    long lenWidth = 8;
    String entry = "First Log Entry";
    byte[] entryBytes = entry.getBytes();
    long width = entryBytes.length + lenWidth;

    @Test
    void append() {
        Store store = Store.make("append-test-wal");

        assert store != null;

        AppendResult result1 = store.append(entryBytes);
        AppendResult result2 = store.append(entryBytes);
        AppendResult result3 = store.append(entryBytes);

        assertEquals(result1.written + result1.position, width);
        assertEquals(result2.written + result2.position, 2 * width);
        assertEquals(result3.written + result3.position, 3 * width);
    }

    @Test
    void read() {
        Store store = Store.make("read-test-wal");

        assert store != null;

        store.append(entryBytes);
        store.append(entryBytes);
        store.append(entryBytes);

        ReadResult result1 = store.read(0 * width);
        ReadResult result2 = store.read(1 * width);
        ReadResult result3 = store.read(2 * width);

        assertEquals(entry, new String(result1.content));
        assertEquals(entry, new String(result2.content));
        assertEquals(entry, new String(result3.content));
    }

}