package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    public static Charset CHARSET = StandardCharsets.US_ASCII;

    private ByteBuffer buffer;

    // Used by BufferManager for creating data buffers
    public Page(int blockSize) {
        buffer = ByteBuffer.allocateDirect(blockSize);
    }

    // Used by LogManager for creating log pages
    public Page(byte[] bytes) {
        buffer = ByteBuffer.wrap(bytes);
    }

    public int getInt(int offset) {
        return buffer.getInt(offset);
    }

    public void setInt(int offset, int n) {
        buffer.putInt(offset, n);
    }

    public byte[] getBytes(int offset) {
        buffer.position(offset);
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    public void setBytes(int offset, byte[] bytes) {
        buffer.position(offset);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    public String getString(int offset) {
        byte[] bytes = getBytes(offset);
        return new String(bytes, CHARSET);
    }

    public void setString(int offset, String string) {
        byte[] bytes = string.getBytes(CHARSET);
        setBytes(offset, bytes);
    }


    public static int maxLength(int strlen) {
        int bytesPerChar = (int) CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * bytesPerChar);
    }

    // a package private method, needed by FileManager
    ByteBuffer contents() {
        buffer.position(0);
        return buffer;
    }
}
