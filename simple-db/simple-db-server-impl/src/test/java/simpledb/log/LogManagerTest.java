package simpledb.log;

import org.junit.jupiter.api.Test;
import simpledb.file.FileManager;
import simpledb.file.Page;

import java.io.File;
import java.util.Iterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogManagerTest {
    static int BLOCK_SIZE = 400;
    static int BUFFER_SIZE = 8;
    static String LOG_FILE = "simpledb.log";

    File dbDirectory = new File("test");
    FileManager fileManager = new FileManager(dbDirectory, BLOCK_SIZE);
    LogManager logManager = new LogManager(fileManager, LOG_FILE);

    @Test
    void test (){
        printLogRecords("The initial empty log file:");  //print an empty log file
        System.out.println("done");
        createRecords(1, 35);
        printLogRecords("The log file now has these records:");
        createRecords(36, 70);
        logManager.flush(65);
        printLogRecords("The log file now has these records:");
        assertTrue(true);
    }

    void printLogRecords(String msg) {
        System.out.println(msg);

        final Iterator<byte[]> iter = logManager.iterator();

        while (iter.hasNext()) {
            byte[] record = iter.next();
            Page page = new Page(record);
            String s = page.getString(0);
            int npos = Page.maxLength(s.length());
            int val = page.getInt(npos);
            System.out.println("[" + s + ", " + val + "]");
        }

        System.out.println();
    }

    void createRecords(int start, int end) {
        System.out.print("Creating records: ");

        for (int i = start; i <= end; i++) {
            byte[] rec = createLogRecord("record" + i, i + 100);
            int lsn = logManager.append(rec);
            System.out.print(lsn + " ");
        }

        System.out.println();
    }

    byte[] createLogRecord(String s, int n) {
        int spos = 0;
        int npos = spos + Page.maxLength(s.length());
        byte[] bytes = new byte[npos + Integer.BYTES];

        Page page = new Page(bytes);
        page.setString(spos, s);
        page.setInt(npos, n);

        return bytes;
    }
}
