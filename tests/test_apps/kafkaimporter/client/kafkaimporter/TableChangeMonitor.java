package kafkaimporter.client.kafkaimporter;

import java.io.IOException;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

public class TableChangeMonitor {
	Client client;
	String table = "";
	String type = "";

    public TableChangeMonitor(Client client, String type, String table) {
    	this.type = type;
    	this.table = table;
    	this.client = client;
    }

	/**
     * Checks the export table to make sure that everything has been successfully
     * processed.
     * @throws ProcCallException
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean waitForStreamedAllocatedMemoryZero() throws ProcCallException,IOException,InterruptedException {
        boolean passed = false;

        VoltTable stats = null;
        long ftime = 0;
        long st = System.currentTimeMillis();
        //Wait 10 mins only
        long end = st + (10 * 60 * 1000);
        while (true) {
            stats = client.callProcedure("@Statistics", "table", 0).getResults()[0];
            boolean passedThisTime = true;
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                System.out.println("Waited too long...");
                System.out.println(stats);
                break;
            }
            if (ctime - st > (3 * 60 * 1000)) {
                System.out.println(stats);
                st = System.currentTimeMillis();
            }
            long ts = 0;
            while (stats.advanceRow()) {
                String ttable = stats.getString("TABLE_NAME");
                String ttype = stats.getString("TABLE_TYPE");
                Long tts = stats.getLong("TIMESTAMP");
                //Get highest timestamp and watch it change
                if (tts > ts) {
                    ts = tts;
                }
                if (type.equals(ttype) && table.equals(ttable)) {
                    if (stats.getLong("TUPLE_ALLOCATED_MEMORY") != 0) {
                        passedThisTime = false;
                        System.out.println(ttable + ": Partition Not Zero.");
                        break;
                    }
                }
            }
            if (passedThisTime) {
                if (ftime == 0) {
                    ftime = ts;
                    continue;
                }
                //we got 0 stats 2 times in row with diff highest timestamp.
                if (ftime != ts) {
                    passed = true;
                    break;
                }
                System.out.println(table + " quiescing but not ready to declare victory.");
            }
            Thread.sleep(5000);
        }
        System.out.println(table + " status is: " + passed);
        System.out.println(stats);
        return passed;
    }

}
