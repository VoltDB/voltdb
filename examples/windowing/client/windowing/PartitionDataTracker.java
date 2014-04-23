package windowing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

public class PartitionDataTracker {

    class PartitionInfo {
        String partitionKey;
        long tupleCount;
    }

    // these values are updated by the UpdatePartitionData class each time it is run
    protected final AtomicReference<Map<Long, PartitionInfo>> partitionData =
            new AtomicReference<Map<Long, PartitionInfo>>(new HashMap<Long, PartitionInfo>());
    protected final AtomicLong globalTupleCount = new AtomicLong(0);
    protected final AtomicLong redundancy = new AtomicLong(1);

    protected final Client client;

    public PartitionDataTracker(Client client) {
        this.client = client;
    }

    public Map<Long, PartitionInfo> getPartitionInfo() {
        return partitionData.get();
    }

    public long getGlobalTupleCount() {
        return globalTupleCount.get();
    }

    public long getRedundancy() {
        return redundancy.get();
    }

    public void update() {
        try {
            Map<Long, PartitionInfo> partitionDataTemp = new HashMap<Long, PartitionInfo>();

            VoltTable partitionKeys = null, tableStats = null;

            try {
                tableStats = client.callProcedure("@Statistics", "TABLE").getResults()[0];
                partitionKeys = client.callProcedure("@GetPartitionKeys", "STRING").getResults()[0];
            }
            catch (IOException | ProcCallException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }

            while (tableStats.advanceRow()) {
                if (!tableStats.getString("TABLE_NAME").equalsIgnoreCase("timedata")) {
                    continue;
                }

                PartitionInfo pinfo = new PartitionInfo();
                long partitionId = tableStats.getLong("PARTITION_ID");
                pinfo.tupleCount = tableStats.getLong("TUPLE_COUNT");
                pinfo.partitionKey = null;

                // If redundancy (k-safety) is enabled, this will put k+1 times per partition,
                // but the tuple count will be the same so it will be ok.
                partitionDataTemp.put(partitionId, pinfo);
            }

            while (partitionKeys.advanceRow()) {
                long partitionId = partitionKeys.getLong("PARTITION_ID");
                PartitionInfo pinfo = partitionDataTemp.get(partitionId);
                if (pinfo == null) {
                    // The set of partitions from the two calls don't match.
                    // Try again next time this is called... Maybe things
                    // will have settled down.
                    return;
                }

                pinfo.partitionKey = partitionKeys.getString("PARTITION_KEY");
            }

            // this is a sanity check to see that every partition has
            // a partition value
            long globalTupleCountTemp = 0;
            boolean allMatched = true;
            for (PartitionInfo pinfo : partitionDataTemp.values()) {
                globalTupleCountTemp += pinfo.tupleCount;

                // a partition has a count, but no key
                if (pinfo.partitionKey == null) {
                    allMatched = false;
                }
            }
            globalTupleCount.set(globalTupleCountTemp);
            redundancy.set(tableStats.getRowCount() / globalTupleCountTemp);
            if (!allMatched) {
                // The set of partitions from the two calls don't match.
                // Try again next time this is called... Maybe things
                // will have settled down.
                return;
            }

            // atomically update the new map for the old one
            partitionData.set(partitionDataTemp);
        }
        catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }
}
