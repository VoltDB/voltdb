package windowing;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

public class ContinuousDeleter extends Thread {

    AtomicBoolean shouldContinue = new AtomicBoolean(true);
    final Client client;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicReference<Set<String>> partitionKeys = new AtomicReference<Set<String>>(new TreeSet<String>());

    long maxRows;
    long maxMb;
    long durationMs;

    class UpdateGlobalRowCount implements Runnable {

        @Override
        public void run() {
            try {
                VoltTable results[] = client.callProcedure("@GetPartitionKeys", "INTEGER").getResults();
            }
            catch (IOException | ProcCallException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

    }

    class UpdatePartitionKeyValueSet implements Runnable {

        @Override
        public void run() {
            try {
                Set<String> localPartitionKeys = new TreeSet<>();
                VoltTable keyTable = client.callProcedure("@GetPartitionKeys", "STRING").getResults()[0];
                while (keyTable.advanceRow()) {
                    String partitionKey = keyTable.getString(0);
                    assert(partitionKey != null);
                    localPartitionKeys.add(partitionKey);
                }

                partitionKeys.set(localPartitionKeys);

            } catch (IOException | ProcCallException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public ContinuousDeleter(Client client, long maxRows, long maxMb, long duration, TimeUnit durationTimeUnit) {
        this.client = client;
        this.maxRows = maxRows;
        this.maxMb = maxMb;
        this.durationMs = durationTimeUnit.toMillis(duration);
    }

    public void shutdown() {
        shouldContinue.set(false);
        assert(Thread.currentThread() != this);
        try { join(); } catch (InterruptedException e) {}
    }

    @Override
    public void run() {

        scheduler.scheduleAtFixedRate(new UpdatePartitionKeyValueSet(), 1, 1, TimeUnit.SECONDS);

        Random rand = new Random();

        byte[] randomData = new byte[64];
        rand.nextBytes(randomData);

        while (shouldContinue.get()) {
            // unique identifier and partition key
            String uuid = UUID.nameUUIDFromBytes(randomData).toString();

            // millisecond timestamp
            Date now = new Date();

            // integral gaussian value with stddev = 1000
            int val = (int) Math.min(Integer.MAX_VALUE, rand.nextGaussian() * 1000);

            try {
                ClientResponse response = client.callProcedure("TIMEDATA.insert", uuid, val, now);
                assert(response.getStatus() == ClientResponse.SUCCESS);
            }
            catch (IOException | ProcCallException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
