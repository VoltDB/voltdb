package windowing;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

public class DataGenerator extends Thread {

    AtomicBoolean shouldContinue = new AtomicBoolean(true);
    Client client;
    static Random rand = new Random();
    static byte[] randomData = new byte[64];
    static {
        rand.nextBytes(randomData);
    }


    DataGenerator(Client client) {
        this.client = client;
    }

    public void shutdown() {
        shouldContinue.set(false);
        assert(Thread.currentThread() != this);
        try { join(); } catch (InterruptedException e) {}
    }

    static void insert(Client client) {
        // unique identifier and partition key
        String uuid = UUID.randomUUID().toString();

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

    @Override
    public void run() {
        while (shouldContinue.get()) {
            insert(client);
        }
    }
}
