/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * This client inserts a specified number of rows into a Volt stream as topic.
 *
 * Another client or clients consume rows from the stream topic.
 */

package topicbenchmark2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

/**
 * Asychronously sends data to topics to test VoltDB export performance.
 */
public class TopicBenchmark2 {

    static VoltLogger log = new VoltLogger("topicbenchmark2");
    // NOTE: must match the procedure name capitalization
    static final String TEST_TOPIC = "test_topic";

    // CSV pre-formatted row template
    static final String ROW_FMT = "\"%010d\",\"The quick brown fox jumps over the lazy dog; "
            + "Portez ce vieux whisky au juge blond, qui l'aime fort\"";

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // Validated CLI config
    final TopicBenchConfig config;

    Client client;

    // Test stats variables
    long totalInserts = 0;
    AtomicLong successfulInserts = new AtomicLong(0);
    AtomicLong failedInserts = new AtomicLong(0);
    AtomicBoolean testFinished = new AtomicBoolean(false);

    static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class TopicBenchConfig extends CLIConfig {
        @Option(desc = "Comma separated list of servers to connect to (using default volt port).")
        String servers = "localhost";

        @Option(desc="Topic port on servers (default 9095)")
        int topicPort = 9095;

        @Option(desc = "How many producer invocations.")
        int count = 0;

        @Override
        public void validate() {
            if (count < 0) exitWithMessageAndUsage("count must be >= 0");
            if (topicPort <= 0) exitWithMessageAndUsage("topicPort must be > 0");
        }
    }

    /**
     * Clean way of exiting from an exception
     * @param message   Message to accompany the exception
     * @param e         The exception thrown
     */
    private void exitWithException(String message, Exception e) {
        log.error(message);
        log.info(e.getLocalizedMessage());
        System.exit(-1);
    }

    /**
     * Creates a new instance of the test to be run.
     * Establishes a client connection to a voltdb server, which should already be running
     * @param args The arguments passed to the program
     */
    public TopicBenchmark2(TopicBenchConfig config) {
        this.config = config;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setTopologyChangeAware(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);
    }


    public VoltTable getTopicStats() throws IOException,InterruptedException{
        long retryStats = 5;
        VoltTable stats = null;
        while (retryStats-- > 0) {
            try {
                stats = client.callProcedure("@Statistics", "topic", 0).getResults()[0];
                break;
            } catch (ProcCallException e) {
                log.warn("Error while calling procedures: ");
                e.printStackTrace();
            }
            Thread.sleep(5000);
        }
        return stats;
    }

    public boolean waitTilTupleCountSettles(long insertCount) throws IOException, InterruptedException {
        /*
         * There might be a delay between the completion of the last insert transaction and when
         * stats register all tuples, that is TUPLE_COUNT = insert count
         *
         * Since at this point, all the stream/partitions are disabled, we have to be careful
         * to count each partition for each table once.
         */
        long st = System.currentTimeMillis();
        //Wait 10 mins only
        long end = st + (10 * 60 * 1000);
        while (true) {
            Map<String, Long> partitionMap = new HashMap<String, Long>();
            VoltTable stats = getTopicStats();
            long totalTupleCount = 0;
            while (stats.advanceRow()) {
                long partitionid = stats.getLong("PARTITION_ID");
                String source = stats.getString("TOPIC");
                Long tupleCount = stats.getLong("LAST_OFFSET");
                String tablePart = source + "_" + partitionid;
                if (! partitionMap.containsKey(tablePart)) {
                    // only put this table+partition count in the map once
                    partitionMap.put(tablePart, tupleCount);
                    totalTupleCount += tupleCount;
                }
            }
            if (totalTupleCount == insertCount) {
                long settleTimeMillis = System.currentTimeMillis() - st;
                log.info("LAST_OFFSET settled in " + settleTimeMillis/1000.0 + " seconds");
                return true;
            }
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                log.info("Waited too long...");
                return false;
            }
            Thread.sleep(1000);
        }
    }

    /**
     * Connect to a single server.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServer(String server) {
        try {
            client.createConnection(server);
        }
        catch (IOException e) {
            log.info("Connection to " + server + " failed");
            return;
        }
        log.info("Connected to VoltDB node at: " + server);
    }

    /**
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    void connect(String servers) throws InterruptedException {
        log.info("Connecting to VoltDB...");

        String[] serverArray = servers.split(",");
        connectToOneServer(serverArray[0]);
    }

    /**
     * Inserts values into the topic: all values have identical sizes
     *
     * @throws InterruptedException
     * @throws NoConnectionsException
     */
    public void doInserts(Client client, String servers, int topicPort) {

        long rowId = 0;
        String[] serverArray = servers.split(",");
        String kafkaBroker = serverArray[0] + ":" + topicPort;
        try {
            Properties props = new Properties();
            // FIXME: we could format a list of hosts
            props.put("bootstrap.servers", kafkaBroker);
            props.put("acks", "all");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<>(props);

            // Insert objects until we've run for long enough
            log.info("Running benchmark...");
            long benchmarkStart = 0;
            benchmarkStart = System.currentTimeMillis();
            while (totalInserts < config.count) {
                // Note - create records with no key for now
                producer.send(new ProducerRecord<String, String>(TEST_TOPIC, String.format(ROW_FMT, ++rowId)));
                ++totalInserts;
            }
            producer.close();

            long endTime = System.currentTimeMillis();
            long duration = endTime - benchmarkStart;

            log.info("Benchmark complete: " + successfulInserts.get() + " successful procedure calls" +
                    " (excludes warmup)");
            log.info("Failed " + failedInserts.get() + " procedure calls");
            // Use this to correlate the total rows exported
            log.info("Total inserts: " + totalInserts + " in " + (duration / 1000) + " seconds");

            if (!waitTilTupleCountSettles(totalInserts)) {
                log.info("TUPLE_COUNT did not settle in 10 minutes");
            }
        }
        catch (Exception e) {
            log.error("Couldn't insert into topic\n");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Runs the topic benchmark test
     * @throws InterruptedException
     * @throws NoConnectionsException
     */
    private void runTest() throws InterruptedException {
        boolean success = true;
        try {
            log.info("Test initialization");
            connect(config.servers);
        } catch (InterruptedException e) {
            log.error("ERROR: Error connecting to VoltDB");
            e.printStackTrace();
            System.exit(1);
        }

        // Do the inserts in a separate thread
        long xyz = System.currentTimeMillis();
        log.info("Creating thread ");
        Thread writes = new Thread(new Runnable() {
          @Override
          public void run() {
                log.info("Running writer thread ....");
                doInserts(client, config.servers, config.topicPort);
            }
        });
        writes.start();
        writes.join();
        long duration = System.currentTimeMillis() - xyz;

        log.info("Finished benchmark in " + (duration / 1000) + " seconds.");
        client.close();

        if (!success) {
            log.error("Test client failed");
            System.exit(-1);
        } else {
            log.info("Test client finished successfully");
            System.exit(0);
        }

    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */
    public static void main(String[] args) {
        TopicBenchConfig config = new TopicBenchConfig();
        config.parse(TopicBenchmark2.class.getName(), args);

        try {
            TopicBenchmark2 bench = new TopicBenchmark2(config);
            bench.runTest();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
