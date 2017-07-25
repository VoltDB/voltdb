/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package metrocard;

import java.util.Calendar;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.voltdb.CLIConfig;
import org.voltdb.client.ClientStatusListenerExt;


public class MetroSimulation {

    // handy, rather than typing this out several times
    public static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final MetroCardConfig config;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;

    private Random rand = new Random();

    // for random data generation
    private RandomCollection<Integer> stations = new RandomCollection<Integer>();
    int[] balances = {5000,2000,1000,500};
    Calendar cal = Calendar.getInstance();
    int cardCount = 0;
    int max_station_id = 0;
    Properties producerConfig;

    /**
     * Prints headings
     */
    public static void printHeading(String heading) {
        System.out.print("\n"+HORIZONTAL_RULE);
        System.out.println(" " + heading);
        System.out.println(HORIZONTAL_RULE);
    }

    /**
     * Uses CLIConfig class to declaratively state command line options
     * with defaults and validation.
     */
    public static class MetroCardConfig extends CLIConfig {

        // STANDARD BENCHMARK OPTIONS
        @Option(desc = "Broker to connect to publish swipes and train activity.")
        String broker = "localhost:9092";


        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 300;

        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Swipes are posted to this topic.")
        String swipe = "card_swipes";

        @Option(desc = "Swipes are posted to this topic.")
        String trains = "train_activity";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
        }
    }

    public static class RandomCollection<E> {
        private final NavigableMap<Double, E> map = new TreeMap<Double, E>();
        private final Random random;
        private double total = 0;

        public RandomCollection() {
            this(new Random());
        }

        public RandomCollection(Random random) {
            this.random = random;
        }

        public void add(double weight, E result) {
            if (weight <= 0) return;
            total += weight;
            map.put(total, result);
        }

        public E next() {
            double value = random.nextDouble() * total;
            return map.ceilingEntry(value).getValue();
        }
    }

    // constructor
    public MetroSimulation(MetroCardConfig config) {
        this.config = config;

        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.broker);

        printHeading("Command Line Configuration");
        System.out.println(config.getConfigDumpString());
    }

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            // if the benchmark is still active
            if ((System.currentTimeMillis() - benchmarkStartTS) < (config.duration * 1000)) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }
        }
    }

    /**
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
     */
    public void schedulePeriodicStats() {
        timer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() { printStatistics(); }
        };
        timer.scheduleAtFixedRate(statsPrinting,
                                  config.displayinterval * 1000,
                                  config.displayinterval * 1000);
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized void printStatistics() {
        //TODO: Print some kafka stats.
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        printHeading("Transaction Results");
    }

    public void initialize() throws Exception {

    }

    public int randomizeNotify() throws Exception {
        // create a small number of text and email notification
        // preferences, settable via random weighting below
        float n = rand.nextFloat();
        if (n > 0.01) {
            return(0);
        }
        if (n > 0.005) {
            return(1);
        }
        return(2);
    }

    abstract class KafkaPublisher extends Thread {
        private volatile int m_totalCount;
        private volatile int m_currentCount = 0;
        private volatile Exception m_error = null;
        private final KafkaProducer m_producer;
        private final String m_topic;
        private Properties m_producerConfig;

        public KafkaPublisher(String topic, Properties producerConfig, int count) {
            m_topic = topic;
            m_totalCount = count;
            m_producerConfig = new Properties();
            m_producerConfig.putAll(producerConfig);
            m_producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            m_producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            m_producer = new KafkaProducer(m_producerConfig);
        }

        protected abstract void initialize();
        protected void close() {
            m_producer.close();
        }
        protected abstract ProducerRecord<String, String> getNextRecord();

        @Override
        public void run() {
            initialize();
            if (m_totalCount == 0) {
                m_totalCount = Integer.MAX_VALUE; // keep running until explicitly stopped
            }
            try {
                while (m_currentCount < m_totalCount) {
                    ProducerRecord<String, String> record = getNextRecord();
                    m_producer.send(record);
                    m_currentCount++;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                m_error = ex;
            } finally {
                close();
            }
        }

        public void shutDown() throws Exception {
            if (m_totalCount == Integer.MAX_VALUE) {
                m_totalCount = 0; // this will stop the pusher's forever loop
            }
            if (m_error != null) {
                throw m_error;
            }
        }
    }

    class TrainActivityPublisher extends KafkaPublisher {
        MetroCardConfig m_config;
        TrainActivityPublisher(String trainId, MetroCardConfig config, Properties producerConfig, int count) {
            super(config.trains, producerConfig, count);
            m_config = config;
        }

        @Override
        protected void initialize() {
        }

        @Override
        protected ProducerRecord<String, String> getNextRecord() {
            return new ProducerRecord<String, String>(m_config.trains, "key", "value");
        }

    }

    class SwipeActivityPublisher extends KafkaPublisher {

        SwipeActivityPublisher(MetroCardConfig config, Properties producerConfig, int count) {
            super(config.swipe, producerConfig, count);
        }

        @Override
        protected void initialize() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        protected ProducerRecord<String, String> getNextRecord() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }

    public void iterate() throws Exception {

    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        printHeading("Setup & Initialization");


        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        System.out.println("\nRunning benchmark...");
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);

        TrainActivityPublisher redLine = new TrainActivityPublisher("1", config, producerConfig, 100);
        redLine.start();
        while (benchmarkEndTime > System.currentTimeMillis()) {
            Thread.sleep(10000);
        }
        redLine.close();

        // cancel periodic stats printing
        timer.cancel();

        // print the summary results
        printResults();

    }

    public static void main(String[] args) throws Exception {
        MetroCardConfig config = new MetroCardConfig();
        config.parse(MetroSimulation.class.getName(), args);

        MetroSimulation benchmark = new MetroSimulation(config);
        benchmark.runBenchmark();

    }
}
