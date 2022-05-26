/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package frauddetection;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.voltdb.CLIConfig;


public class FraudSimulation {

    // handy, rather than typing this out several times
    public static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    private static final double FRAUD_RATIO = 0.002; // 2% fraud

    // validated command line configuration
    final MetroCardConfig config;
    // Benchmark start time
    long benchmarkStartTS;

    int[] balances = {5000,2000,1000,500};
    Calendar cal = Calendar.getInstance();
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

        @Option(desc = "Number of activities per train.")
        long count = 5000;

        @Option(desc = "Number of Cards. If you loaded cards via csv make sure you use that number.")
        int cardcount = 500000;

        @Option(desc = "Swipes are posted to this topic.")
        String swipe = "card_swipes";

        @Option(desc = "Swipes are posted to this topic.")
        String trains = "train_activity";

        @Override
        public void validate() {
            if (count < 0) exitWithMessageAndUsage("count must be > 0");
        }
    }

    public static class RandomCollection<E> {
        private final NavigableMap<Double, E> map = new TreeMap<>();
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
    public FraudSimulation(MetroCardConfig config) {
        this.config = config;

        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.broker);
        producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "metrocard");

        printHeading("Command Line Configuration");
        System.out.println(config.getConfigDumpString());
    }

    abstract class KafkaPublisher extends Thread {
        private volatile long m_totalCount;
        private volatile long m_currentCount = 0;
        private final KafkaProducer m_producer;
        private final Properties m_producerConfig;

        public KafkaPublisher(Properties producerConfig, long count) {
            m_totalCount = count;
            m_producerConfig = new Properties();
            m_producerConfig.putAll(producerConfig);
            m_producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            m_producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            m_producer = new KafkaProducer<>(m_producerConfig);
        }

        protected abstract boolean doEnd();
        protected abstract void initialize();
        protected void close() {
            try {
                m_producer.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        protected abstract List<ProducerRecord<String, String>> getNextRecords();

        @Override
        public void run() {
            initialize();
            if (m_totalCount == 0) {
                m_totalCount = Integer.MAX_VALUE; // keep running until explicitly stopped
            }
            try {
                while (m_currentCount < m_totalCount) {
                    List<ProducerRecord<String, String>> records = getNextRecords();
                    if (records != null) {
                        for (ProducerRecord<String, String> record : records) {
                            m_producer.send(record);
                            m_currentCount++;
                        }
                    }
                    if (doEnd()) {
                        break;
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                close();
            }
        }
    }

    public static int tcnt = 0;
    class TrainActivityPublisher extends KafkaPublisher {
        MetroCardConfig m_config;
        public final String m_trainId;
        public final long stopTime = 60000; //60 seconds randomize?
        public final long lastStopWait = 120000;
        public int curStation = 1;
        public long startTime;
        public long endTime;
        public volatile int lastState = 0; // 0 for arrival, 1 for departure
        public long departureTime;
        public volatile int direction = 1;
        public boolean close = false;
        public Map<Integer,Integer> STATION_TO_NEXT_STATION = new HashMap<>();

        TrainActivityPublisher(String trainId, MetroCardConfig config, Properties producerConfig, long count) {
            super(producerConfig, count);
            m_trainId = trainId;
            m_config = config;
        }

        @Override
        protected void initialize() {
            STATION_TO_NEXT_STATION.put(1, 2*60000);
            STATION_TO_NEXT_STATION.put(2, 2*60000);
            STATION_TO_NEXT_STATION.put(3, 3*60000);
            STATION_TO_NEXT_STATION.put(4, 4*60000);
            STATION_TO_NEXT_STATION.put(5, 2*60000);
            STATION_TO_NEXT_STATION.put(6, 2*60000);
            STATION_TO_NEXT_STATION.put(7, 1*60000);
            STATION_TO_NEXT_STATION.put(8, 2*60000);
            STATION_TO_NEXT_STATION.put(9, 2*60000);
            STATION_TO_NEXT_STATION.put(10, 2*60000);
            STATION_TO_NEXT_STATION.put(11, 2*60000);
            STATION_TO_NEXT_STATION.put(12, 2*60000);
            STATION_TO_NEXT_STATION.put(13, 2*60000);
            STATION_TO_NEXT_STATION.put(14, 3*60000);
            STATION_TO_NEXT_STATION.put(15, 2*60000);
            STATION_TO_NEXT_STATION.put(16, 3*60000);
            STATION_TO_NEXT_STATION.put(17, 3*60000);
            //Shift start time for each train by 1 min
            startTime = System.currentTimeMillis() + ((tcnt++ * 60) * 1000);
            System.out.println("Start time for train " + m_trainId + " Is: " + new Date(startTime));
            endTime = startTime + 2*60*60*1000;
            System.out.println("End time for train " + m_trainId + " Is: " + new Date(endTime));
        }

        @Override
        protected boolean doEnd() {
            return (close || (startTime > endTime));
        }

        @Override
        protected List<ProducerRecord<String, String>> getNextRecords() {
            StringBuilder sb = new StringBuilder();
            long eventTime = startTime;
            sb.append(m_trainId).append(",")
                    .append(Integer.valueOf(curStation)).append(",")
                    .append(Integer.valueOf(lastState)).append(",")
                    .append(Long.valueOf(eventTime*1000));
            ProducerRecord<String, String> rec = new ProducerRecord<>(m_config.trains, m_trainId, sb.toString());
            long wtime = stopTime;
            if (lastState == 1) { // Only move station when departing.
                curStation = curStation + direction;
                if (curStation > 17) {
                    curStation = 17;
                    wtime += lastStopWait;
                    direction = -1;
                } else if (curStation <= 0) {
                    curStation = 1;
                    direction = 1;
                    wtime += lastStopWait;
                }
            }
            if (lastState == 0) { //Stopped
                startTime = startTime + wtime;
            } else { //Moving and arriving
                startTime = startTime + STATION_TO_NEXT_STATION.get(curStation) + wtime;
            }
            if (lastState == 0) {
                lastState = 1;
            } else {
                lastState = 0;
            }
            return Collections.singletonList(rec);
        }

    }

    Map<Integer,Long> cardsEntered = Collections.synchronizedMap(new HashMap<>());
    class SwipeEntryActivityPublisher extends KafkaPublisher {
        private final Random rand = new Random();
        private final int max_station_id = 16;
        private final RandomCollection<Integer> stations = new RandomCollection<>();
        public long startTime;
        public long endTime;
        public long swipeTime;
        final MetroCardConfig m_config;
        final int activity_code;
        boolean massReached = false;

        SwipeEntryActivityPublisher(MetroCardConfig config, Properties producerConfig, long count) {
            super(producerConfig, count);
            activity_code = 1;
            m_config = config;
        }

        @Override
        protected boolean doEnd() {
            return (swipeTime > endTime);
        }

        @Override
        protected void initialize() {
            stations.add(242200,1);
            stations.add(325479,2);
            stations.add(221055,3);
            stations.add(581530,4);
            stations.add(406389,5);
            stations.add(375640,6);
            stations.add(259210,7);
            stations.add(412809,8);
            stations.add(496942,9);
            stations.add(559110,10);
            stations.add(131022,11);
            stations.add(145955,12);
            stations.add(207333,13);
            stations.add(56457,14);
            stations.add(122236,15);
            stations.add(51981,16);
            stations.add(203866,17);
            startTime = System.currentTimeMillis();
            swipeTime = startTime;
            endTime = startTime + 45*60*1000 + 60000;
            System.out.println("End time for swipe ENTERY Is: " + new Date(endTime));
        }

        @Override
        protected List<ProducerRecord<String, String>> getNextRecords() {
            int card_id = -1;
            long atime;
            int amt = 0;
            atime = swipeTime = System.currentTimeMillis() + 5000;
            while (card_id == -1) {
                card_id = rand.nextInt(config.cardcount); // use +1000 so sometimes we get an invalid card_id
                if (cardsEntered.containsKey(card_id)) {
                    if (cardsEntered.size() >= config.cardcount) {
                        try {
                            Thread.yield();
                            Thread.sleep(10);
                        } catch (InterruptedException ex) {
                            return null;
                        }
                    }
                    card_id = -1;
                    continue;
                }
                cardsEntered.put(card_id, atime);
                break;
            }
            //Get a station.
            int station_id = pickStation();

            final List<ProducerRecord<String, String>> records = new ArrayList<>();
            records.add(generateRecord(card_id, atime, amt, station_id));

            // Generate fraudulent transactions 2% of the time
            if (rand.nextDouble() <= FRAUD_RATIO) {
                if (rand.nextBoolean()) {
                    // Entries at different stations in short interval
                    for (int i = 1; i <= 25; i++) {
                        records.add(generateRecord(card_id,
                                                   atime + i,
                                                   amt,
                                                   i % max_station_id));
                    }
                } else {
                    // Consecutive entries at same station in short interval
                    for (int i = 1; i <= 15; i++) {
                        records.add(generateRecord(card_id,
                                                   atime + i,
                                                   amt,
                                                   station_id));
                    }
                }
            }

            return records;
        }

        private ProducerRecord<String, String> generateRecord(int card_id, long atime, int amt, int station_id)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(Integer.valueOf(card_id)).append(",")
              .append(Long.valueOf(atime * 1000)).append(",")
              .append(Integer.valueOf(station_id)).append(",")
              .append(Integer.valueOf(activity_code)).append(",")
              .append(Integer.valueOf(amt));
            return new ProducerRecord<>(m_config.swipe, String.valueOf(card_id), sb.toString());
        }

        private int pickStation() {
            int station_id;
            if (rand.nextInt(16) == 0) {
                station_id = rand.nextInt(max_station_id) + 1; // sometimes pick a random station
            } else {
                station_id = stations.next(); // pick a station based on the weights
            }
            return station_id;
        }
    }

    class SwipeExitActivityPublisher extends KafkaPublisher {
        private final Random rand = new Random();
        private final int max_station_id = 16;
        private final RandomCollection<Integer> stations = new RandomCollection<>();
        public long startTime;
        public long endTime;
        public long swipeTime;
        final MetroCardConfig m_config;
        final int activity_code;
        boolean massReached = false;
        public volatile boolean close = false;

        SwipeExitActivityPublisher(MetroCardConfig config, Properties producerConfig, long count) {
            super(producerConfig, count);
            activity_code = -1;
            m_config = config;
        }

        @Override
        protected boolean doEnd() {
            return close;
        }

        @Override
        protected void initialize() {
            stations.add(242200,1);
            stations.add(325479,2);
            stations.add(221055,3);
            stations.add(581530,4);
            stations.add(406389,5);
            stations.add(375640,6);
            stations.add(259210,7);
            stations.add(412809,8);
            stations.add(496942,9);
            stations.add(559110,10);
            stations.add(131022,11);
            stations.add(145955,12);
            stations.add(207333,13);
            stations.add(56457,14);
            stations.add(122236,15);
            stations.add(51981,16);
            stations.add(203866,17);
            startTime = System.currentTimeMillis();
            swipeTime = startTime + 120000; // Exit time minimum after frst station.
            endTime = startTime + 50*60*1000 + 60000;//1*60*60*1000 + 60000;
            System.out.println("End time for swipe EXIT Is: " + new Date(endTime));
        }

        @Override
        protected List<ProducerRecord<String, String>> getNextRecords() {
            long atime;
            int amt = 0;
            Integer card_id;
            if (cardsEntered.size() <= 0) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException ex) {
                    ;
                }
                return null;
            }

            Integer arr[] = cardsEntered.keySet().toArray(new Integer[0]);
            card_id = arr[rand.nextInt(arr.length)];
            long st = cardsEntered.remove(card_id);
            //Longest journy is 39 min so pick a random exit time from start.
            atime = st + ((rand.nextInt(10)+1)*60000);

            //Get a station.
            int station_id;
            if (rand.nextInt(16) == 0) {
                station_id = rand.nextInt(max_station_id) + 1; // sometimes pick a random station
            } else {
                station_id = stations.next(); // pick a station based on the weights
            }
            StringBuilder sb = new StringBuilder();
            sb.append(card_id).append(",")
                    .append(Long.valueOf(atime*1000)).append(",")
                    .append(Integer.valueOf(station_id)).append(",")
                    .append(Integer.valueOf(activity_code)).append(",")
                    .append(Integer.valueOf(amt));
            ProducerRecord<String, String> rec = new ProducerRecord<>(m_config.swipe, String.valueOf(card_id), sb.toString());
            //System.out.println(sb.toString());
            return Collections.singletonList(rec);
        }

    }

    class SwipeReplenishActivityPublisher extends KafkaPublisher {
        private final Random rand = new Random();
        private final int max_station_id = 16;
        private final RandomCollection<Integer> stations = new RandomCollection<>();
        public long startTime;
        public long swipeTime;
        final MetroCardConfig m_config;
        final int activity_code;
        boolean massReached = false;
        public volatile boolean close = false;

        SwipeReplenishActivityPublisher(MetroCardConfig config, Properties producerConfig, long count) {
            super(producerConfig, count);
            activity_code = 2;
            m_config = config;
        }

        @Override
        protected boolean doEnd() {
            return close;
        }

        @Override
        protected void initialize() {
            stations.add(242200,1);
            stations.add(325479,2);
            stations.add(221055,3);
            stations.add(581530,4);
            stations.add(406389,5);
            stations.add(375640,6);
            stations.add(259210,7);
            stations.add(412809,8);
            stations.add(496942,9);
            stations.add(559110,10);
            stations.add(131022,11);
            stations.add(145955,12);
            stations.add(207333,13);
            stations.add(56457,14);
            stations.add(122236,15);
            stations.add(51981,16);
            stations.add(203866,17);
            startTime = System.currentTimeMillis();
            if (activity_code == 0)
                swipeTime = startTime;
            else
                swipeTime = startTime + 120000; // Exit time minimum after frst station.
        }

        @Override
        protected List<ProducerRecord<String, String>> getNextRecords() {
            int card_id = -1;
            long atime;
            int amt = 0;
            //Every 20 get an amount
            if (rand.nextInt(20) != 0) {
                return null;
            }
            card_id = rand.nextInt(config.cardcount+50); // use +50 so sometimes we get an invalid card_id
            atime = System.currentTimeMillis();
            amt = (rand.nextInt(18)+2)*1000; // $2 to $20

            //Get a station.
            int station_id;
            if (rand.nextInt(16) == 0) {
                station_id = rand.nextInt(max_station_id) + 1; // sometimes pick a random station
            } else {
                station_id = stations.next(); // pick a station based on the weights
            }
            StringBuilder sb = new StringBuilder();
            sb.append(Integer.valueOf(card_id)).append(",")
                    .append(Long.valueOf(atime*1000)).append(",")
                    .append(Integer.valueOf(station_id)).append(",")
                    .append(Integer.valueOf(activity_code)).append(",")
                    .append(Integer.valueOf(amt));
            ProducerRecord<String, String> rec = new ProducerRecord<>(m_config.swipe, String.valueOf(card_id), sb.toString());
            //System.out.println(sb.toString());
            return Collections.singletonList(rec);
        }
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        printHeading("Publishing Train Activities");

        String trains[] = { "1", "2", "3", "4", "5", "6", "7", "8" };
        List<TrainActivityPublisher> trainPubs = new ArrayList<>();
        SwipeEntryActivityPublisher entry = new SwipeEntryActivityPublisher(config, producerConfig, config.count);
        entry.start();
        SwipeExitActivityPublisher exit = new SwipeExitActivityPublisher(config, producerConfig, config.count);
        exit.start();
        SwipeReplenishActivityPublisher replenish = new SwipeReplenishActivityPublisher(config, producerConfig, config.count/5);
        replenish.start();
        //Wait for a min to start trains.
        Thread.sleep(6000);
        System.out.println("Starting All Trains....");
        for (String train : trains) {
            TrainActivityPublisher redLine = new TrainActivityPublisher(train, config, producerConfig, Integer.MAX_VALUE);
            redLine.start();
            trainPubs.add(redLine);
        }
        System.out.println("All Trains Started....");
        entry.join();
        exit.close = true;
        exit.join();
        replenish.close = true;
        replenish.join();
    }

    public static void main(String[] args) throws Exception {
        MetroCardConfig config = new MetroCardConfig();
        config.parse(FraudSimulation.class.getName(), args);

        FraudSimulation benchmark = new FraudSimulation(config);
        benchmark.runBenchmark();

    }
}
