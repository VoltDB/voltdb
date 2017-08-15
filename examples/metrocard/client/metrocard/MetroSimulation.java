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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
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


public class MetroSimulation {

    // handy, rather than typing this out several times
    public static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final MetroCardConfig config;
    // Benchmark start time
    long benchmarkStartTS;

    private Random rand = new Random();

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
    public MetroSimulation(MetroCardConfig config) {
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

    public static final Map<Integer,Integer> STATION_TO_NEXT_STATION = new HashMap<>();
    static {
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
                    if (record != null) {
                        m_producer.send(record);
                        m_currentCount++;
                    } else {
                        //If we didnt get record that may be on purpose so wait.
                        Thread.yield();
                        Thread.sleep(200);
                    }
                    if (doEnd()) {
                        break;
                    }
                }
            } catch (InterruptedException ex) {
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
        public volatile int lastState = 0; // 0 for arrival, 1 for departure
        public long arrivalTime;
        public long departureTime;
        public volatile int direction = 1;
        TrainActivityPublisher(String trainId, MetroCardConfig config, Properties producerConfig, long count) {
            super(producerConfig, count);
            m_trainId = trainId;
            m_config = config;
        }

        @Override
        protected void initialize() {
            //Shift start time for each train by 1 min
            startTime = System.currentTimeMillis() + (tcnt++ * 120*1000);
            arrivalTime = startTime;
        }

        @Override
        protected boolean doEnd() {
            return false;
        }

        @Override
        protected ProducerRecord<String, String> getNextRecord() {
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
            if (lastState == 1) {
                lastState = 0;
            } else {
                lastState = 1;
            }
            return rec;
        }

    }

    Map<Integer,Long> cardsEntered = Collections.synchronizedMap(new HashMap<>());
    class SwipeEntryActivityPublisher extends KafkaPublisher {
        private final Random rand = new Random();
        private final int max_station_id = 16;
        private final RandomCollection<Integer> stations = new RandomCollection<>();
        public long startTime;
        public long swipeTime;
        final MetroCardConfig m_config;
        final int activity_code;
        boolean massReached = false;
        public volatile boolean close = false;

        SwipeEntryActivityPublisher(MetroCardConfig config, Properties producerConfig, long count) {
            super(producerConfig, count);
            activity_code = 1;
            m_config = config;
        }

        @Override
        protected boolean doEnd() {
            return (close && cardsEntered.size() <= 1);
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
        }

        @Override
        protected ProducerRecord<String, String> getNextRecord() {
            int card_id = -1;
            long atime;
            int amt = 0;
            atime = System.currentTimeMillis();
            while (card_id == -1) {
                card_id = rand.nextInt(config.cardcount+1000); // use +1000 so sometimes we get an invalid card_id
                if (cardsEntered.containsKey(card_id)) {
                    card_id = -1;
                    continue;
                }
                cardsEntered.put(card_id, atime);
                break;
            }
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
            return rec;
        }
    }

    class SwipeExitActivityPublisher extends KafkaPublisher {
        private final Random rand = new Random();
        private final int max_station_id = 16;
        private final RandomCollection<Integer> stations = new RandomCollection<>();
        public long startTime;
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
            if (activity_code == -1) return false;
            return (close && cardsEntered.size() <= 1);
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
        }

        @Override
        protected ProducerRecord<String, String> getNextRecord() {
            long atime;
            int amt = 0;
            Integer card_id;
            if (cardsEntered.size() <= 0) return null;

            Integer arr[] = cardsEntered.keySet().toArray(new Integer[0]);
            card_id = arr[rand.nextInt(arr.length)];
            long st = cardsEntered.remove(card_id);
            //Longest journy is 39 min so pick a random exit time from start.
            atime = st + ((rand.nextInt(30)+1)*60000);

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
            return rec;
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
            return (close);
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
        protected ProducerRecord<String, String> getNextRecord() {
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
            return rec;
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

        String trains[] = { "1", "2", "3", "4" };
        List<TrainActivityPublisher> trainPubs = new ArrayList<>();
        long num_activity = (config.count % 17) + config.count;
        for (String train : trains) {
            TrainActivityPublisher redLine = new TrainActivityPublisher(train, config, producerConfig, num_activity);
            trainPubs.add(redLine);
        }
        SwipeEntryActivityPublisher entry = new SwipeEntryActivityPublisher(config, producerConfig, config.count);
        entry.start();
        SwipeExitActivityPublisher exit = new SwipeExitActivityPublisher(config, producerConfig, config.count);
        exit.start();
        //Start trains now.
        for (TrainActivityPublisher trainPub : trainPubs) {
            trainPub.start();
        }
        SwipeReplenishActivityPublisher replenish = new SwipeReplenishActivityPublisher(config, producerConfig, config.count/5);
        replenish.start();
        entry.join();
        exit.close = true;
        exit.join();
        replenish.close = true;
        replenish.join();
        for (TrainActivityPublisher redLine : trainPubs) {
            redLine.join();
        }
    }

    public static void main(String[] args) throws Exception {
        MetroCardConfig config = new MetroCardConfig();
        config.parse(MetroSimulation.class.getName(), args);

        MetroSimulation benchmark = new MetroSimulation(config);
        benchmark.runBenchmark();

    }
}
