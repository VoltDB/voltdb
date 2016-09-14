/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
 * Write n rows of random data (per schema) to m topics, one thread per topic.
 *
 * Lots of options to control number of topics, number of rows, cycles, etc.
 * See options below rather than depend on a soon to be out of date comment.
 */

package client.kafkaimporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;

import com.google_voltpatches.common.util.concurrent.RateLimiter;

public class Producer extends Thread {
    static VoltLogger log = new VoltLogger("KafkaImporter.Producer");
    KafkaProducer<String,String> m_producer;
    String m_topic;
    String m_servers;
    long m_rate;
    int m_cycletime;
    int m_pausetime;
    long m_rows;
    long m_cycles;
    long m_rangemin;
    long m_rangemax;
    boolean m_producerrunning = false;
    JSONObject m_json_obj;

    // Validated CLI config
    KafkaProducerConfig config;

    public Producer(KafkaProducerConfig config, int topicnum) {
        // TODO: add topic check/create, with appropriate replication & partitioning
        // Meanwhile topic creation is done in kafkautils.py from the runapp.py
        // or if auto-create is enabled in the Kafka cluster properties
        this.config = config;
        m_topic = config.topic;
        m_servers = config.brokers;
        m_rate = config.producerrate;
        m_cycletime = config.cycletime;
        m_pausetime = (int) (config.pausetime * Math.random()); // let each thread have its own wait time between 0 and pausetime
        m_rows = config.totalrows;
        long possiblecycles = m_rows / (m_rate * m_cycletime);
        m_cycles = (possiblecycles > config.cycles) ? possiblecycles : config.cycles;

        m_rangemin = m_rows * topicnum; // offset the start so keys don't overlap and cause constraint violations

        // create distinct topic name <m_topic><topicnum>
        m_topic = m_topic + topicnum;

        m_json_obj = new JSONObject();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, m_servers);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        m_producer = new KafkaProducer<String,String>(props);
        log.info("Instantiate Producer: " + m_topic + ", " + m_servers + ", " +
                m_rate + ", " + m_cycletime + ", " + m_pausetime + ", " + m_rows);
    }

    @Override
    public void run() {
        Random rand = new Random();
        Long rowCnt = new Long(m_rangemin); // starting value for key for each topic producer

        final RateLimiter rateLimiter = RateLimiter.create(m_rate);
        m_producerrunning = true;
        for (int cycle = 0; cycle < m_cycles; cycle++) {
            log.info("Kafka producer: starting cycle " + cycle + " to produce " + (m_rows/m_cycles) + " rows at row index " + rowCnt + ".");
            for (long rowsincycle = m_rangemin; rowsincycle < (m_rows/m_cycles+m_rangemin); rowsincycle++) {
                Long value = System.currentTimeMillis();
                rateLimiter.acquire();

                SampleRecord record = new SampleRecord(rowCnt, 1000, rand);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(m_topic, rowCnt.toString(),
                    record.obj.toString());
                // log.info("JSON Row: " + producerRecord.toString());
                m_producer.send(producerRecord);
                rowCnt++;
            }
            try {
                log.info("...Starting pause between cycles -- " + m_pausetime + " seconds.");
                m_producerrunning = false;
                Thread.sleep(m_pausetime*1000);
                m_producerrunning = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        m_producerrunning = false;
    }

    void shutdown() {
        m_producer.close();
    }

    boolean is_ProducerRunning() {
        return m_producerrunning;
    }
    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class KafkaProducerConfig extends CLIConfig {

        @Option(desc = "Kafka topic name <topicbase><number>")
        String topic = "TOPIC";

        @Option(desc = "Kafka broker list, server:port,...")
        String brokers = "";

        @Option(desc = "Number of Kafka topics.")
        int ntopics = 1;    // 1 means 1 topic: TOPIC0, for example

        @Option(desc = "Rate in rows per second")
        int producerrate = 1_000_000;

        @Option(desc = "Cycle Time in seconds.")
        int cycletime = 60;

        @Option (desc = "Pause time in seconds.")
        int pausetime = 10;

        @Option(desc = "Total rows in rows.")
        int totalrows = 6_000_000;

        @Option(desc = "Number of producer cycles")
        int cycles = 5;

        @Override
        public void validate() {
            if (ntopics == 0) ntopics = 1;
            if (topic.length() <= 0) exitWithMessageAndUsage("Topic name required");
            if (brokers.length() < 0) exitWithMessageAndUsage("Broker list required");
            if (producerrate <= 0) exitWithMessageAndUsage("Producer rate must be > 0");
            if (cycletime <= 0) exitWithMessageAndUsage("Cycle time must be > 0");
            if (pausetime <= 0) exitWithMessageAndUsage("Pause time must be > 0");
            if (totalrows <= 0) exitWithMessageAndUsage("Total rows must be > 0");
            if (cycles <= 0) exitWithMessageAndUsage("Cycle count must be > 0");
        }
    }

    public static void main(String[] args) {
        KafkaProducerConfig config = new KafkaProducerConfig();
        config.parse(Producer.class.getName(), args);
        System.out.println(config.getConfigDumpString());

        List<Producer>  producers = new ArrayList<Producer>();
        for (int topic = 0; topic < config.ntopics; topic++) {
            Producer producer = new Producer(config, topic);
            producer.start();
            producers.add(producer);
        }
        try {
            int t = 0;
            for (Producer p : producers) {
                p.join();
                System.out.println("Thread " + t + " done.");
                t++;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("All threads done.");
    }
}
