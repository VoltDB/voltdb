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

package client.kafkaimporter;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google_voltpatches.common.util.concurrent.RateLimiter;
import org.voltcore.logging.VoltLogger;

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
    boolean m_producerrunning = false;

    public Producer(String topic, String servers, long rate, int cycletime, int pausetime, long rows) {
        // TODO: add topic check/create, with appropriate replication & partitioning
        m_topic = topic;
        m_servers = servers;
        m_rate = rate;
        m_cycletime = cycletime;
        m_pausetime = pausetime;
        m_rows = rows;
        m_cycles = m_rows / (m_rate * m_cycletime);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        m_producer = new KafkaProducer<String,String>(props);
        log.info("Instantiate Producer: " + topic + ", " + servers + ", " + rate + ", " + cycletime + ", " + pausetime + ", " + rows);
    }

    @Override
    public void run() {
        Long rowCnt = new Long(0);

        final RateLimiter rateLimiter = RateLimiter.create(m_rate);
        m_producerrunning = true;
        for (int cycle = 0; cycle < m_cycles; cycle++) {
            log.info("Kafka producer: starting cycle " + cycle + " to produce " + (m_rows/m_cycles) + " rows.");
            for (long rowsincycle = 0; rowsincycle < (m_rows/m_cycles); rowsincycle++) {
                Long value = System.currentTimeMillis();
                rateLimiter.acquire();
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(m_topic, rowCnt.toString(),
                    rowCnt.toString()+","+value.toString());
                m_producer.send(producerRecord);
                rowCnt++;
            }
            try {
                log.info("...Starting pause between cycles -- " + m_pausetime + " seconds.");
                m_producerrunning = false;
                Thread.sleep(m_pausetime*1000);
                m_producerrunning = true;
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
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
}
