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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google_voltpatches.common.util.concurrent.RateLimiter;

/**
 * Derived from
 * 		An example using the new java client Producer for Kafka 0.8.2
 *  		Cameron Gregory, http://www.bloke.com/
 */
public class Producer extends Thread {
	KafkaProducer<String,String> m_producer;
	String m_topic;
    String m_servers;
    long m_rate;
    int m_cycletime;
    int m_pausetime;
    long m_rows;
    long m_cycles;

	//@SuppressWarnings({ "unused", "resource" }) // temporary -- hide annoying Eclipse complaints
	public Producer(String topic, String servers, long rate, int cycletime, int pausetime, long rows) {
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

        m_producer = new KafkaProducer<String,String>(props);

	}

    @Override
	public void run() {
    	Long rowCnt = new Long(0);
    	// String key = "abc";
    	// String value = "def";
        // ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(m_topic, key, value);
        // m_producer.send(producerRecord);

        final RateLimiter rateLimiter = RateLimiter.create(m_rate);
        for (int cycle = 0; cycle < m_cycles; cycle++) {
    		Long value = System.currentTimeMillis();
    		rateLimiter.acquire();
    		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(m_topic, rowCnt.toString(), value.toString());
    		m_producer.send(producerRecord);
    		rowCnt++;
        }
    }

    void shutdown() {
    	m_producer.close();
    }
}
