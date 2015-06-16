/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.importclient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.importer.ImportHandlerProxy;

/**
 * Implement a BundleActivator interface and extend ImportHandlerProxy.
 * @author akhanzode
 */
public class KafkaStreamImporter extends ImportHandlerProxy implements BundleActivator {

    private Properties m_properties;
    private String m_procedure;
    private String m_topic;
    private String m_bootstrapServers;
    private Integer m_maxErrors = 100;
    private Integer m_pollInterval = 5000;
    private boolean m_closed = false;

    private KafkaConsumer m_consumer;

    // Register ImportHandlerProxy service.
    @Override
    public void start(BundleContext context) throws Exception {
        context.registerService(ImportHandlerProxy.class.getName(), this, null);
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        //Do any bundle related cleanup.
    }

    @Override
    public void stop() {
        try {
            m_consumer.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            m_closed = true;
        }
    }

    /**
     * Return a name for VoltDB to log with friendly name.
     * @return name of the importer.
     */
    @Override
    public String getName() {
        return "KafkaImporter";
    }

    /**
     * This is called with the properties that are supplied in the deployment.xml
     * Do any initialization here.
     * @param p
     */
    @Override
    public void configure(Properties p) {
        m_properties = (Properties) p.clone();
        m_procedure = (String )m_properties.get("procedure");
        if (m_procedure == null || m_procedure.trim().length() == 0) {
            throw new RuntimeException("Missing procedure.");
        }
        m_topic = (String )m_properties.getProperty("topic");
        if (m_topic == null || m_topic.trim().length() == 0) {
            throw new RuntimeException("Missing topic.");
        }
        //bootstrap.servers
        m_bootstrapServers = (String )m_properties.getProperty("bootstrap.servers");
        if (m_bootstrapServers == null || m_bootstrapServers.trim().length() == 0) {
            throw new RuntimeException("Missing bootstrap.servers");
        }
        String maxErrors = (String )m_properties.getProperty("maxErrors");
        if (maxErrors != null && maxErrors.trim().length() != 0) {
            m_maxErrors = Integer.parseInt(maxErrors);
        }
        String pollInt = (String )m_properties.getProperty("pollInterval");
        if (pollInt != null && pollInt.trim().length() != 0) {
            m_pollInterval = Integer.parseInt(pollInt);
        }
    }

    /**
     * This is called when server is ready to accept any transactions.
     */
    @Override
    public void readyForData() {
        try {
            info("Configured and ready with properties: " + m_properties);
            Properties props = new Properties();
            props.put("bootstrap.servers", m_bootstrapServers);
            props.put("group.id", "voltdb-importer");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("partition.assignment.strategy", "friend");
            m_consumer = new KafkaConsumer(props);
            m_consumer.subscribe(m_topic);
            boolean isRunning = true;
            while (isRunning) {
                if (m_closed) {
                    break;
                }
                Map<String, ConsumerRecords> records = m_consumer.poll(m_pollInterval);
                if (records != null) {
                    System.out.println("Got some records.");
                    process(records);
                }
            }
            m_consumer.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            m_consumer = null;
        }
    }

    //Call client interface
    private Map<TopicPartition, Long> process(Map<String, ConsumerRecords> records) {
         Map<TopicPartition, Long> processedOffsets = new HashMap<TopicPartition, Long>();
         for(Entry<String, ConsumerRecords> recordMetadata : records.entrySet()) {
              List<ConsumerRecord> recordsPerTopic = recordMetadata.getValue().records();
              for(int i = 0;i < recordsPerTopic.size();i++) {
                   ConsumerRecord record = recordsPerTopic.get(i);
                   // process record
                   try {
                    processedOffsets.put(record.topicAndPartition(), record.offset());
                   } catch (Exception e) {
                    e.printStackTrace();
                   }
              }
         }
         return processedOffsets;
     }

}
