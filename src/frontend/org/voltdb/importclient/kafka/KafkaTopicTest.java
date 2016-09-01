/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
package org.voltdb.importclient.kafka;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.FormatterBuilder;
import org.voltdb.importer.formatter.builtin.VoltCSVFormatterFactory;

/**
 *
 * @author akhanzode
 */
public class KafkaTopicTest {

    final static class Runner extends Thread {
        private final KafkaTopicPartitionImporter m_importer;
        private final CountDownLatch m_cdl;

        Runner(KafkaTopicPartitionImporter importer, CountDownLatch cdl) {
            m_importer = importer;
            m_cdl = cdl;
        }

        @Override
        public void run() {
            m_importer.noTransaction = true;
            m_importer.accept();
            System.out.println("Topic Done: " + m_importer.getResourceID());
            m_cdl.countDown();
        }

    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("brokers", args[0]);
        props.put("topics", args[1]);
        String format = args[2];
        props.put("groupid", args[3]);

        props.put("procedure", "fake");

        KafkaStreamImporterFactory factory = new KafkaStreamImporterFactory();
        VoltCSVFormatterFactory ffactory = new VoltCSVFormatterFactory();
        ffactory.create(format, props);
        FormatterBuilder fb = new FormatterBuilder(format, props);
        fb.setFormatterFactory(ffactory);
        Map<URI, ImporterConfig> mmap = factory.createImporterConfigurations(props, fb);
        CountDownLatch cdl = new CountDownLatch(mmap.size());
        for (URI uri : mmap.keySet()) {
            KafkaTopicPartitionImporter importer = new KafkaTopicPartitionImporter((KafkaStreamImporterConfig )mmap.get(uri));
            Runner runner = new Runner(importer, cdl);
            runner.start();
        }
        try {
            cdl.await();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        System.out.println("Exiting.");
    }

}
