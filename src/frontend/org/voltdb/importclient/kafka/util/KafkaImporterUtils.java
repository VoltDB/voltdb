/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.importclient.kafka.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;

import kafka.cluster.Broker;

public class KafkaImporterUtils {

    private static final VoltLogger m_log = new VoltLogger("KAFKAIMPORTER");

    /*
     * Fetch the list of brokers from Zookeeper, and return a list of their URIs.
     */
    static private class ZooKeeperConnection {

        private ZooKeeper zoo;
        private boolean connected = false;
        /*
         * Connect to Zookeeper. Throws an exception if we can't connect for any reason.
         */
        public ZooKeeper connect(String host, int timeoutMillis) throws IOException, InterruptedException {

            final CountDownLatch latch = new CountDownLatch(1);
            Watcher w = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == KeeperState.SyncConnected) {
                        connected = true;
                        latch.countDown();
                     }
                }
            };

            try {
                zoo = new ZooKeeper(host, timeoutMillis, w, new HashSet<Long>());
                latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
            }

            if (connected) {
                return zoo;
            }
            else {
                throw new RuntimeException("Could not connect to Zookeeper at host:" + host);
            }

        }

        public void close() throws InterruptedException {
            zoo.close();
        }

    }

    public static List<HostAndPort> getBrokersFromZookeeper(String zookeeperHost, int timeoutMillis) throws InterruptedException, IOException, KeeperException {

        ZooKeeperConnection zkConnection = new ZooKeeperConnection();

        try {
            ZooKeeper zk = zkConnection.connect(zookeeperHost, timeoutMillis);
            List<String> ids = zk.getChildren("/brokers/ids", false);
            ArrayList<HostAndPort> brokers = new ArrayList<HostAndPort>();

            for (String id : ids) {
                String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
                Broker broker = Broker.createBroker(Integer.valueOf(id), brokerInfo);
                if (broker != null) {
                    m_log.warn("Adding broker: " + broker.connectionString());
                    brokers.add(new HostAndPort(broker.host(), broker.port()));
                }
            }
            return brokers;
        }
        finally {
            zkConnection.close();
        }
    }

    public static String getBrokerKey(String brokers)
    {
        String key = brokers.replace(':', '_');
        key = key.replace(',', '_');
        return key.toLowerCase();
    }

    public static String getNormalizedKey(String brokers) {
        String key = brokers.replace(':', '_');
        key = key.replace(',', '_');
        return key.toLowerCase();
    }
}
