/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;

public class KafkaUtils {

    private static final VoltLogger LOGGER = new VoltLogger("KAFKAIMPORTER");

    /*
     * Fetch the list of brokers from Zookeeper, and return a list of their URIs.
     */
    static private class ZooKeeperConnection {

        private ZooKeeper zk;
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
                zk = new ZooKeeper(host, timeoutMillis, w, new HashSet<Long>());
                latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
            }

            if (!connected) {
                throw new RuntimeException("Could not connect to Zookeeper at host:" + host);
            }
            return zk;
        }

        public void close() throws InterruptedException {
            zk.close();
        }
    }

    public static List<HostAndPort> getBrokersFromZookeeper(String zookeeperHost, int timeoutMillis) throws InterruptedException, IOException, KeeperException, JSONException {

        ZooKeeperConnection zkConnection = new ZooKeeperConnection();
        try {
            ZooKeeper zk = zkConnection.connect(zookeeperHost, timeoutMillis);
            List<String> ids = zk.getChildren("/brokers/ids", false);
            ArrayList<HostAndPort> brokers = new ArrayList<HostAndPort>();
            for (String id : ids) {
                String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
                JSONObject json = new JSONObject(brokerInfo);
                String host = json.getString("host");
                int port = json.getInt("port");
                if (host != null && !host.isEmpty()) {
                    LOGGER.info("Adding broker: " + host + ":" + port);
                    brokers.add(new HostAndPort(host, port));
                }
            }
            return brokers;
        } finally {
            zkConnection.close();
        }
    }

    public static String getBrokerKey(String brokers) {
        String key = brokers.replace(':', '_');
        key = key.replace(',', '_');
        return key.toLowerCase();
    }

    public static String getNormalizedKey(String brokers) {
        String key = brokers.replace(':', '_');
        key = key.replace(',', '_');
        return key.toLowerCase();
    }

    public static String getBrokers(String zookeeper, String brokers) {

        List<HostAndPort> brokerList;
        String brokerListString = null;

        try {
            if (zookeeper != null && !zookeeper.trim().isEmpty()) {
                brokerList = KafkaUtils.getBrokersFromZookeeper(zookeeper, BaseKafkaLoaderCLIArguments.ZK_CONNECTION_TIMEOUT_MILLIS);
                brokerListString = StringUtils.join(brokerList.stream().map(s -> s.getHost() + ":" + s.getPort()).collect(Collectors.toList()), ",");
            } else {
                if (brokers == null || brokers.isEmpty()) {
                    throw new IllegalArgumentException("Kafka broker configuration is missing.");
                }
                brokerListString = brokers.trim();
                brokerList = Arrays.stream(brokerListString.split(",")).map(s -> HostAndPort.fromString(s)).collect(Collectors.toList());
            }
        } catch (Exception e) {
            brokerListString = brokers;
        }

        if (brokerListString == null || brokerListString.isEmpty()) {
            throw new IllegalArgumentException("Kafka broker configuration is missing.");
        }

        return brokerListString;
    }

    public static int backoffSleep(int fetchFailedCount) {
        try {
            Thread.sleep(1000 * fetchFailedCount++);
            if (fetchFailedCount > 10) fetchFailedCount = 1;
        } catch (InterruptedException ie) {
        }
        return fetchFailedCount;
    }
}
