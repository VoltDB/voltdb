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

package org.voltcore.zk;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.common.Constants;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.PortGenerator;
import org.voltdb.StartAction;
import org.voltdb.probe.MeshProber;

import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.net.HostAndPort;

/**
 *
 */
public class ZKTestBase {
    PortGenerator m_ports = new PortGenerator();

    protected Map<Integer, Integer> m_siteIdToZKPort;
    protected ArrayList<HostMessenger> m_messengers;
    protected ArrayList<ZooKeeper> m_clients;

    protected void setUpZK(int sites) throws Exception {
        m_siteIdToZKPort = new TreeMap<Integer, Integer>();
        m_clients = new ArrayList<ZooKeeper>();
        m_messengers = new ArrayList<HostMessenger>();
        String [] coordinators = IntStream.range(0, sites)
                .mapToObj(i -> ":" + (i+Constants.DEFAULT_INTERNAL_PORT))
                .toArray(s -> new String[s]);
        for (int ii = 0; ii < sites; ii++) {
            HostMessenger.Config config = new HostMessenger.Config(false);
            config.internalPort += ii;
            config.acceptor = MeshProber.builder()
                    .coordinators(coordinators)
                    .startAction(StartAction.PROBE)
                    .hostCount(sites)
                    .build();
            int externalPort = m_ports.next();
            config.zkInterface = "127.0.0.1";
            config.zkPort = externalPort;
            m_siteIdToZKPort.put(ii, externalPort);
            config.networkThreads = 1;
            HostMessenger hm = new HostMessenger(config, null, randomHostDisplayName());
            hm.start();
            m_messengers.add(hm);
        }
        for (HostMessenger hm: m_messengers) {
            MeshProber.prober(hm).waitForDetermination();
        }
    }

    protected void setUpZK(MeshProber criteria, boolean waitForDetermination) throws Exception {
        m_siteIdToZKPort = new TreeMap<Integer, Integer>();
        m_clients = new ArrayList<ZooKeeper>();
        m_messengers = new ArrayList<HostMessenger>();
        int i = 0;

        for (String coord: criteria.getCoordinators()) {
            HostAndPort hp = HostAndPort.fromString(coord).withDefaultPort(Constants.DEFAULT_INTERNAL_PORT);
            HostMessenger.Config config = new HostMessenger.Config(false);
            config.acceptor = criteria;
            assert config.internalPort + i == hp.getPort() : "coordinator port mismatches internal port";
            config.internalPort = hp.getPort();
            int externalPort = m_ports.next();
            config.zkInterface = "127.0.0.1";
            config.zkPort = externalPort;
            m_siteIdToZKPort.put(i, externalPort);
            config.networkThreads = 1;
            HostMessenger hm = new HostMessenger(config, null, randomHostDisplayName());
            hm.start();
            m_messengers.add(hm);
            ++i;
        }

        for (; i < criteria.getHostCount(); ++i) {
            HostMessenger.Config config = new HostMessenger.Config(false);
            config.acceptor = criteria;
            config.internalPort += i;
            int externalPort = m_ports.next();
            config.zkInterface = "127.0.0.1";
            config.zkPort = externalPort;
            m_siteIdToZKPort.put(i, externalPort);
            config.networkThreads = 1;
            HostMessenger hm = new HostMessenger(config, null, randomHostDisplayName());
            hm.start();
            m_messengers.add(hm);
        }

        if (waitForDetermination) {
            for (HostMessenger hm: m_messengers) {
                MeshProber.prober(hm).waitForDetermination();
            }
        }
    }

    protected void tearDownZK() throws Exception {
        for (ZooKeeper keeper : m_clients) {
            keeper.close();
        }
        m_clients.clear();
        for (HostMessenger hm : m_messengers) {
            if (hm != null) {
                hm.shutdown();
            }
        }
        m_messengers.clear();
    }

    protected ZooKeeper getClient(int site) throws Exception {
        final Semaphore permit = new Semaphore(0);
        int clientPort = m_siteIdToZKPort.get(site);
        ZooKeeper keeper = new ZooKeeper("127.0.0.1:" + Integer.toString(clientPort), 4000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                    permit.release();
                }
                System.out.println(event);
            }},
            Sets.<Long>newHashSet());
        m_clients.add(keeper);
        permit.acquire();
        return keeper;
    }

    protected String randomHostDisplayName() {
        return RandomStringUtils.random(20);
    }
}
