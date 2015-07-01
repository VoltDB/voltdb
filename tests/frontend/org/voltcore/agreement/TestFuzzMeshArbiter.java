/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltcore.agreement;

import static com.google_voltpatches.common.base.Predicates.equalTo;
import static com.google_voltpatches.common.base.Predicates.not;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.voltcore.agreement.MiniNode.NodeState;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.primitives.Ints;


public class TestFuzzMeshArbiter extends TestCase
{
    public static VoltLogger m_fuzzLog = new VoltLogger("FUZZ");

    FakeMesh m_fakeMesh;
    Map<Long, MiniNode> m_nodes;

    long getHSId(int i)
    {
        return CoreUtils.getHSIdFromHostAndSite(i, HostMessenger.AGREEMENT_SITE_ID);
    }

    int getHostId(long hsid) {
        return CoreUtils.getHostIdFromHSId(hsid);
    }

    MiniNode getNode(int i)
    {
        long HSId = getHSId(i);
        return m_nodes.get(HSId);
    }

    void constructCluster(int nodeCount)
    {
        m_fakeMesh = new FakeMesh();
        m_fakeMesh.start();
        m_nodes = new HashMap<Long, MiniNode>();
        for (int i = 0; i < nodeCount; i++) {
            long HSId = getHSId(i);
            m_nodes.put(HSId, null);
        }
        Set<Long> HSIds = m_nodes.keySet();
        for (long HSId : HSIds) {
            m_nodes.put(HSId, new MiniNode(HSId, HSIds, m_fakeMesh));
            m_nodes.get(HSId).start();
        }
    }

    @Override
    public void tearDown() throws InterruptedException
    {
        for (MiniNode node : m_nodes.values()) {
            node.shutdown();
            node.join();
        }
        m_fakeMesh.shutdown();
        m_fakeMesh.join();
    }

    private Set<Long> getNodesInState(NodeState state)
    {
        Set<Long> nodes = new HashSet<Long>();
        for (Entry<Long, MiniNode> node : m_nodes.entrySet()) {
            if (node.getValue().getNodeState().equals(state)) {
                nodes.add(node.getKey());
            }
        }
        return nodes;
    }

    private boolean checkFullyConnectedGraphs(Set<Long> nodes)
    {
        boolean result = true;
        for (Long node : nodes) {

            MiniNode mnode = m_nodes.get(node);
            if (mnode == null) continue;

            Set<Long> nodeGraph = mnode.getConnectedNodes();
            if (nodeGraph.size() != nodes.size() ||
                !(nodes.containsAll(nodeGraph)))
            {
                System.out.println("Node: " + CoreUtils.hsIdToString(node) +
                        " has an unexpected connected set.");
                System.out.println("Node: " + CoreUtils.hsIdToString(node) +
                        " Expected to see: " + CoreUtils.hsIdCollectionToString(nodes));
                System.out.println("Node: " + CoreUtils.hsIdToString(node) +
                        " says it has: " + CoreUtils.hsIdCollectionToString(nodeGraph));
                result = false;
            }
        }
        return result;
    }

    public void testNodeFail() throws InterruptedException
    {
        constructCluster(4);
        while (!getNodesInState(NodeState.START).isEmpty()) {
            Thread.sleep(50);
        }
        FuzzTestState state = new FuzzTestState(0L, m_nodes.keySet());
        state.killNode(0);
        state.setUpExpectations();

        state.expect();
    }

    public void testLinkFail() throws InterruptedException
    {
        constructCluster(5);
        while (!getNodesInState(NodeState.START).isEmpty()) {
            Thread.sleep(50);
        }
        FuzzTestState state = new FuzzTestState(0L, m_nodes.keySet());
        state.killLink(0, 1);
        state.setUpExpectations();

        state.expect();
    }

    public void testUnidirectionalLinkFailure() throws InterruptedException {
        constructCluster(5);
        while (!getNodesInState(NodeState.START).isEmpty()) {
            Thread.sleep(50);
        }
        FuzzTestState state = new FuzzTestState(0L, m_nodes.keySet());
        state.killUnidirectionalLink(0, 1);
        state.setUpExpectations();

        state.expect();
    }

    class FuzzTestState
    {
        Random m_rand;
        Set<Long> m_expectedLive;
        Set<Integer> m_alreadyPicked;
        Set<Long> m_killSet;
        Map<Long,Integer> m_failedCounts;
        NavigableMap<Integer,Integer> m_expectations;


        FuzzTestState(long seed, Set<Long> startingNodes)
        {
            m_expectedLive = new HashSet<Long>();
            m_alreadyPicked = new HashSet<Integer>();
            m_killSet = new HashSet<Long>();
            m_expectedLive.addAll(startingNodes);
            m_failedCounts = new HashMap<Long, Integer>();
            m_expectations = new TreeMap<Integer,Integer>();
            m_rand = new Random(seed);
        }

        int getRandomLiveNode()
        {
            int i = 0;
            int [] picks = new int [m_nodes.size()];
            for (long HSid: m_nodes.keySet()) {
                picks[i++] = getHostId(HSid);
            }
            i = 0;
            MiniNode mini = null;
            int node = picks[m_rand.nextInt(picks.length)];
            while (m_alreadyPicked.contains(node) || mini == null) {
                node = picks[m_rand.nextInt(picks.length)];
                if ((++i % 10) == 0) {
                    m_fuzzLog.warn("alreadyPicked: " + m_alreadyPicked + ", picks: " + Ints.asList(picks));
                }
                mini = m_nodes.get(getHSId(node));
            }
            m_alreadyPicked.add(node);
            return node;
        }

        void killNode(int node) throws InterruptedException {
            if (m_alreadyPicked.contains(node)) {
                node = getRandomLiveNode();
            }
            m_alreadyPicked.add(node);
            m_expectedLive.remove(getHSId(node));
            m_killSet.add(getHSId(node));
            MiniNode victim = m_nodes.get(getHSId(node));
            victim.shutdown();
            victim.join();
        }

        void killLink(int end1, int end2) {
            if (end1 == end2) {
                end1 = getRandomLiveNode();
                end2 = getRandomLiveNode();
            } else if (m_alreadyPicked.contains(end1)) {
                end1 = getRandomLiveNode();
                if (m_alreadyPicked.contains(end2)) {
                    end2 = getRandomLiveNode();
                }
            }
            int max = Math.max(end1, end2);

            m_expectedLive.remove(getHSId(max));
            m_killSet.add(getHSId(max));
            m_alreadyPicked.add(end1);
            m_alreadyPicked.add(end2);

            m_fakeMesh.failLink(getHSId(end1), getHSId(end2));
            m_fakeMesh.failLink(getHSId(end2), getHSId(end1));
        }

        void killUnidirectionalLink(int end1, int end2) {
            if (end1 == end2) {
                end1 = getRandomLiveNode();
                end2 = getRandomLiveNode();
            } else if (m_alreadyPicked.contains(end1)) {
                end1 = getRandomLiveNode();
                if (m_alreadyPicked.contains(end2)) {
                    end2 = getRandomLiveNode();
                }
            }
            int max = Math.max(end1, end2);

            m_expectedLive.remove(getHSId(max));
            m_killSet.add(getHSId(max));
            m_alreadyPicked.add(end1);
            m_alreadyPicked.add(end2);

            m_fakeMesh.failLink(getHSId(end1), getHSId(end2));
        }

        void killRandomNode() throws InterruptedException
        {
            int nextToDie = getRandomLiveNode();
            m_expectedLive.remove(getHSId(nextToDie));
            m_killSet.add(getHSId(nextToDie));

            System.out.println("Next to die: " + nextToDie);
            int delay = m_rand.nextInt(10) + 1;
            System.out.println("Fuzz delay in ms: " + delay * 5);
            Thread.sleep(delay * 5);

            MiniNode victim = m_nodes.get(getHSId(nextToDie));
            victim.shutdown();
            victim.join();
        }

        void killRandomLink() throws InterruptedException
        {
            int end1 = getRandomLiveNode();
            int end2 = getRandomLiveNode();
            int max = Math.max(end1, end2);

            m_expectedLive.remove(getHSId(max));
            m_killSet.add(getHSId(max));
            System.out.println("Next link to die: " + end1 + ":" + end2);

            int delay = m_rand.nextInt(10) + 1;
            System.out.println("Fuzz delay in ms: " + delay * 5);
            m_fakeMesh.failLink(getHSId(end1), getHSId(end2));

            Thread.sleep(delay * 5);
            m_fakeMesh.failLink(getHSId(end2), getHSId(end1));
        }

        void setUpExpectations() {
            m_expectations.clear();

            Iterator<Integer> itr = m_alreadyPicked.iterator();
            while (itr.hasNext()) {
                MiniNode node = m_nodes.get(getHSId(itr.next()));
                if (node == null || node.getNodeState() != NodeState.STOP) {
                    itr.remove();
                }
            }

            m_failedCounts.clear();

            for (Map.Entry<Long, MiniNode> e: m_nodes.entrySet()) {
                if (e.getValue().getNodeState() == NodeState.STOP) continue;
                MiniSite site = e.getValue().m_miniSite;
                m_failedCounts.put(e.getKey(), site.getFailedSitesCount());
            }

            m_expectations.put(m_killSet.size(), m_expectedLive.size());

            int killSetSize = m_killSet.size();
            Iterator<Long> kitr = m_killSet.iterator();
            while (kitr.hasNext()) {
                MiniNode knode = m_nodes.get(kitr.next());
                if (knode == null || knode.getNodeState() == NodeState.STOP) {
                    kitr.remove();
                }
            }

            if (!m_killSet.isEmpty()) {
                m_expectations.put(
                        m_expectedLive.size() + killSetSize - 1,
                        m_killSet.size()
                        );
            }

            m_killSet.clear();
        }

        NavigableMap<Integer,Integer> getFailedCountMap() {
            TreeMap<Integer,Integer> expectations = Maps.newTreeMap();

            for (Map.Entry<Long, MiniNode> e: m_nodes.entrySet()) {
                if (e.getValue().getNodeState() == NodeState.STOP) continue;

                MiniSite site = e.getValue().m_miniSite;
                if (site.isInArbitration()) return ImmutableSortedMap.of();
                int failedCount = site.getFailedSitesCount() - m_failedCounts.get(e.getKey());

                Integer count = expectations.get(failedCount);
                if (count == null) count = 0;
                expectations.put(failedCount, ++count);
            }
            return expectations;

        }

        boolean hasMetExpectations() {
            NavigableMap<Integer,Integer> expectations = getFailedCountMap();
            if (   expectations.isEmpty()
                || m_expectations.size() > expectations.size()) return false;

            int sumtest = 0;
            int sumfails = 0;

            for (Map.Entry<Integer, Integer> fc: expectations.entrySet()) {
                sumtest += fc.getValue();
                sumfails += fc.getKey();
            }

            int sumexp = 0;
            for (int fc: m_expectations.values()) {
                sumexp += fc;
            }

            return sumfails > 0 && sumexp == sumtest;
        }

        void expect() throws InterruptedException {
            long start = System.currentTimeMillis();
            while (!hasMetExpectations()) {
                long now = System.currentTimeMillis();
                if (now - start > 30000) {
                    start = now;
                    dumpNodeState();
                    m_fuzzLog.info("m_expectations: " + m_expectations
                            + ", failedCountMap: " + getFailedCountMap());
                }
                Thread.sleep(50);
            }
            Map<Integer,Integer> failedCounts = getFailedCountMap();
            if (!m_expectations.equals(failedCounts)) {
                dumpNodeState();
                m_fuzzLog.info("Failed count map: "+ failedCounts);
            }
        }

        void pruneDeadNodes() throws InterruptedException {
            NavigableMap<Integer, Integer> expectations = getFailedCountMap();
            while (expectations.isEmpty()) {
                expect();
                expectations = getFailedCountMap();
            }
            m_fuzzLog.info("expectations at prune: "+ expectations);
            Map<Integer,Integer> laggers =  Maps.filterKeys(
                    expectations,
                    not(equalTo(expectations.firstKey()))
                    );

            int expectedFails = 0;
            Set<Integer> pruneSizes = Sets.newHashSet();
            for (Map.Entry<Integer, Integer> e: laggers.entrySet()) {
                pruneSizes.add(m_nodes.size() - e.getKey());
                expectedFails += e.getValue();
            }

            m_fuzzLog.info("pruneSizes are: " + pruneSizes);

            Map<Long,MiniNode> removed = Maps.newHashMap();
            Iterator<Map.Entry<Long, MiniNode>> itr;

            int attempts = 50;
            int actualFails = 0;

            while (--attempts > 0 && actualFails < expectedFails) {
                itr = m_nodes.entrySet().iterator();
                while (itr.hasNext()) {
                    Map.Entry<Long, MiniNode> e = itr.next();
                    MiniNode node = e.getValue();
                    int connectedCount =
                            node.getNodeState() == NodeState.STOP ? 0 : node.getConnectedNodes().size();
                    m_fuzzLog.debug("Connection count for "
                            + CoreUtils.hsIdToString(e.getKey())
                            + " is " + connectedCount);
                    if (connectedCount == 0 || pruneSizes.contains(connectedCount)) {
                        if (pruneSizes.contains(connectedCount)) {
                            actualFails += 1;
                        }
                        removed.put(e.getKey(),e.getValue());
                        itr.remove();
                    }
                }
                Thread.sleep(100);
            }

            assertEquals("timeout while waiting for mini node to catch up with minisite",expectedFails, actualFails);

            itr = m_nodes.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry<Long, MiniNode> e = itr.next();
                MiniNode node = e.getValue();
                for (long rmd: removed.keySet()) {
                    node.stopTracking(rmd);
                }
            }
            for (Map.Entry<Long, MiniNode> e: removed.entrySet()) {
                MiniNode node = e.getValue();
                if (node.getNodeState() != NodeState.STOP) {
                    node.shutdown();
                    node.join();
                }
            }
            m_fuzzLog.info("pruned "+ removed.size() +" nodes");

            m_expectedLive.clear();
            m_expectedLive.addAll(m_nodes.keySet());
            m_alreadyPicked.clear();
            settleMesh();
        }

        void settleMesh() throws InterruptedException {
            boolean same = false;
            for (int i = 0; i < 150 && !same; ++i) {
                same = true;
                if (i >= 50 && (i % 50) == 0) {
                    for (Map.Entry<Long, MiniNode> e: m_nodes.entrySet()) {
                        m_fuzzLog.info(
                                CoreUtils.hsIdToString(e.getKey())
                                + " is connected to ["
                                + CoreUtils.hsIdCollectionToString(e.getValue().getConnectedNodes())
                                + "]"
                                );
                    }
                }
                Iterator<Map.Entry<Long, MiniNode>>itr = m_nodes.entrySet().iterator();
                Set<Long> base = itr.next().getValue().getConnectedNodes();
                while (same && itr.hasNext()) {
                    same = same && base.equals(itr.next().getValue().getConnectedNodes());
                }
                Thread.sleep(100);
            }
            assertTrue("mesh could not be settled in 15 seconds",same);
        }


        void joinNode(int node) throws InterruptedException {
            Preconditions.checkArgument(!m_nodes.containsKey(getHSId(node)),
                    "node %s is already part of the cluster", node);
            Preconditions.checkArgument(!m_alreadyPicked.contains(node),
                    "%s was already picked for failure",node);
            m_nodes.put(getHSId(node),null);
            MiniNode mini = new MiniNode(getHSId(node),m_nodes.keySet(),m_fakeMesh);
            for (MiniNode mnode: m_nodes.values()) {
                if (mnode == null) continue;
                mnode.joinWith(getHSId(node));
            }
            mini.start();
            while (mini.getNodeState() == NodeState.START) {
                Thread.sleep(50);
            }
            m_nodes.put(getHSId(node),mini);
            m_expectedLive.add(getHSId(node));
        }
    }

    private void dumpNodeState()
    {
        for (Entry<Long, MiniNode> node : m_nodes.entrySet())
        {
            m_fuzzLog.info(node.getValue().toString());
        }
    }

    public void testSimpleJoin() throws InterruptedException {
        final int clusterSize = 5;
        final int killSize = 2;
        long seed = System.currentTimeMillis();
        System.out.println("SEED: " + seed);
        constructCluster(clusterSize);
        while (!getNodesInState(NodeState.START).isEmpty()) {
            Thread.sleep(50);
        }
        FuzzTestState state = new FuzzTestState(seed, m_nodes.keySet());
        int nextid = clusterSize;
        for (int i = 0; i < 8; ++i) {
            for (int k = 0; k < killSize; ++k){
                if ((k % 2) == 1) {
                    state.killRandomLink();
                } else {
                    state.killRandomNode();
                }
            }
            state.setUpExpectations();

            state.expect();

            state.pruneDeadNodes();
            int nodes2join = clusterSize - m_nodes.size();
            for (int j = 0; j < nodes2join; j++) {
                state.joinNode(nextid++);
            }
            state.settleMesh();
        }
    }

    public void testFuzz() throws InterruptedException
    {
        long seed = System.currentTimeMillis();
        System.out.println("SEED: " + seed);
        constructCluster(20);
        while (!getNodesInState(NodeState.START).isEmpty()) {
            Thread.sleep(50);
        }
        FuzzTestState state = new FuzzTestState(seed, m_nodes.keySet());

        for (int i = 0; i < 5; i++) {
            if (state.m_rand.nextInt(100) < 50) {
                state.killRandomNode();
            }
            else {
                state.killRandomLink();
            }
        }
        state.setUpExpectations();

        state.expect();

        state.pruneDeadNodes();

        for (int i = 0; i < 4; i++) {
            if (state.m_rand.nextInt(100) < 50) {
                state.killRandomNode();
            }
            else {
                state.killRandomLink();
            }
        }
        state.setUpExpectations();

        state.expect();
    }

    public void thereBeDragonsHeretestNastyFuzz() throws InterruptedException {
        long seed = System.currentTimeMillis();
        final int clusterSize = 40;
        final int killSize = 16;
        System.out.println("SEED: " + seed);
        constructCluster(clusterSize);
        while (!getNodesInState(NodeState.START).isEmpty()) {
            Thread.sleep(50);
        }
        FuzzTestState state = new FuzzTestState(seed, m_nodes.keySet());
        int nextid = clusterSize;
        for (int i = 0; i < 20; ++i) {
            for (int k = 0; k < killSize; ++k){
                if ((k % 2) == 0) {
                    state.killRandomLink();
                } else {
                    state.killRandomNode();
                }
            }
            state.setUpExpectations();

            state.expect();

            state.pruneDeadNodes();

            int nodes2join = clusterSize - m_nodes.size();
            for (int j = 0; j < nodes2join; j++) {
                state.joinNode(nextid++);
            }
            state.settleMesh();
        }
    }

    // Partition the nodes in subset out of the nodes in nodes
    private void partitionGraph(Set<Long> nodes, Set<Long> subset)
    {
        Set<Long> otherSet = new HashSet<Long>();
        otherSet.addAll(nodes);
        otherSet.removeAll(subset);
        // For every node in one set, kill every link to every node
        // in the other set
        for (Long node : subset) {
            for (Long otherNode : otherSet) {
                m_fakeMesh.failLink(node, otherNode);
                m_fakeMesh.failLink(otherNode, node);
            }
        }
    }

    public void needsWorkTestPartition() throws InterruptedException
    {
        long seed = System.currentTimeMillis();
        System.out.println("SEED: " + seed);
        constructCluster(10);
        while (!getNodesInState(NodeState.START).isEmpty()) {
            Thread.sleep(50);
        }
        FuzzTestState state = new FuzzTestState(seed, m_nodes.keySet());
        // pick a subset of nodes and partition them out
        Set<Long> subset = new HashSet<Long>();
        int subsize = state.m_rand.nextInt((m_nodes.size() / 2) + 1);
        for (int i = 0; i < subsize; i++) {
            long nextNode = getHSId(state.getRandomLiveNode());
            while (subset.contains(nextNode)) {
                nextNode = getHSId(state.getRandomLiveNode());
            }
            subset.add(nextNode);
        }
        partitionGraph(m_nodes.keySet(), subset);

        long start = System.currentTimeMillis();
        while (getNodesInState(NodeState.RESOLVE).isEmpty()) {
            long now = System.currentTimeMillis();
            if (now - start > 30000) {
                start = now;
                dumpNodeState();
            }
            Thread.sleep(50);
        }
        while (!getNodesInState(NodeState.RESOLVE).isEmpty()) {
            long now = System.currentTimeMillis();
            if (now - start > 30000) {
                start = now;
                dumpNodeState();
            }
            Thread.sleep(50);
        }

        Set<Long> otherSet = new HashSet<Long>();
        otherSet.addAll(m_nodes.keySet());
        otherSet.removeAll(subset);
        assertTrue(checkFullyConnectedGraphs(subset));
        assertTrue(checkFullyConnectedGraphs(otherSet));
    }
}
