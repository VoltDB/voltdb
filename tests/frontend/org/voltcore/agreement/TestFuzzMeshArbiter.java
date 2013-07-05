/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.voltcore.agreement.MiniNode.NodeState;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;

import com.google.common.base.Preconditions;

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
            Set<Long> nodeGraph = m_nodes.get(node).getConnectedNodes();
            if (nodeGraph.size() != nodes.size() ||
                !(nodes.containsAll(nodeGraph)))
            {
                System.out.println("Node: " + CoreUtils.hsIdToString(node) +
                        " has an unexpected connected set.");
                System.out.println("Node: " + CoreUtils.hsIdToString(node) +
                        " Expected to see: " + nodes);
                System.out.println("Node: " + CoreUtils.hsIdToString(node) +
                        " says it has: " + nodeGraph);
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
        MiniNode victim = getNode(0);
        victim.shutdown();
        victim = getNode(1);
        victim.shutdown();
        while (getNodesInState(NodeState.RESOLVE).isEmpty()) {
            Thread.sleep(50);
        }
        while (!getNodesInState(NodeState.RESOLVE).isEmpty()) {
            Thread.sleep(50);
        }
        Set<Long> expect = new HashSet<Long>();
        expect.addAll(m_nodes.keySet());
        expect.remove(getHSId(0));
        expect.remove(getHSId(1));
        assertTrue(checkFullyConnectedGraphs(expect));
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

        assertTrue(checkFullyConnectedGraphs(state.m_expectedLive));
        state.pruneDeadNodes();

        state.killLink(2, 3);
        state.setUpExpectations();

        state.expect();

        assertTrue(checkFullyConnectedGraphs(state.m_expectedLive));
        state.pruneDeadNodes();

        state.killLink(0, 2);
        state.setUpExpectations();

        state.expect();

        assertTrue(checkFullyConnectedGraphs(state.m_expectedLive));
    }

    public void testSingleLinkInTriagle() throws InterruptedException {
        constructCluster(3);
        while (!getNodesInState(NodeState.START).isEmpty()) {
            Thread.sleep(50);
        }
        FuzzTestState state = new FuzzTestState(0L, m_nodes.keySet());
        state.killLink(0, 2);
        state.setUpExpectations();

        state.expect();

        assertTrue(checkFullyConnectedGraphs(state.m_expectedLive));
    }

    class FuzzTestState
    {
        Random m_rand;
        Set<Long> m_expectedLive;
        Set<Integer> m_alreadyPicked;
        Set<Long> m_killSet;
        Map<Long,Integer> m_expectations;

        FuzzTestState(long seed, Set<Long> startingNodes)
        {
            m_expectedLive = new HashSet<Long>();
            m_alreadyPicked = new HashSet<Integer>();
            m_killSet = new HashSet<Long>();
            m_expectedLive.addAll(startingNodes);
            m_expectations = new HashMap<Long, Integer>();
            m_rand = new Random(seed);
        }

        int getRandomLiveNode()
        {
            int node = m_rand.nextInt(m_nodes.size());
            while (   !m_expectedLive.contains(getHSId(node))
                    || m_alreadyPicked.contains(node)) {
                node = m_rand.nextInt(m_nodes.size());
            }
            m_alreadyPicked.add(node);
            return node;
        }

        void killNode(int node) throws InterruptedException {
            Preconditions.checkArgument(!m_alreadyPicked.contains(node),
                    "%s was already picked for failure", node);
            m_alreadyPicked.add(node);
            m_expectedLive.remove(getHSId(node));
            m_killSet.add(getHSId(node));
            MiniNode victim = m_nodes.get(getHSId(node));
            victim.shutdown();
        }

        void killLink(int end1, int end2) {
            Preconditions.checkArgument( end1 != end2,
                    "%s and %s may not be equal", end1, end2);
            Preconditions.checkArgument(!m_alreadyPicked.contains(end1),
                    "%s was already picked for failure", end1);
            Preconditions.checkArgument(!m_alreadyPicked.contains(end1),
                    "%s was already picked for failure", end2);
            int max = Math.max(end1, end2);
            m_expectedLive.remove(getHSId(max));
            m_alreadyPicked.add(end1);
            m_alreadyPicked.add(end2);
            m_killSet.add(getHSId(max));
            m_fakeMesh.failLink(getHSId(end1), getHSId(end2));
            m_fakeMesh.failLink(getHSId(end2), getHSId(end1));
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
            Iterator<Integer> itr = m_alreadyPicked.iterator();
            while (itr.hasNext()) {
                if (m_expectedLive.contains(getHSId(itr.next()))) {
                    itr.remove();
                }
            }
            for (Long alive: m_expectedLive) {
                MiniSite site = m_nodes.get(alive).m_miniSite;
                m_expectations.put(alive, site.getFailedSitesCount() + m_killSet.size());
            }
            for (Long dead: m_killSet) {
                m_expectations.put(dead, m_nodes.size() - 1);
            }
            m_killSet.clear();
        }

        boolean hasMetExpectations() {
            boolean met = true;
            for (Map.Entry<Long, MiniNode> e: m_nodes.entrySet()) {
                if (e.getValue().getNodeState() == NodeState.STOP) continue;

                MiniSite site = e.getValue().m_miniSite;
                met = met
                   && (  !site.isInArbitration()
                       && site.getFailedSitesCount() >= m_expectations.get(e.getKey()));
            }
            return met;
        }

        void expect() throws InterruptedException {
            long start = System.currentTimeMillis();
            while (!hasMetExpectations()) {
                long now = System.currentTimeMillis();
                if (now - start > 30000) {
                    start = now;
                    dumpNodeState();
                }
                Thread.sleep(50);
            }
        }

        void pruneDeadNodes() throws InterruptedException {
            Iterator<Map.Entry<Long, MiniNode>> itr = m_nodes.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry<Long, MiniNode> e = itr.next();
                if (!m_expectedLive.contains(e.getKey())) {
                    if (e.getValue().getNodeState() != NodeState.STOP) {
                        e.getValue().shutdown();
                        e.getValue().join();
                    }
                    m_expectations.remove(e.getKey());
                    itr.remove();
                }
            }
            expect();
        }

        void joinNode(int node) throws InterruptedException {
            Preconditions.checkArgument(!m_nodes.containsKey(getHSId(node)),
                    "node %s is already part of the cluster", node);
            Preconditions.checkArgument(!m_alreadyPicked.contains(node),
                    "%s was already picked for failure",node);
            m_nodes.put(getHSId(node),null);
            MiniNode mini = new MiniNode(getHSId(node),m_nodes.keySet(),m_fakeMesh);
            m_nodes.put(getHSId(node),mini);
            m_expectedLive.add(getHSId(node));
            mini.start();
            while (mini.getNodeState() == NodeState.START) {
                Thread.sleep(50);
            }
        }
    }

    private void dumpNodeState()
    {
        for (Entry<Long, MiniNode> node : m_nodes.entrySet())
        {
            m_fuzzLog.info(node.getValue().toString());
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

        assertTrue(checkFullyConnectedGraphs(state.m_expectedLive));
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

        assertTrue(checkFullyConnectedGraphs(state.m_expectedLive));

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

    public void testPartition() throws InterruptedException
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
