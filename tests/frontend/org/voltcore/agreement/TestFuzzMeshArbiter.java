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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.voltcore.agreement.MiniNode.NodeState;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;

public class TestFuzzMeshArbiter extends TestCase
{
    public static VoltLogger m_fuzzLog = new VoltLogger("FUZZ");

    FakeMesh m_fakeMesh;
    Map<Long, MiniNode> m_nodes;

    long getHSId(int i)
    {
        return CoreUtils.getHSIdFromHostAndSite(i, HostMessenger.AGREEMENT_SITE_ID);
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
        constructCluster(4);
        while (!getNodesInState(NodeState.START).isEmpty()) {
            Thread.sleep(50);
        }
        m_fakeMesh.failLink(getHSId(0), getHSId(1));
        m_fakeMesh.failLink(getHSId(1), getHSId(0));
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

        // This set should change to include 0 with the
        // better single-link failure algorithm
        Set<Long> expect = new HashSet<Long>();
        expect.addAll(m_nodes.keySet());
        // expect.remove(getHSId(0));
        expect.remove(getHSId(1));
        assertTrue(checkFullyConnectedGraphs(expect));
    }

    class FuzzTestState
    {
        Random m_rand;
        Set<Long> m_expectedLive;

        FuzzTestState(long seed, Set<Long> startingNodes)
        {
            m_expectedLive = new HashSet<Long>();
            m_expectedLive.addAll(startingNodes);
            m_rand = new Random(seed);
        }

        int getRandomLiveNode()
        {
            int node = m_rand.nextInt(m_nodes.size());
            while (!m_expectedLive.contains(getHSId(node))) {
                node = m_rand.nextInt(m_nodes.size());
            }
            return node;
        }
    }

    private void dumpNodeState()
    {
        for (Entry<Long, MiniNode> node : m_nodes.entrySet())
        {
            m_fuzzLog.info(node.getValue().toString());
        }
    }

    void killRandomNode(FuzzTestState state) throws InterruptedException
    {
        int nextToDie = state.getRandomLiveNode();
        state.m_expectedLive.remove(getHSId(nextToDie));
        System.out.println("Next to die: " + nextToDie);
        int delay = state.m_rand.nextInt(10) + 1;
        System.out.println("Fuzz delay in ms: " + delay * 5);
        Thread.sleep(delay * 5);
        MiniNode victim = m_nodes.get(getHSId(nextToDie));
        victim.shutdown();
    }

    void killRandomLink(FuzzTestState state) throws InterruptedException
    {
        int end1 = state.getRandomLiveNode();
        int end2 = state.getRandomLiveNode();
        while (end1 == end2) {
            end2 = state.getRandomLiveNode();
        }
        int max = Math.max(end1, end2);
        state.m_expectedLive.remove(getHSId(max));
        System.out.println("Next link to die: " + end1 + ":" + end2);
        int delay = state.m_rand.nextInt(10) + 1;
        System.out.println("Fuzz delay in ms: " + delay * 5);
        m_fakeMesh.failLink(getHSId(end1), getHSId(end2));
        Thread.sleep(delay * 5);
        m_fakeMesh.failLink(getHSId(end2), getHSId(end1));
    }

    public void testFuzz() throws InterruptedException
    {
        long seed = System.currentTimeMillis();
        System.out.println("SEED: " + seed);
        constructCluster(10);
        while (!getNodesInState(NodeState.START).isEmpty()) {
            Thread.sleep(50);
        }
        FuzzTestState state = new FuzzTestState(seed, m_nodes.keySet());

        for (int i = 0; i < 2; i++) {
            if (state.m_rand.nextInt(100) < 50) {
                killRandomNode(state);
            }
            else {
                killRandomLink(state);
            }
        }

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
