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
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;

public class TestFuzzMeshArbiter extends TestCase
{
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
        while (getNodesInState(NodeState.RESOLVE).isEmpty()) {
            Thread.sleep(50);
        }
        while (!getNodesInState(NodeState.RESOLVE).isEmpty()) {
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

    public void testNodeFuzz() throws InterruptedException
    {
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        System.out.println("SEED: " + seed);
        constructCluster(40);
        while (!getNodesInState(NodeState.START).isEmpty()) {
            Thread.sleep(50);
        }
        Set<Long> expect = new HashSet<Long>();
        expect.addAll(m_nodes.keySet());

        for (int i = 0; i < 10; i++) {
            int nextToDie = rand.nextInt(40);
            while (!expect.contains(getHSId(nextToDie))) {
                nextToDie = rand.nextInt(40);
            }
            expect.remove(getHSId(nextToDie));
            System.out.println("Next to die: " + nextToDie);
            int delay = rand.nextInt(10) + 1;
            System.out.println("Fuzz delay in ms: " + delay * 5);
            Thread.sleep(delay * 5);
            MiniNode victim = m_nodes.get(getHSId(nextToDie));
            victim.shutdown();
        }

        while (getNodesInState(NodeState.RESOLVE).isEmpty()) {
            Thread.sleep(50);
        }
        while (!getNodesInState(NodeState.RESOLVE).isEmpty()) {
            Thread.sleep(50);
        }

        assertTrue(checkFullyConnectedGraphs(expect));
    }
}
