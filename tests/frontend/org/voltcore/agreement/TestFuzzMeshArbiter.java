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
import java.util.Map;
import java.util.Set;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;

import junit.framework.TestCase;

public class TestFuzzMeshArbiter extends TestCase
{
    FakeMesh m_fakeMesh;
    Map<Long, MiniNode> m_nodes;

    MiniNode getNode(int i)
    {
        long HSId = CoreUtils.getHSIdFromHostAndSite(i, HostMessenger.AGREEMENT_SITE_ID);
        return m_nodes.get(HSId);
    }

    void constructCluster(int nodeCount)
    {
        m_fakeMesh = new FakeMesh();
        m_fakeMesh.start();
        m_nodes = new HashMap<Long, MiniNode>();
        for (int i = 0; i < nodeCount; i++) {
            long HSId = CoreUtils.getHSIdFromHostAndSite(i, HostMessenger.AGREEMENT_SITE_ID);
            m_nodes.put(HSId, null);
        }
        Set<Long> HSIds = m_nodes.keySet();
        for (long HSId : HSIds) {
            m_nodes.put(HSId, new MiniNode(HSId, HSIds, m_fakeMesh));
            m_nodes.get(HSId).start();
        }
    }

    public void tearDown() throws InterruptedException
    {
        for (MiniNode node : m_nodes.values()) {
            node.shutdown();
            node.join();
        }
        m_fakeMesh.shutdown();
        m_fakeMesh.join();
    }

    public void testNodeFail() throws InterruptedException    {
        constructCluster(4);
        Thread.sleep(30000);
        MiniNode victim = getNode(0);
        victim.shutdown();
        while (true) {}
    }
}
