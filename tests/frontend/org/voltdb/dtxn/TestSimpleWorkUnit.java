/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.dtxn;

import junit.framework.TestCase;

import org.voltdb.MockVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;

public class TestSimpleWorkUnit extends TestCase
{
    static final VoltMessage work = new InitiateResponseMessage();
    VoltTable t1;
    VoltTable t2;
    MockVoltDB m_voltdb;
    SiteTracker st;

    // automatically generate a limited set of cluster topologies
    public void setUpSites(int numHosts, int numParts, int replicas)
    {
        assert (numHosts >= replicas);
        assert(((numParts * replicas) % numHosts) == 0);
        assert(numHosts % replicas == 0);
        int sites_per_host = (numParts * replicas) / numHosts;
        for (int i = 0; i < numParts * replicas; i++)
        {
            System.out.println("Adding site host " + (i / sites_per_host) + " site " + i);
            m_voltdb.addSite(CoreUtils.getHSIdFromHostAndSite(i / sites_per_host, i), i % replicas);
        }
        st = m_voltdb.getSiteTracker();
    }

    @Override
    public void setUp()
    {
        m_voltdb = new MockVoltDB();
        VoltDB.crashMessage = null;
        VoltDB.wasCrashCalled = false;
        VoltDB.ignoreCrash = true;
        VoltDB.replaceVoltDBInstanceForTest(m_voltdb);

        VoltTable.ColumnInfo[] cols1 =
        { new VoltTable.ColumnInfo("name", VoltType.STRING) };

        VoltTable.ColumnInfo[] cols2 =
        { new VoltTable.ColumnInfo("age", VoltType.INTEGER) };

        t1 = new VoltTable(cols1, 1);
        t1.addRow("dude");
        t2 = new VoltTable(cols2, 1);
        t2.addRow(10);
        st = m_voltdb.getSiteTracker();
    }

    @Override
    public void tearDown() throws Exception {
        m_voltdb.shutdown(null);
        st = null;
    }

    public void testNoDependenciesNoReplicas() {
        setUpSites(1, 2, 1);
        WorkUnit w = new WorkUnit(m_voltdb.getSiteTracker(),
                                  work, new int[]{}, 0, null, false);
        assertTrue(w.allDependenciesSatisfied());
        assertEquals(work, w.getPayload());
        assertNull(w.getDependencies());
        assertNull(w.getDependency(0));

        w = new WorkUnit(m_voltdb.getSiteTracker(), work, null,
                         0, null, false);
        assertTrue(w.allDependenciesSatisfied());
    }

    public void testDependenciesNoReplicas() {
        setUpSites(1, 2, 1);
        System.out.println(m_voltdb.getCatalogContext().catalog.serialize());
        int multi_dep = 5 | DtxnConstants.MULTIPARTITION_DEPENDENCY;
        WorkUnit w = new WorkUnit(m_voltdb.getSiteTracker(),
                                  work, new int[]{ 4, multi_dep }, 0L,
                                  new long[]{CoreUtils.getHSIdFromHostAndSite(0, 1)}, false);
        assertFalse(w.allDependenciesSatisfied());
        assertEquals(w.getDependency(4).size(), 0);
        assertEquals(w.getDependency(multi_dep).size(), 0);
        w.putDependency(4, 0, t1, st);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, 0, t2, st);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, CoreUtils.getHSIdFromHostAndSite(0, 1), t2, st);
        assertTrue(w.allDependenciesSatisfied());
        assertEquals(t1, w.getDependency(4).get(0));
        assertEquals(t2, w.getDependency(multi_dep).get(0));
    }

    public void testBadPutDependencyNoReplicas() {
        setUpSites(1, 2, 1);
        WorkUnit w = new WorkUnit(m_voltdb.getSiteTracker(),
                                  work, new int[]{ 4, 5 }, 0L,
                                  new long[]{1}, false);

        // Put a dependency that does not exist
        try {
            w.putDependency(0, 0, t1, st);
            fail("assertion expected");
        } catch (AssertionError e) {}

        // Put a dependency with a null value
        try {
            w.putDependency(4, 0, null, st);
            fail("assertion expected");
        } catch (AssertionError e) {}

        // Put a dependency twice
        w.putDependency(4, 0, t1, st);
        try {
            w.putDependency(4, 0, t1, st);
            fail("assertion expected");
        } catch (AssertionError e) {}
    }

    public void testDependenciesWithReplicas()
    {
        setUpSites(2, 2, 2);
        System.out.println(m_voltdb.getCatalogContext().catalog.serialize());
        int multi_dep = 5 | DtxnConstants.MULTIPARTITION_DEPENDENCY;
        WorkUnit w = new WorkUnit(m_voltdb.getSiteTracker(),
                                  work, new int[]{ 4, multi_dep }, 0L,
                                  new long[]{
                                        CoreUtils.getHSIdFromHostAndSite( 0, 1),
                                        CoreUtils.getHSIdFromHostAndSite( 1, 2),
                                        CoreUtils.getHSIdFromHostAndSite( 1, 3)}, false);
        assertFalse(w.allDependenciesSatisfied());
        assertEquals(w.getDependency(4).size(), 0);
        assertEquals(w.getDependency(5).size(), 0);
        w.putDependency(4, 0, t1, st);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, 0, t2, st);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, CoreUtils.getHSIdFromHostAndSite( 0, 1), t2, st);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, CoreUtils.getHSIdFromHostAndSite( 1, 2), t2, st);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, CoreUtils.getHSIdFromHostAndSite( 1, 3), t2, st);
        assertTrue(w.allDependenciesSatisfied());
        assertEquals(1, w.getDependency(4).size());
        assertEquals(t1, w.getDependency(4).get(0));
        assertEquals(2, w.getDependency(multi_dep).size());
        assertEquals(t2, w.getDependency(multi_dep).get(0));
    }

    public void testReplicaDependencyWithMismatchedResults()
    {
        VoltTable.ColumnInfo[] cols2 =
        { new VoltTable.ColumnInfo("age", VoltType.INTEGER) };

        VoltTable t3 = new VoltTable(cols2, 1);
        t3.addRow(11);

        setUpSites(2, 2, 1);
        int multi_dep = 5 | DtxnConstants.MULTIPARTITION_DEPENDENCY;
        WorkUnit w = new WorkUnit(m_voltdb.getSiteTracker(),
                                  work, new int[]{ 4, multi_dep }, 0L,
                                  new long[] { 1, 2, 3}, false);
        assertFalse(w.allDependenciesSatisfied());
        assertEquals(w.getDependency(4).size(), 0);
        assertEquals(w.getDependency(5).size(), 0);
        w.putDependency(4, 0, t1, st);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, 0, t2, st);
        assertFalse(w.allDependenciesSatisfied());
        boolean threw = false;
        try
        {
            w.putDependency(multi_dep, 1, t3, st);
        }
        catch (RuntimeException e)
        {
            threw = true;
        }
        assertTrue(threw);
    }

    public void testDependenciesWithReplicasAndFailure()
    {
        setUpSites(2, 2, 2);
        System.out.println(m_voltdb.getCatalogContext().catalog.serialize());
        int multi_dep = 5 | DtxnConstants.MULTIPARTITION_DEPENDENCY;
        WorkUnit w = new WorkUnit(m_voltdb.getSiteTracker(),
                                  work, new int[]{ 4, multi_dep }, 0L,
                                  new long[]{
                                      CoreUtils.getHSIdFromHostAndSite( 0, 1),
                                      CoreUtils.getHSIdFromHostAndSite( 1, 2),
                                      CoreUtils.getHSIdFromHostAndSite( 1, 3)},
                                      false);
        assertFalse(w.allDependenciesSatisfied());
        assertEquals(w.getDependency(4).size(), 0);
        assertEquals(w.getDependency(5).size(), 0);
        w.putDependency(4, 0, t1, st);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, 0, t2, st);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, CoreUtils.getHSIdFromHostAndSite( 0, 1), t2, st);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, CoreUtils.getHSIdFromHostAndSite( 1, 2), t2, st);
        assertFalse(w.allDependenciesSatisfied());
        w.removeSite(CoreUtils.getHSIdFromHostAndSite( 1, 3));
        assertTrue(w.allDependenciesSatisfied());
        assertEquals(1, w.getDependency(4).size());
        assertEquals(t1, w.getDependency(4).get(0));
        assertEquals(2, w.getDependency(multi_dep).size());
        assertEquals(t2, w.getDependency(multi_dep).get(0));
    }

    // add tests for Node-level dependency
}
