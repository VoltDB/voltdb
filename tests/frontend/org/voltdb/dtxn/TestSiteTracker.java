/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.dtxn;

import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.voltdb.MockVoltDB;

public class TestSiteTracker extends TestCase
{
    public void testNoReplication() throws Exception
    {
        throw new UnsupportedOperationException("getOneSiteForPartition was removed?");
//        MockVoltDB helper = new MockVoltDB();
//
//        helper.addHost(0);
//        helper.addHost(1);
//        helper.addPartition(0);
//        helper.addPartition(1);
//        helper.addPartition(2);
//        helper.addPartition(3);
//        helper.addSite(1, 0, 0, true);
//        helper.addSite(2, 0, 1, true);
//        helper.addSite(101, 1, 2, true);
//        helper.addSite(102, 1, 3, true);
//
//        SiteTracker tracker = helper.getSiteTracker();
//        assertEquals(1, tracker.getOneSiteForPartition(0));
//        assertEquals(2, tracker.getOneSiteForPartition(1));
//        assertEquals(101, tracker.getOneSiteForPartition(2));
//        assertEquals(102, tracker.getOneSiteForPartition(3));
//        assertEquals(1, (int)tracker.getAllSitesForPartition(0).get(0));
//        assertEquals(2, (int)tracker.getAllSitesForPartition(1).get(0));
//        assertEquals(101, (int)tracker.getAllSitesForPartition(2).get(0));
//        assertEquals(102, (int)tracker.getAllSitesForPartition(3).get(0));
//        for (int i = 0; i < 4; i++)
//        {
//            assertEquals(1, tracker.getAllSitesForPartition(i).size());
//        }
//        int[] sites = tracker.getAllSitesForEachPartition(new int[] {1, 2, 3});
//        assertEquals(3, sites.length);
//        for (int site : sites)
//        {
//            assertTrue(site == 2 || site == 101 || site == 102);
//        }
//        assertEquals(0, tracker.getPartitionForSite(1));
//        assertEquals(1, tracker.getPartitionForSite(2));
//        assertEquals(2, tracker.getPartitionForSite(101));
//        assertEquals(3, tracker.getPartitionForSite(102));
//
//        helper.shutdown(null);
    }

    public void testEasyReplication() throws Exception
    {
        throw new UnsupportedOperationException("getAllSitesForPartition was removed?");
//        MockVoltDB helper = new MockVoltDB();
//
//        helper.addHost(0);
//        helper.addHost(1);
//        helper.addPartition(0);
//        helper.addPartition(1);
//        helper.addSite(1, 0, 0, true);
//        helper.addSite(2, 0, 1, true);
//        helper.addSite(101, 1, 0, true);
//        helper.addSite(102, 1, 1, true);
//
//        SiteTracker tracker = helper.getSiteTracker();
//        assertTrue(tracker.getAllSitesForPartition(0).contains(1));
//        assertTrue(tracker.getAllSitesForPartition(0).contains(101));
//        assertTrue(tracker.getAllSitesForPartition(1).contains(2));
//        assertTrue(tracker.getAllSitesForPartition(1).contains(102));
//        for (int i = 0; i < 2; i++)
//        {
//            assertEquals(2, tracker.getAllSitesForPartition(i).size());
//        }
//        int[] sites = tracker.getAllSitesForEachPartition(new int[] {0, 1});
//        assertEquals(4, sites.length);
//        for (int site : sites)
//        {
//            assertTrue(site == 1 || site == 2 || site == 101 || site == 102);
//        }
//        assertEquals(0, tracker.getPartitionForSite(1));
//        assertEquals(1, tracker.getPartitionForSite(2));
//        assertEquals(0, tracker.getPartitionForSite(101));
//        assertEquals(1, tracker.getPartitionForSite(102));
//        helper.shutdown(null);
    }

    public void testHostToSites() throws Exception
    {
        MockVoltDB helper = new MockVoltDB();

        helper.addHost(0);
        helper.addHost(1);
        helper.addPartition(0);
        helper.addPartition(1);
        helper.addSite(0, 0, 0, false, true);
        helper.addSite(1, 0, 0, true, true);
        helper.addSite(2, 0, 1, true, true);
        helper.addSite(3, 0, 1, true, false);
        helper.addSite(100, 1, 0, false, true);
        helper.addSite(101, 1, 0, true, true);
        helper.addSite(102, 1, 1, true, false);
        helper.addSite(103, 1, 1, true, true);

        SiteTracker tracker = helper.getSiteTracker();
        List<Long> host0 = tracker.getSitesForHost(0);
        assertTrue(host0.contains(0L));
        assertTrue(host0.contains(1L));
        assertTrue(host0.contains(2L));
        assertFalse(host0.contains(101L));
        host0 = tracker.getSitesForHost(0);
        assertFalse(host0.contains(0L));
        assertTrue(host0.contains(1L));
        assertTrue(host0.contains(2L));
        assertFalse(host0.contains(3L));
        assertFalse(host0.contains(101L));
        List<Long> host1 = tracker.getSitesForHost(1);
        assertTrue(host1.contains(100L));
        assertTrue(host1.contains(101L));
        assertTrue(host1.contains(102L));
        assertFalse(host1.contains(1L));
        host1 = tracker.getSitesForHost(1);
        assertFalse(host1.contains(100L));
        assertTrue(host1.contains(101L));
        assertFalse(host1.contains(102L));
        assertTrue(host1.contains(103L));
        assertFalse(host1.contains(1L));
        helper.shutdown(null);
    }

    public void testUpSites() throws Exception
    {
        throw new UnsupportedOperationException("getUpSites was removed?");
//        MockVoltDB helper = new MockVoltDB();
//
//        helper.addHost(0);
//        helper.addHost(1);
//        helper.addPartition(0);
//        helper.addPartition(1);
//        helper.addSite(0, 0, 0, false, true);
//        helper.addSite(1, 0, 0, true, true);
//        helper.addSite(2, 0, 1, true, true);
//        helper.addSite(100, 1, 0, false, false);
//        helper.addSite(101, 1, 0, true, false);
//        helper.addSite(102, 1, 1, true, false);
//
//        SiteTracker tracker = helper.getSiteTracker();
//        ArrayDeque<Site> up_sites = tracker.getUpSites();
//        assertTrue(up_sites.contains(helper.getSite(0)));
//        assertTrue(up_sites.contains(helper.getSite(1)));
//        assertTrue(up_sites.contains(helper.getSite(2)));
//        assertFalse(up_sites.contains(helper.getSite(100)));
//        assertFalse(up_sites.contains(helper.getSite(101)));
//        assertFalse(up_sites.contains(helper.getSite(102)));
//
//        assertEquals(2, tracker.getLiveSiteCount());
//        helper.shutdown(null);
    }

    public void testExecutionSiteIds() throws Exception
    {
        MockVoltDB helper = new MockVoltDB();

        helper.addHost(0);
        helper.addHost(1);
        helper.addPartition(0);
        helper.addPartition(1);
        helper.addSite(0, 0, 0, false);
        helper.addSite(1, 0, 0, true);
        helper.addSite(2, 0, 1, true);
        helper.addSite(100, 1, 0, false);
        helper.addSite(101, 1, 0, true);
        helper.addSite(102, 1, 1, true);

        SiteTracker tracker = helper.getSiteTracker();
        Set<Long> exec_sites = tracker.getAllSites();
        assertFalse(exec_sites.contains(0L));
        assertTrue(exec_sites.contains(1L));
        assertTrue(exec_sites.contains(2L));
        assertFalse(exec_sites.contains(100L));
        assertTrue(exec_sites.contains(101L));
        assertTrue(exec_sites.contains(102L));
        assertEquals((Long) 1L, tracker.getLowestSiteForHost(0));
        assertEquals((Long) 101L, tracker.getLowestSiteForHost(1));

        helper.shutdown(null);
    }

    public void testLiveSitesForPartitions() throws Exception
    {
        MockVoltDB helper = new MockVoltDB();
        helper.addHost(0);
        helper.addHost(1);
        helper.addPartition(0);
        helper.addPartition(1);
        helper.addSite(0, 0, 0, false, true);
        helper.addSite(1, 0, 0, true, true);
        helper.addSite(2, 0, 1, true, true);
        helper.addSite(100, 1, 0, false, false);
        helper.addSite(101, 1, 0, true, false);
        helper.addSite(102, 1, 1, true, false);

        SiteTracker tracker = helper.getSiteTracker();
        assertEquals(1, tracker.getSitesForPartition(0).size());
        assertEquals(1, tracker.getSitesForPartition(1).size());
        assertTrue(tracker.getSitesForPartition(0).contains(1));
        assertFalse(tracker.getSitesForPartition(0).contains(101));

        List<Long> sites = tracker.getSitesForPartitions(new int[] {0, 1});
        assertEquals(2, sites.size());
        for (long site : sites)
        {
            assertTrue(site == 1 || site == 2);
        }

        helper.shutdown(null);
    }

    public void testClusterViable() throws Exception
    {
        MockVoltDB helper = new MockVoltDB();

        helper.addHost(0);
        helper.addHost(1);
        helper.addPartition(0);
        helper.addPartition(1);
        helper.addSite(0, 0, 0, false, true);
        helper.addSite(1, 0, 0, true, true);
        helper.addSite(2, 0, 1, true, true);
        helper.addSite(100, 1, 0, false, false);
        helper.addSite(101, 1, 0, true, false);
        helper.addSite(102, 1, 1, true, false);

        SiteTracker tracker = helper.getSiteTracker();
        assertEquals(0, tracker.getFailedPartitions().size());
        helper.killSite(2);
        tracker = helper.getSiteTracker();
        assertEquals(1, tracker.getFailedPartitions().size());
        assertEquals((Integer) 1, tracker.getFailedPartitions().get(0));

        helper.shutdown(null);
    }
}
