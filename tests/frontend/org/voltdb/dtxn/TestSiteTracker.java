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

package org.voltdb.dtxn;

import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.voltcore.utils.CoreUtils;
import org.voltdb.MockVoltDB;
import org.voltdb.VoltZK.MailboxType;

public class TestSiteTracker extends TestCase
{
    public void testNoReplication() throws Exception
    {
        MockVoltDB helper = new MockVoltDB();

        Long site1 = CoreUtils.getHSIdFromHostAndSite( 0, 1);
        Long site2 = CoreUtils.getHSIdFromHostAndSite( 0, 2);
        Long site101 = CoreUtils.getHSIdFromHostAndSite( 1, 101);
        Long site102 = CoreUtils.getHSIdFromHostAndSite( 1, 102);
        helper.addSite(site1, 0);
        helper.addSite(site2, 1);
        helper.addSite(site101, 2);
        helper.addSite(site102, 3);

        SiteTracker tracker = helper.getSiteTrackerForSnapshot();
        assertEquals(site1, tracker.getSitesForPartition(0).get(0));
        assertEquals(site2, tracker.getSitesForPartition(1).get(0));
        assertEquals(site101, tracker.getSitesForPartition(2).get(0));
        assertEquals(site102, tracker.getSitesForPartition(3).get(0));
        for (int i = 0; i < 4; i++)
        {
            assertEquals(1, tracker.getSitesForPartition(i).size());
        }
        long[] sites = tracker.getSitesForPartitionsAsArray(new int[] {1, 2, 3});
        assertEquals(3, sites.length);
        for (long site : sites)
        {
            assertTrue(site == site2 || site == site101 || site == site102);
        }
        assertEquals(0, tracker.getPartitionForSite(site1));
        assertEquals(1, tracker.getPartitionForSite(site2));
        assertEquals(2, tracker.getPartitionForSite(site101));
        assertEquals(3, tracker.getPartitionForSite(site102));

        helper.shutdown(null);
    }

    public void testEasyReplication() throws Exception
    {
        MockVoltDB helper = new MockVoltDB();

        Long site1 = CoreUtils.getHSIdFromHostAndSite( 0, 1);
        Long site2 = CoreUtils.getHSIdFromHostAndSite( 0, 2);
        Long site101 = CoreUtils.getHSIdFromHostAndSite( 1, 101);
        Long site102 = CoreUtils.getHSIdFromHostAndSite( 1, 102);
        helper.addSite(site1, 0);
        helper.addSite(site2, 1);
        helper.addSite(site101, 0);
        helper.addSite(site102, 1);

        SiteTracker tracker = helper.getSiteTrackerForSnapshot();
        assertTrue(tracker.getSitesForPartition(0).contains(site1));
        assertTrue(tracker.getSitesForPartition(0).contains(site101));
        assertTrue(tracker.getSitesForPartition(1).contains(site2));
        assertTrue(tracker.getSitesForPartition(1).contains(site102));
        for (int i = 0; i < 2; i++)
        {
            assertEquals(2, tracker.getSitesForPartition(i).size());
        }
        long[] sites = tracker.getSitesForPartitionsAsArray(new int[] {0, 1});
        assertEquals(4, sites.length);
        for (long site : sites)
        {
            assertTrue(site == site1 || site == site2 || site == site101 || site == site102);
        }
        assertEquals(0, tracker.getPartitionForSite(site1));
        assertEquals(1, tracker.getPartitionForSite(site2));
        assertEquals(0, tracker.getPartitionForSite(site101));
        assertEquals(1, tracker.getPartitionForSite(site102));
        helper.shutdown(null);
    }

    public void testHostToSites() throws Exception
    {
        MockVoltDB helper = new MockVoltDB();

        Long site0 = CoreUtils.getHSIdFromHostAndSite( 0, 0);
        Long site1 = CoreUtils.getHSIdFromHostAndSite( 0, 1);
        Long site2 = CoreUtils.getHSIdFromHostAndSite( 0, 2);
        Long site3 = CoreUtils.getHSIdFromHostAndSite( 0, 3);
        Long site100 = CoreUtils.getHSIdFromHostAndSite( 1, 100);
        Long site101 = CoreUtils.getHSIdFromHostAndSite( 1, 101);
        Long site102 = CoreUtils.getHSIdFromHostAndSite( 1, 102);
        Long site103 = CoreUtils.getHSIdFromHostAndSite( 1, 103);
        helper.addSite(site0, MailboxType.Initiator);
        helper.addSite(site1, 0);
        helper.addSite(site2, 1);
        helper.addSite(site3, 1);
        helper.addSite(site100, MailboxType.Initiator);
        helper.addSite(site101, 0);
        helper.addSite(site102, 1);
        helper.addSite(site103, 1);

        SiteTracker tracker = helper.getSiteTrackerForSnapshot();
        List<Long> host0 = tracker.getSitesForHost(0);
        assertFalse(host0.contains(site0));
        assertTrue(host0.contains(site1));
        assertTrue(host0.contains(site2));
        assertTrue(host0.contains(site3));
        assertFalse(host0.contains(site101));

        List<Long> host1 = tracker.getSitesForHost(1);
        assertFalse(host1.contains(site0));
        assertFalse(host1.contains(site1));
        assertTrue(host1.contains(site101));
        assertTrue(host1.contains(site102));
        assertTrue(host1.contains(site103));
        helper.shutdown(null);
    }

    public void testExecutionSiteIds() throws Exception
    {
        MockVoltDB helper = new MockVoltDB();

        Long site0 = CoreUtils.getHSIdFromHostAndSite( 0, 0);
        Long site1 = CoreUtils.getHSIdFromHostAndSite( 0, 1);
        Long site2 = CoreUtils.getHSIdFromHostAndSite( 0, 2);
        Long site100 = CoreUtils.getHSIdFromHostAndSite( 1, 100);
        Long site101 = CoreUtils.getHSIdFromHostAndSite( 1, 101);
        Long site102 = CoreUtils.getHSIdFromHostAndSite( 1, 102);

        helper.addSite(site0, MailboxType.Initiator);
        helper.addSite(site1, 0);
        helper.addSite(site2, 1);
        helper.addSite(site100, MailboxType.Initiator);
        helper.addSite(site101, 0);
        helper.addSite(site102, 1);

        SiteTracker tracker = helper.getSiteTrackerForSnapshot();
        Set<Long> exec_sites = tracker.getAllSites();
        assertFalse(exec_sites.contains(site0));
        assertTrue(exec_sites.contains(site1));
        assertTrue(exec_sites.contains(site2));
        assertFalse(exec_sites.contains(site100));
        assertTrue(exec_sites.contains(site101));
        assertTrue(exec_sites.contains(site102));
        assertEquals(site1, tracker.getLowestSiteForHost(0));
        assertEquals(site101, tracker.getLowestSiteForHost(1));

        helper.shutdown(null);
    }
}
