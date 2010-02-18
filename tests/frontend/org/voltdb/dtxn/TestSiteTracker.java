/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import org.voltdb.catalog.Catalog;
import org.voltdb.sysprocs.saverestore.CatalogCreatorTestHelper;

import junit.framework.TestCase;

public class TestSiteTracker extends TestCase
{
    public void testNoReplication()
    {
        // TODO: move the test helper somewhere more common perhaps.  Or, reuse
        // or combine with MockVoltDB-esque think like in TestSimpleWorkUnit.
        // Create a pretty stupid topology.  2 hosts, 2 sites per host, no rep.
        CatalogCreatorTestHelper helper =
            new CatalogCreatorTestHelper("cluster", "database");
        helper.addHost(0);
        helper.addHost(1);
        helper.addPartition(0);
        helper.addPartition(1);
        helper.addPartition(2);
        helper.addPartition(3);
        helper.addSite(1, 0, 0, true);
        helper.addSite(2, 0, 1, true);
        helper.addSite(101, 1, 2, true);
        helper.addSite(102, 1, 3, true);
        Catalog catalog = helper.getCatalog();

        SiteTracker tracker = new SiteTracker(catalog.getClusters().get("cluster").getSites());
        assertEquals(1, tracker.getOneSiteForPartition(0));
        assertEquals(2, tracker.getOneSiteForPartition(1));
        assertEquals(101, tracker.getOneSiteForPartition(2));
        assertEquals(102, tracker.getOneSiteForPartition(3));
        assertEquals(1, (int)tracker.getAllSitesForPartition(0).get(0));
        assertEquals(2, (int)tracker.getAllSitesForPartition(1).get(0));
        assertEquals(101, (int)tracker.getAllSitesForPartition(2).get(0));
        assertEquals(102, (int)tracker.getAllSitesForPartition(3).get(0));
        for (int i = 0; i < 4; i++)
        {
            assertEquals(1, tracker.getAllSitesForPartition(i).size());
        }
        int[] sites = tracker.getAllSitesForEachPartition(new int[] {1, 2, 3});
        assertEquals(3, sites.length);
        for (int site : sites)
        {
            assertTrue(site == 2 || site == 101 || site == 102);
        }
        assertEquals(0, tracker.getPartitionForSite(1));
        assertEquals(1, tracker.getPartitionForSite(2));
        assertEquals(2, tracker.getPartitionForSite(101));
        assertEquals(3, tracker.getPartitionForSite(102));
    }

    public void testEasyReplication()
    {
        CatalogCreatorTestHelper helper =
            new CatalogCreatorTestHelper("cluster", "database");
        helper.addHost(0);
        helper.addHost(1);
        helper.addPartition(0);
        helper.addPartition(1);
        helper.addSite(1, 0, 0, true);
        helper.addSite(2, 0, 1, true);
        helper.addSite(101, 1, 0, true);
        helper.addSite(102, 1, 1, true);
        Catalog catalog = helper.getCatalog();

        SiteTracker tracker = new SiteTracker(catalog.getClusters().get("cluster").getSites());
        assertTrue(tracker.getAllSitesForPartition(0).contains(1));
        assertTrue(tracker.getAllSitesForPartition(0).contains(101));
        assertTrue(tracker.getAllSitesForPartition(1).contains(2));
        assertTrue(tracker.getAllSitesForPartition(1).contains(102));
        for (int i = 0; i < 2; i++)
        {
            assertEquals(2, tracker.getAllSitesForPartition(i).size());
        }
        int[] sites = tracker.getAllSitesForEachPartition(new int[] {0, 1});
        assertEquals(4, sites.length);
        for (int site : sites)
        {
            assertTrue(site == 1 || site == 2 || site == 101 || site == 102);
        }
        assertEquals(0, tracker.getPartitionForSite(1));
        assertEquals(1, tracker.getPartitionForSite(2));
        assertEquals(0, tracker.getPartitionForSite(101));
        assertEquals(1, tracker.getPartitionForSite(102));
    }
}
