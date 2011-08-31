/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.compiler;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;

import junit.framework.TestCase;

public class TestClusterCompiler extends TestCase
{
    public void testNonZeroReplicationFactor()
    {
        ClusterConfig config = new ClusterConfig(3, 1, 2);
        Catalog catalog = new Catalog();
        catalog.execute("add / clusters cluster");
        ClusterCompiler.compile(catalog, config);
        System.out.println(catalog.serialize());
        Cluster cluster = catalog.getClusters().get("cluster");
        CatalogMap<Partition> partitions = cluster.getPartitions();
        // despite 3 hosts, should only have 1 partition with k-safety of 2
        assertEquals(1, partitions.size());
        // All the execution sites should have the same relative index
        int part_guid = partitions.get("0").getRelativeIndex();
        for (Site site : cluster.getSites())
        {
            if (site.getIsexec())
            {
                assertEquals(part_guid, site.getPartition().getRelativeIndex());
            }
        }
    }

    public void testSufficientHostsToReplicate()
    {
        // 2 hosts, 6 sites per host, 2 copies of each partition.
        // there are sufficient execution sites, but insufficient hosts
        ClusterConfig config = new ClusterConfig(2, 6, 2);
        Catalog catalog = new Catalog();
        catalog.execute("add / clusters cluster");
        boolean caught = false;
        try
        {
            ClusterCompiler.compile(catalog, config);
        }
        catch (RuntimeException e)
        {
            if (e.getMessage().contains("servers required"))
            {
                caught = true;
            }
        }
        assertTrue(caught);
    }
}
