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

package org.voltdb.sysprocs.saverestore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.DevNullSnapshotTarget;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.sysprocs.SnapshotSave;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

public class TestIndexSnapshotWritePlan {
    protected static Database database;
    protected static SystemProcedureExecutionContext context;

    protected SiteTracker tracker;
    protected Map<Integer, Long> pidToHSId;
    protected Set<Integer> involvedPartitions;

    // test all partitions needed
    @Test
    public void testAllPartitionsInvolved() throws JSONException, IOException
    {
        planAndCheck(/* partitionCountOnNode */ 1, /* involvedPartitionCount */ 1);
        planAndCheck(/* partitionCountOnNode */ 4, /* involvedPartitionCount */ 4);
        planAndCheck(/* partitionCountOnNode */ 12, /* involvedPartitionCount */ 12);
    }

    // test no partitions needed
    @Test
    public void testNoPartitionsInvolved() throws IOException, JSONException
    {
        planAndCheck(/* partitionCountOnNode */ 1, /* involvedPartitionCount */ 0);
        planAndCheck(/* partitionCountOnNode */ 4, /* involvedPartitionCount */ 0);
        planAndCheck(/* partitionCountOnNode */ 12, /* involvedPartitionCount */ 0);
    }

    // test partial partitions needed
    @Test
    public void testPartialPartitionsInvolved() throws IOException, JSONException
    {
        planAndCheck(/* partitionCountOnNode */ 2, /* involvedPartitionCount */ 1);
        planAndCheck(/* partitionCountOnNode */ 4, /* involvedPartitionCount */ 3);
        planAndCheck(/* partitionCountOnNode */ 12, /* involvedPartitionCount */ 7);
    }

    private void planAndCheck(int partitionCountOnNode, int involvedPartitionCount)
        throws JSONException, IOException
    {
        setUpSiteTracker(partitionCountOnNode);
        List<Table> tables = getPartitionedTables();
        JSONObject jsObj = generateConfig(involvedPartitionCount, tables);
        VoltTable result = SnapshotSave.constructNodeResultsTable();

        IndexSnapshotWritePlan plan = new IndexSnapshotWritePlan();
        plan.createSetupInternal(null,         // path
                                 "Join_index", // nonce
                                 1,            // txnid
                                 null,         // partition txnid
                                 jsObj,        // config
                                 context,      // context
                                 null,         // hostname
                                 result,       // result table
                                 null,         // export seq num
                                 tracker,      // site tracker
                                 null,         // hashinator data
                                 0);           // ts

        // check
        checkPlan(plan);
    }

    private void setUpSiteTracker(int partitionCountOnNode)
    {
        pidToHSId = Maps.newHashMap();
        for (int i = 1; i <= partitionCountOnNode; i++) {
            // HSID is pid * 1000
            pidToHSId.put(i, i * 1000l);
        }

        tracker = mock(SiteTracker.class);
        // local sites
        doReturn(Longs.toArray(pidToHSId.values())).when(tracker).getLocalSites();
        // partition to site
        for (Map.Entry<Integer, Long> e : pidToHSId.entrySet()) {
            doReturn(e.getKey()).when(tracker).getPartitionForSite(e.getValue());
        }
    }

    private void checkPlan(IndexSnapshotWritePlan plan)
    {
        Set<Long> involvedSites = Sets.newHashSet();
        for (Integer pid : involvedPartitions) {
            involvedSites.add(pidToHSId.get(pid));
        }

        assertEquals(involvedSites, plan.m_taskListsForHSIds.keySet());
        if (involvedSites.isEmpty()) {
            assertTrue(plan.m_targets.isEmpty());
        } else {
            assertEquals(8 /* 8 partitioned tables in tpcc */, plan.m_targets.size());
            for (SnapshotDataTarget target : plan.m_targets) {
                assertTrue(target instanceof DevNullSnapshotTarget);
            }
        }
    }

    private JSONObject generateConfig(int involvedPartitionCount, List<Table> tables)
        throws JSONException
    {
        involvedPartitions = Sets.newHashSet();
        List<IndexSnapshotRequestConfig.PartitionRanges> pRanges =
            new ArrayList<IndexSnapshotRequestConfig.PartitionRanges>();

        for (int pid : pidToHSId.keySet()) {
            if (involvedPartitionCount-- == 0) {
                // Only the involved partitions will have ranges
                break;
            }

            Map<Long, Long> ranges = new TreeMap<Long, Long>();

            // 5 discontinuous ranges per partition
            long rangeStart = pid * 100l;
            for (int i = 0; i < 5; i++) {
                ranges.put(rangeStart, rangeStart + 5);
                rangeStart += 10;
            }

            pRanges.add(new IndexSnapshotRequestConfig.PartitionRanges(pid, ranges));
            involvedPartitions.add(pid);
        }

        IndexSnapshotRequestConfig config = new IndexSnapshotRequestConfig(pRanges);
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        config.toJSONString(stringer);
        stringer.endObject();

        return new JSONObject(stringer.toString());
    }

    private static List<Table> getPartitionedTables()
    {
        // only partitioned tables
        List<Table> tables = Lists.newArrayList();
        for (Table table : database.getTables()) {
            if (!table.getIsreplicated()) {
                tables.add(table);
            }
        }
        return tables;
    }

    @BeforeClass
    public static void setupOnce() throws IOException
    {
        database = TPCCProjectBuilder.getTPCCSchemaCatalog()
                                     .getClusters().get("cluster")
                                     .getDatabases().get("database");

        context = mock(SystemProcedureExecutionContext.class);
        doReturn(database).when(context).getDatabase();
        doReturn(1).when(context).getHostId();
    }
}
