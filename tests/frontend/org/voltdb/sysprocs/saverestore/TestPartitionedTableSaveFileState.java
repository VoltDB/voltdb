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

package org.voltdb.sysprocs.saverestore;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.MockVoltDB;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.VoltTable;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.catalog.Table;
import org.voltdb.sysprocs.SysProcFragmentId;

import junit.framework.TestCase;


public class TestPartitionedTableSaveFileState extends TestCase
{
    private static final String TABLE_NAME = "test_table";
    private static final String DATABASE_NAME = "database";

    @Override
    public void setUp()
    {
        m_state = new PartitionedTableSaveFileState(TABLE_NAME, 0);
        m_siteInput =
            ClusterSaveFileState.constructEmptySaveFileStateVoltTable();
        m_voltDB = new MockVoltDB();
        VoltDB.replaceVoltDBInstanceForTest(m_voltDB);
        m_voltDB.addTable(TABLE_NAME, false);
    }

    @Override
    public void tearDown() throws Exception {
        m_voltDB.shutdown(null);
    }

    public void testLoadOperation()
    {
        assertEquals(m_state.getTableName(), TABLE_NAME);

        /**
         * Original Host 1 has partitions 0,1
         * Original Host 2 has partition 2
         * Original Host 3 has partition 3,0
         */
        addSiteToTestData(0, 1, 0, 4);
        addSiteToTestData(0, 1, 1, 4);

        addSiteToTestData(1, 1, 1, 4);
        addSiteToTestData(1, 1, 0, 4);
        addSiteToTestData(1, 2, 2, 4);

        addSiteToTestData(2, 3, 3, 4);
        addSiteToTestData(2, 3, 0, 4);

        addSiteToTestData(3, 3, 3, 4);
        addSiteToTestData(3, 3, 0, 4);

        m_siteInput.resetRowPosition();
        while (m_siteInput.advanceRow())
        {
            try
            {
                // this will add the active row of m_siteInput
                m_state.addHostData(m_siteInput);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                assertTrue(false);
            }
        }
        assertEquals(m_state.getTotalPartitions(), 4);
        assertTrue(m_state.isConsistent());
        assertTrue(m_state.getConsistencyResult().
                   contains("has consistent savefile state"));

        Set<Integer> partitions = m_state.getPartitionSet();
        assertEquals(partitions.size(), 4);
        assertTrue(partitions.contains(0));
        assertTrue(partitions.contains(1));
        assertTrue(partitions.contains(2));
        assertTrue(partitions.contains(3));

        Set<Pair<Integer, Integer>> host_0_partitions = m_state.getPartitionsAtHost(0);
        assertEquals(host_0_partitions.size(), 2);
        assertTrue(host_0_partitions.contains(new Pair<Integer, Integer>(0, 1)));
        assertTrue(host_0_partitions.contains(new Pair<Integer, Integer>(1, 1)));

        Set<Pair<Integer, Integer>> host_1_partitions = m_state.getPartitionsAtHost(1);
        assertEquals(host_1_partitions.size(), 3);
        assertTrue(host_1_partitions.contains(new Pair<Integer, Integer>(1, 1)));
        assertTrue(host_1_partitions.contains(new Pair<Integer, Integer>(0, 1)));
        assertTrue(host_1_partitions.contains(new Pair<Integer, Integer>(2, 2)));

        Set<Pair<Integer, Integer>> host_2_partitions = m_state.getPartitionsAtHost(2);
        assertEquals(host_2_partitions.size(), 2);
        assertTrue(host_2_partitions.contains(new Pair<Integer, Integer>(3, 3)));
        assertTrue(host_2_partitions.contains(new Pair<Integer, Integer>(0, 3)));

        Set<Pair<Integer, Integer>> host_3_partitions = m_state.getPartitionsAtHost(2);
        assertEquals(host_3_partitions.size(), 2);
        assertTrue(host_3_partitions.contains(new Pair<Integer, Integer>(3, 3)));
        assertTrue(host_3_partitions.contains(new Pair<Integer, Integer>(0, 3)));
    }

    public void testInconsistentIsReplicated()
    {
        addSiteToTestData(0, 0, 0, 4);
        addBadSiteToTestData(1);
        m_siteInput.resetRowPosition();
        while (m_siteInput.advanceRow())
        {
            try
            {
                // this will add the active row of m_siteInput
                m_state.addHostData(m_siteInput);
            }
            catch (IOException e)
            {
                assertTrue(m_state.getConsistencyResult().
                           contains("but has a savefile which indicates replication at site"));
                return;
            }
        }
        assertTrue(false);
    }

    public void testInconsistentTotalPartitions()
    {
        addSiteToTestData(0, 0, 0, 4);
        addSiteToTestData(1, 0, 1, 5);
        m_siteInput.resetRowPosition();
        while (m_siteInput.advanceRow())
        {
            try
            {
                // this will add the active row of m_siteInput
                m_state.addHostData(m_siteInput);
            }
            catch (IOException e)
            {
                assertTrue(m_state.getConsistencyResult().
                           contains("with an inconsistent number of total partitions"));
                return;
            }
        }
        assertTrue(false);
    }

    public void testMissingPartitions()
    {
        addSiteToTestData(0, 0, 0, 3);
        addSiteToTestData(1, 1, 1, 3);
        m_siteInput.resetRowPosition();
        while (m_siteInput.advanceRow())
        {
            try
            {
                // this will add the active row of m_siteInput
                m_state.addHostData(m_siteInput);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                assertTrue(false);
            }
        }
        assertFalse(m_state.isConsistent());
        assertTrue(m_state.getConsistencyResult().
                   contains("is missing 1 out of 3 total partitions"));
    }

    // Things to consider testing:
    // number of partitions changed between savefile data and current catalog

    /*
     * Test the easiest restore case for partitioning: the table is and was
     * partitioned and the number of partitions hasn't changed.
     */
    public void testEasyRestorePlan()
    {
        int number_of_partitions = 4;

        Set<Integer> partitionsToDistribute = new HashSet<Integer>();
        for (int i = 0; i < number_of_partitions; ++i)
        {
            addSiteToTestData( i, i, i, 4);
            partitionsToDistribute.add(i);
            addSiteInfoToCatalog( i, i, i, true);
        }

        // Add some non-exec sites for more test coverage
        m_voltDB.addSite(CoreUtils.getHSIdFromHostAndSite( 0,number_of_partitions), MailboxType.Initiator);
        m_voltDB.addSite(CoreUtils.getHSIdFromHostAndSite( 1, number_of_partitions + 1), MailboxType.Initiator);
        m_siteInput.resetRowPosition();
        while (m_siteInput.advanceRow())
        {
            try
            {
                // this will add the active row of m_siteInput
                m_state.addHostData(m_siteInput);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                assertTrue(false);
            }
        }

        SynthesizedPlanFragment[] test_plan = checkPlanFragments(partitionsToDistribute);
        for (int i = 0; i < number_of_partitions; i++)
        {
            assertEquals(CoreUtils.getHSIdFromHostAndSite(i,i), test_plan[i].siteId);
        }
    }

    public void testRemovePartitions() {
        int original_partitions = 4;
        int current_partitions = 2;
        Set<Integer> partitionsToDistribute = new HashSet<Integer>();
        for (int i = 0; i < original_partitions; ++i)
        {
            addSiteToTestData(i % current_partitions, i, i, 4);
            partitionsToDistribute.add(i);
            addSiteInfoToCatalog(i, i, i, true);
        }

        // Add some non-exec sites for more test coverage
        m_voltDB.addSite(CoreUtils.getHSIdFromHostAndSite( 0, original_partitions), MailboxType.Initiator);
        m_voltDB.addSite(CoreUtils.getHSIdFromHostAndSite( 1, original_partitions + 1), MailboxType.Initiator);
        m_siteInput.resetRowPosition();
        while (m_siteInput.advanceRow())
        {
            try
            {
                // this will add the active row of m_siteInput
                m_state.addHostData(m_siteInput);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                assertTrue(false);
            }
        }

        checkPlanFragments(partitionsToDistribute);
    }

    public void testAddPartitions()
    {
        int original_partitions = 4;

        Set<Integer> partitionsToDistribute = new HashSet<Integer>();
        for (int i = 0; i < original_partitions; ++i)
        {
            addSiteToTestData(i, i, i, 4);
            partitionsToDistribute.add(i);
            addSiteInfoToCatalog(i, i, i, true);
        }

        // Add some extra partitions and exec sites to reflect new partitioning
        addSiteInfoToCatalog(original_partitions,
                             original_partitions,
                             original_partitions,
                             true);
        addSiteInfoToCatalog(original_partitions + 1,
                             original_partitions + 1,
                             original_partitions + 1,
                             true);
        // Add some non-exec sites for more test coverage
        m_voltDB.addSite(CoreUtils.getHSIdFromHostAndSite( 0, original_partitions + 2), MailboxType.Initiator);
        m_voltDB.addSite(CoreUtils.getHSIdFromHostAndSite( 1, original_partitions + 3), MailboxType.Initiator);
        m_siteInput.resetRowPosition();
        while (m_siteInput.advanceRow())
        {
            try
            {
                // this will add the active row of m_siteInput
                m_state.addHostData(m_siteInput);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                assertTrue(false);
            }
        }

        checkPlanFragments(partitionsToDistribute);
    }

    public void testDuplicateHostsWithPartition()
    {
        int number_of_partitions = 4;

        HashSet<Integer> partitionsToDistribute = new HashSet<Integer>();
        for (int i = 0; i < number_of_partitions; ++i)
        {
            addSiteToTestData( 0, 0, i, 4);
            addSiteToTestData( i, i, i, 4);
            partitionsToDistribute.add(i);
            addSiteInfoToCatalog(i, i, i, true);
        }
        // Add some non-exec sites for more test coverage
        m_voltDB.addSite(CoreUtils.getHSIdFromHostAndSite( 0,number_of_partitions), MailboxType.Initiator);
        m_voltDB.addSite(CoreUtils.getHSIdFromHostAndSite( 1, number_of_partitions + 1), MailboxType.Initiator);
        m_siteInput.resetRowPosition();
        while (m_siteInput.advanceRow())
        {
            try
            {
                // this will add the active row of m_siteInput
                m_state.addHostData(m_siteInput);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                assertTrue(false);
            }
        }

        checkPlanFragments(partitionsToDistribute);
    }

    private void addSiteToTestData(int hostId, int originalHostId, int partitionId,
                                   int totalPartitions)
    {
        m_siteInput.addRow(hostId, "host", originalHostId, "ohost", "cluster", DATABASE_NAME,
                           TABLE_NAME, 0, "FALSE", partitionId, totalPartitions);
    }

    private void addBadSiteToTestData(int siteId)
    {
        m_siteInput.addRow(10, "host", siteId, "ohost", "cluster", DATABASE_NAME,
                           TABLE_NAME, 0, "TRUE", 0, 1);
    }

    private void addSiteInfoToCatalog(int siteId, int hostId, int partitionId,
                                      boolean isExec)
    {
        if (isExec) {
            m_voltDB.addSite(CoreUtils.getHSIdFromHostAndSite(hostId, siteId), partitionId);
        } else {
            m_voltDB.addSite(CoreUtils.getHSIdFromHostAndSite(hostId, siteId), MailboxType.Initiator);
        }
    }

    /*
     * Check that each partition is distributed exactly one time. Since there is
     * one partition per host this is a valid check. In reality a partition
     * should only be distributed by one HOST because multiple sites on that
     * host can parallelize the work of distributing the partition data.
     */
    private SynthesizedPlanFragment[] checkPlanFragments(Set<Integer> partitionsToDistribute)
    {
        Table testTable = m_voltDB.getTable(TABLE_NAME);
        SynthesizedPlanFragment[] plan = m_state.generateRestorePlan(new SnapshotTableInfo(testTable),
                VoltDB.instance().getSiteTrackerForSnapshot());

        HashSet<Integer> partitionsDistributed = new HashSet<Integer>();
        for (int i = 0; i < plan.length - 1; ++i)
        {
            assertEquals(SysProcFragmentId.PF_restoreDistributePartitionedTableAsPartitioned,
                         plan[i].fragmentId);
            assertTrue(plan[i].siteId == 0 || plan[i].siteId == CoreUtils.getHSIdFromHostAndSite(i,i));
            assertFalse(plan[i].multipartition);
            assertEquals(TABLE_NAME, plan[i].parameters.toArray()[0]);
            for (Integer partition : ((int[])plan[i].parameters.toArray()[2])) {
                boolean inserted = partitionsDistributed.add(partition);
                if (!inserted) {
                    fail("Plan inserts partition " + partition + " more then once");
                }
            }
        }
        assertTrue(partitionsDistributed.containsAll(partitionsToDistribute));
        assertEquals(SysProcFragmentId.
                     PF_restoreReceiveResultTables,
                     plan[plan.length - 1].fragmentId);
        assertFalse(plan[plan.length - 1].multipartition);
        assertEquals(m_state.getRootDependencyId(),
                     plan[plan.length - 1].parameters.toArray()[0]);

        return plan;
    }

    private PartitionedTableSaveFileState m_state;
    private VoltTable m_siteInput;
    private MockVoltDB m_voltDB;
}
