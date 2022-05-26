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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.voltcore.utils.CoreUtils;
import org.voltdb.FlakyTestRule;
import org.voltdb.FlakyTestRule.Flaky;
import org.voltdb.MockVoltDB;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.VoltTable;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.catalog.Table;
import org.voltdb.sysprocs.SysProcFragmentId;


public class TestReplicatedTableSaveFileState
{
    @Rule
    public FlakyTestRule ftRule = new FlakyTestRule();

    private static final String TABLE_NAME = "test_table";
    private static final String DATABASE_NAME = "database";

    @Before
    public void setUp()
    {
        m_state = new ReplicatedTableSaveFileState(TABLE_NAME, 0);
        m_siteInput =
            ClusterSaveFileState.constructEmptySaveFileStateVoltTable();
    }

    @Test
    public void testLoadOperation()
    {
        assertEquals(m_state.getTableName(), TABLE_NAME);

        addHostToTestData(0);
        addHostToTestData(1);
        addHostToTestData(3);
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
        assertTrue(m_state.isConsistent());
        assertTrue(m_state.getConsistencyResult().
                   contains("has consistent savefile state"));
        Set<Integer> sites = m_state.getHostsWithThisTable();
        assertTrue(sites.contains(0));
        assertTrue(sites.contains(1));
        assertFalse(sites.contains(2));
        assertTrue(sites.contains(3));
    }

    @Test
    public void testInconsistentIsReplicated()
    {
        addHostToTestData(0);
        addBadHostToTestData(1);
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
                           contains("but has a savefile which indicates partitioning at site"));
                return;
            }
        }
        assertTrue(false);
    }

    // Things that should get added:
    // Add some non-exec sites
    //

    /*
     * Test the easiest possible restore plan: table is replicated before and
     * after save/restore, and every site has a copy of the table
     */
    @Test
    @Flaky(description="TestReplicatedTableSaveFileState.testEasyRestorePlan")
    public void testEasyRestorePlan() throws Exception
    {
        MockVoltDB catalog_creator =
            new MockVoltDB();
        VoltDB.replaceVoltDBInstanceForTest(catalog_creator);
        catalog_creator.addTable(TABLE_NAME, true);

        int number_of_sites = 4;

        for (int i = 0; i < number_of_sites; ++i)
        {
            addHostToTestData(i);
            catalog_creator.addSite(CoreUtils.getHSIdFromHostAndSite( i, i), i);
        }
        // Add some non-exec sites for more test coverage
        catalog_creator.addSite(CoreUtils.getHSIdFromHostAndSite( 0, number_of_sites), MailboxType.Initiator);
        catalog_creator.addSite(CoreUtils.getHSIdFromHostAndSite( 1, number_of_sites + 1), MailboxType.Initiator);
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

        SynthesizedPlanFragment[] test_plan = generateRestorePlan(catalog_creator);
        assertEquals(test_plan.length, number_of_sites + 1);
        for (int i = 0; i < number_of_sites - 1; ++i)
        {
            assertEquals(test_plan[i].fragmentId,
                         SysProcFragmentId.PF_restoreLoadReplicatedTable);
            assertFalse(test_plan[i].multipartition);
            assertEquals((int)test_plan[i].siteId, i);
            assertEquals(test_plan[i].parameters.toArray()[0], TABLE_NAME);
        }
        assertEquals(test_plan[number_of_sites].fragmentId,
                     SysProcFragmentId.PF_restoreReceiveResultTables);
        assertFalse(test_plan[number_of_sites].multipartition);
        assertEquals(test_plan[number_of_sites].parameters.toArray()[0],
                     m_state.getRootDependencyId());
        catalog_creator.shutdown(null);
    }

    /*
     * Test the restore plan when one of the sites doesn't have access to
     * a copy of the table
     */
    @Test
    public void testSiteMissingTableRestorePlan() throws Exception
    {
        MockVoltDB catalog_creator = new MockVoltDB();
        VoltDB.replaceVoltDBInstanceForTest(catalog_creator);
        catalog_creator.addTable(TABLE_NAME, true);

        int number_of_sites = 4;

        for (int i = 0; i < number_of_sites - 1; ++i)
        {
            addHostToTestData(i);
            catalog_creator.addSite(CoreUtils.getHSIdFromHostAndSite( i, i), i);
        }
        catalog_creator.addSite(CoreUtils.getHSIdFromHostAndSite( number_of_sites - 1, number_of_sites - 1),
                                number_of_sites - 1);
        // Add some non-exec sites for more test coverage
        catalog_creator.addSite(CoreUtils.getHSIdFromHostAndSite( 0, number_of_sites), MailboxType.Initiator);
        catalog_creator.addSite(CoreUtils.getHSIdFromHostAndSite( 1, number_of_sites + 1), MailboxType.Initiator);

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

        SynthesizedPlanFragment[] test_plan = generateRestorePlan(catalog_creator);
        assertEquals(test_plan.length, number_of_sites + 1);
        for (int i = 0; i < number_of_sites - 1; ++i)
        {
            assertEquals(test_plan[i].fragmentId,
                         SysProcFragmentId.PF_restoreLoadReplicatedTable);
            assertFalse(test_plan[i].multipartition);
            assertEquals(test_plan[i].siteId, CoreUtils.getHSIdFromHostAndSite( i, i));
            assertEquals(test_plan[i].parameters.toArray()[0], TABLE_NAME);
        }
        assertEquals(test_plan[number_of_sites - 1].fragmentId,
                     SysProcFragmentId.PF_restoreDistributeReplicatedTableAsReplicated);
        assertEquals(test_plan[number_of_sites - 1].siteId, 0);
        assertFalse(test_plan[number_of_sites - 1].multipartition);
        assertEquals(test_plan[number_of_sites - 1].parameters.toArray()[0],
                     TABLE_NAME);
        assertEquals(3, test_plan[number_of_sites - 1].parameters.toArray()[1]);

        assertEquals(test_plan[number_of_sites].fragmentId,
                     SysProcFragmentId.PF_restoreReceiveResultTables);
        assertFalse(test_plan[number_of_sites].multipartition);
        assertEquals(test_plan[number_of_sites].parameters.toArray()[0],
                     m_state.getRootDependencyId());
        catalog_creator.shutdown(null);
    }

    private void addHostToTestData(int hostId)
    {
        m_siteInput.addRow(hostId, "host", hostId, "ohost", "cluster", DATABASE_NAME,
                           TABLE_NAME, 0, "TRUE", 0, 1);
    }

    private void addBadHostToTestData(int hostId)
    {
        m_siteInput.addRow(hostId, "host", hostId, "ohost", "cluster", DATABASE_NAME,
                           TABLE_NAME, 0, "FALSE", 0, 2);
    }

    private SynthesizedPlanFragment[] generateRestorePlan(MockVoltDB mvdb) {
        Table testTable = mvdb.getTable(TABLE_NAME);

        return m_state.generateRestorePlan(new SnapshotTableInfo(testTable),
                VoltDB.instance().getSiteTrackerForSnapshot());
    }

    private ReplicatedTableSaveFileState m_state;
    private VoltTable m_siteInput;
}
