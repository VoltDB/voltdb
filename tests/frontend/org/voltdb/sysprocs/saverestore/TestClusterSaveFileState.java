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
import java.util.Set;

import org.voltcore.utils.Pair;
import org.voltdb.VoltTable;

import junit.framework.TestCase;


public class TestClusterSaveFileState extends TestCase
{
    private static String CLUSTER_NAME = "test_cluster";
    private static String DATABASE_NAME = "test_database";
    private static String REPL_TABLE_NAME_1 = "test_table_1";
    private static String REPL_TABLE_NAME_2 = "test_table_2";
    private static String PART_TABLE_NAME_1 = "test_table_3";
    private static String PART_TABLE_NAME_2 = "test_table_4";
    private static int NUMBER_OF_SITES_PER_HOST = 4;
    private static int NUMBER_OF_HOSTS = 4;
    private static int NUMBER_OF_SITES = NUMBER_OF_SITES_PER_HOST * NUMBER_OF_HOSTS;

    @Override
    public void setUp()
    {
        m_siteInput =
            ClusterSaveFileState.constructEmptySaveFileStateVoltTable();
    }

    // XXX other things to check
    // duplicate siteId/table name entry

    public void testBasicOperation()
    {
        for (int i = 0; i < NUMBER_OF_HOSTS; ++i)
        {
            addReplicatedTableToTestData(i, REPL_TABLE_NAME_1);
            addReplicatedTableToTestData(i, REPL_TABLE_NAME_2);
            for (int ii = 0; ii < NUMBER_OF_SITES_PER_HOST; ++ii) {
                addPartitionToTestData(i, PART_TABLE_NAME_1, i, ii + (NUMBER_OF_SITES_PER_HOST * i), NUMBER_OF_SITES);
                addPartitionToTestData(i, PART_TABLE_NAME_2, i,  ii + (NUMBER_OF_SITES_PER_HOST * i), NUMBER_OF_SITES);
            }
        }

        ClusterSaveFileState state = null;
        try
        {
            state = new ClusterSaveFileState(m_siteInput);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            assertTrue(false);
        }
        Set<String> table_names = state.getSavedTableNames();
        assertTrue(table_names.contains(REPL_TABLE_NAME_1));
        assertTrue(table_names.contains(REPL_TABLE_NAME_2));
        assertTrue(table_names.contains(PART_TABLE_NAME_1));
        assertTrue(table_names.contains(PART_TABLE_NAME_2));
        checkReplicatedTableState(REPL_TABLE_NAME_1, state);
        checkReplicatedTableState(REPL_TABLE_NAME_2, state);
        checkPartitionedTableState(PART_TABLE_NAME_1, state);
        checkPartitionedTableState(PART_TABLE_NAME_2, state);
    }

    public void testInconsistentClusterName()
    {
        addReplicatedTableToTestData(0, REPL_TABLE_NAME_1);
        addBadSiteToTestData("BAD_CLUSTER", DATABASE_NAME);
        try
        {
            @SuppressWarnings("unused")
            ClusterSaveFileState state =
                new ClusterSaveFileState(m_siteInput);
        }
        catch (IOException e)
        {
            assertTrue(true);
            return;
        }
        assertTrue(false);
    }

    public void testInconsistentDatabaseName()
    {
        addReplicatedTableToTestData(0, REPL_TABLE_NAME_1);
        addBadSiteToTestData(CLUSTER_NAME, "BAD_DATABASE");
        try
        {
            @SuppressWarnings("unused")
            ClusterSaveFileState state =
                new ClusterSaveFileState(m_siteInput);
        }
        catch (IOException e)
        {
            assertTrue(true);
            return;
        }
        assertTrue(false);
    }

    public void testInconsistentReplicatedTable()
    {
        addReplicatedTableToTestData(0, REPL_TABLE_NAME_1);
        addPartitionToTestData(1, REPL_TABLE_NAME_1, 0, 1, 2);
        try
        {
            @SuppressWarnings("unused")
            ClusterSaveFileState state =
                new ClusterSaveFileState(m_siteInput);
        }
        catch (IOException e)
        {
            assertTrue(true);
            return;
        }
        assertTrue(false);
    }

    public void testReplicatedTableWithDifferentTxnId()
    {
        addReplicatedTableToTestData(0, REPL_TABLE_NAME_1);
        addReplicatedTableWithTxnId(0, REPL_TABLE_NAME_1, 1);
        try
        {
            @SuppressWarnings("unused")
            ClusterSaveFileState state =
                new ClusterSaveFileState(m_siteInput);
        }
        catch (IOException e)
        {
            assertTrue(true);
            return;
        }
        assertTrue(false);
    }

    public void testMissingPartitionTable()
    {
        addPartitionToTestData(0, PART_TABLE_NAME_1, 0, 0, 3);
        addPartitionToTestData(1, PART_TABLE_NAME_1, 1, 1, 3);
        try
        {
            @SuppressWarnings("unused")
            ClusterSaveFileState state =
                new ClusterSaveFileState(m_siteInput);
        }
        catch (IOException e)
        {
            assertTrue(true);
            return;
        }
        assertTrue(false);
    }

    private void checkReplicatedTableState(String tableName,
                                           ClusterSaveFileState state)
    {
        TableSaveFileState table_state = state.getTableState(tableName);
        assertNotNull(table_state);
        assertTrue(table_state instanceof ReplicatedTableSaveFileState);
        ReplicatedTableSaveFileState repl_table_state =
            (ReplicatedTableSaveFileState) table_state;
        for (int i = 0; i < NUMBER_OF_HOSTS; ++i)
        {
            assert(repl_table_state.getHostsWithThisTable().contains(i));
        }
    }

    private void checkPartitionedTableState(String tableName,
                                            ClusterSaveFileState state)
    {
        TableSaveFileState table_state = state.getTableState(tableName);
        assertNotNull(table_state);
        assertTrue(table_state instanceof PartitionedTableSaveFileState);
        PartitionedTableSaveFileState part_table_state =
            (PartitionedTableSaveFileState) table_state;
        for (int ii = 0; ii < NUMBER_OF_HOSTS; ii++) {
            Set<Pair<Integer, Integer>> partitionsAtHost = part_table_state.getPartitionsAtHost(ii);
            for (int i = 0; i < NUMBER_OF_SITES_PER_HOST; ++i)
            {
                assert(part_table_state.getPartitionSet().contains(i + (ii * NUMBER_OF_HOSTS)));
                assert(partitionsAtHost.contains(new Pair<Integer, Integer>(i + (ii * NUMBER_OF_HOSTS), ii)));
            }
        }
    }

    private void addPartitionToTestData(int hostId, String tableName,
                                        int originalHostId,
                                        int originalPartitionId, int totalPartitions)
    {
        m_siteInput.addRow(hostId, "host", originalHostId, "ohost", CLUSTER_NAME, DATABASE_NAME,
                           tableName, 0, "FALSE", originalPartitionId, totalPartitions);
    }

    private void addReplicatedTableToTestData(int currentHostId, String tableName)
    {
        m_siteInput.addRow(currentHostId, "host", currentHostId, "ohost", CLUSTER_NAME, DATABASE_NAME,
                           tableName, 0, "TRUE", 0, 1);
    }

    private void addReplicatedTableWithTxnId(int currentHostId, String tableName, long txnId)
    {
        m_siteInput.addRow(currentHostId, "host", currentHostId, "ohost", CLUSTER_NAME, DATABASE_NAME,
                           tableName, txnId, "TRUE", 0, 1);
    }

    private void addBadSiteToTestData(String clusterName, String databaseName)
    {
        m_siteInput.addRow(10, "host", 10, "ohost", clusterName, databaseName, "dontcare",
                           0, "FALSE", 1, 2);
    }

    private VoltTable m_siteInput;
}
