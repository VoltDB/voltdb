/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs;

import org.voltdb.VoltSystemProcedure;

public class SysProcFragmentId
{
    // @LastCommittedTransaction
    public static final long PF_lastCommittedScan = 1;
    public static final long PF_lastCommittedResults = 2;

    // @UpdateLogging
    public static final long PF_updateLoggers = 3;

    // @Statistics
    public static final long PF_starvationData = 4;
    public static final long PF_starvationDataAggregator = 5;
    public static final long PF_tableData = 6;
    public static final long PF_tableAggregator = 7;
    public static final long PF_indexData = 8;
    public static final long PF_indexAggregator = 9;
    public static final long PF_nodeMemory = 10;
    public static final long PF_nodeMemoryAggregator = 11;
    public static final long PF_procedureData = 13;
    public static final long PF_procedureAggregator = 14;
    public static final long PF_initiatorData = 15;
    public static final long PF_initiatorAggregator = 16;
    public static final long PF_partitionCount = 17;
    public static final long PF_ioData = 18;
    public static final long PF_ioDataAggregator = 19;
    public static final long PF_liveClientData = 20;
    public static final long PF_liveClientDataAggregator = 21;
    public static final long PF_plannerData = 22;
    public static final long PF_plannerAggregator = 23;

    // @Shutdown
    public static final long PF_shutdownSync = 26;
    public static final long PF_shutdownSyncDone = 27;
    public static final long PF_shutdownCommand = 28;
    public static final long PF_procedureDone = 29;

    // @AdHoc
    public static final long PF_runAdHocFragment = 31;

    // @SnapshotSave
    /*
     * Create and distribute tasks and targets to each EE
     */
    public static final long PF_createSnapshotTargets = 42;
    /*
     * Confirm the targets were successfully created
     */
    public static final long PF_createSnapshotTargetsResults = 43;

    public static boolean isSnapshotSaveFragment(byte[] planHash) {
        long fragId = VoltSystemProcedure.hashToFragId(planHash);
        return (fragId == PF_createSnapshotTargets);
    }

    public static boolean isFirstSnapshotFragment(byte[] planHash) {
        return isSnapshotSaveFragment(planHash);
    }

    //This method exists because there is no procedure name in fragment task message
    // for sysprocs and we cant distinguish if this needs to be replayed or not.
    public static boolean isDurableFragment(byte[] planHash) {
        long fragId = VoltSystemProcedure.hashToFragId(planHash);
        return (fragId == PF_prepBalancePartitions  ||
                fragId == PF_balancePartitions ||
                fragId == PF_balancePartitionsData ||
                fragId == PF_balancePartitionsClearIndex ||
                fragId == PF_distribute);
    }

    // @LoadMultipartitionTable
    public static final long PF_distribute = 50;
    public static final long PF_aggregate = 51;

    // @SnapshotRestore
    public static final long PF_restoreScan = 60;
    public static final long PF_restoreScanResults = 61;
    public static final long PF_restoreDigestScan = 62;
    public static final long PF_restoreDigestScanResults = 63;
    public static final long PF_restoreDistributeExportAndPartitionSequenceNumbers = 64;
    public static final long PF_restoreDistributeExportAndPartitionSequenceNumbersResults = 65;
    public static final long PF_restoreAsyncRunLoop = 66;
    public static final long PF_restoreAsyncRunLoopResults = 67;
    public static final long PF_restoreLoadTable = 70;                                  // called by 4 distribute cases, to load received table
    public static final long PF_restoreReceiveResultTables = 71;                        // union received result tables
    public static final long PF_restoreLoadReplicatedTable = 72;                        // special case for replicated-to-replicated
    public static final long PF_restoreDistributeReplicatedTableAsReplicated = 73;      // replicated to replicated
    public static final long PF_restoreDistributePartitionedTableAsPartitioned = 74;    // partitioned to partitioned
    public static final long PF_restoreDistributePartitionedTableAsReplicated = 75;     // partitioned to replicated
    public static final long PF_restoreDistributeReplicatedTableAsPartitioned = 76;     // replicated to replicated
    public static final long PF_restoreHashinatorScan = 77;
    public static final long PF_restoreHashinatorScanResults = 78;
    public static final long PF_restoreDistributeHashinator = 79;
    public static final long PF_restoreDistributeHashinatorResults = 80;

    // @StartSampler
    public static final long PF_startSampler = 90;

    // @Quiesce
    public static final long PF_quiesce_sites = 100;
    public static final long PF_quiesce_processed_sites = 101;

    // @SnapshotScan
    public static final long PF_snapshotDigestScan = 124;
    public static final long PF_snapshotDigestScanResults = 125;
    public static final long PF_snapshotScan = 120;
    public static final long PF_snapshotScanResults = 121;
    public static final long PF_hostDiskFreeScan = 122;
    public static final long PF_hostDiskFreeScanResults = 123;

    // @SnapshotScan
    public static final long PF_snapshotDelete = 130;
    public static final long PF_snapshotDeleteResults = 131;

    // @InstanceId
    public static final long PF_retrieveInstanceId = 160;
    public static final long PF_retrieveInstanceIdAggregator = 161;
    public static final long PF_setInstanceId = 162;
    public static final long PF_setInstanceIdAggregator = 163;

    // @Rejoin
    public static final long PF_rejoinBlock = 170;
    public static final long PF_rejoinPrepare = 171;
    public static final long PF_rejoinCommit = 172;
    public static final long PF_rejoinRollback = 173;
    public static final long PF_rejoinAggregate = 174;

    // @SystemInformation
    public static final long PF_systemInformationDeployment = 190;
    public static final long PF_systemInformationAggregate = 191;
    public static final long PF_systemInformationOverview = 192;
    public static final long PF_systemInformationOverviewAggregate = 193;

    // @Update application catalog
    public static final long PF_updateCatalogPrecheckAndSync = 210;
    public static final long PF_updateCatalogPrecheckAndSyncAggregate = 211;
    public static final long PF_updateCatalog = 212;
    public static final long PF_updateCatalogAggregate = 213;

    public static boolean isCatalogUpdateFragment(byte[] planHash) {
        long fragId = VoltSystemProcedure.hashToFragId(planHash);

        return (fragId == PF_updateCatalog || fragId == PF_updateCatalogAggregate ||
                fragId == PF_updateCatalogPrecheckAndSync || fragId == PF_updateCatalogPrecheckAndSyncAggregate);
    }

    // @BalancePartitions
    public static final long PF_prepBalancePartitions = 228;
    public static final long PF_prepBalancePartitionsAggregate = 229;
    public static final long PF_balancePartitions = 230;
    public static final long PF_balancePartitionsAggregate = 231;
    public static final long PF_balancePartitionsData = 232;
    public static final long PF_balancePartitionsClearIndex = 233;
    public static final long PF_balancePartitionsClearIndexAggregate = 234;

    public static final long PF_validatePartitioning = 240;
    public static final long PF_validatePartitioningResults = 241;

    public static final long PF_matchesHashinator = 250;
    public static final long PF_matchesHashinatorResults = 251;
}
