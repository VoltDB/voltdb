/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
    // @LastCommittedTransaction -- UNUSED
    public static final int PF_lastCommittedScan = 1;
    public static final int PF_lastCommittedResults = 2;

    // @UpdateLogging -- UNUSED
    public static final int PF_updateLoggers = 3;

    // @Statistics -- UNUSED
    public static final int PF_starvationData = 4;
    public static final int PF_starvationDataAggregator = 5;
    public static final int PF_tableData = 6;
    public static final int PF_tableAggregator = 7;
    public static final int PF_indexData = 8;
    public static final int PF_indexAggregator = 9;
    public static final int PF_nodeMemory = 10;
    public static final int PF_nodeMemoryAggregator = 11;
    public static final int PF_procedureData = 13;
    public static final int PF_procedureAggregator = 14;
    public static final int PF_initiatorData = 15;
    public static final int PF_initiatorAggregator = 16;
    public static final int PF_partitionCount = 17;
    public static final int PF_ioData = 18;
    public static final int PF_ioDataAggregator = 19;
    public static final int PF_liveClientData = 20;
    public static final int PF_liveClientDataAggregator = 21;
    public static final int PF_plannerData = 22;
    public static final int PF_plannerAggregator = 23;

    // @Shutdown
    public static final int PF_shutdownSync = 26;
    public static final int PF_shutdownSyncDone = 27;
    public static final int PF_shutdownCommand = 28;
    public static final int PF_procedureDone = 29;

    // @AdHoc -- UNUSED
    public static final int PF_runAdHocFragment = 31;

    // @SnapshotSave
    /*
     * Create and distribute tasks and targets to each EE
     */
    public static final int PF_createSnapshotTargets = 42;
    /*
     * Confirm the targets were successfully created
     */
    public static final int PF_createSnapshotTargetsResults = 43;

    public static boolean isSnapshotSaveFragment(byte[] planHash) {
        long fragId = VoltSystemProcedure.hashToFragId(planHash);
        return (fragId == PF_createSnapshotTargets);
    }

    public static boolean isFirstSnapshotFragment(byte[] planHash) {
        return isSnapshotSaveFragment(planHash);
    }

    // @LoadMultipartitionTable
    public static final int PF_distribute = 50;
    public static final int PF_aggregate = 51;

    // @SnapshotRestore
    public static final int PF_restoreScan = 60;
    public static final int PF_restoreScanResults = 61;
    /*
     * Plan fragments for retrieving the digests
     * for the snapshot visible at every node. Can't be combined
     * with the other scan because only one result table can be returned
     * by a plan fragment.
     */
    public static final int PF_restoreDigestScan = 62;
    public static final int PF_restoreDigestScanResults = 63;
    /*
     * Plan fragments for distributing the full set of export sequence numbers
     * to every partition where the relevant ones can be selected
     * and forwarded to the EE. Also distributes the txnId of the snapshot
     * which is used to truncate export data on disk from after the snapshot
     */
    public static final int PF_restoreDistributeExportAndPartitionSequenceNumbers = 64;
    public static final int PF_restoreDistributeExportAndPartitionSequenceNumbersResults = 65;
    /*
     * Plan fragment for entering an asynchronous run loop that generates a mailbox
     * and sends the generated mailbox id to the MP coordinator which then propagates the info.
     * The MP coordinator then sends plan fragments through this async mailbox,
     * bypassing the master/slave replication system that doesn't understand plan fragments
     * directed at individual executions sites.
     */
    public static final int PF_restoreAsyncRunLoop = 66;
    public static final int PF_restoreAsyncRunLoopResults = 67;
    public static final int PF_restoreLoadTable = 70; // called by 4 distribute cases, to load received table
    public static final int PF_restoreReceiveResultTables = 71; // union received result tables
    public static final int PF_restoreLoadReplicatedTable = 72; // special case for replicated-to-replicated
    public static final int PF_restoreDistributeReplicatedTableAsReplicated = 73; // replicated to replicated
    public static final int PF_restoreDistributePartitionedTableAsPartitioned = 74; // partitioned to partitioned
    public static final int PF_restoreDistributePartitionedTableAsReplicated = 75; // partitioned to replicated
    public static final int PF_restoreDistributeReplicatedTableAsPartitioned = 76; // replicated to replicated
    /*
     * Plan fragments for retrieving the hashinator data
     * for the snapshot visible at every node. Can't be combined
     * with the other scan because only one result table can be returned
     * by a plan fragment.
     */
    public static final int PF_restoreHashinatorScan = 77;
    public static final int PF_restoreHashinatorScanResults = 78;
    /*
     * Plan fragments for retrieving the hashinator data
     * for the snapshot visible at every node. Can't be combined
     * with the other scan because only one result table can be returned
     * by a plan fragment.
     */
    public static final int PF_restoreDistributeHashinator = 79;
    public static final int PF_restoreDistributeHashinatorResults = 80;

    // @StartSampler -- UNUSED
    public static final int PF_startSampler = 90;

    // @Quiesce
    public static final int PF_quiesce_sites = 100;
    public static final int PF_quiesce_processed_sites = 101;

    // @SnapshotScan -- UNUSED
    public static final int PF_snapshotDigestScan = 124;
    public static final int PF_snapshotDigestScanResults = 125;
    public static final int PF_snapshotScan = 120;
    public static final int PF_snapshotScanResults = 121;
    public static final int PF_hostDiskFreeScan = 122;
    public static final int PF_hostDiskFreeScanResults = 123;

    // @SnapshotScan
    public static final int PF_snapshotDelete = 130;
    public static final int PF_snapshotDeleteResults = 131;

    // @InstanceId -- UNUSED
    public static final int PF_retrieveInstanceId = 160;
    public static final int PF_retrieveInstanceIdAggregator = 161;
    public static final int PF_setInstanceId = 162;
    public static final int PF_setInstanceIdAggregator = 163;

    // @Rejoin -- UNUSED
    public static final int PF_rejoinBlock = 170;
    public static final int PF_rejoinPrepare = 171;
    public static final int PF_rejoinCommit = 172;
    public static final int PF_rejoinRollback = 173;
    public static final int PF_rejoinAggregate = 174;

    // @SystemInformation
    public static final int PF_systemInformationDeployment = 190;
    public static final int PF_systemInformationAggregate = 191;
    public static final int PF_systemInformationOverview = 192;
    public static final int PF_systemInformationOverviewAggregate = 193;

    // @Update application catalog
    public static final int PF_updateCatalogPrecheckAndSync = 210;
    public static final int PF_updateCatalogPrecheckAndSyncAggregate = 211;
    public static final int PF_updateCatalog = 212;
    public static final int PF_updateCatalogAggregate = 213;

    public static boolean isCatalogUpdateFragment(byte[] planHash) {
        long fragId = VoltSystemProcedure.hashToFragId(planHash);

        return (fragId == PF_updateCatalog || fragId == PF_updateCatalogAggregate ||
                fragId == PF_updateCatalogPrecheckAndSync || fragId == PF_updateCatalogPrecheckAndSyncAggregate);
    }

    // @BalancePartitions
    public static final int PF_prepBalancePartitions = 228;
    public static final int PF_prepBalancePartitionsAggregate = 229;
    public static final int PF_balancePartitions = 230;
    public static final int PF_balancePartitionsAggregate = 231;
    public static final int PF_balancePartitionsData = 232;
    public static final int PF_balancePartitionsClearIndex = 233;
    public static final int PF_balancePartitionsClearIndexAggregate = 234;

    // @ValidatePartitioning
    public static final int PF_validatePartitioning = 240;
    public static final int PF_validatePartitioningResults = 241;

    // @MatchesHashinator
    public static final int PF_matchesHashinator = 250;
    public static final int PF_matchesHashinatorResults = 251;

    // @ApplyBinaryLog
    public static final int PF_applyBinaryLog = 260;
    public static final int PF_applyBinaryLogAggregate = 261;

    // @LoadVoltTable
    public static final int PF_loadVoltTable = 270;
    public static final int PF_loadVoltTableAggregate = 271;

    // @ResetDR
    public static final int PF_resetDR = 280;
    public static final int PF_resetDRAggregate = 281;

    // @ResetDRSingle
    public static final int PF_preResetDRSingle = 282;
    public static final int PF_preResetDRSingleAggregate = 283;
    public static final int PF_postResetDRSingle = 284;
    public static final int PF_postResetDRSingleAggregate = 285;

    // @DropDRSelf
    public static final int PF_DropDRSelf = 286;
    public static final int PF_DropDRSelfAggregate = 287;

    // @ExecuteTask
    public static final int PF_executeTask = 290;
    public static final int PF_executeTaskAggregate = 291;

    // @UpdatedSettings
    public static final int PF_updateSettingsBarrier = 300;
    public static final int PF_updateSettingsBarrierAggregate = 301;
    public static final int PF_updateSettings = 302;
    public static final int PF_updateSettingsAggregate = 303;

    // @PrepareShutdown
    public static final int PF_prepareShutdown = 310;
    public static final int PF_prepareShutdownAggregate = 311;

    // @SwapTables
    public static final int PF_swapTables = 320;
    public static final int PF_swapTablesAggregate = 321;

    // @PingPartitions
    public static final int PF_pingPartitions = 330;
    public static final int PF_pingPartitionsAggregate = 331;
    public static final int PF_enableScoreboard = 332;
    public static final int PF_enableScoreboardAggregate = 333;

    // Pause/resume materialized views
    public static final int PF_setViewEnabled = 340;

    // @ExportControl
    public static final int PF_exportControl = 350;
    public static final int PF_exportControlAggregate = 351;

    // @CancelShutdown
    public static final int PF_cancelShutdown = 360;
    public static final int PF_cancelShutdownAggregate = 361;

    // @MigrateRowsAcked_MP
    public static final int PF_migrateRows = 370;
    public static final int PF_migrateRowsAggregate = 371;

    // @ElasticRemove
    public static final int PF_elasticRemoveSites = 380;
    public static final int PF_elasticRemoveSitesAggregate = 381;
    public static final int PF_elasticRemoveResume = 382;
    public static final int PF_elasticRemoveResumeAggregate = 383;

    // @License
    public static final int PF_systemInformationLicense = 390;

    // @CollectDrTrackers
    public static final int PF_collectDrTrackers = 400;
    public static final int PF_collectDrTrackersAgg = 401;

    // @StopReplicas
    public static final int PF_stopReplicas = 402;
    public static final int PF_stopReplicasAggregate = 403;

    // @TopicControl
    public static final int PF_topicControl = 410;
    public static final int PF_topicControlAggregate = 411;

    // @SetReplicableTables
    public static final int PF_setReplicableTables = 412;
    public static final int PF_setReplicableTablesAggregate = 413;

    public static boolean isEnableScoreboardFragment(byte[] planHash) {
        long fragId = VoltSystemProcedure.hashToFragId(planHash);

        return (fragId == PF_enableScoreboard);
    }
}
