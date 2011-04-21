/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs;

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

    // @Shutdown
    public static final long PF_shutdownCommand = 28;
    public static final long PF_procedureDone = 29;

    // @AdHoc
    public static final long PF_runAdHocFragment = 31;

    // @SnapshotSave
    /*
     * Once per host confirm the file is accessible
     */
    public static final long PF_saveTest = 40;
    /*
     * Agg test results
     */
    public static final long PF_saveTestResults = 41;
    /*
     * Create and distribute tasks and targets to each EE
     */
    public static final long PF_createSnapshotTargets = 42;
    /*
     * Confirm the targets were successfully created
     */
    public static final long PF_createSnapshotTargetsResults = 43;
    /*
     * Quiesce the export data as part of the snapshot
     */
    public static final long PF_snapshotSaveQuiesce = 44;
    /*
     * Aggregate the results of snapshot quiesce
     */
    public static final long PF_snapshotSaveQuiesceResults = 45;

    // @LoadMultipartitionTable
    public static final long PF_distribute = 50;
    public static final long PF_aggregate = 51;

    // @SnapshotRestore
    public static final long PF_restoreScan = 60;
    public static final long PF_restoreScanResults = 61;
    public static final long PF_restoreLoadReplicatedTable = 62;
    public static final long PF_restoreLoadReplicatedTableResults = 63;
    public static final long PF_restoreDistributeReplicatedTable = 64;
    public static final long PF_restoreDistributePartitionedTable = 65;
    public static final long PF_restoreDistributePartitionedTableResults = 66;
    public static final long PF_restoreSendReplicatedTable = 67;
    public static final long PF_restoreSendReplicatedTableResults = 68;
    public static final long PF_restoreSendPartitionedTable = 69;
    public static final long PF_restoreSendPartitionedTableResults = 70;
    public static final long PF_restoreDigestScan = 71;
    public static final long PF_restoreDigestScanResults = 72;
    public static final long PF_restoreDistributeExportSequenceNumbers = 73;
    public static final long PF_restoreDistributeExportSequenceNumbersResults = 74;

    // @StartSampler
    public static final long PF_startSampler = 80;

    // @Quiesce
    public static final long PF_quiesce_sites = 100;
    public static final long PF_quiesce_processed_sites = 101;

    // @SnapshotStatus
    public static final long PF_scanSnapshotRegistries = 110;
    public static final long PF_scanSnapshotRegistriesResults = 111;

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
}
