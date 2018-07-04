/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.InstanceId;
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil.StringCallback;
import org.voltdb.ClientResponseImpl;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.DependencyPair;
import org.voltdb.DeprecatedProcedureAPIAccess;
import org.voltdb.ExtensibleSnapshotDigestData;
import org.voltdb.ParameterSet;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.StartAction;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TableCompressor;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.VoltZK;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.UndoAction;
import org.voltdb.export.ExportManager;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.sysprocs.SnapshotRestoreResultSet.RestoreResultKey;
import org.voltdb.sysprocs.saverestore.ClusterSaveFileState;
import org.voltdb.sysprocs.saverestore.DuplicateRowHandler;
import org.voltdb.sysprocs.saverestore.HashinatorSnapshotData;
import org.voltdb.sysprocs.saverestore.SavedTableConverter;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.TableSaveFile;
import org.voltdb.sysprocs.saverestore.TableSaveFileState;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.VoltFile;
import org.voltdb.utils.VoltTableUtil;

import com.google_voltpatches.common.primitives.Longs;

public class SnapshotRestore extends VoltSystemProcedure {
    private static final VoltLogger TRACE_LOG = new VoltLogger(SnapshotRestore.class.getName());

    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");
    private static final VoltLogger CONSOLE_LOG = new VoltLogger("CONSOLE");
    private static final long LOG_SUPPRESSION_INTERVAL_SECONDS = 60;
    /*
     * Data is being loaded as a partitioned table, log all duplicates
     */
    public static final int K_CHECK_UNIQUE_VIOLATIONS_PARTITIONED = 0;

    /*
     * Data is being loaded as a replicated table, only log duplicates at 1 node/site
     */
    public static final int K_CHECK_UNIQUE_VIOLATIONS_REPLICATED = 1;

    private static final int DEP_restoreScan = (int)
            SysProcFragmentId.PF_restoreScan | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_restoreScanResults = (int)
            SysProcFragmentId.PF_restoreScanResults;

    /*
     * Plan fragments for retrieving the digests
     * for the snapshot visible at every node. Can't be combined
     * with the other scan because only one result table can be returned
     * by a plan fragment.
     */
    private static final int DEP_restoreDigestScan = (int)
            SysProcFragmentId.PF_restoreDigestScan | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_restoreDigestScanResults = (int)
            SysProcFragmentId.PF_restoreDigestScanResults;

    /*
     * Plan fragments for retrieving the hashinator data
     * for the snapshot visible at every node. Can't be combined
     * with the other scan because only one result table can be returned
     * by a plan fragment.
     */
    private static final int DEP_restoreHashinatorScan = (int)
            SysProcFragmentId.PF_restoreHashinatorScan | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_restoreHashinatorScanResults = (int)
            SysProcFragmentId.PF_restoreHashinatorScanResults;

    /*
     * Plan fragments for retrieving the hashinator data
     * for the snapshot visible at every node. Can't be combined
     * with the other scan because only one result table can be returned
     * by a plan fragment.
     */
    private static final int DEP_restoreDistributeHashinator = (int)
            SysProcFragmentId.PF_restoreDistributeHashinator | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_restoreDistributeHashinatorResults = (int)
            SysProcFragmentId.PF_restoreDistributeHashinatorResults;

    /*
     * Plan fragments for distributing the full set of export sequence numbers
     * to every partition where the relevant ones can be selected
     * and forwarded to the EE. Also distributes the txnId of the snapshot
     * which is used to truncate export data on disk from after the snapshot
     */
    private static final int DEP_restoreDistributeExportAndPartitionSequenceNumbers = (int)
            SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbers | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_restoreDistributeExportAndPartitionSequenceNumbersResults = (int)
            SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbersResults;

    /*
     * Plan fragment for entering an asynchronous run loop that generates a mailbox
     * and sends the generated mailbox id to the MP coordinator which then propagates the info.
     * The MP coordinator then sends plan fragments through this async mailbox,
     * bypassing the master/slave replication system that doesn't understand plan fragments
     * directed at individual executions sites.
     */
    private static final int DEP_restoreAsyncRunLoop = (int)
            SysProcFragmentId.PF_restoreAsyncRunLoop | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_restoreAsyncRunLoopResults = (int)
            SysProcFragmentId.PF_restoreAsyncRunLoopResults;

    private static final int DEP_setViewEnabled = (int)SysProcFragmentId.PF_setViewEnabled;

    private static HashSet<String>  m_initializedTableSaveFileNames = new HashSet<String>();
    private static ArrayDeque<TableSaveFile> m_saveFiles = new ArrayDeque<TableSaveFile>();

    private static volatile DuplicateRowHandler m_duplicateRowHandler = null;

    private final static String HASHINATOR_ALL_BAD = "All hashinator snapshots are bad (%s).";
    // These keep track of count per table that are reported restored by the snapshotrestore process.
    static final Map<String, AtomicLong> m_reportStats = new HashMap<String, AtomicLong>();
    static final Map<String, Integer> m_selectedReportPartition = new HashMap<String, Integer>();
    static long m_nextReportTime = 0;
    //Report every minute.
    static final long m_reportInterval = 60000;
    static DateFormat m_reportDateFormat = new SimpleDateFormat("HH:mm:ss");

    private static synchronized void initializeTableSaveFiles(
            String filePath,
            String fileNonce,
            String tableName,
            int originalHostIds[],
            int relevantPartitionIds[],
            SiteTracker st) throws IOException {
        // This check ensures that only one site per host attempts to
        // distribute this table.  @SnapshotRestore sends plan fragments
        // to every site on this host with the tables and partition ID that
        // this host is going to distribute to the cluster.  The first
        // execution site to get into this synchronized method is going to
        // 'win', add the table it's doing to this set, and+ then do the rest
        // of the work.  Subsequent sites will just return here.
        if (!m_initializedTableSaveFileNames.add(tableName)) {
            return;
        }

        // To avoid pulling duplicate rows when we have multiple files
        // that contain the data for a partition, we're going to assign
        // all of the partition IDs that were passed in to one and only one
        // TableSaveFile.  We'll pull them out of this set as we find
        // files for them, and then once the set is empty we can bail out of
        // this loop.  The restore planner called in @SnapshotRestore should
        // ensure that we can, in fact, find files for all these partitions.
        HashSet<Integer> relevantPartitionSet =
                new HashSet<Integer>();
        for (int part_id : relevantPartitionIds)
        {
            relevantPartitionSet.add(part_id);
        }

        for (int originalHostId : originalHostIds) {
            final File f = getSaveFileForPartitionedTable(filePath, fileNonce,
                    tableName,
                    originalHostId);
            TableSaveFile savefile = getTableSaveFile(
                    f,
                    st.getLocalSites().length * 2,
                    relevantPartitionSet.toArray(new Integer[relevantPartitionSet.size()]));

            m_saveFiles.offer(savefile);
            for (int part_id : savefile.getPartitionIds())
            {
                relevantPartitionSet.remove(part_id);
            }
            if (relevantPartitionSet.isEmpty())
            {
                break;
            }
            assert(m_saveFiles.peekLast().getCompleted());
        }
    }

    private static synchronized boolean hasMoreChunks() throws IOException {
        boolean hasMoreChunks = false;
        while (!hasMoreChunks && m_saveFiles.peek() != null) {
            TableSaveFile f = m_saveFiles.peek();
            hasMoreChunks = f.hasMoreChunks();
            if (!hasMoreChunks) {
                try {
                    f.close();
                } catch (IOException e) {
                }
                m_saveFiles.poll();
            }
        }
        return hasMoreChunks;
    }

    private static synchronized BBContainer getNextChunk() throws IOException {
        BBContainer c = null;
        while (c == null && m_saveFiles.peek() != null) {
            TableSaveFile f = m_saveFiles.peek();
            c = f.getNextChunk();
            if (c == null) {
                f.close();
                m_saveFiles.poll();
            }
        }
        return c;
    }

    @Override
    public long[] getPlanFragmentIds()
    {
        m_siteId = CoreUtils.getSiteIdFromHSId(m_site.getCorrespondingSiteId());
        m_hostId = m_site.getCorrespondingHostId();
        // XXX HACK GIANT HACK given the current assumption that there is
        // only one database per cluster, I'm asserting this and then
        // skirting around the need to have the database name in order to get
        // to the set of tables. --izzy
        assert(m_cluster.getDatabases().size() == 1);
        m_database = m_cluster.getDatabases().get("database");

        return new long[]{
            SysProcFragmentId.PF_restoreScan,
            SysProcFragmentId.PF_restoreScanResults,
            SysProcFragmentId.PF_restoreDigestScan,
            SysProcFragmentId.PF_restoreDigestScanResults,
            SysProcFragmentId.PF_restoreHashinatorScan,
            SysProcFragmentId.PF_restoreHashinatorScanResults,
            SysProcFragmentId.PF_restoreDistributeHashinator,
            SysProcFragmentId.PF_restoreDistributeHashinatorResults,
            SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbers,
            SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbersResults,
            SysProcFragmentId.PF_restoreAsyncRunLoop,
            SysProcFragmentId.PF_restoreAsyncRunLoopResults,
            SysProcFragmentId.PF_restoreLoadTable,
            SysProcFragmentId.PF_restoreReceiveResultTables,
            SysProcFragmentId.PF_restoreLoadReplicatedTable,
            SysProcFragmentId.PF_restoreDistributeReplicatedTableAsReplicated,
            SysProcFragmentId.PF_restoreDistributePartitionedTableAsPartitioned,
            SysProcFragmentId.PF_restoreDistributePartitionedTableAsReplicated,
            SysProcFragmentId.PF_restoreDistributeReplicatedTableAsPartitioned
            };
    }

    @Override
    public DependencyPair
    executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId, ParameterSet params,
            SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbers)
        {
            assert(params.toArray()[0] != null);
            assert(params.toArray().length == 6);
            assert(params.toArray()[0] instanceof byte[]);
            assert(params.toArray()[2] instanceof long[]);
            assert(params.toArray()[3] instanceof Long);
            assert(params.toArray()[4] instanceof Long);
            assert(params.toArray()[5] instanceof Integer);
            VoltTable result = new VoltTable(new VoltTable.ColumnInfo("RESULT", VoltType.STRING));
            byte[] jsonDigest = (byte[])params.toArray()[0];
            long snapshotTxnId = ((Long)params.toArray()[1]).longValue();
            long perPartitionTxnIds[] = (long[])params.toArray()[2];
            long clusterCreateTime = (Long)params.toArray()[3];
            long drVersion = (Long)params.toArray()[4];
            // Hack-ish because ParameterSet don't allow us to pass Boolean
            boolean isRecover = (Integer)params.toArray()[5] == 1;

            /*
             * Use the per partition txn ids to set the initial txnid value from the snapshot
             * All the values are sent in, but only the one for the appropriate partition
             * will be used
             */
            context.getSiteProcedureConnection().setPerPartitionTxnIds(perPartitionTxnIds, false);
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(jsonDigest);
                ObjectInputStream ois = new ObjectInputStream(bais);

                //Sequence numbers for every table and partition
                @SuppressWarnings("unchecked")
                Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers =
                        (Map<String, Map<Integer, Pair<Long, Long>>>)ois.readObject();

                @SuppressWarnings("unchecked")
                Map<Integer, Long> drSequenceNumbers = (Map<Integer, Long>)ois.readObject();

                //Last seen unique ids from remote data centers, load each local site
                @SuppressWarnings("unchecked")
                Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> drMixedClusterSizeConsumerState =
                        (Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>>)ois.readObject();

                performRestoreDigeststate(context, isRecover, snapshotTxnId, perPartitionTxnIds, exportSequenceNumbers);

                if (isRecover) {
                    performRecoverDigestState(context, snapshotTxnId, perPartitionTxnIds, clusterCreateTime,
                            drVersion, drSequenceNumbers, drMixedClusterSizeConsumerState);
                }
            } catch (Exception e) {
                e.printStackTrace();//l4j doesn't print the stack trace
                SNAP_LOG.error(e);
                result.addRow("FAILURE");
            }
            return new DependencyPair.TableDependencyPair(DEP_restoreDistributeExportAndPartitionSequenceNumbers, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbersResults)
        {
            if(TRACE_LOG.isTraceEnabled()){
                TRACE_LOG.trace("Aggregating digest scan state");
            }
            assert(dependencies.size() > 0);
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_restoreDistributeExportAndPartitionSequenceNumbers));
            return new DependencyPair.TableDependencyPair(DEP_restoreDistributeExportAndPartitionSequenceNumbersResults, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDigestScan)
        {
            VoltTable result = new VoltTable(
                    new VoltTable.ColumnInfo("DIGEST_CONTINUED", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("DIGEST", VoltType.STRING),
                    new VoltTable.ColumnInfo("RESULT", VoltType.STRING),
                    new VoltTable.ColumnInfo("ERR_MSG", VoltType.STRING));
            // Choose the lowest site ID on this host to do the file scan
            // All other sites should just return empty results tables.
            if (context.isLowestSiteId())
            {
                try {
                    // implicitly synchronized by the way restore operates.
                    // this scan must complete on every site and return results
                    // to the coordinator for aggregation before it will send out
                    // distribution fragments, so two sites on the same node
                    // can't be attempting to set and clear this HashSet simultaneously
                    if(TRACE_LOG.isTraceEnabled()){
                        TRACE_LOG.trace("Checking saved table digest state for restore of: "
                                + m_filePath + ", " + m_fileNonce);
                    }
                    List<JSONObject> digests =
                            SnapshotUtil.retrieveDigests(m_filePath, m_fileNonce, SNAP_LOG);

                    for (JSONObject obj : digests) {
                        String jsonDigest = obj.toString();
                        for (int start = 0; start < jsonDigest.length(); start += VoltType.MAX_VALUE_LENGTH) {
                            String block = jsonDigest.substring(start, Math.min(jsonDigest.length(), start + VoltType.MAX_VALUE_LENGTH));
                            byte digestContinued = (start+VoltType.MAX_VALUE_LENGTH < jsonDigest.length()) ? (byte)1 : (byte)0;
                            result.addRow(digestContinued, block, "SUCCESS", null);
                        }
                    }
                } catch (Exception e) {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    pw.flush();
                    e.printStackTrace();//l4j doesn't print stack traces
                    SNAP_LOG.error(e);
                    result.addRow(null, "FAILURE", sw.toString());
                    return new DependencyPair.TableDependencyPair(DEP_restoreDigestScan, result);
                }
            }
            return new DependencyPair.TableDependencyPair(DEP_restoreDigestScan, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDigestScanResults)
        {
            if(TRACE_LOG.isTraceEnabled()){
                TRACE_LOG.trace("Aggregating digest scan state");
            }
            assert(dependencies.size() > 0);
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_restoreDigestScan));
            return new DependencyPair.TableDependencyPair(DEP_restoreDigestScanResults, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreHashinatorScan)
        {
            VoltTable result = new VoltTable(
                    new VoltTable.ColumnInfo("HASH", VoltType.VARBINARY),
                    new VoltTable.ColumnInfo("RESULT", VoltType.STRING),
                    new VoltTable.ColumnInfo("ERR_MSG", VoltType.STRING));

            // Choose the lowest site ID on this host to do the file scan
            // All other sites should just return empty results tables.
            if (context.isLowestSiteId())
            {
                if(TRACE_LOG.isTraceEnabled()){
                    TRACE_LOG.trace("Checking saved hashinator state for restore of: "
                            + m_filePath + ", " + m_fileNonce);
                }
                List<ByteBuffer> configs;
                try {
                    configs = SnapshotUtil.retrieveHashinatorConfigs(
                                    m_filePath, m_fileNonce, 1, SNAP_LOG);
                    for (ByteBuffer config : configs) {
                        assert(config.hasArray());
                        result.addRow(config.array(), "SUCCESS", null);
                    }
                }
                catch (IOException e) {
                    String errMsg = e.toString();
                    SNAP_LOG.error(errMsg);
                    result.addRow(null, "FAILURE", errMsg);
                }
            }
            return new DependencyPair.TableDependencyPair(DEP_restoreHashinatorScan, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreHashinatorScanResults)
        {
            if(TRACE_LOG.isTraceEnabled()){
                TRACE_LOG.trace("Aggregating hashinator state");
            }
            assert(dependencies.size() > 0);
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_restoreHashinatorScan));
            return new DependencyPair.TableDependencyPair(DEP_restoreHashinatorScanResults, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDistributeHashinator)
        {
            Object paramsArray[] = params.toArray();
            assert(paramsArray.length == 1);
            assert(paramsArray[0] != null);
            assert(paramsArray[0] instanceof byte[]);
            VoltTable result = new VoltTable(
                    new VoltTable.ColumnInfo("RESULT", VoltType.STRING),
                    new VoltTable.ColumnInfo("ERR_MSG", VoltType.STRING));
            // The config is serialized in a more compressible format.
            // Need to convert to the standard format for internal and EE use.
            byte[] hashConfig = (byte[])paramsArray[0];
            try {
                @SuppressWarnings("deprecation")
                Pair<? extends UndoAction, TheHashinator> hashinatorPair =
                        TheHashinator.updateConfiguredHashinator(
                                DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this),
                                hashConfig);
                // Update C++ hashinator.
                context.updateHashinator(hashinatorPair.getSecond());
                result.addRow("SUCCESS", null);
            }
            catch (RuntimeException e) {
                SNAP_LOG.error("Error updating hashinator in snapshot restore", e);
                result.addRow("FAILURE", CoreUtils.throwableToString(e));
            }
            return new DependencyPair.TableDependencyPair(DEP_restoreDistributeHashinator, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDistributeHashinatorResults)
        {
            if(TRACE_LOG.isTraceEnabled()){
                TRACE_LOG.trace("Aggregating hashinator distribution state");
            }
            assert(dependencies.size() > 0);
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_restoreDistributeHashinator));
            return new DependencyPair.TableDependencyPair(DEP_restoreDistributeHashinatorResults, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreScan)
        {
            Object paramsArray[] = params.toArray();
            assert(paramsArray[0] != null);
            assert(paramsArray[1] != null);
            String hostname = CoreUtils.getHostnameOrAddress();
            VoltTable result = ClusterSaveFileState.constructEmptySaveFileStateVoltTable();

            // Choose the lowest site ID on this host to do the file scan
            // All other sites should just return empty results tables.
            if (context.isLowestSiteId())
            {
                // implicitly synchronized by the way restore operates.
                // this scan must complete on every site and return results
                // to the coordinator for aggregation before it will send out
                // distribution fragments, so two sites on the same node
                // can't be attempting to set and clear this HashSet simultaneously
                m_initializedTableSaveFileNames.clear();
                m_saveFiles.clear();//Tests will reused a VoltDB process that fails a restore

                m_filePath = (String) params.toArray()[0];
                m_filePathType = (String) params.toArray()[1];
                m_filePath = SnapshotUtil.getRealPath(SnapshotPathType.valueOf(m_filePathType), m_filePath);
                m_fileNonce = (String) params.toArray()[2];
                /*
                 * Initialize a duplicate row handling policy for this restore.
                 * if path type is not SNAP_PATH use local path specified by type
                 */
                m_duplicateRowHandler = null;
                String dupPath = (String )params.toArray()[3];
                if (dupPath != null) {
                    dupPath = (SnapshotPathType.valueOf(m_filePathType) == SnapshotPathType.SNAP_PATH ?
                            dupPath : m_filePath);

                    VoltFile outputPath = new VoltFile(dupPath);
                    String errorMsg = null;
                    if (!outputPath.exists()) {
                        errorMsg = "Output path for Json duplicatesPath \"" + outputPath + "\" does not exist";
                    }
                    if (!outputPath.canExecute()) {
                        errorMsg = "Output path for Json duplicatesPath \"" + outputPath + "\" is not executable";
                    }
                    // error check and early return
                    if (errorMsg != null) {
                        result.addRow(m_hostId, hostname, ClusterSaveFileState.ERROR_CODE, errorMsg,
                                null, null, null, null, null, null, null);
                        return new DependencyPair.TableDependencyPair(DEP_restoreScan, result);
                    }

                    m_duplicateRowHandler = new DuplicateRowHandler(dupPath, getTransactionTime());
                    CONSOLE_LOG.info("Duplicate rows will be output to: " + dupPath + " nonce: " + m_fileNonce);
                }
                if (TRACE_LOG.isTraceEnabled()) {
                    TRACE_LOG.trace("Checking saved table state for restore of: "
                            + m_filePath + ", " + m_fileNonce);
                }
                File[] savefiles = SnapshotUtil.retrieveRelevantFiles(m_filePath, m_fileNonce);
                if (savefiles == null) {
                    return new DependencyPair.TableDependencyPair(DEP_restoreScan, result);
                }
                for (File file : savefiles)
                {
                    TableSaveFile savefile = null;
                    try
                    {
                        savefile = getTableSaveFile(file, 1, null);
                        try {

                            if (!savefile.getCompleted()) {
                                continue;
                            }

                            String is_replicated = "FALSE";
                            if (savefile.isReplicated())
                            {
                                is_replicated = "TRUE";
                            }
                            int partitionIds[] = savefile.getPartitionIds();
                            for (int pid : partitionIds) {
                                result.addRow(
                                        m_hostId,
                                        hostname,
                                        savefile.getHostId(),
                                        savefile.getHostname(),
                                        savefile.getClusterName(),
                                        savefile.getDatabaseName(),
                                        savefile.getTableName(),
                                        savefile.getTxnId(),
                                        is_replicated,
                                        pid,
                                        savefile.getTotalPartitions());
                            }
                        } finally {
                            savefile.close();
                        }
                    }
                    catch (FileNotFoundException e)
                    {
                        // retrieveRelevantFiles should always generate a list
                        // of valid present files in m_filePath, so if we end up
                        // getting here, something has gone very weird.
                        e.printStackTrace();
                    }
                    catch (IOException e)
                    {
                        // For the time being I'm content to treat this as a
                        // missing file and let the coordinator complain if
                        // it discovers that it can't build a consistent
                        // database out of the files it sees available.
                        //
                        // Maybe just a log message?  Later.
                        e.printStackTrace();
                    }
                }
            }

            return new DependencyPair.TableDependencyPair(DEP_restoreScan, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreScanResults)
        {
            if(TRACE_LOG.isTraceEnabled()){
                TRACE_LOG.trace("Aggregating saved table state");
            }
            assert(dependencies.size() > 0);
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_restoreScan));
            return new DependencyPair.TableDependencyPair(DEP_restoreScanResults, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreAsyncRunLoop)
        {
            Object paramsArray[] = params.toArray();
            assert(paramsArray.length == 1);
            assert(paramsArray[0] instanceof Long);
            long coordinatorHSId = (Long)paramsArray[0];
            Mailbox m = VoltDB.instance().getHostMessenger().createMailbox();
            m_mbox = m;
            if(TRACE_LOG.isTraceEnabled()){
                TRACE_LOG.trace(
                        "Entering async run loop at " + CoreUtils.hsIdToString(context.getSiteId()) +
                        " listening on mbox " + CoreUtils.hsIdToString(m.getHSId()));
            }
            /*
             * Send the generated mailbox id to the coordinator mapping
             * from the actual execution site id to the mailbox that will
             * be used for restore
             */
            ByteBuffer responseBuffer = ByteBuffer.allocate(16);
            responseBuffer.putLong(m_site.getCorrespondingSiteId());
            responseBuffer.putLong(m.getHSId());

            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], responseBuffer.array());
            m.send(coordinatorHSId, bpm);
            bpm = null;

            /*
             * Retrieve the mapping from actual site ids
             * to the site ids generated for mailboxes used for restore
             * The coordinator will generate this once it has heard from all sites
             */
            while (true) {
                bpm = (BinaryPayloadMessage)m.recvBlocking();
                if (bpm == null) continue;
                ByteBuffer wrappedMap = ByteBuffer.wrap(bpm.m_payload);

                while (wrappedMap.hasRemaining()) {
                    long actualHSId = wrappedMap.getLong();
                    long generatedHSId = wrappedMap.getLong();
                    m_actualToGenerated.put(actualHSId, generatedHSId);
                }
                break;
            }

            //Acknowledge receipt of map from coordinator
            bpm = new BinaryPayloadMessage(new byte[0], new byte[0]);
            m.send(coordinatorHSId, bpm);
            bpm = null;

            /*
             * Loop until the termination signal is received. Execute any plan fragments that
             * are received
             */
            while (true) {
                VoltMessage vm = m.recvBlocking(1000);
                if (vm == null) continue;

                if (vm instanceof FragmentTaskMessage) {
                    FragmentTaskMessage ftm = (FragmentTaskMessage)vm;
                    if(TRACE_LOG.isTraceEnabled()){
                        TRACE_LOG.trace(
                                CoreUtils.hsIdToString(context.getSiteId()) + " received fragment id " +
                                        VoltSystemProcedure.hashToFragId(ftm.getPlanHash(0)));
                    }
                    DependencyPair dp =
                            m_runner.executeSysProcPlanFragment(
                                    m_runner.getTxnState(),
                                    null,
                                    VoltSystemProcedure.hashToFragId(ftm.getPlanHash(0)),
                                    ftm.getParameterSetForFragment(0));
                    if (dp != null) {
                        // Like SysProcFragmentId.PF_setViewEnabled, the execution returns null.
                        FragmentResponseMessage frm = new FragmentResponseMessage(ftm, m.getHSId());
                        frm.addDependency(dp);
                        m.send(ftm.getCoordinatorHSId(), frm);
                    }
                } else if (vm instanceof BinaryPayloadMessage) {
                    if (context.isLowestSiteId() && m_duplicateRowHandler != null) {
                        try {
                            m_duplicateRowHandler.close();
                        } catch (Exception e) {
                            VoltDB.crashLocalVoltDB("Error closing duplicate row handler during snapshot restore",
                                                     true,
                                                     e);
                        }
                    }
                    //Null result table is intentional
                    //The results of the process are propagated through a future in performTableRestoreWork
                    VoltTable emptyResult = constructResultsTable();
                    return new DependencyPair.TableDependencyPair( DEP_restoreAsyncRunLoop, emptyResult);
                }
            }
        } else if (fragmentId == SysProcFragmentId.PF_restoreAsyncRunLoopResults) {
            VoltTable emptyResult = constructResultsTable();
            return new DependencyPair.TableDependencyPair(DEP_restoreAsyncRunLoopResults, emptyResult);
        }

        // called by: performDistributeReplicatedTable() and performDistributePartitionedTable
        // handle all 4 LOADING tasks:
        //          1. load a replicated table as replicated table
        //          2. load a partitioned table as replicated table
        //          3. load a partitioned table as partitioned table (need to check unique violation)
        //          4. load a partitioned table as replicated table (need to check unique violation)
        else if (fragmentId == SysProcFragmentId.PF_restoreLoadTable) {
            // the last parameter could be null for the replicatedToReplicated case
            // and this parameter is used for log only for both load as replicated cases
            assert (params.toArray()[0] != null);
            assert (params.toArray()[1] != null);
            assert (params.toArray()[2] != null);
            assert (params.toArray()[3] != null);
            String table_name = (String) params.toArray()[0];
            int dependency_id = (Integer) params.toArray()[1];
            byte compressedTable[] = (byte[]) params.toArray()[2];
            int checkUniqueViolations = (Integer) params.toArray()[3];
            int[] partition_ids = (int[]) params.toArray()[4];
            boolean isRecover = "true".equals(params.toArray()[5]);

            if(checkUniqueViolations == K_CHECK_UNIQUE_VIOLATIONS_PARTITIONED) {
                assert(partition_ids != null && partition_ids.length == 1);
            }
            if(TRACE_LOG.isTraceEnabled()){
                TRACE_LOG.trace("Received table: " + table_name +
                        (partition_ids == null ? "[REPLICATED]" : " of partition [" + partition_ids.toString()) + "]");
            }
            String result_str = "SUCCESS";
            String error_msg = "";
            long cnt = 0;
            try {
                VoltTable table = PrivateVoltTableFactory.createVoltTableFromBuffer(
                                ByteBuffer.wrap(CompressionService.decompressBytes(compressedTable)), true);
                @SuppressWarnings("deprecation")
                byte uniqueViolations[] =
                        DeprecatedProcedureAPIAccess.voltLoadTable(
                                this,
                                context.getCluster().getTypeName(),
                                context.getDatabase().getTypeName(),
                                table_name,
                                table,
                                m_duplicateRowHandler != null,
                                false);
                if(uniqueViolations != null && !isRecover){
                    result_str = "FAILURE";
                    error_msg = "Constraint violations in table " + table_name;
                    SNAP_LOG.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS, Level.WARN, null,error_msg);
                }
                handleUniqueViolations(table_name, uniqueViolations, checkUniqueViolations, context);
                cnt = table.getRowCount();
            } catch (Exception e) {
                result_str = "FAILURE";
                error_msg = CoreUtils.throwableToString(e);
            }
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, CoreUtils.getHostnameOrAddress(), CoreUtils.getSiteIdFromHSId(m_siteId), table_name,
                            ((checkUniqueViolations == K_CHECK_UNIQUE_VIOLATIONS_PARTITIONED) ? partition_ids[0] : -1),
                    result_str, error_msg);
            reportProgress(table_name, cnt, (partition_ids == null), context.getPartitionId());
            return new DependencyPair.TableDependencyPair(dependency_id, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreReceiveResultTables) {
            assert (params.toArray()[0] != null);
            assert (params.toArray()[1] != null);
            int dependency_id = (Integer) params.toArray()[0];
            if(TRACE_LOG.isTraceEnabled()){
                String tracingLogMsg = (String) params.toArray()[1];
                TRACE_LOG.trace(tracingLogMsg);
            }

            /*
             * Capture and de-dupe the results.
             * Low-level multi-partition results are per fragment. The result
             * codes and error messages need to be consolidated so that there is
             * one result per unique host/partition/table combo.
             */
            SnapshotRestoreResultSet resultSet = new SnapshotRestoreResultSet();
            VoltTable result = null;
            for (int dep_id : dependencies.keySet())
            {
                for (VoltTable vt : dependencies.get(dep_id)) {
                    if (vt != null) {
                        while (vt.advanceRow()) {
                            resultSet.parseRestoreResultRow(vt);
                        }
                        if (result == null) {
                            result = new VoltTable(VoltTableUtil.extractTableSchema(vt));
                            result.setStatusCode(vt.getStatusCode());
                        }
                    }
                }
            }

            // Copy de-duped results to output table.
            if (result != null) {
                for (RestoreResultKey key : resultSet.keySet()) {
                    // Expect success since keys come from the set.
                    boolean success = resultSet.addRowsForKey(key, result);
                    assert(success);
                }
            }

            if (result == null) {
                return new DependencyPair.TableDependencyPair(dependency_id, null);
            }
            else
                return new DependencyPair.TableDependencyPair(dependency_id, result);
        }

        else if (fragmentId == SysProcFragmentId.PF_restoreLoadReplicatedTable)
        {
            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            String table_name = (String) params.toArray()[0];
            int dependency_id = (Integer) params.toArray()[1];
            if(TRACE_LOG.isTraceEnabled()){
                TRACE_LOG.trace("Loading replicated table: " + table_name);
            }
            String result_str = "SUCCESS";
            String error_msg = "";
            TableSaveFile savefile = null;
            boolean isRecover = "true".equals(params.toArray()[2]);

            /**
             * For replicated tables this will do the slow thing and read the file
             * once for each ExecutionSite. This could use optimization like
             * is done with the partitioned tables.
             */
            try
            {
                savefile =
                        getTableSaveFile(getSaveFileForReplicatedTable(table_name), 3, null);
                assert(savefile.getCompleted());
            }
            catch (IOException e)
            {
                String hostname = CoreUtils.getHostnameOrAddress();
                VoltTable result = constructResultsTable();
                result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), table_name, -1,
                        "FAILURE", "Unable to load table: " + table_name + " error:\n" + CoreUtils.throwableToString(e), 0, 0);
                return new DependencyPair.TableDependencyPair(dependency_id, result);
            }

            long cnt = 0;
            try {
                final Table new_catalog_table = getCatalogTable(table_name);
                final boolean shouldPreserveDRHiddenColumn =
                    DrRoleType.XDCR.value().equals(m_cluster.getDrrole()) && new_catalog_table.getIsdred();

                Boolean needsConversion = null;
                while (savefile.hasMoreChunks())
                {
                    VoltTable table = null;

                    final org.voltcore.utils.DBBPool.BBContainer c = savefile.getNextChunk();
                    if (c == null) {
                        continue;//Should be equivalent to break
                    }
                    try {
                        if (needsConversion == null) {
                            VoltTable old_table =
                                    PrivateVoltTableFactory.createVoltTableFromBuffer(c.b().duplicate(), true);
                            needsConversion = SavedTableConverter.needsConversion(old_table, new_catalog_table,
                                                                                  shouldPreserveDRHiddenColumn);
                        }

                        if (needsConversion) {
                            VoltTable old_table =
                                    PrivateVoltTableFactory.createVoltTableFromBuffer(c.b() , true);
                            table = SavedTableConverter.convertTable(old_table, new_catalog_table,
                                                                     shouldPreserveDRHiddenColumn);
                        } else {
                            ByteBuffer copy = ByteBuffer.allocate(c.b().remaining());
                            copy.put(c.b());
                            copy.flip();
                            table = PrivateVoltTableFactory.createVoltTableFromBuffer(copy, true);
                        }
                    } finally {
                        c.discard();
                    }

                    try
                    {
                        @SuppressWarnings("deprecation")
                        byte uniqueViolations[] = DeprecatedProcedureAPIAccess.voltLoadTable(
                                this,
                                context.getCluster().getTypeName(),
                                context.getDatabase().getTypeName(),
                                table_name,
                                table,
                                m_duplicateRowHandler != null,
                                false);

                        if(uniqueViolations != null && !isRecover){
                            result_str = "FAILURE";
                            error_msg = "Constraint violations in table " + table_name;
                            SNAP_LOG.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS, Level.WARN, null,
                                    error_msg);

                        }

                        handleUniqueViolations(table_name,
                                               uniqueViolations,
                                               K_CHECK_UNIQUE_VIOLATIONS_REPLICATED,
                                context);
                        cnt += table.getRowCount();
                    }
                    catch (Exception e)
                    {
                        result_str = "FAILURE";
                        error_msg = CoreUtils.throwableToString(e);
                        break;
                    }
                }

            } catch (IOException e) {
                String hostname = CoreUtils.getHostnameOrAddress();
                VoltTable result = constructResultsTable();
                result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), table_name,
                        -1, "FAILURE", "Unable to load table: " + table_name + " error:\n" + CoreUtils.throwableToString(e));
                return new DependencyPair.TableDependencyPair(dependency_id, result);
            } catch (VoltTypeException e) {
                String hostname = CoreUtils.getHostnameOrAddress();
                VoltTable result = constructResultsTable();
                result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), table_name, -1,
                        "FAILURE", "Unable to load table: " + table_name + " error:\n" + CoreUtils.throwableToString(e));
                return new DependencyPair.TableDependencyPair(dependency_id, result);
            } finally {
                try {
                    savefile.close();
                } catch (IOException e) {
                    SNAP_LOG.warn("Error closing table file", e);
                }
            }

            String hostname = CoreUtils.getHostnameOrAddress();
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), table_name, -1, result_str,
                    error_msg);

            reportProgress(table_name, cnt, true, context.getPartitionId());
            return new DependencyPair.TableDependencyPair(dependency_id, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDistributeReplicatedTableAsReplicated)
        {
            // XXX I tested this with a hack that cannot be replicated
            // in a unit test since it requires hacks to this sysproc that
            // effectively break it
            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            assert(params.toArray()[2] != null);
            String table_name = (String) params.toArray()[0];
            long site_id = (Long) params.toArray()[1];
            int dependency_id = (Integer) params.toArray()[2];
            boolean isRecover = "true".equals(params.toArray()[3]);
            if(TRACE_LOG.isTraceEnabled()){
                TRACE_LOG.trace(CoreUtils.hsIdToString(context.getSiteId()) + " distributing replicated table: " + table_name +
                        " to: " + CoreUtils.hsIdToString(site_id) + " recover:" + isRecover);
            }
            VoltTable result = performDistributeReplicatedTable(table_name, context, site_id, false, isRecover);
            assert(result != null);
            return new DependencyPair.TableDependencyPair(dependency_id, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDistributePartitionedTableAsPartitioned)
        {
            Object paramsA[] = params.toArray();
            assert(paramsA[0] != null);
            assert(paramsA[1] != null);
            assert(paramsA[2] != null);
            assert(paramsA[3] != null);

            String table_name = (String) paramsA[0];
            int originalHosts[] = (int[]) paramsA[1];
            int relevantPartitions[]  = (int[]) paramsA[2];
            int dependency_id = (Integer) paramsA[3];
            boolean isRecover = "true".equals(paramsA[4]);

            if(TRACE_LOG.isTraceEnabled()){
                for (int partition_id : relevantPartitions) {
                    TRACE_LOG.trace("Distributing partitioned table: " + table_name +
                            " partition id: " + partition_id + " recover:" + isRecover);
                }
            }
            VoltTable result =
                    performDistributePartitionedTable(table_name, originalHosts,
                            relevantPartitions, context, false, isRecover);
            assert(result != null);
            return new DependencyPair.TableDependencyPair(dependency_id, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDistributePartitionedTableAsReplicated) {
            Object paramsA[] = params.toArray();
            assert (paramsA[0] != null);
            assert (paramsA[1] != null);
            assert (paramsA[2] != null);
            assert (paramsA[3] != null);

            String table_name = (String) paramsA[0];
            int originalHosts[] = (int[]) paramsA[1];
            int relevantPartitions[] = (int[]) paramsA[2];
            int dependency_id = (Integer) paramsA[3];
            boolean isRecover = "true".equals(paramsA[4]);
            if(TRACE_LOG.isTraceEnabled()){
                for (int partition_id : relevantPartitions) {
                    TRACE_LOG.trace("Loading partitioned-to-replicated table: " + table_name
                            + " partition id: " + partition_id);
                }
            }
            VoltTable result = performDistributePartitionedTable(table_name,
                    originalHosts, relevantPartitions, context, true, isRecover);
            assert(result != null);
            return new DependencyPair.TableDependencyPair(dependency_id, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDistributeReplicatedTableAsPartitioned) {
            assert (params.toArray()[0] != null);
            assert (params.toArray()[1] != null);
            String table_name = (String) params.toArray()[0];
            int dependency_id = (Integer) params.toArray()[1];
            boolean isRecover = "true".equals(params.toArray()[2]);

            if(TRACE_LOG.isTraceEnabled()){
                TRACE_LOG.trace("Loading replicated-to-partitioned table: " + table_name);
            }

            VoltTable result = performDistributeReplicatedTable(table_name, context, -1, true, isRecover);
            assert(result != null);
            return new DependencyPair.TableDependencyPair(dependency_id, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_setViewEnabled) {
            Object[] paramArray = params.toArray();
            assert(paramArray[0] != null && paramArray[1] != null);
            boolean enabled = (int)paramArray[0] > 0 ? true : false;
            String commaSeparatedViewNames = (String)paramArray[1];
            m_runner.getExecutionEngine().setViewsEnabled(commaSeparatedViewNames, enabled);
            // Can an error from here stop the snapshot? I don't think so.
            // So I intentionally let this fragment return nothing.
            return null;
        }

        assert (false);
        return null;
    }

    private void handleUniqueViolations(String table_name,
                                        byte[] uniqueViolations,
                                        int checkUniqueViolations,
                                        SystemProcedureExecutionContext context) throws Exception {
        if (uniqueViolations != null && m_duplicateRowHandler == null) {
            VoltDB.crashLocalVoltDB(
                    "Shouldn't get unique violations returned when duplicate row handler is null",
                    true,
                    null);
        }
        if (uniqueViolations != null) {
            /*
             * If this is a replicated table that is having unique constraint violations
             * Only log at the lowest site on the lowest node.
             */
            if (checkUniqueViolations == K_CHECK_UNIQUE_VIOLATIONS_REPLICATED) {
                if (context.isLowestSiteId() &&
                        context.getHostId() == 0) {
                    m_duplicateRowHandler.handleDuplicates(table_name, uniqueViolations);
                }
            } else {
                m_duplicateRowHandler.handleDuplicates(table_name, uniqueViolations);
            }
        }
    }

    private static final String RESTORE_FAILED = "Restore failed to complete. See response table for additional info.";

    @SuppressWarnings("deprecation")
    public VoltTable[] run(SystemProcedureExecutionContext ctx,
                           String json) throws Exception
    {
        JSONObject jsObj = new JSONObject(json);
        String path = jsObj.getString(SnapshotUtil.JSON_PATH);
        String pathType = jsObj.optString(SnapshotUtil.JSON_PATH_TYPE, SnapshotPathType.SNAP_PATH.toString());
        final String nonce = jsObj.getString(SnapshotUtil.JSON_NONCE);
        final String dupsPath = jsObj.optString(SnapshotUtil.JSON_DUPLICATES_PATH, null);
        final boolean useHashinatorData = jsObj.optBoolean(SnapshotUtil.JSON_HASHINATOR);
        final boolean isRecover = jsObj.optBoolean(SnapshotUtil.JSON_IS_RECOVER);
        final int partitionCount = jsObj.optInt(SnapshotUtil.JSON_PARTITION_COUNT);
        final int newPartitionCount = jsObj.optInt(SnapshotUtil.JSON_NEW_PARTITION_COUNT);

        path = SnapshotUtil.getRealPath(SnapshotPathType.valueOf(pathType), path);
        final long startTime = System.currentTimeMillis();
        CONSOLE_LOG.info("Restoring from path: " + path + " with nonce: " + nonce);

        // Fetch all the savefile metadata from the cluster
        VoltTable[] savefile_data;
        savefile_data = performRestoreScanWork(path, pathType, nonce, dupsPath);

        while (savefile_data[0].advanceRow()) {
            long originalHostId = savefile_data[0].getLong("ORIGINAL_HOST_ID");
            // empty error messages indicate SUCCESS
            if (originalHostId == ClusterSaveFileState.ERROR_CODE) {
                Long hostId = savefile_data[0].getLong("CURRENT_HOST_ID");
                String hostName = savefile_data[0].getString("CURRENT_HOSTNAME");
                // hack to store the error messages without changing API
                String errorMsg = savefile_data[0].getString("ORIGINAL_HOSTNAME");
                throw new VoltAbortException("Error scanning restore work from host id " + hostId + " hostname "
                        + hostName + ":" + errorMsg);
            }
        }
        savefile_data[0].resetRowPosition();

        List<JSONObject> digests;
        Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers;
        Map<Integer, Long> drSequenceNumbers;
        long perPartitionTxnIds[];
        Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> remoteDCLastSeenIds;
        long clusterCreateTime;
        long drVersion;
        try {
            // Digest scan.
            DigestScanResult digestScanResult =
                    performRestoreDigestScanWork(isRecover);
            digests = digestScanResult.digests;
            exportSequenceNumbers = digestScanResult.exportSequenceNumbers;
            drSequenceNumbers = digestScanResult.drSequenceNumbers;
            perPartitionTxnIds = digestScanResult.perPartitionTxnIds;
            remoteDCLastSeenIds = digestScanResult.remoteDCLastSeenIds;
            clusterCreateTime = digestScanResult.clusterCreateTime;
            drVersion = digestScanResult.drVersion;

            if (!isRecover || perPartitionTxnIds.length == 0) {
                perPartitionTxnIds = new long[] {
                        DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this)
                };
            }

            // Hashinator scan and distribution.
            // Missing digests will be officially handled later.
            if (useHashinatorData && !digests.isEmpty()) {
                // Need the instance ID for sanity checks.
                InstanceId iid = null;
                if (digests.get(0).has("instanceId")) {
                    iid = new InstanceId(digests.get(0).getJSONObject("instanceId"));
                }
                byte[] hashConfig = performRestoreHashinatorScanWork(iid);
                if (hashConfig != null) {
                     VoltTable[] hashinatorResults = performRestoreHashinatorDistributeWork(hashConfig);
                    while (hashinatorResults[0].advanceRow()) {
                        if (hashinatorResults[0].getString("RESULT").equals("FAILURE")) {
                            throw new VoltAbortException("Error distributing hashinator.");
                        }
                    }
                }
            }
        }
        catch (VoltAbortException e) {
            ColumnInfo[] result_columns = new ColumnInfo[2];
            int ii = 0;
            result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
            result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
            VoltTable results[] = new VoltTable[] { new VoltTable(result_columns) };
            results[0].addRow("FAILURE", e.toString());
            noteOperationalFailure(RESTORE_FAILED);
            return results;
        }

        ClusterSaveFileState savefile_state = null;
        try
        {
            savefile_state = new ClusterSaveFileState(savefile_data[0]);
        }
        catch (IOException e)
        {
            throw new VoltAbortException(e);
        }

        HashSet<String> relevantTableNames = new HashSet<String>();
        try {
            if (digests.isEmpty()) {
                throw new Exception("No snapshot related digests files found");
            }
            for (JSONObject obj : digests) {
                JSONArray tables = obj.getJSONArray("tables");
                for (int ii = 0; ii < tables.length(); ii++) {
                    relevantTableNames.add(tables.getString(ii));
                }
            }
        } catch (Exception e) {
            ColumnInfo[] result_columns = new ColumnInfo[2];
            int ii = 0;
            result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
            result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
            VoltTable results[] = new VoltTable[] { new VoltTable(result_columns) };
            results[0].addRow("FAILURE", e.toString());
            noteOperationalFailure(RESTORE_FAILED);
            return results;
        }
        assert(relevantTableNames != null);

        // ENG-1078: I think this giant for/if block is only good for
        // checking if there are no files for a table listed in the digest.
        // There appear to be redundant checks for that, and then the per-table
        // consistency check is preempted by the ClusterSaveFileState constructor
        // called above.
        VoltTable[] results = null;
        for (String tableName : relevantTableNames) {
            if (!savefile_state.getSavedTableNames().contains(tableName)) {
                if (results == null) {
                    ColumnInfo[] result_columns = new ColumnInfo[2];
                    int ii = 0;
                    result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
                    result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
                    results = new VoltTable[] { new VoltTable(result_columns) };
                }
                results[0].addRow("FAILURE", "Save data contains no information for table " + tableName);
                break;
            }

            final TableSaveFileState saveFileState = savefile_state.getTableState(tableName);
            if (saveFileState == null)
            {
                // Pretty sure this is unreachable
                // See ENG-1078
                if (results == null) {
                    ColumnInfo[] result_columns = new ColumnInfo[2];
                    int ii = 0;
                    result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
                    result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
                    results = new VoltTable[] { new VoltTable(result_columns) };
                }
                results[0].addRow( "FAILURE", "Save data contains no information for table " + tableName);
            }
            else if (!saveFileState.isConsistent())
            {
                // Also pretty sure this is unreachable
                // See ENG-1078
                if (results == null) {
                    ColumnInfo[] result_columns = new ColumnInfo[2];
                    int ii = 0;
                    result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
                    result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
                    results = new VoltTable[] { new VoltTable(result_columns) };
                }
                results[0].addRow( "FAILURE", saveFileState.getConsistencyResult());
            }
        }
        if (results != null) {
            noteOperationalFailure(RESTORE_FAILED);
            return results;
        }

        /*
         * This list stores all the partition transaction ids ever seen even if the partition
         * is no longer present. The values from here are added to snapshot digests to propagate
         * partitions that were remove/add several time by SnapshotSave.
         *
         * Only the partitions that are no longer part of the cluster will have their ids retrieved,
         * those that are active will populate their current values manually because they change after startup
         *
         * This is necessary to make sure that sequence numbers never go backwards as a result of a partition
         * being removed and then added back by save restore sequences.
         *
         * They will be retrieved from ZK by the snapshot daemon
         * and passed to @SnapshotSave which will use it to fill in transaction ids for
         * partitions that are no longer present
         */
        try {
            updatePerPartitionTxnIdsToZK(perPartitionTxnIds);
        } catch (Exception e) {
            ColumnInfo[] result_columns = new ColumnInfo[2];
            int i = 0;
            result_columns[i++] = new ColumnInfo("RESULT", VoltType.STRING);
            result_columns[i++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
            results = new VoltTable[] { new VoltTable(result_columns) };
            results[0].addRow("FAILURE", e.toString());
            noteOperationalFailure(RESTORE_FAILED);
            return results;
        }

        // if this is a truncation snapshot that is on the boundary of partition count change
        // we need to populate -1 as dr sequence number for the new partitions since all txns
        // that touch the new partitions will be in the command log and we need to truncate
        // the DR log for the new partitions completely before replaying the command log.
        for (int i = partitionCount; i < newPartitionCount; ++i) {
            drSequenceNumbers.put(i, -1L);
        }

        /*
         * Serialize all the export sequence numbers and then distribute them in a
         * plan fragment and each receiver will pull the relevant information for
         * itself.
         * Also distribute the restored hashinator config.
         *
         * Also chucking the last seen unique ids from remote data center in this message
         * and loading them as part of the same distribution process
         */
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(exportSequenceNumbers);
            oos.writeObject(drSequenceNumbers);
            oos.writeObject(remoteDCLastSeenIds);
            oos.flush();
            byte exportSequenceNumberBytes[] = baos.toByteArray();
            oos.close();

            /*
             * Also set the perPartitionTxnIds locally at the multi-part coordinator.
             * The coord will have to forward this value to all the idle coordinators.
             */
            ctx.getSiteProcedureConnection().setPerPartitionTxnIds(perPartitionTxnIds, false);

            results =
                    performDistributeDigestState(
                            exportSequenceNumberBytes,
                            digests.get(0).getLong("txnId"),
                            perPartitionTxnIds,
                            clusterCreateTime, drVersion, isRecover);
        } catch (IOException e) {
            throw new VoltAbortException(e);
        } catch (JSONException e) {
            throw new VoltAbortException(e);
        }

        while (results[0].advanceRow()) {
            if (results[0].getString("RESULT").equals("FAILURE")) {
                throw new VoltAbortException("Error distributing export sequence numbers");
            }
        }

        results = performTableRestoreWork(savefile_state, ctx.getSiteTrackerForSnapshot(), isRecover);

        final long endTime = System.currentTimeMillis();
        final double duration = (endTime - startTime) / 1000.0;
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        pw.toString();
        pw.printf("%.2f", duration);
        for (String tableName : m_reportStats.keySet()) {
            SNAP_LOG.info("Table " + tableName + " "
                    + m_reportStats.get(tableName) + " tuples restored from snapshot. (final)");
        }
        m_reportStats.clear();
        m_selectedReportPartition.clear();
        CONSOLE_LOG.info("Finished restore of " + path + " with nonce: "
                + nonce + " in " + sw.toString() + " seconds");
        //        m_sampler.setShouldStop();
        //        try {
        //            m_sampler.join();
        //        } catch (InterruptedException e) {
        //            e.printStackTrace();
        //        }

        /*
         * ENG-1858, make data loaded by snapshot restore durable
         * immediately by starting a truncation snapshot if
         * the command logging is enabled and the database start action
         * was create
         */
        final StartAction startAction = VoltDB.instance().getConfig().m_startAction;
        final org.voltdb.OperationMode mode = VoltDB.instance().getMode();

        /*
         * Is this the start action and no recovery is being performed. The mode
         * will not be INITIALIZING, it will PAUSED or RUNNING. If that is the case,
         * we do want a truncation snapshot if CL is enabled.
         */
        final boolean isStartWithNoAutomatedRestore =
            startAction == StartAction.CREATE && mode != org.voltdb.OperationMode.INITIALIZING;

        final boolean isCLEnabled =
            VoltDB.instance().getCommandLog().getClass().getSimpleName().equals("CommandLogImpl");

        final boolean isStartedWithCreateAction = startAction == StartAction.CREATE;

        if ( isCLEnabled && (isStartedWithCreateAction || isStartWithNoAutomatedRestore)) {

            final ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
            SNAP_LOG.info("Requesting truncation snapshot to make data loaded by snapshot restore durable.");
            zk.create(
                    VoltZK.request_truncation_snapshot_node,
                    null,
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL,
                    new StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx,
                                String name) {
                            if (rc != 0) {
                                KeeperException.Code code = KeeperException.Code.get(rc);
                                SNAP_LOG.warn(
                                        "Don't expect this ZK response when requesting a truncation snapshot "
                                        + code);
                            }
                        }},
                    null);
        }
        return results;
    }

    private void updatePerPartitionTxnIdsToZK(long[] perPartitionTxnIds) throws Exception {
        ZooKeeper zooKeeper = VoltDB.instance().getHostMessenger().getZK();
        Stat stat = zooKeeper.exists(VoltZK.perPartitionTxnIds, false);
        if (stat == null) {
            // Create a new znode.
            ByteBuffer buf = ByteBuffer.allocate(perPartitionTxnIds.length * 8 + 4);
            buf.putInt(perPartitionTxnIds.length);
            for (long txnId : perPartitionTxnIds) {
                buf.putLong(txnId);
            }
            zooKeeper.create(VoltZK.perPartitionTxnIds, buf.array(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            // Znode exists, update data. Make sure perPartitionTxnIds never go back.
            Map<Integer, Long> oldData = new HashMap<>();
            ByteBuffer values = ByteBuffer.wrap(zooKeeper.getData(VoltZK.perPartitionTxnIds, false, stat));
            int numOfKeys = values.getInt();
            for (int i = 0; i < numOfKeys; i++) {
                long txnId = values.getLong();
                int partitionId = TxnEgo.getPartitionId(txnId);
                oldData.put(partitionId, txnId);
            }
            for (long txnId : perPartitionTxnIds) {
                int partitionId = TxnEgo.getPartitionId(txnId);
                Long oldTxnId = oldData.get(partitionId);
                if (oldTxnId == null || oldTxnId < txnId) {
                    oldData.put(partitionId, txnId);
                }
            }
            ByteBuffer buf = ByteBuffer.allocate(oldData.size() * 8 + 4);
            buf.putInt(oldData.size());
            for (Long txnId : oldData.values()) {
                buf.putLong(txnId);
            }
            zooKeeper.setData(VoltZK.perPartitionTxnIds, buf.array(), stat.getVersion());
        }
    }

    private VoltTable[] performDistributeDigestState(
            byte[] exportSequenceNumberBytes,
            long txnId,
            long perPartitionTxnIds[],
            long clusterCreateTime,
            long drVersion,
            boolean isRecover) {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site to confirm the likely
        // success of writing tables to disk
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbers;
        pfs[0].outputDepId = DEP_restoreDistributeExportAndPartitionSequenceNumbers;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(exportSequenceNumberBytes, txnId, perPartitionTxnIds, clusterCreateTime, drVersion, isRecover? 1 : 0);

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbersResults;
        pfs[1].outputDepId = DEP_restoreDistributeExportAndPartitionSequenceNumbersResults;
        pfs[1].inputDepIds = new int[] { DEP_restoreDistributeExportAndPartitionSequenceNumbers };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_restoreDistributeExportAndPartitionSequenceNumbersResults);
        return results;
    }

    private void performRestoreDigeststate(
            SystemProcedureExecutionContext context,
            boolean isRecover,
            long snapshotTxnId,
            long perPartitionTxnIds[],
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers) {

        // Choose the lowest site ID on this host to truncate export data
        if (isRecover && context.isLowestSiteId()) {
            ExportManager.instance().
                    truncateExportToTxnId(snapshotTxnId, perPartitionTxnIds);
        }

        Database db = context.getDatabase();
        Integer myPartitionId = context.getPartitionId();

        //Iterate the export tables
        for (Table t : db.getTables()) {
            if (!CatalogUtil.isTableExportOnly(db, t))
                continue;

            String signature = t.getSignature();
            String name = t.getTypeName();

            //Sequence numbers for this table for every partition
            Map<Integer, Pair<Long, Long>> sequenceNumberPerPartition = exportSequenceNumbers.get(name);
            if (sequenceNumberPerPartition == null) {
                SNAP_LOG.warn("Could not find export sequence number for table " + name +
                        ". This warning is safe to ignore if you are loading a pre 1.3 snapshot" +
                        " which would not contain these sequence numbers (added in 1.3)." +
                        " If this is a post 1.3 snapshot then the restore has failed and export sequence " +
                        " are reset to 0");
                continue;
            }

            Pair<Long, Long> pair =
                    sequenceNumberPerPartition.get(myPartitionId);
            if (pair == null) {
                SNAP_LOG.warn("Could not find an export sequence number for table " + name +
                        " partition " + myPartitionId +
                        ". This warning is safe to ignore if you are loading a pre 1.3 snapshot " +
                        " which would not contain these sequence numbers (added in 1.3)." +
                        " If this is a post 1.3 snapshot then the restore has failed and export sequence " +
                        " are reset to 0");
                continue;
            }
            long uso = pair.getFirst();
            long sequenceNumber = pair.getSecond();

            //Forward the sequence number to the EE
            context.getSiteProcedureConnection().exportAction(
                    true,
                    uso,
                    sequenceNumber,
                    myPartitionId,
                    signature);
        }
    }

    private void performRecoverDigestState(
            SystemProcedureExecutionContext context,
            long snapshotTxnId,
            long perPartitionTxnIds[],
            long clusterCreateTime,
            long drVersion,
            Map<Integer, Long> drSequenceNumbers,
            Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> drMixedClusterSizeConsumerState) {
        // If this is a truncation snapshot restored during recover, try to set DR protocol version
        if (drVersion != 0) {
            context.getSiteProcedureConnection().setDRProtocolVersion((int)drVersion);
        }

        // Choose the lowest site ID so the ClusterCreateTime is only set once in RealVoltDB
        if (context.isLowestSiteId()) {
            VoltDB.instance().setClusterCreateTime(clusterCreateTime);
        }

        //Last seen unique ids from remote data centers, load each local site
        Map<Integer, Map<Integer, DRSiteDrIdTracker>> drMixedClusterSizeConsumerStateForSite =
                drMixedClusterSizeConsumerState.get(context.getPartitionId());
        if (drMixedClusterSizeConsumerStateForSite != null) {
            context.recoverWithDrAppliedTrackers(drMixedClusterSizeConsumerStateForSite);
        }

        Integer myPartitionId = context.getPartitionId();

        Long drSequenceNumber = drSequenceNumbers.get(myPartitionId);
        Long mpDRSequenceNumber = drSequenceNumbers.get(MpInitiator.MP_INIT_PID);
        context.getSiteProcedureConnection().setDRSequenceNumbers(drSequenceNumber, mpDRSequenceNumber);
        if (VoltDB.instance().getNodeDRGateway() != null && context.isLowestSiteId()) {
            VoltDB.instance().getNodeDRGateway().cacheSnapshotRestoreTruncationPoint(drSequenceNumbers);
        }
    }

    private VoltTable constructResultsTable()
    {
        ColumnInfo[] result_columns = new ColumnInfo[7];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo(CNAME_HOST_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo(CNAME_SITE_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("TABLE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo(CNAME_PARTITION_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
        return new VoltTable(result_columns);
    }

    private File getSaveFileForReplicatedTable(String tableName)
    {
        StringBuilder filename_builder = new StringBuilder(m_fileNonce);
        filename_builder.append("-");
        filename_builder.append(tableName);
        filename_builder.append(".vpt");
        return new VoltFile(m_filePath, new String(filename_builder));
    }

    private static File getSaveFileForPartitionedTable(
            String filePath,
            String fileNonce,
            String tableName,
            int originalHostId)
    {
        StringBuilder filename_builder = new StringBuilder(fileNonce);
        filename_builder.append("-");
        filename_builder.append(tableName);
        filename_builder.append("-host_");
        filename_builder.append(originalHostId);
        filename_builder.append(".vpt");
        return new VoltFile(filePath, new String(filename_builder));
    }

    private static TableSaveFile getTableSaveFile(
            File saveFile,
            int readAheadChunks,
            Integer relevantPartitionIds[]) throws IOException
            {
        FileInputStream savefile_input = new FileInputStream(saveFile);
        TableSaveFile savefile =
                new TableSaveFile(
                        savefile_input,
                        readAheadChunks,
                        relevantPartitionIds);
        return savefile;
            }

    /*
     * Block the execution site thread distributing the async mailbox fragment.
     * Has to be done from this thread because it uses the existing plumbing
     * that pops into the EE to do stats periodically and that relies on thread locals
     */
    private final VoltTable[] distributeAsyncMailboxFragment(final long coordinatorHSId) {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        //This fragment causes every ES to generate a mailbox and
        //enter an async run loop to do restore work out of that mailbox
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_restoreAsyncRunLoop;
        pfs[0].outputDepId = DEP_restoreAsyncRunLoop;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(coordinatorHSId);

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_restoreAsyncRunLoopResults;
        pfs[1].outputDepId = DEP_restoreAsyncRunLoopResults;
        pfs[1].inputDepIds = new int[] { DEP_restoreAsyncRunLoop };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        return executeSysProcPlanFragments(pfs, DEP_restoreAsyncRunLoopResults);
    }

    private final VoltTable[] performRestoreScanWork(String filePath, String pathType,
            String fileNonce,
            String dupsPath)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site to confirm the likely
        // success of writing tables to disk
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_restoreScan;
        pfs[0].outputDepId = DEP_restoreScan;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(filePath, pathType, fileNonce, dupsPath);

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_restoreScanResults;
        pfs[1].outputDepId = DEP_restoreScanResults;
        pfs[1].inputDepIds = new int[] { DEP_restoreScan };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_restoreScanResults);
        return results;
    }

    //Keep track of count per table and if replicated take value from first partition result that arrives.
    //Display counts every 1 minute.
    public void reportProgress(String tableName, long count, boolean replicated, int partitionId) {

        if (replicated) {
            synchronized (m_selectedReportPartition) {
                //replicated tables we only process first one who starts reporting.
                Integer reportingPartition = m_selectedReportPartition.get(tableName);
                if (reportingPartition != null && reportingPartition != partitionId) {
                    return;
                }
                m_selectedReportPartition.put(tableName, partitionId);
            }
        }
        AtomicLong counter;
        synchronized (m_reportStats) {
            counter = m_reportStats.get(tableName);
            if (counter == null) {
                counter = new AtomicLong(0);
                m_reportStats.put(tableName, counter);
            }
        }
        if (count != 0) {
            //we add regardless of displaying....final count is displayed at the end.
            count = counter.addAndGet(count);
            long curTime = System.currentTimeMillis();
            if (m_nextReportTime == 0 || curTime > m_nextReportTime) {
                m_nextReportTime = curTime + m_reportInterval;
                SNAP_LOG.info("Table " + tableName + ": " + count
                        + " tuples restored from snapshot. Next progress report at "
                        + m_reportDateFormat.format(new Date(m_nextReportTime)));
            }
        }
    }

    private static class DigestScanResult {
        List<JSONObject> digests;
        Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers;
        Map<Integer, Long> drSequenceNumbers;
        long perPartitionTxnIds[];
        Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> remoteDCLastSeenIds;
        long clusterCreateTime;
        long drVersion;
    }

    private final DigestScanResult performRestoreDigestScanWork(boolean isRecover)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site to confirm the likely
        // success of writing tables to disk
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_restoreDigestScan;
        pfs[0].outputDepId = DEP_restoreDigestScan;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.emptyParameterSet();

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_restoreDigestScanResults;
        pfs[1].outputDepId = DEP_restoreDigestScanResults;
        pfs[1].inputDepIds = new int[] { DEP_restoreDigestScan };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_restoreDigestScanResults);

        HashMap<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers =
                new HashMap<String, Map<Integer, Pair<Long, Long>>>();
        Map<Integer, Long> drSequenceNumbers = new HashMap<>();

        Long digestTxnId = null;
        ArrayList<JSONObject> digests = new ArrayList<JSONObject>();
        Set<Long> perPartitionTxnIds = new HashSet<Long>();
        Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> remoteDCLastSeenIds = new HashMap<>();
        long clusterCreateTime = VoltDB.instance().getHostMessenger().getInstanceId().getTimestamp();
        long drVersion = 0;

        /*
         * Retrieve and aggregate the per table per partition sequence numbers from
         * all the digest files retrieved across the cluster
         */
        try {
            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    throw new VoltAbortException(results[0].getString("ERR_MSG"));
                }
                StringBuilder sb = new StringBuilder();
                sb.append(results[0].getString("DIGEST"));
                while (results[0].getLong("DIGEST_CONTINUED") == 1) {
                    results[0].advanceRow();
                    sb.append(results[0].getString("DIGEST"));
                }
                JSONObject digest = new JSONObject(sb.toString());
                digests.add(digest);

                /*
                 * Validate that the digests are all from the same snapshot
                 */
                if (digestTxnId == null) {
                    digestTxnId = digest.getLong("txnId");
                } else {
                    if (digest.getLong("txnId") != digestTxnId) {
                        throw new VoltAbortException("Retrieved a digest with txnId " + digest.getLong("txnId") +
                                " that doesn't match the txnId seen previously " + digestTxnId + " inspect the digests" +
                                " with the provided nonce and ensure that they are all really from the same snapshot");
                    }
                }

                /*
                 * Validate that command log recovery is from the same cluster
                 */
                if (isRecover) {
                    int clusterId = m_site.getCorrespondingClusterId();
                    if (digest.has("clusterid")) {
                        if (clusterId != digest.getInt("clusterid")) {
                            throw new VoltAbortException("Current clusterId [" + clusterId + "] doesn't match the clusterId [" +
                                    digest.getInt("clusterid") + "] retrieved from the digest, inspect the deployment file to" +
                                    " ensure that cluster id is the same from the time snapshot has taken");
                        }
                    }
                }

                if (digest.has("drVersion")) {
                    drVersion = digest.getLong("drVersion");
                }

                /*
                 * Snapshots from pre 1.3 VoltDB won't have sequence numbers
                 * Doing nothing will default it to zero.
                 */
                if (digest.has(ExtensibleSnapshotDigestData.EXPORT_SEQUENCE_NUMBER_ARR)) {
                    /* An array of entries for each table */
                    boolean warningLogged = false;
                    JSONArray sequenceNumbers = digest.getJSONArray(ExtensibleSnapshotDigestData.EXPORT_SEQUENCE_NUMBER_ARR);
                    for (int ii = 0; ii < sequenceNumbers.length(); ii++) {
                        /*
                         * An object containing all the sequence numbers for its partitions
                         * in this table. This will be a subset since it is from a single digest
                         */
                        JSONObject tableSequenceNumbers = sequenceNumbers.getJSONObject(ii);
                        String tableName = tableSequenceNumbers.getString(ExtensibleSnapshotDigestData.EXPORT_TABLE_NAME);

                        Map<Integer,Pair<Long, Long>> partitionSequenceNumbers =
                                exportSequenceNumbers.get(tableName);
                        if (partitionSequenceNumbers == null) {
                            partitionSequenceNumbers = new HashMap<Integer, Pair<Long, Long>>();
                            exportSequenceNumbers.put(tableName, partitionSequenceNumbers);
                        }

                        /*
                         * Array of objects containing partition and sequence number pairs
                         */
                        JSONArray sourcePartitionSequenceNumbers =
                                tableSequenceNumbers.getJSONArray(ExtensibleSnapshotDigestData.SEQUENCE_NUM_PER_PARTITION);
                        for (int zz = 0; zz < sourcePartitionSequenceNumbers.length(); zz++) {
                            JSONObject obj = sourcePartitionSequenceNumbers.getJSONObject(zz);
                            int partition = obj.getInt(ExtensibleSnapshotDigestData.PARTITION);
                            long sequenceNumber = obj.getInt(ExtensibleSnapshotDigestData.EXPORT_SEQUENCE_NUMBER);
                            long uso = 0;
                            // Snapshots didn't save export USOs pre-8.1
                            if (obj.has(ExtensibleSnapshotDigestData.EXPORT_USO)) {
                                uso = obj.getLong(ExtensibleSnapshotDigestData.EXPORT_USO);
                            } else if (!warningLogged){
                                SNAP_LOG.warn("Could not find export USOs in snapshot. " +
                                        "This warning is safe to ignore if you are loading a pre 8.1 snapshot" +
                                        " which would not contain these USOs (added in 8.1)." +
                                        " If this is a post 8.1 snapshot then the restore has failed and export USOs " +
                                        " are reset to 0");
                                warningLogged = true;
                            }
                            partitionSequenceNumbers.put(partition, new Pair<Long, Long>(uso, sequenceNumber));
                        }
                    }
                }
                if (digest.has("drTupleStreamStateInfo")) {
                    JSONObject stateInfo = digest.getJSONObject("drTupleStreamStateInfo");
                    Iterator<String> keys = stateInfo.keys();
                    while (keys.hasNext()) {
                        String partitionIdString = keys.next();
                        Integer partitionId = Integer.valueOf(partitionIdString);
                        Long oldSequenceNumber = drSequenceNumbers.get(partitionId);
                        Long newSequenceNumber = stateInfo.getJSONObject(partitionIdString).getLong("sequenceNumber");
                        if (oldSequenceNumber == null || newSequenceNumber > oldSequenceNumber) {
                            drSequenceNumbers.put(partitionId, newSequenceNumber);
                        }
                    }
                }

                // Get cluster create time that was recorded in the snapshot
                if (!digests.isEmpty() && digests.get(0).has("clusterCreateTime")) {
                    clusterCreateTime = digests.get(0).getLong("clusterCreateTime");
                }

                if (digest.has("partitionTransactionIds")) {
                    JSONObject partitionTxnIds = digest.getJSONObject("partitionTransactionIds");
                    Iterator<String> keys = partitionTxnIds.keys();
                    while (keys.hasNext()) {
                        perPartitionTxnIds.add(partitionTxnIds.getLong(keys.next()));
                    }
                }

                /*
                 * Extract last seen unique ids from remote data centers into a map
                 * for each DC and return in the result. This will merge and return
                 * the largest ID for each DC and partition
                 */
                if (digest.has("drMixedClusterSizeConsumerState")) {
                    JSONObject consumerPartitions = digest.getJSONObject("drMixedClusterSizeConsumerState");
                    Iterator<String> cpKeys = consumerPartitions.keys();
                    while (cpKeys.hasNext()) {
                        final String consumerPartitionIdStr = cpKeys.next();
                        final Integer consumerPartitionId = Integer.valueOf(consumerPartitionIdStr);
                        JSONObject siteInfo = consumerPartitions.getJSONObject(consumerPartitionIdStr);
                        remoteDCLastSeenIds.put(consumerPartitionId, ExtensibleSnapshotDigestData.buildConsumerSiteDrIdTrackersFromJSON(siteInfo, false));
                    }
                }
            }
        } catch (JSONException e) {
            throw new VoltAbortException(e);
        }

        DigestScanResult result = new DigestScanResult();
        result.digests = digests;
        result.exportSequenceNumbers = exportSequenceNumbers;
        result.drSequenceNumbers = drSequenceNumbers;
        result.perPartitionTxnIds = Longs.toArray(perPartitionTxnIds);
        result.remoteDCLastSeenIds = remoteDCLastSeenIds;
        result.clusterCreateTime = clusterCreateTime;
        result.drVersion = drVersion;
        return result;
    }

    private final byte[] performRestoreHashinatorScanWork(InstanceId iid)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site to confirm the likely
        // success of writing tables to disk
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_restoreHashinatorScan;
        pfs[0].outputDepId = DEP_restoreHashinatorScan;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.emptyParameterSet();

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_restoreHashinatorScanResults;
        pfs[1].outputDepId = DEP_restoreHashinatorScanResults;
        pfs[1].inputDepIds = new int[] { DEP_restoreHashinatorScan };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        /*
         *  Use the first one.
         *  Sanity checks:
         *      - The CRC matches - done by restoreFromBuffer() call.
         *      - All versions are identical.
         *      - The instance IDs match the digest.
         */
        VoltTable[] results = executeSysProcPlanFragments(pfs, DEP_restoreHashinatorScanResults);
        byte[] result = null;
        int ioErrors = 0;
        int iidErrors = 0;
        TreeMap<Long, HashinatorSnapshotData> versions = new TreeMap<Long, HashinatorSnapshotData>();
        while (results[0].advanceRow()) {
            if (results[0].getString("RESULT").equals("FAILURE")) {
                throw new VoltAbortException(results[0].getString("ERR_MSG"));
            }
            ByteBuffer buf = ByteBuffer.wrap(results[0].getVarbinary("HASH"));
            HashinatorSnapshotData hashData = new HashinatorSnapshotData();
            try {
                InstanceId iidSnap = hashData.restoreFromBuffer(buf);
                assert(iidSnap != null);
                buf.clear();
                versions.put(hashData.m_version, hashData);
                if (!iidSnap.equals(iid)) {
                    iidErrors++;
                }
                //Always take the most recent version of the hashinator
                result = versions.lastEntry().getValue().m_serData;
            }
            catch (IOException e) {
                // Skip it and count the failures.
                ioErrors++;
            }
        }
        if (result == null) {
            throw new VoltAbortException(String.format(HASHINATOR_ALL_BAD, "final"));
        }
        if (ioErrors > 0) {
            // Tolerate load failures as long as we have a good one to use.
            SNAP_LOG.warn(String.format("Failed to load %d of %d hashinator snapshot data files.",
                                        ioErrors, results[0].getRowCount()));
        }
        boolean abort = false;
        if (iidErrors > 0) {
            SNAP_LOG.error(String.format("%d hashinator snapshot files have the wrong instance ID.",
                                         iidErrors));
            abort = true;
        }
        if (versions.size() > 1) {
            SNAP_LOG.error(String.format("Expect one version across all hashinator snapshots. "
                                         + "Found %d.",
                                         versions.size()));
            abort = true;
        }
        if (abort) {
            throw new VoltAbortException("Failed to load hashinator snapshot data.");
        }
        return result;
    }

    private final VoltTable[]  performRestoreHashinatorDistributeWork(byte[] hashConfig)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site to confirm the likely
        // success of writing tables to disk
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_restoreDistributeHashinator;
        pfs[0].outputDepId = DEP_restoreDistributeHashinator;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;

        pfs[0].parameters = ParameterSet.fromArrayNoCopy(new Object[]{hashConfig});

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_restoreDistributeHashinatorResults;
        pfs[1].outputDepId = DEP_restoreDistributeHashinatorResults;
        pfs[1].inputDepIds = new int[] { DEP_restoreDistributeHashinator };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        return executeSysProcPlanFragments(pfs, DEP_restoreDistributeHashinatorResults);
    }

    private Set<Table> getTablesToRestore(Set<String> savedTableNames,
                                          StringBuilder commaSeparatedViewNamesToDisable) {
        Set<Table> tables_to_restore = new HashSet<Table>();
        for (Table table : m_database.getTables()) {
            if (savedTableNames.contains(table.getTypeName())) {
                if (CatalogUtil.isSnapshotablePersistentTableView(m_database, table)) {
                    // If the table is a snapshotted persistent table view, we will try to
                    // temporarily disable its maintenance job to boost restore performance.
                    commaSeparatedViewNamesToDisable.append(table.getTypeName()).append(",");
                }
                tables_to_restore.add(table);
            }
            else if (! CatalogUtil.isTableExportOnly(m_database, table)) {
                SNAP_LOG.info("Table: " + table.getTypeName() +
                              " does not have any savefile data and so will not be loaded from disk.");
            }
        }
        if (commaSeparatedViewNamesToDisable.length() > 0) {
            commaSeparatedViewNamesToDisable.setLength(commaSeparatedViewNamesToDisable.length() - 1);
        }
        // XXX consider logging the list of tables that were saved but not
        // in the current catalog
        return tables_to_restore;
    }

    /**
     * Generate a FragmentTaskMessage to instruct the SP sites the pause/resume
     * the view maintenance on specified view tables.
     * @param commaSeparatedViewNames The names of the views that we want to set the flag, concatenated by commas.
     * @param enabled True if want the views enabled, false otherwise.
     * @return The generated FragmentTaskMessage
     */
    private FragmentTaskMessage generateSetViewEnabledMessage(long coordinatorHSId,
                                                              String commaSeparatedViewNames,
                                                              boolean enabled) {
        int enabledAsInt = enabled ? 1 : 0;
        /*
         * The only real data is the fragment id and parameters.
         * Transactions ids, output dep id, readonly-ness, and finality-ness are unused.
         */
        return FragmentTaskMessage.createWithOneFragment(
                        0,            // initiatorHSId
                        coordinatorHSId,
                        0,            // txnId
                        0,            // uniqueId
                        false,        // isReadOnly
                        fragIdToHash(SysProcFragmentId.PF_setViewEnabled), //planHash
                        DEP_setViewEnabled,
                        ParameterSet.fromArrayNoCopy(enabledAsInt, commaSeparatedViewNames),
                        false,        // isFinal
                        m_runner.getTxnState().isForReplay(),
                        false,        // isNPartTxn
                        m_runner.getTxnState().getTimetamp());
    }

    private void verifyRestoreWorkResult(VoltTable[] results, VoltTable[] restore_results) {
        while (results[0].advanceRow()) {
            // this will actually add the active row of results[0]
            restore_results[0].add(results[0]);

            // if any table at any site fails... then the whole proc fails
            if (results[0].getString("RESULT").equalsIgnoreCase("FAILURE")) {
                noteOperationalFailure(RESTORE_FAILED);
            }
        }
    }

    private VoltTable[] performTableRestoreWork(
            final ClusterSaveFileState savefileState,
            final SiteTracker st,
            final boolean isRecover) throws Exception
    {
        /*
         * Create a mailbox to use to send fragment work to execution sites
         */
        final Mailbox m = VoltDB.instance().getHostMessenger().createMailbox();

        /*
         * Create a separate thread to do the work of coordinating the restore
         * while this execution sites's thread (or the MP coordinator in IV2)
         * is blocked in distributing the async mailbox plan fragment. It
         * has to be threaded this way because invoking the async mailbox plan fragment
         * enters the EE to service stats stuff which relies on thread locals.
         */
        ExecutorService es =  Executors.newSingleThreadExecutor(CoreUtils.getThreadFactory("Snapshot Restore"));
        Future<VoltTable[]> ft = es.submit(new Callable<VoltTable[]>() {
            @Override
            public VoltTable[] call() throws Exception {
                int discoveredMailboxes = 0;
                int totalMailboxes = st.m_numberOfExecutionSites;

                /*
                 * First two loops handle picking up the generated mailbox ids
                 * and then distributing the entire map to all sites
                 * so they can convert between actual site ids to mailbox ids
                 * used for restore
                 */
                Map<Long, Long> actualToGenerated = new HashMap<Long, Long>();
                while (discoveredMailboxes < totalMailboxes) {
                    BinaryPayloadMessage bpm = (BinaryPayloadMessage)m.recvBlocking();
                    if (bpm == null) continue;
                    discoveredMailboxes++;
                    ByteBuffer payload = ByteBuffer.wrap(bpm.m_payload);

                    long actualHSId = payload.getLong();
                    long asyncMailboxHSId = payload.getLong();

                    actualToGenerated.put( actualHSId, asyncMailboxHSId);
                }

                ByteBuffer generatedToActualBuf = ByteBuffer.allocate(actualToGenerated.size() * 16);
                for (Map.Entry<Long, Long> e : actualToGenerated.entrySet()) {
                    generatedToActualBuf.putLong(e.getKey());
                    generatedToActualBuf.putLong(e.getValue());
                }

                for (Long generatedHSId : actualToGenerated.values()) {
                   BinaryPayloadMessage bpm =
                           new BinaryPayloadMessage(
                                   new byte[0],
                                   Arrays.copyOf(generatedToActualBuf.array(), generatedToActualBuf.capacity()));
                   m.send(generatedHSId, bpm);
                }

                /*
                 * Wait for all sites to receive the map before sending out fragments
                 * otherwise sites will start sending each other fragments and you can end
                 * up with a site blocked waiting for the map receiving a fragment and not being ready
                 * for it. See ENG-6132
                 */
                int acksReceived = 0;
                while (acksReceived < totalMailboxes) {
                    BinaryPayloadMessage bpm = (BinaryPayloadMessage)m.recvBlocking();
                    if (bpm == null) continue;
                    acksReceived++;
                }

                /*
                 * Do the usual restore planning to generate the plan fragments for execution at each
                 * site
                 */
                StringBuilder commaSeparatedViewNamesToDisable = new StringBuilder();
                Set<Table> tables_to_restore =
                        getTablesToRestore(savefileState.getSavedTableNames(), commaSeparatedViewNamesToDisable);
                VoltTable[] restore_results = new VoltTable[1];
                restore_results[0] = constructResultsTable();
                ArrayList<SynthesizedPlanFragment[]> restorePlans =
                        new ArrayList<SynthesizedPlanFragment[]>();

                // Disable the views before the table restore work starts.
                m.send(Longs.toArray(actualToGenerated.values()),
                       generateSetViewEnabledMessage(m.getHSId(), commaSeparatedViewNamesToDisable.toString(), false));

                for (Table t : tables_to_restore) {
                    TableSaveFileState table_state =
                            savefileState.getTableState(t.getTypeName());
                    table_state.setIsRecover(isRecover);
                    SynthesizedPlanFragment[] restore_plan =
                            table_state.generateRestorePlan( t, st);
                    if (restore_plan == null) {
                        SNAP_LOG.error(
                                "Unable to generate restore plan for " + t.getTypeName() + " table not restored");
                        throw new VoltAbortException(
                                "Unable to generate restore plan for " + t.getTypeName() + " table not restored");
                    }
                    restorePlans.add(restore_plan);
                }

                /*
                 * Now distribute the plan fragments for restoring each table.
                 */
                Iterator<Table> tableIterator = tables_to_restore.iterator();
                VoltTable[] results = null;
                for (SynthesizedPlanFragment[] restore_plan : restorePlans)
                {
                    Table table = tableIterator.next();
                    if (TRACE_LOG.isTraceEnabled()){
                        TRACE_LOG.trace("Performing restore for table: " + table.getTypeName());
                        TRACE_LOG.trace("Plan has fragments: " + restore_plan.length);
                    }
                    for (int ii = 0; ii < restore_plan.length - 1; ii++) {
                        restore_plan[ii].siteId = actualToGenerated.get(restore_plan[ii].siteId);
                    }
                    SNAP_LOG.info("Performing restore for table: " + table.getTypeName());

                    /*
                     * This isn't ye olden executeSysProcPlanFragments. It uses the provided mailbox
                     * and has it's own tiny run loop to process incoming fragments.
                     */
                    results = executeSysProcPlanFragments(restore_plan, m);
                    verifyRestoreWorkResult(results, restore_results);
                }

                // Re-enable the views after the table restore work completes.
                m.send(Longs.toArray(actualToGenerated.values()),
                       generateSetViewEnabledMessage(m.getHSId(), commaSeparatedViewNamesToDisable.toString(), true));

                /*
                 * Send a termination message. This will cause the async mailbox plan fragment to stop
                 * executing allowing the coordinator thread to get back to work.
                 */
                for (long hsid : actualToGenerated.values()) {
                    BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], new byte[0]);
                    m.send(hsid, bpm);
                }

                return restore_results;
            }
        });

        /*
         * Distribute the task of doing the async run loop
         * for restore. It will block on generating the response from the end of the run loop
         * the response doesn't contain any information
         */
        distributeAsyncMailboxFragment(m.getHSId());

        //Wait for the thread that was created to terminate to prevent concurrent access.
        //It should already have finished if distributeAsyncMailboxFragment returned
        //because that means that the term message was sent
        VoltTable restore_results[] =  ft.get();
        es.shutdown();
        es.awaitTermination(365, TimeUnit.DAYS);

        return restore_results;
    }

    // XXX I hacked up a horrible one-off in my world to test this code.
    // I believe that it will work for at least one new node, but
    // there's not a good way to add a unit test for this at the moment,
    // so the emma coverage is weak.
    private VoltTable performDistributeReplicatedTable(
            String tableName,
            SystemProcedureExecutionContext ctx,    // only used in replicated-to-partitioned case
            long siteId,                            // only used in replicated-to-replicated case
            boolean asPartitioned,
            boolean isRecover)
    {
        String hostname = CoreUtils.getHostnameOrAddress();
        TableSaveFile savefile = null;
        try
        {
            savefile =
                    getTableSaveFile(getSaveFileForReplicatedTable(tableName), 3, null);
            assert(savefile.getCompleted());
        }
        catch (IOException e)
        {
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName, -1,
                    "FAILURE", "Unable to load table: " + tableName + " error:\n" + CoreUtils.throwableToString(e));
            return result;
        }

        VoltTable[] results = new VoltTable[] { constructResultsTable() };
        results[0].addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName, -1,
                "SUCCESS", "NO DATA TO DISTRIBUTE");
        final Table new_catalog_table = getCatalogTable(tableName);
        final boolean shouldPreserveDRHiddenColumn =
            DrRoleType.XDCR.value().equals(m_cluster.getDrrole()) && new_catalog_table.getIsdred();
        Boolean needsConversion = null;
        Map<Long, Integer> sites_to_partitions = null;
        int partitionCount = ctx.getNumberOfPartitions();
        Map<Integer, MutableInt> partition_to_siteCount = null;
        TreeMap<Integer, VoltTable> partitioned_table_cache = null;
        if (asPartitioned) {
            partitioned_table_cache = new TreeMap<>();
            partition_to_siteCount = new HashMap<>(partitionCount*2);
            for (int pid=0; pid<partitionCount; pid++) {
                partition_to_siteCount.put(pid, new MutableInt());
            }
            sites_to_partitions = new HashMap<Long, Integer>();
            SiteTracker tracker = ctx.getSiteTrackerForSnapshot();
            sites_to_partitions.putAll(tracker.getSitesToPartitions());
            for (Map.Entry<Long, Integer> e : sites_to_partitions.entrySet()) {
                partition_to_siteCount.get(e.getValue()).increment();
            }
        }

        try {
            while (savefile.hasMoreChunks())
            {
                VoltTable table = null;
                final org.voltcore.utils.DBBPool.BBContainer c = savefile.getNextChunk();
                if (c == null) {
                    continue;   // Should be equivalent to break
                }
                try {
                    if (needsConversion == null) {
                        VoltTable old_table =
                                PrivateVoltTableFactory.createVoltTableFromBuffer(c.b().duplicate(), true);
                        needsConversion = SavedTableConverter.needsConversion(old_table, new_catalog_table,
                                                                              shouldPreserveDRHiddenColumn);
                    }

                    final VoltTable old_table = PrivateVoltTableFactory
                            .createVoltTableFromBuffer(c.b(), true);
                    if (needsConversion) {
                        table = SavedTableConverter.convertTable(old_table, new_catalog_table,
                                                                 shouldPreserveDRHiddenColumn);
                    } else {
                        table = old_table;
                    }

                    Map<Integer, byte[]> partitioned_tables = null;
                    SynthesizedPlanFragment[] pfs = null;
                    if (asPartitioned) {
                        partitioned_tables = createPartitionedTables(
                                tableName, table, partitionCount, partitioned_table_cache);
                        if (partitioned_tables.isEmpty()) {
                            continue;
                        }
                        int depIdCnt = 0;
                        for (int pid : partitioned_tables.keySet()) {
                            depIdCnt += partition_to_siteCount.get(pid).getValue();
                        }
                        int[] dependencyIds = new int[depIdCnt];
                        pfs = new SynthesizedPlanFragment[depIdCnt + 1];

                        int pfs_index = 0;

                        for (long site_id : sites_to_partitions.keySet()) {
                            int partition_id = sites_to_partitions.get(site_id);
                            byte[] tableBytes = partitioned_tables.get(partition_id);
                            if (tableBytes != null) {
                                dependencyIds[pfs_index] = TableSaveFileState.getNextDependencyId();
                                SynthesizedPlanFragment loadFragment = new SynthesizedPlanFragment();
                                loadFragment.fragmentId = SysProcFragmentId.PF_restoreLoadTable;
                                loadFragment.siteId = m_actualToGenerated.get(site_id);
                                loadFragment.multipartition = false;
                                loadFragment.outputDepId = dependencyIds[pfs_index];
                                loadFragment.inputDepIds = new int[] {};
                                loadFragment.parameters = ParameterSet.fromArrayNoCopy(
                                        tableName,
                                        dependencyIds[pfs_index],
                                        tableBytes,
                                        K_CHECK_UNIQUE_VIOLATIONS_PARTITIONED,
                                        new int[] {partition_id},
                                        Boolean.toString(isRecover));

                                pfs[pfs_index++] = loadFragment;
                            }
                        }
                        int result_dependency_id = TableSaveFileState
                                .getNextDependencyId();
                        SynthesizedPlanFragment aggregatorFragment = new SynthesizedPlanFragment();
                        aggregatorFragment.fragmentId = SysProcFragmentId.PF_restoreReceiveResultTables;
                        aggregatorFragment.multipartition = false;
                        aggregatorFragment.outputDepId = result_dependency_id;
                        aggregatorFragment.inputDepIds = dependencyIds;
                        aggregatorFragment.parameters = ParameterSet.fromArrayNoCopy(
                                result_dependency_id,
                                "Received confirmation of successful partitioned-to-replicated table load");
                        pfs[pfs_index] = aggregatorFragment;
                    }
                    else {
                        byte compressedTable[] = TableCompressor.getCompressedTableBytes(table);
                        pfs = new SynthesizedPlanFragment[2];

                        int result_dependency_id = TableSaveFileState.getNextDependencyId();
                        pfs[0] = new SynthesizedPlanFragment();
                        pfs[0].fragmentId = SysProcFragmentId.PF_restoreLoadTable;
                        pfs[0].siteId = m_actualToGenerated.get(siteId);
                        pfs[0].outputDepId = result_dependency_id;
                        pfs[0].inputDepIds = new int[] {};
                        pfs[0].multipartition = false;
                        pfs[0].parameters = ParameterSet.fromArrayNoCopy(
                                tableName, result_dependency_id, compressedTable,
                                K_CHECK_UNIQUE_VIOLATIONS_REPLICATED, null, Boolean.toString(isRecover));

                        int final_dependency_id = TableSaveFileState.getNextDependencyId();
                        pfs[1] = new SynthesizedPlanFragment();
                        pfs[1].fragmentId =
                                SysProcFragmentId.PF_restoreReceiveResultTables;
                        pfs[1].outputDepId = final_dependency_id;
                        pfs[1].inputDepIds = new int[] { result_dependency_id };
                        pfs[1].multipartition = false;
                        pfs[1].parameters = ParameterSet.fromArrayNoCopy(
                                final_dependency_id,
                                "Received confirmation of successful replicated table load at " + siteId);
                        if(TRACE_LOG.isTraceEnabled()){
                            TRACE_LOG.trace("Sending replicated table: " + tableName + " to site id:" +
                                    siteId);
                        }
                    }
                    results = executeSysProcPlanFragments(pfs, m_mbox);
                } finally {
                    c.discard();
                }
            }
        } catch (Exception e) {
            VoltTable result = PrivateVoltTableFactory.createUninitializedVoltTable();
            result = constructResultsTable();
            result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName, -1,
                    "FAILURE", "Unable to load table: " + tableName + " error:\n" + CoreUtils.throwableToString(e));
            return result;
        } finally {
            try {
                savefile.close();
            } catch (IOException e) {
                SNAP_LOG.warn("Error closing table file", e);
            }
        }

        return results[0];
    }

    private VoltTable performDistributePartitionedTable(String tableName,
            int originalHostIds[],
            int relevantPartitionIds[],
            SystemProcedureExecutionContext ctx,
            boolean asReplicated,
            boolean isRecover)
    {
        final int partitionCount = ctx.getNumberOfPartitions();
        String hostname = CoreUtils.getHostnameOrAddress();
        // XXX This is all very similar to the splitting code in
        // LoadMultipartitionTable.  Consider ways to consolidate later
        HashMap<Long, Integer> sites_to_partitions =
                new HashMap<Long, Integer>();
        Map<Integer, MutableInt> partition_to_siteCount = new HashMap<>(partitionCount*2);
        SiteTracker tracker = ctx.getSiteTrackerForSnapshot();
        sites_to_partitions.putAll(tracker.getSitesToPartitions());
        // Keep track of the number of replicas (sites) associated to each partition so we know how many
        // results to expect for each partitioned table we send
        for (Map.Entry<Long, Integer> e : sites_to_partitions.entrySet()) {
            MutableInt targetPartition = partition_to_siteCount.get(e.getValue());
            if (targetPartition != null) {
                targetPartition.increment();
            }
            else {
                partition_to_siteCount.put(e.getValue(), new MutableInt(1));
            }
        }
        assert(partition_to_siteCount.size() == partitionCount);

        try
        {
            initializeTableSaveFiles(
                    m_filePath,
                    m_fileNonce,
                    tableName,
                    originalHostIds,
                    relevantPartitionIds,
                    tracker);
        }
        catch (IOException e)
        {
            synchronized (SnapshotRestore.class) {
                TableSaveFile tsf = null;
                while ((tsf = m_saveFiles.poll()) != null) {
                    try {
                        tsf.close();
                    } catch (Exception ex) {
                        SNAP_LOG.warn("Error closing save files on failure", ex);
                    }
                }
            }
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName, relevantPartitionIds[0],
                    "FAILURE", "Unable to load table: " + tableName + " error:\n" + CoreUtils.throwableToString(e));
            return result;
        }

        Boolean needsConversion = null;
        org.voltcore.utils.DBBPool.BBContainer c = null;
        TreeMap<Integer, VoltTable> partitioned_table_cache = new TreeMap<>();
        SnapshotRestoreResultSet resultSet = new SnapshotRestoreResultSet();
        VoltTable firstResult = null;

        try {
            final Table new_catalog_table = getCatalogTable(tableName);
            final boolean shouldPreserveDRHiddenColumn =
                DrRoleType.XDCR.value().equals(m_cluster.getDrrole()) && new_catalog_table.getIsdred();
            while (hasMoreChunks()) {
                VoltTable table = null;

                c = null;
                c = getNextChunk();
                if (c == null) {
                    continue;//Should be equivalent to break
                }

                // use if will load as partitioned table
                Map<Integer, byte[]> partitioned_tables = null;
                // use if will load as replicated table
                byte compressedTable[] = null;
                int[] dependencyIds = null;
                SynthesizedPlanFragment[] pfs = null;
                try {
                    if (needsConversion == null) {
                        VoltTable old_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b().duplicate(), true);
                        needsConversion = SavedTableConverter.needsConversion(old_table, new_catalog_table,
                                                                              shouldPreserveDRHiddenColumn);
                    }

                    final VoltTable old_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b(), true);
                    if (needsConversion) {
                        table = SavedTableConverter.convertTable(old_table, new_catalog_table,
                                                                 shouldPreserveDRHiddenColumn);
                    } else {
                        table = old_table;
                    }

                    if (asReplicated) {
                        compressedTable = TableCompressor.getCompressedTableBytes(table);
                        dependencyIds = new int[sites_to_partitions.size()];
                        pfs = new SynthesizedPlanFragment[sites_to_partitions.size() + 1];
                    } else {
                        partitioned_tables = createPartitionedTables(tableName, table, partitionCount, partitioned_table_cache);
                        if (partitioned_tables.isEmpty()) {
                            continue;
                        }
                        int depIdCnt = 0;
                        for (int pid : partitioned_tables.keySet()) {
                            depIdCnt += partition_to_siteCount.get(pid).getValue();
                        }
                        dependencyIds = new int[depIdCnt];
                        pfs = new SynthesizedPlanFragment[depIdCnt + 1];
                    }
                } finally {
                    c.discard();
                }

                int pfs_index = 0;
                for (long site_id : sites_to_partitions.keySet())
                {
                    if(asReplicated) {
                        dependencyIds[pfs_index] = TableSaveFileState.getNextDependencyId();
                        SynthesizedPlanFragment loadFragment = new SynthesizedPlanFragment();
                        loadFragment.fragmentId = SysProcFragmentId.PF_restoreLoadTable;
                        loadFragment.siteId = m_actualToGenerated.get(site_id);
                        loadFragment.multipartition = false;
                        loadFragment.outputDepId = dependencyIds[pfs_index];
                        loadFragment.inputDepIds = new int [] {};
                        loadFragment.parameters = ParameterSet.fromArrayNoCopy(
                                tableName,
                                dependencyIds[pfs_index],
                                compressedTable,
                                K_CHECK_UNIQUE_VIOLATIONS_REPLICATED,
                                relevantPartitionIds,
                                Boolean.toString(isRecover));
                        pfs[pfs_index++] = loadFragment;
                    } else {
                        int partition_id = sites_to_partitions.get(site_id);
                        byte[] tableBytes = partitioned_tables.get(partition_id);
                        if (tableBytes != null) {
                            dependencyIds[pfs_index] = TableSaveFileState.getNextDependencyId();
                            SynthesizedPlanFragment loadFragment = new SynthesizedPlanFragment();
                            loadFragment.fragmentId = SysProcFragmentId.PF_restoreLoadTable;
                            loadFragment.siteId = m_actualToGenerated.get(site_id);
                            loadFragment.multipartition = false;
                            loadFragment.outputDepId = dependencyIds[pfs_index];
                            loadFragment.inputDepIds = new int [] {};
                            loadFragment.parameters = ParameterSet.fromArrayNoCopy(
                                    tableName,
                                    dependencyIds[pfs_index],
                                    tableBytes,
                                    K_CHECK_UNIQUE_VIOLATIONS_PARTITIONED,
                                    new int[] {partition_id},
                                    Boolean.toString(isRecover));
                            pfs[pfs_index++] = loadFragment;
                        }
                    }
                }
                int result_dependency_id = TableSaveFileState.getNextDependencyId();
                SynthesizedPlanFragment aggregatorFragment = new SynthesizedPlanFragment();
                aggregatorFragment.fragmentId =
                        SysProcFragmentId.PF_restoreReceiveResultTables;
                aggregatorFragment.multipartition = false;
                aggregatorFragment.outputDepId = result_dependency_id;
                aggregatorFragment.inputDepIds = dependencyIds;
                if(asReplicated) {
                    aggregatorFragment.parameters = ParameterSet.fromArrayNoCopy(
                            result_dependency_id,
                            "Received confirmation of successful partitioned-to-replicated table load");
                } else {
                    aggregatorFragment.parameters = ParameterSet.fromArrayNoCopy(
                            result_dependency_id,
                            "Received confirmation of successful partitioned-to-partitioned table load");
                }
                assert(pfs.length == pfs_index+1);
                pfs[pfs_index] = aggregatorFragment;
                VoltTable[] results = executeSysProcPlanFragments(pfs, m_mbox);
                VoltTable vt = results[0];
                if (firstResult == null) {
                    firstResult = vt;
                }
                while (vt.advanceRow()) {
                    resultSet.parseRestoreResultRow(vt);
                }
            }
        } catch (Exception e) {
            VoltTable result = PrivateVoltTableFactory.createUninitializedVoltTable();
            result = constructResultsTable();
            result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName,
                    relevantPartitionIds[0], "FAILURE",
                    "Unable to load table: " + tableName + " error:\n" + CoreUtils.throwableToString(e));
            return result;
        } finally {
            synchronized (SnapshotRestore.class) {
                TableSaveFile tsf = null;
                while ((tsf = m_saveFiles.poll()) != null) {
                    try {
                        tsf.close();
                    } catch (Exception e) {
                        SNAP_LOG.warn("Error closing save files on failure", e);
                    }
                }
            }
        }

        VoltTable result = null;
        if (!resultSet.isEmpty()) {
            result = new VoltTable(VoltTableUtil.extractTableSchema(firstResult));
            result.setStatusCode(firstResult.getStatusCode());
            for (RestoreResultKey key : resultSet.keySet()) {
                resultSet.addRowsForKey(key, result);
            }
        }
        else {
            result = constructResultsTable();
            result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName, 0,
                    "SUCCESS", "NO DATA TO DISTRIBUTE");
        }
        return result;
    }

    private HashMap<Integer, byte[]> createPartitionedTables(String tableName, VoltTable loadedTable,
                int number_of_partitions, TreeMap<Integer, VoltTable> partitioned_table_cache) throws Exception
    {
        Table catalog_table = m_database.getTables().getIgnoreCase(tableName);
        assert(!catalog_table.getIsreplicated());
        // XXX blatantly stolen from LoadMultipartitionTable
        // find the index and type of the partitioning attribute
        int partition_col;
        VoltType partition_type;
        Table viewSource = catalog_table.getMaterializer();
        if (viewSource != null && CatalogUtil.isTableExportOnly(m_database, viewSource)) {
            String pname = viewSource.getPartitioncolumn().getName();
            //Get partition column name and find index and type in view table
            Column c = catalog_table.getColumns().get(pname);
            if (c != null) {
                partition_col = c.getIndex();
                partition_type = VoltType.get((byte) c.getType());
            } else {
                // Bad table in snapshot it should not be present.
                SNAP_LOG.error("Bad table in snapshot, export view without partitioning column in group by should not be in snapshot.");
                throw new RuntimeException("Bad table in snapshot, export view without partitioning column in group by should not be in snapshot.");
            }
        } else {
            partition_col = catalog_table.getPartitioncolumn().getIndex();
            partition_type = VoltType.get((byte) catalog_table.getPartitioncolumn().getType());
        }
        final VoltType partitionParamType = VoltType.get(partition_type.getValue());

        // create a table for each partition
        VoltTable[] partitioned_tables = new VoltTable[number_of_partitions];
        HashSet<Integer> usedPartitions = new HashSet<>(number_of_partitions*2);
        TheHashinator hashinator = TheHashinator.getCurrentHashinator();
        // split the input table into per-partition units
        while (loadedTable.advanceRow())
        {
            int partition = 0;
            try
            {
                partition = hashinator.getHashedPartitionForParameter(partitionParamType,
                        loadedTable.get(partition_col, partition_type));
            }
            catch (Exception e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            VoltTable cacheTable = partitioned_tables[partition];
            if (cacheTable == null) {
                if (partitioned_table_cache.isEmpty()) {
                    // create a table that is the chunk size divided by partition count
                    // then add a 1.3x buffer to eliminate a bunch of extra allocations when the
                    //  partitioning isn't perfectly even.
                    int partitionedTableSize =
                            (int)(1.3 * PrivateVoltTableFactory.getUnderlyingBufferSize(loadedTable) /
                            number_of_partitions);
                    // next optimization to make is to keep these buffers around for more than one
                    //  chunk... or make clone use a pooled buffer... something like that to reduce
                    //  allocations.
                    cacheTable = loadedTable.clone(partitionedTableSize);
                }
                else {
                    // Get the largest buffer first under the assumption that if partitioning has
                    // not changed all rows will be copied into this first buffer.
                    Map.Entry<Integer, VoltTable> e = partitioned_table_cache.lastEntry();
                    partitioned_table_cache.remove(e.getKey());
                    cacheTable = e.getValue();
                    cacheTable.clearRowData();
                }
                partitioned_tables[partition] = cacheTable;
                usedPartitions.add(partition);
            }
            // this adds the active row of loadedTable
            cacheTable.add(loadedTable);
        }
        HashMap<Integer, byte[]> repartitionedTables = new HashMap<>(usedPartitions.size()*2);
        for (int pid : usedPartitions) {
            VoltTable tab = partitioned_tables[pid];
            partitioned_table_cache.put(tab.getBuffer().capacity(), tab);
            byte compressedTable[] = TableCompressor.getCompressedTableBytes(tab);
            repartitionedTables.put(pid, compressedTable);
        }
        return repartitionedTables;
    }

    private Table getCatalogTable(String tableName)
    {
        return m_database.getTables().get(tableName);
    }

    /*
     * Do parameter checking for the pre-JSON version of @SnapshotRestore old version
     */
    public static ClientResponseImpl transformRestoreParamsToJSON(StoredProcedureInvocation task) {
        Object params[] = task.getParams().toArray();
        if (params.length == 1) {
            return null;
        } else if (params.length == 2) {
            if (params[0] == null) {
                return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                        new VoltTable[0],
                        "@SnapshotRestore parameter 0 was null",
                        task.getClientHandle());
            }
            if (params[1] == null) {
                return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                        new VoltTable[0],
                        "@SnapshotRestore parameter 1 was null",
                        task.getClientHandle());
            }
            if (!(params[0] instanceof String)) {
                return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                        new VoltTable[0],
                        "@SnapshotRestore param 0 (path) needs to be a string, but was type "
                        + params[0].getClass().getSimpleName(),
                        task.getClientHandle());
            }
            if (!(params[1] instanceof String)) {
                return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                        new VoltTable[0],
                        "@SnapshotRestore param 1 (nonce) needs to be a string, but was type "
                        + params[1].getClass().getSimpleName(),
                        task.getClientHandle());
            }
            JSONObject jsObj = new JSONObject();
            try {
                jsObj.put(SnapshotUtil.JSON_PATH, params[0]);
                jsObj.put(SnapshotUtil.JSON_PATH_TYPE, SnapshotPathType.SNAP_PATH);
                jsObj.put(SnapshotUtil.JSON_NONCE, params[1]);
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
            task.setParams( jsObj.toString() );
            return null;
        } else {
            return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    "@SnapshotRestore supports a single json document parameter or two parameters (path, nonce), " +
                    params.length + " parameters provided",
                    task.getClientHandle());
        }
    }

    /*
     * A helper method for snapshot restore that manages a mailbox run loop and dependency tracking.
     * The mailbox is a dedicated mailbox for snapshot restore. This assumes a very specific plan fragment
     * worklow where fragments 0 - (N - 1) all have a single output dependency that is aggregated
     * by fragment N which uses their output dependencies as it's input dependencies.
     *
     * This matches the workflow of snapshot restore
     *
     * This is not safe to use after restore because it doesn't do failure handling that would deal with
     * dropped plan fragments
     */
    public VoltTable[] executeSysProcPlanFragments(SynthesizedPlanFragment pfs[], Mailbox m) {
        Set<Integer> dependencyIds = new HashSet<Integer>();
        VoltTable results[] = new VoltTable[1];

        /*
         * Iterate the plan fragments and distribute them. Each
         * plan fragment goes to an individual site.
         * The output dependency of each fragment is added to the
         * set of expected dependencies
         */
        for (int ii = 0; ii < pfs.length - 1; ii++) {
            SynthesizedPlanFragment pf = pfs[ii];
            dependencyIds.add(pf.outputDepId);

            if(TRACE_LOG.isTraceEnabled()){
                TRACE_LOG.trace(
                        "Sending fragment " + pf.fragmentId + " dependency " + pf.outputDepId +
                        " from " + CoreUtils.hsIdToString(m.getHSId()) + "-" +
                        CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()) + " to " +
                        CoreUtils.hsIdToString(pf.siteId));
            }
            /*
             * The only real data is the fragment id, output dep id,
             * and parameters. Transactions ids, readonly-ness, and finality-ness
             * are unused.
             */
            FragmentTaskMessage ftm =
                    FragmentTaskMessage.createWithOneFragment(
                            0,
                            m.getHSId(),
                            0,
                            0,
                            false,
                            fragIdToHash(pf.fragmentId),
                            pf.outputDepId,
                            pf.parameters,
                            false,
                            m_runner.getTxnState().isForReplay(),
                            false,
                            m_runner.getTxnState().getTimetamp());
            m.send(pf.siteId, ftm);
        }

        /*
         * Track the received dependencies. Stored as a list because executePlanFragment for
         * the aggregator plan fragment expects the tables as a list in the dependency map,
         * but sysproc fragments only every have a single output dependency.
         */
        Map<Integer, List<VoltTable>> receivedDependencyIds = new HashMap<Integer, List<VoltTable>>();

        /*
         * This loop will wait for all the responses to the fragment that was sent out,
         * but will also respond to incoming fragment tasks by executing them.
         */
        while (true) {
            //Lightly spinning makes debugging easier by allowing inspection
            //of stuff on the stack
            VoltMessage vm = m.recvBlocking(1000);
            if (vm == null) continue;

            if (vm instanceof FragmentTaskMessage) {
                FragmentTaskMessage ftm = (FragmentTaskMessage)vm;
                DependencyPair dp =
                        m_runner.executeSysProcPlanFragment(
                                m_runner.getTxnState(),
                                null,
                                hashToFragId(ftm.getPlanHash(0)),
                                ftm.getParameterSetForFragment(0));
                FragmentResponseMessage frm = new FragmentResponseMessage(ftm, m.getHSId());
                frm.addDependency(dp);
                m.send(ftm.getCoordinatorHSId(), frm);

                if (!m_unexpectedDependencies.isEmpty()) {
                    for (Integer dependencyId : dependencyIds) {
                        if (m_unexpectedDependencies.containsKey(dependencyId)) {
                            receivedDependencyIds.put(dependencyId, m_unexpectedDependencies.remove(dependencyId));
                        }
                    }

                    /*
                     * This predicate exists below in FRM handling, they have to match
                     */
                    if (receivedDependencyIds.size() == dependencyIds.size() &&
                            receivedDependencyIds.keySet().equals(dependencyIds)) {
                        break;
                    }
                }
            } else if (vm instanceof FragmentResponseMessage) {
                FragmentResponseMessage frm = (FragmentResponseMessage)vm;
                final int dependencyId = frm.getTableDependencyIdAtIndex(0);
                if (dependencyIds.contains(dependencyId)) {
                    receivedDependencyIds.put(
                            dependencyId,
                            Arrays.asList(new VoltTable[] {frm.getTableAtIndex(0)}));
                    if(TRACE_LOG.isTraceEnabled()){
                        TRACE_LOG.trace("Received dependency at " + CoreUtils.hsIdToString(m.getHSId()) +
                                "-" + CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()) +
                                " from " + CoreUtils.hsIdToString(frm.m_sourceHSId) +
                                " have " + receivedDependencyIds.size() + " " + receivedDependencyIds.keySet() +
                                " and need " + dependencyIds.size() + " " + dependencyIds);
                    }
                    /*
                     * This predicate exists above in FTM handling, they have to match
                     */
                    if (receivedDependencyIds.size() == dependencyIds.size() &&
                            receivedDependencyIds.keySet().equals(dependencyIds)) {
                        break;
                    }
                } else {
                    /*
                     * Stash the dependency intended for a different fragment
                     */
                    if (m_unexpectedDependencies.put(
                            dependencyId,
                            Arrays.asList(new VoltTable[] {frm.getTableAtIndex(0)})) != null) {
                        VoltDB.crashGlobalVoltDB("Received a duplicate dependency", true, null);
                    }
                }
            }
        }

        /*
         * Executing the last aggregator plan fragment in the list produces the result
         */
        results[0] =
                m_runner.executeSysProcPlanFragment(
                        m_runner.getTxnState(),
                        receivedDependencyIds,
                        pfs[pfs.length - 1].fragmentId,
                        pfs[pfs.length - 1].parameters).getTableDependency();
        return results;
    }

    /*
     * Ariel's comment:
     * When restoring replicated tables I found that a single site can receive multiple fragments instructing it to
     * distribute a replicated table. It processes each fragment causing it to enter executeSysProcPlanFragments
     * twice. Each time it enters it has generated a task for some other site, and is waiting on dependencies.
     *
     * The problem is that they will come back out of order, the dependency for the first task comes while
     * the site is waiting for the dependency of the second task. When the second dependency arrives we fail
     * to drop out because of extra/mismatches dependencies.
     *
     * The solution is to recognize unexpected dependencies, stash them away, and then check for them each time
     * we finish running a plan fragment. This doesn't allow you to process the dependencies immediately
     * (Continuations anyone?), but it doesn't deadlock and is good enough for restore.
     */
    private final Map<Integer, List<VoltTable>> m_unexpectedDependencies =
            new HashMap<Integer, List<VoltTable>>();

    private Mailbox m_mbox;
    private final Map<Long, Long> m_actualToGenerated = new HashMap<Long, Long>();
    private Database m_database;
    private long m_siteId;
    private int m_hostId;
    private static volatile String m_filePath;
    private static volatile String m_filePathType;
    private static volatile String m_fileNonce;
}
