/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.zk.ZKUtil.StringCallback;
import org.voltdb.ClientResponseImpl;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.ProcInfo;
import org.voltdb.StartAction;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.VoltZK;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.export.ExportManager;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.sysprocs.saverestore.ClusterSaveFileState;
import org.voltdb.sysprocs.saverestore.DuplicateRowHandler;
import org.voltdb.sysprocs.saverestore.SavedTableConverter;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.TableSaveFile;
import org.voltdb.sysprocs.saverestore.TableSaveFileState;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.VoltFile;
import org.voltdb.utils.VoltTableUtil;

import com.google.common.base.Throwables;
import com.google.common.primitives.Longs;

@ProcInfo (
        singlePartition = false
        )
public class SnapshotRestore extends VoltSystemProcedure
{
    private static final VoltLogger TRACE_LOG = new VoltLogger(SnapshotRestore.class.getName());

    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");
    private static final VoltLogger CONSOLE_LOG = new VoltLogger("CONSOLE");

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

    private static HashSet<String>  m_initializedTableSaveFileNames = new HashSet<String>();
    private static ArrayDeque<TableSaveFile> m_saveFiles = new ArrayDeque<TableSaveFile>();

    private static volatile DuplicateRowHandler m_duplicateRowHandler = null;

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
                    st.getLocalSites().length * 4,
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
    public void init()
    {
        registerPlanFragment(SysProcFragmentId.PF_restoreScan);
        registerPlanFragment(SysProcFragmentId.PF_restoreScanResults);
        registerPlanFragment(SysProcFragmentId.PF_restoreDigestScan);
        registerPlanFragment(SysProcFragmentId.PF_restoreDigestScanResults);
        registerPlanFragment(SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbers);
        registerPlanFragment(SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbersResults);
        registerPlanFragment(SysProcFragmentId.PF_restoreAsyncRunLoop);
        registerPlanFragment(SysProcFragmentId.PF_restoreAsyncRunLoopResults);
        registerPlanFragment(SysProcFragmentId.PF_restoreLoadTable);
        registerPlanFragment(SysProcFragmentId.PF_restoreReceiveResultTables);
        registerPlanFragment(SysProcFragmentId.PF_restoreLoadReplicatedTable);
        registerPlanFragment(SysProcFragmentId.PF_restoreDistributeReplicatedTableAsReplicated);
        registerPlanFragment(SysProcFragmentId.PF_restoreDistributePartitionedTableAsPartitioned);
        registerPlanFragment(SysProcFragmentId.PF_restoreDistributePartitionedTableAsReplicated);
        registerPlanFragment(SysProcFragmentId.PF_restoreDistributeReplicatedTableAsPartitioned);
        m_siteId = CoreUtils.getSiteIdFromHSId(m_site.getCorrespondingSiteId());
        m_hostId = m_site.getCorrespondingHostId();
        // XXX HACK GIANT HACK given the current assumption that there is
        // only one database per cluster, I'm asserting this and then
        // skirting around the need to have the database name in order to get
        // to the set of tables. --izzy
        assert(m_cluster.getDatabases().size() == 1);
        m_database = m_cluster.getDatabases().get("database");
    }

    @Override
    public DependencyPair
    executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId, ParameterSet params,
            SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbers)
        {
            assert(params.toArray()[0] != null);
            assert(params.toArray().length == 3);
            assert(params.toArray()[0] instanceof byte[]);
            assert(params.toArray()[2] instanceof long[]);
            VoltTable result = new VoltTable(new VoltTable.ColumnInfo("RESULT", VoltType.STRING));
            long snapshotTxnId = ((Long)params.toArray()[1]).longValue();
            long perPartitionTxnIds[] = (long[])params.toArray()[2];

            /*
             * Use the per partition txn ids to set the initial txnid value from the snapshot
             * All the values are sent in, but only the one for the appropriate partition
             * will be used
             */
            context.getSiteProcedureConnection().setPerPartitionTxnIds(perPartitionTxnIds);

            // Choose the lowest site ID on this host to truncate export data
            if (context.isLowestSiteId())
            {
                ExportManager.instance().
                truncateExportToTxnId(snapshotTxnId, perPartitionTxnIds);
            }
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream((byte[])params.toArray()[0]);
                ObjectInputStream ois = new ObjectInputStream(bais);

                //Sequence numbers for every table and partition
                @SuppressWarnings("unchecked")
                Map<String, Map<Integer, Long>> exportSequenceNumbers =
                        (Map<String, Map<Integer, Long>>)ois.readObject();
                Database db = context.getDatabase();
                Integer myPartitionId = context.getPartitionId();

                //Iterate the export tables
                for (Table t : db.getTables()) {
                    if (!CatalogUtil.isTableExportOnly( db, t))
                        continue;

                    String signature = t.getSignature();
                    String name = t.getTypeName();

                    //Sequence numbers for this table for every partition
                    Map<Integer, Long> sequenceNumberPerPartition = exportSequenceNumbers.get(name);
                    if (sequenceNumberPerPartition == null) {
                        SNAP_LOG.warn("Could not find export sequence number for table " + name +
                                ". This warning is safe to ignore if you are loading a pre 1.3 snapshot" +
                                " which would not contain these sequence numbers (added in 1.3)." +
                                " If this is a post 1.3 snapshot then the restore has failed and export sequence " +
                                " are reset to 0");
                        continue;
                    }

                    Long sequenceNumber =
                            sequenceNumberPerPartition.get(myPartitionId);
                    if (sequenceNumber == null) {
                        SNAP_LOG.warn("Could not find an export sequence number for table " + name +
                                " partition " + myPartitionId +
                                ". This warning is safe to ignore if you are loading a pre 1.3 snapshot " +
                                " which would not contain these sequence numbers (added in 1.3)." +
                                " If this is a post 1.3 snapshot then the restore has failed and export sequence " +
                                " are reset to 0");
                        continue;
                    }
                    //Forward the sequence number to the EE
                    context.getSiteProcedureConnection().exportAction(
                            true,
                            0,
                            sequenceNumber,
                            myPartitionId,
                            signature);
                }
            } catch (Exception e) {
                e.printStackTrace();//l4j doesn't print the stack trace
                SNAP_LOG.error(e);
                result.addRow("FAILURE");
            }
            return new DependencyPair(DEP_restoreDistributeExportAndPartitionSequenceNumbers, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbersResults)
        {
            TRACE_LOG.trace("Aggregating digest scan state");
            assert(dependencies.size() > 0);
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_restoreDistributeExportAndPartitionSequenceNumbers));
            return new DependencyPair(DEP_restoreDistributeExportAndPartitionSequenceNumbersResults, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDigestScan)
        {
            VoltTable result = new VoltTable(
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
                    TRACE_LOG.trace("Checking saved table digest state for restore of: "
                            + m_filePath + ", " + m_fileNonce);
                    List<JSONObject> digests =
                            SnapshotUtil.retrieveDigests(m_filePath, m_fileNonce, SNAP_LOG);

                    for (JSONObject obj : digests) {
                        result.addRow(obj.toString(), "SUCCESS", null);
                    }
                } catch (Exception e) {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    pw.flush();
                    e.printStackTrace();//l4j doesn't print stack traces
                    SNAP_LOG.error(e);
                    result.addRow(null, "FAILURE", sw.toString());
                    return new DependencyPair(DEP_restoreDigestScan, result);
                }
            }
            return new DependencyPair(DEP_restoreDigestScan, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDigestScanResults)
        {
            TRACE_LOG.trace("Aggregating digest scan state");
            assert(dependencies.size() > 0);
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_restoreDigestScan));
            return new DependencyPair(DEP_restoreDigestScanResults, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreScan)
        {
            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            String hostname = CoreUtils.getHostnameOrAddress();
            VoltTable result = ClusterSaveFileState.constructEmptySaveFileStateVoltTable();
            // Choose the lowest site ID on this host to do the file scan
            // All other sites should just return empty results tables.
            if (context.isLowestSiteId())
            {
                /*
                 * Initialize a duplicate row handling policy for this restore
                 */
                m_duplicateRowHandler = null;
                if (params.toArray()[2] != null) {
                    m_duplicateRowHandler =
                            new DuplicateRowHandler(
                                    (String)params.toArray()[2],
                                    getTransactionTime());
                }

                // implicitly synchronized by the way restore operates.
                // this scan must complete on every site and return results
                // to the coordinator for aggregation before it will send out
                // distribution fragments, so two sites on the same node
                // can't be attempting to set and clear this HashSet simultaneously
                m_initializedTableSaveFileNames.clear();
                m_saveFiles.clear();//Tests will reused a VoltDB process that fails a restore

                m_filePath = (String) params.toArray()[0];
                m_fileNonce = (String) params.toArray()[1];
                TRACE_LOG.trace("Checking saved table state for restore of: "
                        + m_filePath + ", " + m_fileNonce);
                File[] savefiles = SnapshotUtil.retrieveRelevantFiles(m_filePath, m_fileNonce);
                if (savefiles == null) {
                    return new DependencyPair(DEP_restoreScan, result);
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
                                result.addRow(m_hostId,
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
            return new DependencyPair(DEP_restoreScan, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreScanResults)
        {
            TRACE_LOG.trace("Aggregating saved table state");
            assert(dependencies.size() > 0);
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_restoreScan));
            return new DependencyPair(DEP_restoreScanResults, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreAsyncRunLoop)
        {
            Object paramsArray[] = params.toArray();
            assert(paramsArray.length == 1);
            assert(paramsArray[0] instanceof Long);
            long coordinatorHSId = (Long)paramsArray[0];
            Mailbox m = VoltDB.instance().getHostMessenger().createMailbox();
            m_mbox = m;
            TRACE_LOG.trace(
                    "Entering async run loop at " + CoreUtils.hsIdToString(context.getSiteId()) +
                    " listening on mbox " + CoreUtils.hsIdToString(m.getHSId()));

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

            /*
             * Loop until the termination signal is received. Execute any plan fragments that
             * are received
             */
            while (true) {
                VoltMessage vm = m.recvBlocking(1000);
                if (vm == null) continue;

                if (vm instanceof FragmentTaskMessage) {
                    FragmentTaskMessage ftm = (FragmentTaskMessage)vm;
                    TRACE_LOG.trace(
                            CoreUtils.hsIdToString(context.getSiteId()) + " received fragment id " +
                    VoltSystemProcedure.hashToFragId(ftm.getPlanHash(0)));
                    DependencyPair dp =
                            m_runner.executeSysProcPlanFragment(
                                    m_runner.getTxnState(),
                                    null,
                                    VoltSystemProcedure.hashToFragId(ftm.getPlanHash(0)),
                                    ftm.getParameterSetForFragment(0));
                    FragmentResponseMessage frm = new FragmentResponseMessage(ftm, m.getHSId());
                    frm.addDependency(dp.depId, dp.dependency);
                    m.send(ftm.getCoordinatorHSId(), frm);
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
                    return new DependencyPair( DEP_restoreAsyncRunLoop, constructResultsTable());
                }
            }
        } else if (fragmentId == SysProcFragmentId.PF_restoreAsyncRunLoopResults) {
            return new DependencyPair(DEP_restoreAsyncRunLoopResults, constructResultsTable());
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

            if(checkUniqueViolations == K_CHECK_UNIQUE_VIOLATIONS_PARTITIONED) {
                assert(partition_ids != null && partition_ids.length == 1);
            }

            TRACE_LOG.trace("Received table: " + table_name +
                    (partition_ids == null ? "[REPLICATED]" : "of partition [" + partition_ids.toString()) + "]");

            String result_str = "SUCCESS";
            String error_msg = "";
            try {
                    VoltTable table = PrivateVoltTableFactory.createVoltTableFromBuffer(
                                    ByteBuffer.wrap(CompressionService.decompressBytes(compressedTable)), true);

                    byte uniqueViolations[] =
                            voltLoadTable(context.getCluster().getTypeName(),
                                          context.getDatabase().getTypeName(),
                                          table_name,
                                          table,
                                          m_duplicateRowHandler != null);
                    handleUniqueViolations(table_name, uniqueViolations, checkUniqueViolations, context);
            } catch (Exception e) {
                result_str = "FAILURE";
                error_msg = e.getMessage();
            }
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, CoreUtils.getHostnameOrAddress(), CoreUtils.getSiteIdFromHSId(m_siteId), table_name,
                            ((checkUniqueViolations == K_CHECK_UNIQUE_VIOLATIONS_PARTITIONED) ? partition_ids[0] : -1),
                            result_str, error_msg);
            return new DependencyPair(dependency_id, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreReceiveResultTables) {
            assert (params.toArray()[0] != null);
            assert (params.toArray()[1] != null);
            int dependency_id = (Integer) params.toArray()[0];
            String tracingLogMsg = (String) params.toArray()[1];

            TRACE_LOG.trace(tracingLogMsg);

            List<VoltTable> table_list = new ArrayList<VoltTable>();
            for (int dep_id : dependencies.keySet())
            {
                table_list.addAll(dependencies.get(dep_id));
            }
            assert(table_list.size() == dependencies.size());
            VoltTable result = VoltTableUtil.unionTables(table_list);
            return new DependencyPair(dependency_id, result);
        }

        else if (fragmentId == SysProcFragmentId.PF_restoreLoadReplicatedTable)
        {
            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            String table_name = (String) params.toArray()[0];
            int dependency_id = (Integer) params.toArray()[1];
            TRACE_LOG.trace("Loading replicated table: " + table_name);
            String result_str = "SUCCESS";
            String error_msg = "";
            TableSaveFile savefile = null;

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
                result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), table_name, -1, "FAILURE",
                        "Unable to load table: " + table_name +
                        " error: " + e.getMessage());
                return new DependencyPair(dependency_id, result);
            }

            try {
                final Table new_catalog_table = getCatalogTable(table_name);
                Boolean needsConversion = null;
                while (savefile.hasMoreChunks())
                {
                    VoltTable table = null;

                    final org.voltcore.utils.DBBPool.BBContainer c = savefile.getNextChunk();
                    if (c == null) {
                        continue;//Should be equivalent to break
                    }

                    if (needsConversion == null) {
                        VoltTable old_table =
                                PrivateVoltTableFactory.createVoltTableFromBuffer(c.b.duplicate(), true);
                        needsConversion = SavedTableConverter.needsConversion(old_table, new_catalog_table);
                    }

                    if (needsConversion.booleanValue()) {
                        VoltTable old_table =
                                PrivateVoltTableFactory.createVoltTableFromBuffer(c.b , true);
                        table = SavedTableConverter.convertTable(old_table,
                                new_catalog_table);
                    } else {
                        ByteBuffer copy = ByteBuffer.allocate(c.b.remaining());
                        copy.put(c.b);
                        copy.flip();
                        table = PrivateVoltTableFactory.createVoltTableFromBuffer(copy, true);
                    }
                    c.discard();

                    try
                    {
                        byte uniqueViolations[] = voltLoadTable(context.getCluster().getTypeName(),
                                context.getDatabase().getTypeName(),
                                table_name, table, m_duplicateRowHandler != null);
                        handleUniqueViolations(table_name,
                                               uniqueViolations,
                                               K_CHECK_UNIQUE_VIOLATIONS_REPLICATED,
                                               context);
                    }
                    catch (Exception e)
                    {
                        result_str = "FAILURE";
                        error_msg = e.getMessage();
                        break;
                    }
                }

            } catch (IOException e) {
                String hostname = CoreUtils.getHostnameOrAddress();
                VoltTable result = constructResultsTable();
                result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), table_name, -1, "FAILURE",
                        "Unable to load table: " + table_name +
                        " error: " + e.getMessage());
                return new DependencyPair(dependency_id, result);
            } catch (VoltTypeException e) {
                String hostname = CoreUtils.getHostnameOrAddress();
                VoltTable result = constructResultsTable();
                result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), table_name, -1, "FAILURE",
                        "Unable to load table: " + table_name +
                        " error: " + e.getMessage());
                return new DependencyPair(dependency_id, result);
            }

            String hostname = CoreUtils.getHostnameOrAddress();
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), table_name, -1, result_str,
                    error_msg);
            try {
                savefile.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return new DependencyPair( dependency_id, result);
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
            TRACE_LOG.trace(CoreUtils.hsIdToString(context.getSiteId()) + " distributing replicated table: " + table_name +
                    " to: " + CoreUtils.hsIdToString(site_id));
            VoltTable result = performDistributeReplicatedTable(table_name, context, site_id, false);
            return new DependencyPair(dependency_id, result);
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

            for (int partition_id : relevantPartitions) {
                TRACE_LOG.trace("Distributing partitioned table: " + table_name +
                        " partition id: " + partition_id);
            }

            VoltTable result =
                    performDistributePartitionedTable(table_name, originalHosts,
                            relevantPartitions, context, false);
            return new DependencyPair( dependency_id, result);
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

            for (int partition_id : relevantPartitions) {
                TRACE_LOG.trace("Loading partitioned-to-replicated table: " + table_name
                        + " partition id: " + partition_id);
            }

            VoltTable result = performDistributePartitionedTable(table_name,
                    originalHosts, relevantPartitions, context, true);
            return new DependencyPair(dependency_id, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_restoreDistributeReplicatedTableAsPartitioned) {
            assert (params.toArray()[0] != null);
            assert (params.toArray()[1] != null);
            String table_name = (String) params.toArray()[0];
            int dependency_id = (Integer) params.toArray()[1];

            TRACE_LOG.trace("Loading replicated-to-partitioned table: " + table_name);

            VoltTable result = performDistributeReplicatedTable(table_name, context, -1, true);
            return new DependencyPair(dependency_id, result);

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

    public static final String JSON_PATH = "path";
    public static final String JSON_NONCE = "nonce";
    public static final String JSON_DUPLICATES_PATH = "duplicatesPath";

    public VoltTable[] run(SystemProcedureExecutionContext ctx,
                           String json) throws Exception
            {
        JSONObject jsObj = new JSONObject(json);
        final String path = jsObj.getString(JSON_PATH);
        final String nonce = jsObj.getString(JSON_NONCE);
        final String dupsPath = jsObj.optString(JSON_DUPLICATES_PATH, null);
        final long startTime = System.currentTimeMillis();
        if (dupsPath != null) {
            CONSOLE_LOG.info("Restoring from path: " + path + " with nonce: " +
                             nonce + " and duplicate rows will be output to " + dupsPath);
        } else {
            CONSOLE_LOG.info("Restoring from path: " + path + " with nonce: " + nonce);
        }

        // Fetch all the savefile metadata from the cluster
        VoltTable[] savefile_data;
        savefile_data = performRestoreScanWork(path, nonce, dupsPath);

        List<JSONObject> digests;
        Map<String, Map<Integer, Long>> exportSequenceNumbers;
        long perPartitionTxnIds[];
        try {
            DigestScanResult digestScanResult =
                    performRestoreDigestScanWork();
            digests = digestScanResult.digests;
            exportSequenceNumbers = digestScanResult.exportSequenceNumbers;
            perPartitionTxnIds = digestScanResult.perPartitionTxnIds;
            if (perPartitionTxnIds.length == 0) {
                perPartitionTxnIds = new long[] {ctx.getCurrentTxnId()};
            }
        } catch (VoltAbortException e) {
            ColumnInfo[] result_columns = new ColumnInfo[2];
            int ii = 0;
            result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
            result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
            VoltTable results[] = new VoltTable[] { new VoltTable(result_columns) };
            results[0].addRow("FAILURE", e.toString());
            noteOperationalFailure("Restore failed to complete. See response table for additional info.");
            return results;
        }

        ClusterSaveFileState savefile_state = null;
        try
        {
            savefile_state = new ClusterSaveFileState(savefile_data[0]);
        }
        catch (IOException e)
        {
            throw new VoltAbortException(e.getMessage());
        }

        HashSet<String> relevantTableNames = new HashSet<String>();
        try {
            if (digests.isEmpty()) {
                throw new Exception("No digests found");
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
            noteOperationalFailure("Restore failed to complete. See response table for additional info.");
            return results;
        }
        assert(relevantTableNames != null);
        assert(relevantTableNames.size() > 0);

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
            noteOperationalFailure("Restore failed to complete. See response table for additional info.");
            return results;
        }

        // Post a notice that a restore has started OR notice a prior post that one has started
        // and exit rather than attempt to start another.
        // Ideally this happens only just before any serious restore work begins.
        // If it comes too soon, a first attempt at restore that fails early sanity checks
        // wastes the one shot for a successful restore until the next cluster restart.
        // If it comes too late, a second attempt to restore could needlessly spin its wheels or even
        // start significant work before being called off by the detection of the prior notice.
        // A possible alternative would be to do this earlier, but then "undo" the post in cases where the
        // restore failed but left the database in a state that could reasonable be restored by a later attempt.
        try {
            VoltDB.instance().getHostMessenger().
                    getZK().create(VoltZK.restoreMarker, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            throw new VoltAbortException("Cluster has already been restored or has failed a restore." +
                " Restart the cluster before doing another restore.");
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
        ByteBuffer buf = ByteBuffer.allocate(perPartitionTxnIds.length * 8 + 4);
        buf.putInt(perPartitionTxnIds.length);
        for (long txnid : perPartitionTxnIds) {
            buf.putLong(txnid);
        }
        VoltDB.instance().getHostMessenger().
                getZK().create(VoltZK.perPartitionTxnIds, buf.array(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        /*
         * Serialize all the export sequence numbers and then distribute them in a
         * plan fragment and each receiver will pull the relevant information for
         * itself
         */
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(exportSequenceNumbers);
            oos.flush();
            byte exportSequenceNumberBytes[] = baos.toByteArray();
            oos.close();

            /*
             * Also set the perPartitionTxnIds locally at the multi-part coordinator.
             * The coord will have to forward this value to all the idle coordinators.
             */
            ctx.getSiteProcedureConnection().setPerPartitionTxnIds(perPartitionTxnIds);

            results =
                    performDistributeExportSequenceNumbers(
                            exportSequenceNumberBytes,
                            digests.get(0).getLong("txnId"),
                            perPartitionTxnIds);
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

        results = performTableRestoreWork(savefile_state, ctx.getSiteTrackerForSnapshot());

        final long endTime = System.currentTimeMillis();
        final double duration = (endTime - startTime) / 1000.0;
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        pw.toString();
        pw.printf("%.2f", duration);
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
                    VoltZK.request_truncation_snapshot,
                    null,
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT,
                    new StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx,
                                String name) {
                            if (rc != 0) {
                                KeeperException.Code code = KeeperException.Code.get(rc);
                                if (code != KeeperException.Code.NODEEXISTS) {
                                    SNAP_LOG.warn(
                                            "Don't expect this ZK response when requesting a truncation snapshot "
                                            + code);
                                }
                            }
                        }},
                    null);
        }
        return results;
    }

    private VoltTable[] performDistributeExportSequenceNumbers(
            byte[] exportSequenceNumberBytes,
            long txnId,
            long perPartitionTxnIds[]) {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site to confirm the likely
        // success of writing tables to disk
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_restoreDistributeExportAndPartitionSequenceNumbers;
        pfs[0].outputDepId = DEP_restoreDistributeExportAndPartitionSequenceNumbers;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(exportSequenceNumberBytes, txnId, perPartitionTxnIds);

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
        @SuppressWarnings("resource")
        FileInputStream savefile_input = new FileInputStream(saveFile);
        TableSaveFile savefile =
                new TableSaveFile(
                        savefile_input.getChannel(),
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

    private final VoltTable[] performRestoreScanWork(String filePath,
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
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(filePath, fileNonce, dupsPath);

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

    private static class DigestScanResult {
        List<JSONObject> digests;
        Map<String, Map<Integer, Long>> exportSequenceNumbers;
        long perPartitionTxnIds[];
    }

    private final DigestScanResult performRestoreDigestScanWork()
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

        HashMap<String, Map<Integer, Long>> exportSequenceNumbers =
                new HashMap<String, Map<Integer, Long>>();

        Long digestTxnId = null;
        ArrayList<JSONObject> digests = new ArrayList<JSONObject>();
        Set<Long> perPartitionTxnIds = new HashSet<Long>();

        /*
         * Retrieve and aggregate the per table per partition sequence numbers from
         * all the digest files retrieved across the cluster
         */
        try {
            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    throw new VoltAbortException(results[0].getString("ERR_MSG"));
                }
                JSONObject digest = new JSONObject(results[0].getString(0));
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
                 * Snapshots from pre 1.3 VoltDB won't have sequence numbers
                 * Doing nothing will default it to zero.
                 */
                if (digest.has("exportSequenceNumbers")) {
                    /*
                     * An array of entries for each table
                     */
                    JSONArray sequenceNumbers = digest.getJSONArray("exportSequenceNumbers");
                    for (int ii = 0; ii < sequenceNumbers.length(); ii++) {
                        /*
                         * An object containing all the sequence numbers for its partitions
                         * in this table. This will be a subset since it is from a single digest
                         */
                        JSONObject tableSequenceNumbers = sequenceNumbers.getJSONObject(ii);
                        String tableName = tableSequenceNumbers.getString("exportTableName");

                        Map<Integer,Long> partitionSequenceNumbers =
                                exportSequenceNumbers.get(tableName);
                        if (partitionSequenceNumbers == null) {
                            partitionSequenceNumbers = new HashMap<Integer,Long>();
                            exportSequenceNumbers.put(tableName, partitionSequenceNumbers);
                        }

                        /*
                         * Array of objects containing partition and sequence number pairs
                         */
                        JSONArray sourcePartitionSequenceNumbers =
                                tableSequenceNumbers.getJSONArray("sequenceNumberPerPartition");
                        for (int zz = 0; zz < sourcePartitionSequenceNumbers.length(); zz++) {
                            int partition = sourcePartitionSequenceNumbers.getJSONObject(zz).getInt("partition");
                            long sequenceNumber =
                                    sourcePartitionSequenceNumbers.getJSONObject(zz).getInt("exportSequenceNumber");
                            partitionSequenceNumbers.put(partition, sequenceNumber);
                        }
                    }
                }
                if (digest.has("partitionTransactionIds")) {
                    JSONObject partitionTxnIds = digest.getJSONObject("partitionTransactionIds");
                    @SuppressWarnings("unchecked")
                    Iterator<String> keys = partitionTxnIds.keys();
                    while (keys.hasNext()) {
                        perPartitionTxnIds.add(partitionTxnIds.getLong(keys.next()));
                    }
                }
            }
        } catch (JSONException e) {
            throw new VoltAbortException(e);
        }
        DigestScanResult result = new DigestScanResult();
        result.digests = digests;
        result.exportSequenceNumbers = exportSequenceNumbers;
        result.perPartitionTxnIds = Longs.toArray(perPartitionTxnIds);
        return result;
    }

    private Set<Table> getTablesToRestore(Set<String> savedTableNames)
    {
        Set<Table> tables_to_restore = new HashSet<Table>();
        for (Table table : m_database.getTables())
        {
            if (savedTableNames.contains(table.getTypeName()))
            {
                if (table.getMaterializer() == null)
                {
                    tables_to_restore.add(table);
                }
                else
                {
                    // LOG_TRIAGE reconsider info level here?
                    SNAP_LOG.info("Table: " + table.getTypeName() + " was saved " +
                            "but is now a materialized table and will " +
                            "not be loaded from disk");
                }
            }
            else
            {
                if (table.getMaterializer() == null && !CatalogUtil.isTableExportOnly(m_database, table))
                {
                    SNAP_LOG.info("Table: " + table.getTypeName() + " does not have " +
                            "any savefile data and so will not be loaded " +
                            "from disk");
                }
            }
        }
        // XXX consider logging the list of tables that were saved but not
        // in the current catalog
        return tables_to_restore;
    }

    private VoltTable[]
            performTableRestoreWork(
                    final ClusterSaveFileState savefileState,
                    final SiteTracker st) throws Exception
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
                 * Do the usual restore planning to generate the plan fragments for execution at each
                 * site
                 */
                Set<Table> tables_to_restore =
                        getTablesToRestore(savefileState.getSavedTableNames());
                VoltTable[] restore_results = new VoltTable[1];
                restore_results[0] = constructResultsTable();
                ArrayList<SynthesizedPlanFragment[]> restorePlans =
                        new ArrayList<SynthesizedPlanFragment[]>();

                for (Table t : tables_to_restore) {
                    TableSaveFileState table_state =
                            savefileState.getTableState(t.getTypeName());
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
                for (SynthesizedPlanFragment[] restore_plan : restorePlans)
                {
                    Table table = tableIterator.next();
                    TRACE_LOG.trace("Performing restore for table: " + table.getTypeName());
                    TRACE_LOG.trace("Plan has fragments: " + restore_plan.length);
                    for (int ii = 0; ii < restore_plan.length - 1; ii++) {
                        restore_plan[ii].siteId = actualToGenerated.get(restore_plan[ii].siteId);
                    }

                    /*
                     * This isn't ye olden executeSysProcPlanFragments. It uses the provided mailbox
                     * and has it's own tiny run loop to process incoming fragments.
                     */
                    VoltTable[] results =
                            executeSysProcPlanFragments(restore_plan, m);
                    while (results[0].advanceRow())
                    {
                        // this will actually add the active row of results[0]
                        restore_results[0].add(results[0]);

                        // if any table at any site fails... then the whole proc fails
                        if (results[0].getString("RESULT").equalsIgnoreCase("FAILURE")) {
                            noteOperationalFailure("Restore failed to complete. See response table for additional info.");
                        }
                    }
                }

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
            boolean asPartitioned)
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
            result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName, -1, "FAILURE",
                    "Unable to load table: " + tableName +
                    " error: " + e.getMessage());
            return result;
        }

        VoltTable[] results = new VoltTable[] { constructResultsTable() };
        results[0].addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName, -1,
                "SUCCESS", "NO DATA TO DISTRIBUTE");
        final Table new_catalog_table = getCatalogTable(tableName);
        Boolean needsConversion = null;

        try {
            while (savefile.hasMoreChunks())
            {
                VoltTable table = null;
                final org.voltcore.utils.DBBPool.BBContainer c = savefile.getNextChunk();
                if (c == null) {
                    continue;   // Should be equivalent to break
                }

                if (needsConversion == null) {
                    VoltTable old_table =
                            PrivateVoltTableFactory.createVoltTableFromBuffer(c.b.duplicate(), true);
                    needsConversion = SavedTableConverter.needsConversion(old_table, new_catalog_table);
                }

                final VoltTable old_table = PrivateVoltTableFactory
                        .createVoltTableFromBuffer(c.b, true);
                if (needsConversion) {
                    table = SavedTableConverter.convertTable(old_table, new_catalog_table);
                } else {
                    table = old_table;
                }

                SynthesizedPlanFragment[] pfs = null;
                if (asPartitioned) {
                    byte[][] partitioned_tables = createPartitionedTables(
                            tableName, table, ctx.getNumberOfPartitions());
                    Map<Long, Integer> sites_to_partitions = new HashMap<Long, Integer>();
                    SiteTracker tracker = ctx.getSiteTrackerForSnapshot();
                    sites_to_partitions.putAll(tracker.getSitesToPartitions());

                    int[] dependencyIds = new int[sites_to_partitions.size()];
                    pfs = new SynthesizedPlanFragment[sites_to_partitions.size() + 1];
                    int pfs_index = 0;

                    for (long site_id : sites_to_partitions.keySet()) {
                        int partition_id = sites_to_partitions.get(site_id);
                        dependencyIds[pfs_index] = TableSaveFileState
                                .getNextDependencyId();
                        SynthesizedPlanFragment loadFragment = new SynthesizedPlanFragment();
                        loadFragment.fragmentId = SysProcFragmentId.PF_restoreLoadTable;
                        loadFragment.siteId = m_actualToGenerated.get(site_id);
                        loadFragment.multipartition = false;
                        loadFragment.outputDepId = dependencyIds[pfs_index];
                        loadFragment.inputDepIds = new int[] {};
                        loadFragment.parameters = ParameterSet.fromArrayNoCopy(
                                tableName,
                                dependencyIds[pfs_index],
                                partitioned_tables[partition_id],
                                K_CHECK_UNIQUE_VIOLATIONS_PARTITIONED,
                                new int[] {partition_id});

                        pfs[pfs_index++] = loadFragment;
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
                    pfs[sites_to_partitions.size()] = aggregatorFragment;
                } else {
                    byte compressedTable[] = table.getCompressedBytes();
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
                            K_CHECK_UNIQUE_VIOLATIONS_REPLICATED, null);

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
                    TRACE_LOG.trace("Sending replicated table: " + tableName + " to site id:" +
                            siteId);
                }
                c.discard();
                results = executeSysProcPlanFragments(pfs, m_mbox);
            }
        } catch (Exception e) {
            VoltTable result = PrivateVoltTableFactory.createUninitializedVoltTable();
            result = constructResultsTable();
            result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName, -1, "FAILURE",
                    "Unable to load table: " + tableName +
                    " error: " + e.getMessage());
            return result;
        }

        return results[0];
    }

    private VoltTable performDistributePartitionedTable(String tableName,
            int originalHostIds[],
            int relevantPartitionIds[],
            SystemProcedureExecutionContext ctx,
            boolean asReplicated)
    {
        String hostname = CoreUtils.getHostnameOrAddress();
        // XXX This is all very similar to the splitting code in
        // LoadMultipartitionTable.  Consider ways to consolidate later
        Map<Long, Integer> sites_to_partitions =
                new HashMap<Long, Integer>();
        SiteTracker tracker = ctx.getSiteTrackerForSnapshot();
        sites_to_partitions.putAll(tracker.getSitesToPartitions());

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
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName, relevantPartitionIds[0], "FAILURE",
                    "Unable to load table: " + tableName +
                    " error: " + e.getMessage());
            return result;
        }

        VoltTable[] results = new VoltTable[] { constructResultsTable() };
        results[0].addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName, 0,
                "SUCCESS", "NO DATA TO DISTRIBUTE");
        final Table new_catalog_table = getCatalogTable(tableName);
        Boolean needsConversion = null;
        org.voltcore.utils.DBBPool.BBContainer c = null;
        try {
            while (hasMoreChunks())
            {
                VoltTable table = null;

                c = null;
                c = getNextChunk();
                if (c == null) {
                    continue;//Should be equivalent to break
                }

                if (needsConversion == null) {
                    VoltTable old_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b.duplicate(), true);
                    needsConversion = SavedTableConverter.needsConversion(old_table, new_catalog_table);
                }

                final VoltTable old_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b, true);
                if (needsConversion) {
                    table = SavedTableConverter.convertTable(old_table,
                            new_catalog_table);
                } else {
                    table = old_table;
                }

                // use if will load as partitioned table
                byte[][] partitioned_tables = null;
                // use if will load as replicated table
                byte compressedTable[] = null;

                if (asReplicated) {
                    compressedTable = table.getCompressedBytes();
                } else {
                    partitioned_tables = createPartitionedTables(tableName, table, ctx.getNumberOfPartitions());
                }

                if (c != null) {
                    c.discard();
                }

                int[] dependencyIds = new int[sites_to_partitions.size()];
                SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[sites_to_partitions.size() + 1];
                int pfs_index = 0;
                for (long site_id : sites_to_partitions.keySet())
                {
                    dependencyIds[pfs_index] = TableSaveFileState.getNextDependencyId();
                    SynthesizedPlanFragment loadFragment = new SynthesizedPlanFragment();
                    loadFragment.fragmentId = SysProcFragmentId.PF_restoreLoadTable;
                    loadFragment.siteId = m_actualToGenerated.get(site_id);
                    loadFragment.multipartition = false;
                    loadFragment.outputDepId = dependencyIds[pfs_index];
                    loadFragment.inputDepIds = new int [] {};

                    if(asReplicated) {
                        loadFragment.parameters = ParameterSet.fromArrayNoCopy(
                                tableName,
                                dependencyIds[pfs_index],
                                compressedTable,
                                K_CHECK_UNIQUE_VIOLATIONS_REPLICATED,
                                relevantPartitionIds);
                    } else {
                        int partition_id = sites_to_partitions.get(site_id);
                        loadFragment.parameters = ParameterSet.fromArrayNoCopy(
                                tableName,
                                dependencyIds[pfs_index],
                                partitioned_tables[partition_id],
                                K_CHECK_UNIQUE_VIOLATIONS_PARTITIONED,
                                new int[] {partition_id});
                    }
                    pfs[pfs_index++] = loadFragment;
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
                pfs[sites_to_partitions.size()] = aggregatorFragment;
                results = executeSysProcPlanFragments(pfs, m_mbox);
            }
        } catch (Exception e) {
            VoltTable result = PrivateVoltTableFactory.createUninitializedVoltTable();
            result = constructResultsTable();
            result.addRow(m_hostId, hostname, CoreUtils.getSiteIdFromHSId(m_siteId), tableName, relevantPartitionIds[0],
                    "FAILURE", "Unable to load table: " + tableName +
                    " error: " + e.getMessage());
            return result;
        }

        return results[0];
    }

    private byte[][] createPartitionedTables(String tableName,
            VoltTable loadedTable, int number_of_partitions) throws Exception
            {
        Table catalog_table = m_database.getTables().getIgnoreCase(tableName);
        assert(!catalog_table.getIsreplicated());
        // XXX blatantly stolen from LoadMultipartitionTable
        // find the index and type of the partitioning attribute
        int partition_col = catalog_table.getPartitioncolumn().getIndex();
        VoltType partition_type =
                VoltType.get((byte) catalog_table.getPartitioncolumn().getType());

        // create a table for each partition
        VoltTable[] partitioned_tables = new VoltTable[number_of_partitions];
        for (int i = 0; i < partitioned_tables.length; i++) {
            partitioned_tables[i] =
                    loadedTable.clone(loadedTable.getUnderlyingBufferSize() /
                            number_of_partitions);
        }

        // split the input table into per-partition units
        while (loadedTable.advanceRow())
        {
            int partition = 0;
            try
            {
                partition =
                    TheHashinator.getPartitionForParameter(partition_type.getValue(),
                            loadedTable.get(partition_col, partition_type));
            }
            catch (Exception e)
            {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
            // this adds the active row of loadedTable
            partitioned_tables[partition].add(loadedTable);
        }

        byte compressedTables[][] = new byte[number_of_partitions][];
        for (int ii = 0; ii < compressedTables.length; ii++) {
            compressedTables[ii] = partitioned_tables[ii].getCompressedBytes();
        }
        return compressedTables;
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
                jsObj.put(SnapshotRestore.JSON_PATH, (String)params[0]);
                jsObj.put(SnapshotRestore.JSON_NONCE, (String)params[1]);
            } catch (JSONException e) {
                Throwables.propagate(e);
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

    private Mailbox m_mbox;
    private final Map<Long, Long> m_actualToGenerated = new HashMap<Long, Long>();
    private Database m_database;
    private long m_siteId;
    private int m_hostId;
    private static volatile String m_filePath;
    private static volatile String m_fileNonce;
}
