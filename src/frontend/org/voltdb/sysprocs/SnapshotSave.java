/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Deque;
import java.util.LinkedList;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Semaphore;
import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.DependencyPair;
import org.voltdb.SnapshotSiteProcessor.SnapshotTableTask;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.*;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Host;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltLoggerFactory;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

@ProcInfo(singlePartition = false)
public class SnapshotSave extends VoltSystemProcedure
{
    private static final Logger TRACE_LOG =
        Logger.getLogger(SnapshotSave.class.getName(),
                         VoltLoggerFactory.instance());

    private static final Logger HOST_LOG =
        Logger.getLogger("HOST", VoltLoggerFactory.instance());

    private static final int DEP_saveTest = (int)
        SysProcFragmentId.PF_saveTest | DtxnConstants.MULTINODE_DEPENDENCY;
    private static final int DEP_saveTestResults = (int)
        SysProcFragmentId.PF_saveTestResults;
    private static final int DEP_createSnapshotTargets = (int)
        SysProcFragmentId.PF_createSnapshotTargets | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_createSnapshotTargetsResults = (int)
        SysProcFragmentId.PF_createSnapshotTargetsResults;

    /**
     * Ensure the first thread to run the fragment does the creation
     * of the targets and the distribution of the work.
     */
    private static final Semaphore m_snapshotCreateSetupPermit = new Semaphore(1);
    /**
     * Only proceed once permits are available after setup completes
     */
    private static Semaphore m_snapshotPermits = new Semaphore(0);
    private static final LinkedList<Deque<SnapshotTableTask>>
        m_taskListsForSites = new LinkedList<Deque<SnapshotTableTask>>();

    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster)
    {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_saveTest, this);
        site.registerPlanFragment(SysProcFragmentId.PF_saveTestResults, this);
        site.registerPlanFragment(SysProcFragmentId.PF_createSnapshotTargets, this);
        site.registerPlanFragment(SysProcFragmentId.PF_createSnapshotTargetsResults, this);
    }

    @Override
    public DependencyPair
    executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies, long fragmentId, ParameterSet params,
                        SystemProcedureExecutionContext context)
    {
        String hostname = "";
        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            hostname = localMachine.getHostName();
        } catch (java.net.UnknownHostException uhe) {
        }
        if (fragmentId == SysProcFragmentId.PF_saveTest)
        {
            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            String file_path = (String) params.toArray()[0];
            String file_nonce = (String) params.toArray()[1];
            TRACE_LOG.trace("Checking feasibility of save with path and nonce: "
                     + file_path + ", " + file_nonce);
            VoltTable result = constructNodeResultsTable();

            if (SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.get() != -1) {
                result.addRow(
                        context.getSite().getHost().getTypeName(),
                        hostname,
                        "",
                        "FAILURE",
                        "SNAPSHOT IN PROGRESS");
                return new DependencyPair( DEP_saveTest, result);
            }

            for (Table table : getTablesToSave(context))
            {
                File saveFilePath =
                    constructFileForTable(table, file_path, file_nonce,
                                          context.getSite().getHost().getTypeName());
                TRACE_LOG.trace("Host ID " + context.getSite().getHost().getTypeName() +
                         " table: " + table.getTypeName() +
                         " to path: " + saveFilePath);
                String file_valid = "SUCCESS";
                String err_msg = "";
                if (saveFilePath.exists())
                {
                    file_valid = "FAILURE";
                    err_msg = "SAVE FILE ALREADY EXISTS: " + saveFilePath;
                }
                else if (!saveFilePath.getParentFile().canWrite())
                {
                    file_valid = "FAILURE";
                    err_msg = "FILE LOCATION UNWRITABLE: " + saveFilePath;
                }
                else
                {
                    try
                    {
                        saveFilePath.createNewFile();
                    }
                    catch (IOException ex)
                    {
                        file_valid = "FAILURE";
                        err_msg = "FILE CREATION OF " + saveFilePath +
                        "RESULTED IN IOException: " + ex.getMessage();
                    }
                }
                result.addRow(context.getSite().getHost().getTypeName(),
                                 hostname,
                                 table.getTypeName(),
                                 file_valid,
                                 err_msg);
            }
            return new DependencyPair(DEP_saveTest, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_saveTestResults)
        {
            TRACE_LOG.trace("Aggregating save feasiblity results");
            assert (dependencies.size() > 0);
            List<VoltTable> dep = dependencies.get(DEP_saveTest);
            VoltTable result = constructNodeResultsTable();
            for (VoltTable table : dep)
            {
                while (table.advanceRow())
                {
                    // this will add the active row of table
                    result.add(table);
                }
            }
            return new
                DependencyPair( DEP_saveTestResults, result);
        } else if (fragmentId == SysProcFragmentId.PF_createSnapshotTargets) {
            TRACE_LOG.trace("Creating snapshot target and handing to EEs");
            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            assert(params.toArray()[2] != null);
            assert(params.toArray()[3] != null);
            final String file_path = (String) params.toArray()[0];
            final String file_nonce = (String) params.toArray()[1];
            byte block = (Byte)params.toArray()[3];
            final VoltTable result = constructNodeResultsTable();
            boolean willDoSetup = m_snapshotCreateSetupPermit.tryAcquire();
            final int numLocalSites = VoltDB.instance().getLocalSites().values().size();
            if (willDoSetup) {
                try {
                    assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.get() == -1);
                    final long startTime = (Long)params.toArray()[2];
                    final ArrayDeque<SnapshotTableTask> partitionedSnapshotTasks =
                        new ArrayDeque<SnapshotTableTask>();
                    final ArrayList<SnapshotTableTask> replicatedSnapshotTasks =
                        new ArrayList<SnapshotTableTask>();

                    final ArrayList<String> tableNames = new ArrayList<String>();
                    for (final Table table : getTablesToSave(context))
                    {
                        tableNames.add(table.getTypeName());
                    }
                    SnapshotUtil.recordSnapshotTableList(
                            startTime,
                            file_path,
                            file_nonce,
                            tableNames);
                    final AtomicInteger numTables = new AtomicInteger(tableNames.size());
                    final SnapshotRegistry.Snapshot snapshotRecord =
                        SnapshotRegistry.startSnapshot(
                                startTime,
                                file_path,
                                file_nonce,
                                tableNames.toArray(new String[0]));
                    for (final Table table : getTablesToSave(context))
                    {
                        String canSnapshot = "SUCCESS";
                        String err_msg = "";
                        final File saveFilePath =
                            constructFileForTable(table, file_path, file_nonce,
                                                  context.getSite().getHost().getTypeName());
                        try {
                            final SnapshotDataTarget sdt =
                                constructSnapshotDataTargetForTable(
                                        context,
                                        saveFilePath,
                                        table,
                                        context.getSite().getHost(),
                                        context.getCluster().getPartitions().size(),
                                        startTime);

                            final Runnable onClose = new Runnable() {
                                @Override
                                public void run() {
                                    final long now = System.currentTimeMillis();
                                    snapshotRecord.updateTable(table.getTypeName(),
                                            new SnapshotRegistry.Snapshot.TableUpdater() {
                                        @Override
                                        public SnapshotRegistry.Snapshot.Table update(
                                                SnapshotRegistry.Snapshot.Table registryTable) {
                                            return snapshotRecord.new Table(
                                                    registryTable,
                                                    sdt.getBytesWritten(),
                                                    now,
                                                    sdt.getLastWriteException());
                                        }
                                    });
                                    int tablesLeft = numTables.decrementAndGet();
                                    if (tablesLeft == 0) {
                                        final SnapshotRegistry.Snapshot completed =
                                            SnapshotRegistry.finishSnapshot(snapshotRecord);
                                        final double duration =
                                            (completed.timeFinished - completed.timeStarted) / 1000.0;
                                        HOST_LOG.info(
                                                "Snapshot " + snapshotRecord.nonce + " finished at " +
                                                 completed.timeFinished + " and took " + duration
                                                 + " seconds ");
                                    }
                                }
                            };

                            sdt.setOnCloseHandler(onClose);

                            final SnapshotTableTask task =
                                new SnapshotTableTask(
                                        table.getRelativeIndex(),
                                        sdt,
                                        table.getIsreplicated(),
                                        table.getTypeName());

                            if (table.getIsreplicated()) {
                                replicatedSnapshotTasks.add(task);
                            } else {
                                partitionedSnapshotTasks.offer(task);
                            }
                        } catch (IOException ex) {
                            canSnapshot = "FAILURE";
                            err_msg = "SNAPSHOT INITIATION OF " + saveFilePath +
                            "RESULTED IN IOException: " + ex.getMessage();
                        }

                        result.addRow(context.getSite().getHost().getTypeName(),
                                hostname,
                                table.getTypeName(),
                                canSnapshot,
                                err_msg);
                    }

                    synchronized (m_taskListsForSites) {
                        if (!partitionedSnapshotTasks.isEmpty() || !replicatedSnapshotTasks.isEmpty()) {
                            SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.set(
                                    VoltDB.instance().getLocalSites().values().size());
                        } else {
                            SnapshotRegistry.discardSnapshot(snapshotRecord);
                        }

                        /**
                         * Distribute the writing of replicated tables to exactly one partition.
                         */
                        for (int ii = 0; ii < numLocalSites && !partitionedSnapshotTasks.isEmpty(); ii++) {
                            m_taskListsForSites.add(new ArrayDeque<SnapshotTableTask>(partitionedSnapshotTasks));
                        }

                        int siteIndex = 0;
                        for (SnapshotTableTask t : replicatedSnapshotTasks) {
                            m_taskListsForSites.get(siteIndex++ % numLocalSites).offer(t);
                        }
                    }
                } catch (Exception ex) {
                    result.addRow(
                            context.getSite().getHost().getTypeName(),
                            hostname,
                            "",
                            "FAILURE",
                            "SNAPSHOT INITIATION OF " + file_path + file_nonce +
                            "RESULTED IN Exception: " + ex.getMessage());
                    HOST_LOG.error(ex);
                } finally {
                    m_snapshotPermits.release(numLocalSites);
                }
            }

            try {
                m_snapshotPermits.acquire();
            } catch (Exception e) {
                result.addRow(context.getSite().getHost().getTypeName(),
                        hostname,
                        "",
                        "FAILURE",
                        e.toString());
                return new DependencyPair( DEP_createSnapshotTargets, result);
            } finally {
                /*
                 * The last thead to acquire a snapshot permit has to be the one
                 * to release the setup permit to ensure that a thread
                 * doesn't come late and think it is supposed to do the setup work
                 */
                synchronized (m_snapshotPermits) {
                    if (m_snapshotPermits.availablePermits() == 0 &&
                            m_snapshotCreateSetupPermit.availablePermits() == 0) {
                        m_snapshotCreateSetupPermit.release();
                    }
                }
            }

            synchronized (m_taskListsForSites) {
                final Deque<SnapshotTableTask> m_taskList = m_taskListsForSites.poll();
                if (m_taskList == null) {
                    return new DependencyPair( DEP_createSnapshotTargets, result);
                } else {
                    if (m_taskListsForSites.isEmpty()) {
                        assert(m_snapshotCreateSetupPermit.availablePermits() == 1);
                        assert(m_snapshotPermits.availablePermits() == 0);
                    }
                    assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.get() > 0);
                    context.getExecutionSite().initiateSnapshots(m_taskList);
                }
            }

            if (block != 0) {
                HashSet<Exception> failures = null;
                String status = "SUCCESS";
                String err = "";
                try {
                    failures = context.getExecutionSite().completeSnapshotWork();
                } catch (InterruptedException e) {
                    status = "FAILURE";
                    err = e.toString();
                }
                final VoltTable blockingResult = constructPartitionResultsTable();

                if (failures.isEmpty()) {
                    blockingResult.addRow(
                            context.getSite().getHost().getTypeName(),
                            hostname,
                            context.getSite().getTypeName(),
                            status,
                            err);
                } else {
                    status = "FAILURE";
                    for (Exception e : failures) {
                        err = e.toString();
                    }
                    blockingResult.addRow(
                            context.getSite().getHost().getTypeName(),
                            hostname,
                            context.getSite().getTypeName(),
                            status,
                            err);
                }
                return new DependencyPair( DEP_createSnapshotTargets, blockingResult);
            }

            return new DependencyPair( DEP_createSnapshotTargets, result);
        } else if (fragmentId == SysProcFragmentId.PF_createSnapshotTargetsResults)
        {
            TRACE_LOG.trace("Aggregating create snapshot target results");
            assert (dependencies.size() > 0);
            List<VoltTable> dep = dependencies.get(DEP_createSnapshotTargets);
            VoltTable result = constructNodeResultsTable();
            for (VoltTable table : dep)
            {
                while (table.advanceRow())
                {
                    // this will add the active row of table
                    result.add(table);
                }
            }
            return new
                DependencyPair( DEP_createSnapshotTargetsResults, result);
        }
        assert (false);
        return null;
    }

    public VoltTable[] run(String path, String nonce, long block) throws VoltAbortException
    {
        final long startTime = System.currentTimeMillis();
        HOST_LOG.info("Saving database to path: " + path + ", ID: " + nonce + " at " + System.currentTimeMillis());

        if (path == null || path.equals("")) {
            ColumnInfo[] result_columns = new ColumnInfo[1];
            int ii = 0;
            result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
            VoltTable results[] = new VoltTable[] { new VoltTable(result_columns) };
            results[0].addRow("Provided path was null or the empty string");
            return results;
        }

        if (nonce == null || nonce.equals("")) {
            ColumnInfo[] result_columns = new ColumnInfo[1];
            int ii = 0;
            result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
            VoltTable results[] = new VoltTable[] { new VoltTable(result_columns) };
            results[0].addRow("Provided nonce was null or the empty string");
            return results;
        }

        if (nonce.contains("-") || nonce.contains(",")) {
            ColumnInfo[] result_columns = new ColumnInfo[1];
            int ii = 0;
            result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
            VoltTable results[] = new VoltTable[] { new VoltTable(result_columns) };
            results[0].addRow("Provided nonce " + nonce + " contains a prohitibited character (- or ,)");
            return results;
        }

        // See if we think the save will succeed
        VoltTable[] results;
        results = performSaveFeasibilityWork(path, nonce);

        // Test feasibility results for fail
        while (results[0].advanceRow())
        {
            if (results[0].getString("RESULT").equals("FAILURE"))
            {
                // Something lost, bomb out and just return the whole
                // table of results to the client for analysis
                return results;
            }
        }

        results = performSnapshotCreationWork( path, nonce, startTime, (byte)block);

        final long finishTime = System.currentTimeMillis();
        final long duration = finishTime - startTime;
        HOST_LOG.info("Snapshot initiation took " + duration + " milliseconds");
        return results;
    }

    // XXX this could maybe move to be a method on
    // SystemProcedureExecutionContext?
    private final List<Table>
    getTablesToSave(SystemProcedureExecutionContext context)
    {
        CatalogMap<Table> all_tables = context.getDatabase().getTables();
        ArrayList<Table> my_tables = new ArrayList<Table>();
        for (Table table : all_tables)
        {
            // We're responsible for saving any table that isn't replicated and
            // all the replicated tables if we're the lowest site ID on our host
            // Also, we ignore all materialized tables as those should get
            // regenerated when we restore
            // NOTE: this assumes that all partitioned tables have partitions on
            // all execution sites.
            if (table.getMaterializer() == null)
            {
                    my_tables.add(table);
            }
        }
        return my_tables;
    }

    private final File constructFileForTable(Table table,
                                             String filePath,
                                             String fileNonce,
                                             String hostId)
    {
        StringBuilder filename_builder = new StringBuilder(fileNonce);
        filename_builder.append("-");
        filename_builder.append(table.getTypeName());
        if (!table.getIsreplicated())
        {
            filename_builder.append("-host_");
            filename_builder.append(hostId);
        }
        filename_builder.append(".vpt");//Volt partitioned table
        return new File(filePath, new String(filename_builder));
    }

    private final SnapshotDataTarget constructSnapshotDataTargetForTable(
                                             SystemProcedureExecutionContext context,
                                             File f,
                                             Table table,
                                             Host h,
                                             int numPartitions,
                                             long createTime) throws IOException
    {
        return new DefaultSnapshotDataTarget(
                f,
                Integer.parseInt(h.getTypeName()),
                context.getCluster().getTypeName(),
                context.getDatabase().getTypeName(),
                table.getTypeName(),
                numPartitions,
                table.getIsreplicated(),
                getPartitionsOnHost(context, h),
                CatalogUtil.getVoltTable(table),
                createTime);
    }

    private final VoltTable constructNodeResultsTable()
    {
        return new VoltTable(nodeResultsColumns);
    }

    public static final ColumnInfo nodeResultsColumns[] = new ColumnInfo[] {
        new ColumnInfo("HOST_ID", VoltType.STRING),
        new ColumnInfo("HOSTNAME", VoltType.STRING),
        new ColumnInfo("TABLE", VoltType.STRING),
        new ColumnInfo("RESULT", VoltType.STRING),
        new ColumnInfo("ERR_MSG", VoltType.STRING)
    };

    public static final ColumnInfo partitionResultsColumns[] = new ColumnInfo[] {
        new ColumnInfo("HOST_ID", VoltType.STRING),
        new ColumnInfo("HOSTNAME", VoltType.STRING),
        new ColumnInfo("SITE_ID", VoltType.STRING),
        new ColumnInfo("RESULT", VoltType.STRING),
        new ColumnInfo("ERR_MSG", VoltType.STRING)
    };

    private final VoltTable constructPartitionResultsTable()
    {
        return new VoltTable(partitionResultsColumns);
    }

    private final VoltTable[] performSaveFeasibilityWork(String filePath,
                                                         String fileNonce)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site to confirm the likely
        // success of writing tables to disk
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_saveTest;
        pfs[0].outputDepId = DEP_saveTest;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = true;
        ParameterSet params = new ParameterSet();
        params.setParameters(filePath, fileNonce);
        pfs[0].parameters = params;

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_saveTestResults;
        pfs[1].outputDepId = DEP_saveTestResults;
        pfs[1].inputDepIds = new int[] { DEP_saveTest };
        pfs[1].multipartition = false;
        pfs[1].nonExecSites = false;
        pfs[1].parameters = new ParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_saveTestResults);
        return results;
    }

    private final VoltTable[] performSnapshotCreationWork(String filePath,
            String fileNonce,
            long startTime,
            byte block)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site to confirm the likely
        // success of writing tables to disk
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_createSnapshotTargets;
        pfs[0].outputDepId = DEP_createSnapshotTargets;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].nonExecSites = false;
        ParameterSet params = new ParameterSet();
        params.setParameters(filePath, fileNonce, startTime, block);
        pfs[0].parameters = params;

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_createSnapshotTargetsResults;
        pfs[1].outputDepId = DEP_createSnapshotTargetsResults;
        pfs[1].inputDepIds = new int[] { DEP_createSnapshotTargets };
        pfs[1].multipartition = false;
        pfs[1].nonExecSites = false;
        pfs[1].parameters = new ParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_createSnapshotTargetsResults);
        return results;
    }

    private int[] getPartitionsOnHost(
            SystemProcedureExecutionContext c, Host h) {
        final ArrayList<Partition> results = new ArrayList<Partition>();
        for (final Site s : VoltDB.instance().getCatalogContext().siteTracker.getUpSites()) {
            if (s.getHost().getTypeName().equals(h.getTypeName())) {
                if (s.getPartition() != null) {
                    results.add(s.getPartition());
                }
            }
        }
        final int retval[] = new int[results.size()];
        int ii = 0;
        for (final Partition p : results) {
            retval[ii++] = Integer.parseInt(p.getTypeName());
        }
        return retval;
    }
}
