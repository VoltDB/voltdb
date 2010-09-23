/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.SnapshotSiteProcessor.SnapshotTableTask;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Table;
import org.voltdb.logging.VoltLogger;
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.sysprocs.SnapshotSave;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.CatalogUtil;

/**
 * SnapshotSaveAPI extracts reusuable snapshot production code
 * that can be called from the SnapshotSave stored procedure or
 * directly from an ExecutionSite thread, perhaps has a message
 * or failure action.
 */
public class SnapshotSaveAPI
{
    private static final VoltLogger TRACE_LOG = new VoltLogger(SnapshotSaveAPI.class.getName());
    private static final VoltLogger HOST_LOG = new VoltLogger("HOST");

    /**
     * The only public method: do all the work to start a snapshot.
     * Assumes that a snapshot is feasible, that the caller has validated it can
     * be accomplished, that the caller knows this is a consistent or useful
     * transaction point at which to snapshot.
     *
     * @param file_path
     * @param file_nonce
     * @param block
     * @param startTime
     * @param context
     * @param hostname
     * @return VoltTable describing the results of the snapshot attempt
     */
    public VoltTable startSnapshotting(String file_path, String file_nonce, byte block,
            long startTime, SystemProcedureExecutionContext context, String hostname)
    {
        TRACE_LOG.trace("Creating snapshot target and handing to EEs");
        final VoltTable result = SnapshotSave.constructNodeResultsTable();

        // One site wins the race to create the snapshot targets, populating
        // m_taskListsForSites for the other sites and creating an appropriate
        // number of snapshot permits
        if (SnapshotSiteProcessor.m_snapshotCreateSetupPermit.tryAcquire()) {
            createSetup(file_path, file_nonce, startTime, context, hostname, result);
        }

        // All sites wait for a permit to start their individual snapshot tasks
        VoltTable error = acquireSnapshotPermit(context, hostname, result);
        if (error != null) {
            return error;
        }

        synchronized (SnapshotSiteProcessor.m_taskListsForSites) {
            final Deque<SnapshotTableTask> m_taskList = SnapshotSiteProcessor.m_taskListsForSites.poll();
            if (m_taskList == null) {
                return result;
            } else {
                if (SnapshotSiteProcessor.m_taskListsForSites.isEmpty()) {
                    assert(SnapshotSiteProcessor.m_snapshotCreateSetupPermit.availablePermits() == 1);
                    assert(SnapshotSiteProcessor.m_snapshotPermits.availablePermits() == 0);
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
            final VoltTable blockingResult = SnapshotSave.constructPartitionResultsTable();

            if (failures.isEmpty()) {
                blockingResult.addRow(
                        Integer.parseInt(context.getSite().getHost().getTypeName()),
                        hostname,
                        Integer.parseInt(context.getSite().getTypeName()),
                        status,
                        err);
            } else {
                status = "FAILURE";
                for (Exception e : failures) {
                    err = e.toString();
                }
                blockingResult.addRow(
                        Integer.parseInt(context.getSite().getHost().getTypeName()),
                        hostname,
                        Integer.parseInt(context.getSite().getTypeName()),
                        status,
                        err);
            }
            return blockingResult;
        }

        return result;
    }


    private void createSetup(String file_path, String file_nonce,
            long startTime, SystemProcedureExecutionContext context,
            String hostname, final VoltTable result) {
        {
            final int numLocalSites = VoltDB.instance().getLocalSites().values().size();

            /*
             * Used to close targets on failure
             */
            final ArrayList<SnapshotDataTarget> targets = new ArrayList<SnapshotDataTarget>();
            try {
                final ArrayDeque<SnapshotTableTask> partitionedSnapshotTasks =
                    new ArrayDeque<SnapshotTableTask>();
                final ArrayList<SnapshotTableTask> replicatedSnapshotTasks =
                    new ArrayList<SnapshotTableTask>();
                assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.get() == -1);

                final List<Table> tables = SnapshotUtil.getTablesToSave(context.getDatabase());

                SnapshotUtil.recordSnapshotTableList(
                        startTime,
                        file_path,
                        file_nonce,
                        tables);
                final AtomicInteger numTables = new AtomicInteger(tables.size());
                final SnapshotRegistry.Snapshot snapshotRecord =
                    SnapshotRegistry.startSnapshot(
                            startTime,
                            context.getExecutionSite().getCorrespondingHostId(),
                            file_path,
                            file_nonce,
                            tables.toArray(new Table[0]));
                for (final Table table : SnapshotUtil.getTablesToSave(context.getDatabase()))
                {
                    String canSnapshot = "SUCCESS";
                    String err_msg = "";
                    final File saveFilePath =
                        SnapshotUtil.constructFileForTable(table, file_path, file_nonce,
                                              context.getSite().getHost().getTypeName());
                    SnapshotDataTarget sdt = null;
                    try {
                        sdt =
                            constructSnapshotDataTargetForTable(
                                    context,
                                    saveFilePath,
                                    table,
                                    context.getSite().getHost(),
                                    context.getCluster().getPartitions().size(),
                                    startTime);
                        targets.add(sdt);
                        final SnapshotDataTarget sdtFinal = sdt;
                        final Runnable onClose = new Runnable() {
                            @Override
                            public void run() {
                                snapshotRecord.updateTable(table.getTypeName(),
                                        new SnapshotRegistry.Snapshot.TableUpdater() {
                                    @Override
                                    public SnapshotRegistry.Snapshot.Table update(
                                            SnapshotRegistry.Snapshot.Table registryTable) {
                                        return snapshotRecord.new Table(
                                                registryTable,
                                                sdtFinal.getBytesWritten(),
                                                sdtFinal.getLastWriteException());
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
                        /*
                         * Creation of this specific target failed. Close it if it was created.
                         * Continue attempting the snapshot anyways so that at least some of the data
                         * can be retrieved.
                         */
                        try {
                            if (sdt != null) {
                                targets.remove(sdt);
                                sdt.close();
                            }
                        } catch (Exception e) {
                            HOST_LOG.error(e);
                        }

                        StringWriter sw = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        ex.printStackTrace(pw);
                        pw.flush();
                        canSnapshot = "FAILURE";
                        err_msg = "SNAPSHOT INITIATION OF " + saveFilePath +
                        "RESULTED IN IOException: \n" + sw.toString();
                    }

                    result.addRow(Integer.parseInt(context.getSite().getHost().getTypeName()),
                            hostname,
                            table.getTypeName(),
                            canSnapshot,
                            err_msg);
                }

                synchronized (SnapshotSiteProcessor.m_taskListsForSites) {
                    if (!partitionedSnapshotTasks.isEmpty() || !replicatedSnapshotTasks.isEmpty()) {
                        SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.set(
                                VoltDB.instance().getLocalSites().values().size());
                        for (int ii = 0; ii < numLocalSites; ii++) {
                            SnapshotSiteProcessor.m_taskListsForSites.add(new ArrayDeque<SnapshotTableTask>());
                        }
                    } else {
                        SnapshotRegistry.discardSnapshot(snapshotRecord);
                    }

                    /**
                     * Distribute the writing of replicated tables to exactly one partition.
                     */
                    for (int ii = 0; ii < numLocalSites && !partitionedSnapshotTasks.isEmpty(); ii++) {
                        SnapshotSiteProcessor.m_taskListsForSites.get(ii).addAll(partitionedSnapshotTasks);
                    }

                    int siteIndex = 0;
                    for (SnapshotTableTask t : replicatedSnapshotTasks) {
                        SnapshotSiteProcessor.m_taskListsForSites.get(siteIndex++ % numLocalSites).offer(t);
                    }
                }
            } catch (Exception ex) {
                /*
                 * Close all the targets to release the threads. Don't let sites get any tasks.
                 */
                SnapshotSiteProcessor.m_taskListsForSites.clear();
                for (SnapshotDataTarget sdt : targets) {
                    try {
                        sdt.close();
                    } catch (Exception e) {
                        HOST_LOG.error(ex);
                    }
                }

                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                ex.printStackTrace(pw);
                pw.flush();
                result.addRow(
                        Integer.parseInt(context.getSite().getHost().getTypeName()),
                        hostname,
                        "",
                        "FAILURE",
                        "SNAPSHOT INITIATION OF " + file_path + file_nonce +
                        "RESULTED IN Exception: \n" + sw.toString());
                HOST_LOG.error(ex);
            } finally {
                SnapshotSiteProcessor.m_snapshotPermits.release(numLocalSites);
            }
        }
    }

    private VoltTable acquireSnapshotPermit(SystemProcedureExecutionContext context,
            String hostname, final VoltTable result) {
        try {
            SnapshotSiteProcessor.m_snapshotPermits.acquire();
        } catch (Exception e) {
            result.addRow(Integer.parseInt(context.getSite().getHost().getTypeName()),
                    hostname,
                    "",
                    "FAILURE",
                    e.toString());
            return result;
        } finally {
            /*
             * The last thread to acquire a snapshot permit has to be the one
             * to release the setup permit to ensure that a thread
             * doesn't come late and think it is supposed to do the setup work
             */
            synchronized (SnapshotSiteProcessor.m_snapshotPermits) {
                if (SnapshotSiteProcessor.m_snapshotPermits.availablePermits() == 0 &&
                        SnapshotSiteProcessor.m_snapshotCreateSetupPermit.availablePermits() == 0) {
                    SnapshotSiteProcessor.m_snapshotCreateSetupPermit.release();
                }
            }
        }
        return null;
    }


    private final SnapshotDataTarget constructSnapshotDataTargetForTable(
            SystemProcedureExecutionContext context,
            File f,
            Table table,
            Host h,
            int numPartitions,
            long createTime)
    throws IOException
    {
        return new DefaultSnapshotDataTarget(f,
                                             Integer.parseInt(h.getTypeName()),
                                             context.getCluster().getTypeName(),
                                             context.getDatabase().getTypeName(),
                                             table.getTypeName(),
                                             numPartitions,
                                             table.getIsreplicated(),
                                             SnapshotUtil.getPartitionsOnHost(context, h),
                                             CatalogUtil.getVoltTable(table),
                                             createTime);
    }

}
