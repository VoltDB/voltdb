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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.json_voltpatches.JSONStringer;
import org.voltdb.*;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.*;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.logging.VoltLogger;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

@ProcInfo(singlePartition = false)
public class SnapshotSave extends VoltSystemProcedure
{
    private static final VoltLogger TRACE_LOG = new VoltLogger(SnapshotSave.class.getName());
    private static final VoltLogger HOST_LOG = new VoltLogger("HOST");

    private static final int DEP_saveTest = (int)
        SysProcFragmentId.PF_saveTest | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_saveTestResults = (int)
        SysProcFragmentId.PF_saveTestResults;
    private static final int DEP_createSnapshotTargets = (int)
        SysProcFragmentId.PF_createSnapshotTargets | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_createSnapshotTargetsResults = (int)
        SysProcFragmentId.PF_createSnapshotTargetsResults;
    private static final int DEP_snapshotSaveQuiesce = (int)
        SysProcFragmentId.PF_snapshotSaveQuiesce | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_snapshotSaveQuiesceResults = (int)
        SysProcFragmentId.PF_snapshotSaveQuiesceResults;

    public static final ColumnInfo nodeResultsColumns[] =
        new ColumnInfo[] {
            new ColumnInfo(CNAME_HOST_ID, CTYPE_ID),
            new ColumnInfo("HOSTNAME", VoltType.STRING),
            new ColumnInfo("TABLE", VoltType.STRING),
            new ColumnInfo("RESULT", VoltType.STRING),
            new ColumnInfo("ERR_MSG", VoltType.STRING)
    };

    public static final ColumnInfo partitionResultsColumns[] =
        new ColumnInfo[] {
                          new ColumnInfo(CNAME_HOST_ID, CTYPE_ID),
                          new ColumnInfo("HOSTNAME", VoltType.STRING),
                          new ColumnInfo(CNAME_SITE_ID, CTYPE_ID),
                          new ColumnInfo("RESULT", VoltType.STRING),
                          new ColumnInfo("ERR_MSG", VoltType.STRING)
    };

    public static final VoltTable constructNodeResultsTable()
    {
        return new VoltTable(nodeResultsColumns);
    }

    public static final VoltTable constructPartitionResultsTable()
    {
        return new VoltTable(partitionResultsColumns);
    }


    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster)
    {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_saveTest, this);
        site.registerPlanFragment(SysProcFragmentId.PF_saveTestResults, this);
        site.registerPlanFragment(SysProcFragmentId.PF_createSnapshotTargets, this);
        site.registerPlanFragment(SysProcFragmentId.PF_createSnapshotTargetsResults, this);
        site.registerPlanFragment(SysProcFragmentId.PF_snapshotSaveQuiesce, this);
        site.registerPlanFragment(SysProcFragmentId.PF_snapshotSaveQuiesceResults, this);
    }

    @Override
    public DependencyPair
    executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies,
                        long fragmentId,
                        ParameterSet params,
                        SystemProcedureExecutionContext context)
    {
        String hostname = ConnectionUtil.getHostnameOrAddress();
        if (fragmentId == SysProcFragmentId.PF_saveTest)
        {
            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            String file_path = (String) params.toArray()[0];
            String file_nonce = (String) params.toArray()[1];
            return saveTest(file_path, file_nonce, context, hostname);
        }
        else if (fragmentId == SysProcFragmentId.PF_saveTestResults)
        {
            return saveTestResults(dependencies);
        }
        else if (fragmentId == SysProcFragmentId.PF_createSnapshotTargets)
        {
            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            assert(params.toArray()[2] != null);
            assert(params.toArray()[3] != null);
            final String file_path = (String) params.toArray()[0];
            final String file_nonce = (String) params.toArray()[1];
            final long txnId = (Long)params.toArray()[2];
            byte block = (Byte)params.toArray()[3];
            SnapshotSaveAPI saveAPI = new SnapshotSaveAPI();
            VoltTable result = saveAPI.startSnapshotting(file_path, file_nonce, block, txnId, context, hostname);
            return new DependencyPair(SnapshotSave.DEP_createSnapshotTargets, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_createSnapshotTargetsResults)
        {
            return createSnapshotTargetsResults(dependencies);
        } else if (fragmentId == SysProcFragmentId.PF_snapshotSaveQuiesce) {
            // tell each site to quiesce
            context.getExecutionEngine().quiesce(context.getLastCommittedTxnId());
            VoltTable results = new VoltTable(new ColumnInfo("id", VoltType.INTEGER));
            results.addRow(Integer.parseInt(context.getSite().getTypeName()));
            return new DependencyPair(DEP_snapshotSaveQuiesce, results);
        }
        else if (fragmentId == SysProcFragmentId.PF_snapshotSaveQuiesceResults) {
            VoltTable dummy = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            dummy.addRow(VoltSystemProcedure.STATUS_OK);
            return new DependencyPair(DEP_snapshotSaveQuiesceResults, dummy);
        }
        assert (false);
        return null;
    }

    private DependencyPair createSnapshotTargetsResults(
            HashMap<Integer, List<VoltTable>> dependencies) {
        {
            TRACE_LOG.trace("Aggregating create snapshot target results");
            assert (dependencies.size() > 0);
            List<VoltTable> dep = dependencies.get(DEP_createSnapshotTargets);
            VoltTable result = null;
            for (VoltTable table : dep)
            {
                /**
                 * XXX Ning: There are two different tables here. We have to
                 * detect which table we are looking at in order to create the
                 * result table with the proper schema. Maybe we should make the
                 * result table consistent?
                 */
                if (result == null) {
                    if (table.getColumnType(2).equals(VoltType.INTEGER))
                        result = constructPartitionResultsTable();
                    else
                        result = constructNodeResultsTable();
                }

                while (table.advanceRow())
                {
                    // this will add the active row of table
                    result.add(table);
                }
            }
            return new
                DependencyPair( DEP_createSnapshotTargetsResults, result);
        }
    }

    private DependencyPair saveTest(String file_path, String file_nonce,
            SystemProcedureExecutionContext context, String hostname) {
        {
            VoltTable result = constructNodeResultsTable();
            // Choose the lowest site ID on this host to do the file scan
            // All other sites should just return empty results tables.
            int host_id = context.getExecutionSite().getCorrespondingHostId();
            Integer lowest_site_id =
                VoltDB.instance().getCatalogContext().siteTracker.
                getLowestLiveExecSiteIdForHost(host_id);
            if (context.getExecutionSite().getSiteId() == lowest_site_id)
            {
                TRACE_LOG.trace("Checking feasibility of save with path and nonce: "
                                + file_path + ", " + file_nonce);

                if (SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.get() != -1) {
                    result.addRow(
                                  Integer.parseInt(context.getSite().getHost().getTypeName()),
                                  hostname,
                                  "",
                                  "FAILURE",
                    "SNAPSHOT IN PROGRESS");
                    return new DependencyPair( DEP_saveTest, result);
                }

                for (Table table : SnapshotUtil.getTablesToSave(context.getDatabase()))
                {
                    File saveFilePath =
                        SnapshotUtil.constructFileForTable(table, file_path, file_nonce,
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
                            /*
                             * Sanity check that the file can be created
                             * and then delete it so empty files aren't
                             * orphaned if another part of the snapshot
                             * test fails.
                             */
                            if (saveFilePath.createNewFile()) {
                                saveFilePath.delete();
                            }
                        }
                        catch (IOException ex)
                        {
                            file_valid = "FAILURE";
                            err_msg = "FILE CREATION OF " + saveFilePath +
                            "RESULTED IN IOException: " + ex.getMessage();
                        }
                    }
                    result.addRow(Integer.parseInt(context.getSite().getHost().getTypeName()),
                                  hostname,
                                  table.getTypeName(),
                                  file_valid,
                                  err_msg);
                }
            }
            return new DependencyPair(DEP_saveTest, result);
        }
    }

    private DependencyPair saveTestResults(
            HashMap<Integer, List<VoltTable>> dependencies) {
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
            return new DependencyPair( DEP_saveTestResults, result);
        }
    }





    public VoltTable[] run(SystemProcedureExecutionContext ctx,
            String path, String nonce, long block) throws VoltAbortException
    {
        final long startTime = System.currentTimeMillis();
        String async = (block == 0) ? "Asynchronously" : "Synchronously";
        HOST_LOG.info(async + " saving database to path: " + path + ", ID: " + nonce + " at " + startTime);

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
                results[0].resetRowPosition();
                return results;
            }
        }

        performQuiesce();

        results = performSnapshotCreationWork( path, nonce, ctx.getCurrentTxnId(), (byte)block);
        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            stringer.key("txnId").value(ctx.getCurrentTxnId());
            stringer.endObject();
            setAppStatusString(stringer.toString());
        } catch (Exception e) {
            HOST_LOG.warn(e);
        }

        final long finishTime = System.currentTimeMillis();
        final long duration = finishTime - startTime;
        HOST_LOG.info("Snapshot initiation took " + duration + " milliseconds");
        return results;
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
        pfs[0].multipartition = true;
        ParameterSet params = new ParameterSet();
        params.setParameters(filePath, fileNonce);
        pfs[0].parameters = params;

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_saveTestResults;
        pfs[1].outputDepId = DEP_saveTestResults;
        pfs[1].inputDepIds = new int[] { DEP_saveTest };
        pfs[1].multipartition = false;
        pfs[1].parameters = new ParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_saveTestResults);
        return results;
    }

    private final VoltTable[] performSnapshotCreationWork(String filePath,
            String fileNonce,
            long txnId,
            byte block)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution node to create the files
        // that will be written to during the snapshot
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_createSnapshotTargets;
        pfs[0].outputDepId = DEP_createSnapshotTargets;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        ParameterSet params = new ParameterSet();
        params.setParameters(filePath, fileNonce, txnId, block);
        pfs[0].parameters = params;

        // This fragment aggregates the results of creating those files
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_createSnapshotTargetsResults;
        pfs[1].outputDepId = DEP_createSnapshotTargetsResults;
        pfs[1].inputDepIds = new int[] { DEP_createSnapshotTargets };
        pfs[1].multipartition = false;
        pfs[1].parameters = new ParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_createSnapshotTargetsResults);
        return results;
    }

    private final VoltTable[] performQuiesce()
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site flush export
        // data to disk with a sync
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_snapshotSaveQuiesce;
        pfs[0].outputDepId = DEP_snapshotSaveQuiesce;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].parameters = new ParameterSet();

        // This fragment aggregates the quiesce results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_snapshotSaveQuiesceResults;
        pfs[1].outputDepId = DEP_snapshotSaveQuiesceResults;
        pfs[1].inputDepIds = new int[] { DEP_snapshotSaveQuiesce };
        pfs[1].multipartition = false;
        pfs[1].parameters = new ParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_snapshotSaveQuiesceResults);
        return results;
    }
}
