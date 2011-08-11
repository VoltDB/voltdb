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
import java.io.FileFilter;
import java.util.HashMap;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.VoltFile;

@ProcInfo(singlePartition = false)
public class SnapshotDelete extends VoltSystemProcedure {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private static final VoltLogger TRACE_LOG = new VoltLogger(SnapshotStatus.class.getName());

    private static final int DEP_snapshotDelete = (int)
        SysProcFragmentId.PF_snapshotDelete | DtxnConstants.MULTIPARTITION_DEPENDENCY;

    private static final int DEP_snapshotDeleteResults = (int)
        SysProcFragmentId.PF_snapshotDeleteResults;

    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster)
    {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_snapshotDelete, this);
        site.registerPlanFragment(SysProcFragmentId.PF_snapshotDeleteResults, this);
    }

    private String errorString = null;

    @Override
    public DependencyPair
    executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies, long fragmentId, final ParameterSet params,
                        final SystemProcedureExecutionContext context)
    {
        String hostname = ConnectionUtil.getHostnameOrAddress();
        errorString = null;
        VoltTable result = constructFragmentResultsTable();
        if (fragmentId == SysProcFragmentId.PF_snapshotDelete)
        {
            // Choose the lowest site ID on this host to do the deletion.
            // All other sites should just return empty results tables.
            int host_id = context.getExecutionSite().getCorrespondingHostId();
            Integer lowest_site_id =
                VoltDB.instance().getCatalogContext().siteTracker.
                getLowestLiveExecSiteIdForHost(host_id);
            if (context.getExecutionSite().getSiteId() == lowest_site_id)
            {
                new Thread("Async snapshot deletion thread") {
                    @Override
                    public void run() {
                        assert(params.toArray()[0] != null);
                        assert(params.toArray()[0] instanceof String[]);
                        assert(((String[])params.toArray()[0]).length > 0);
                        assert(params.toArray()[1] != null);
                        assert(params.toArray()[1] instanceof String[]);
                        assert(((String[])params.toArray()[1]).length > 0);
                        assert(((String[])params.toArray()[0]).length == ((String[])params.toArray()[1]).length);

                        final String paths[] = (String[])params.toArray()[0];
                        final String nonces[] = (String[])params.toArray()[1];
                        for (int ii = 0; ii < paths.length; ii++) {
                            List<File> relevantFiles = retrieveRelevantFiles(paths[ii], nonces[ii]);
                            if (relevantFiles != null) {
                                for (final File f : relevantFiles) {
                                    //long size = f.length();
                                    boolean deleted = f.delete();
                                }
                            }
                        }
                    }
                }.start();
            }

            return new DependencyPair( DEP_snapshotDelete, result);
        } else if (fragmentId == SysProcFragmentId.PF_snapshotDeleteResults) {
            final VoltTable results = constructFragmentResultsTable();
            TRACE_LOG.trace("Aggregating Snapshot Delete  results");
            assert (dependencies.size() > 0);
            List<VoltTable> dep = dependencies.get(DEP_snapshotDelete);
            for (VoltTable table : dep)
            {
                while (table.advanceRow())
                {
                    // this will add the active row of table
                    results.add(table);
                }
            }
            return new
                DependencyPair( DEP_snapshotDeleteResults, results);
        }
        assert (false);
        return null;
    }

    private VoltTable constructFragmentResultsTable() {

        ColumnInfo[] result_columns = new ColumnInfo[9];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo(CNAME_HOST_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("PATH", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("NONCE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("NAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("SIZE", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("DELETED", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);

        return new VoltTable(result_columns);
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx,
            String paths[], String nonces[]) throws VoltAbortException
    {
        final long startTime = System.currentTimeMillis();
        VoltTable results[] = new VoltTable[1];
        ColumnInfo[] result_columns = new ColumnInfo[1];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
        results[0] = new VoltTable(result_columns);

        if (paths == null || paths.length == 0) {
            results[0].addRow("No paths supplied");
            return results;
        }

        for (String path : paths) {
            if (path == null || path.equals("")) {
                results[0].addRow("A path was null or the empty string");
                return results;
            }
        }

        if (nonces == null || nonces.length == 0) {
            results[0].addRow("No nonces supplied");
            return results;
        }

        for (String nonce : nonces) {
            if (nonce == null || nonce.equals("")) {
                results[0].addRow("A nonce was null or the empty string");
                return results;
            }
        }

        if (paths.length != nonces.length) {
            results[0].addRow("A path must be provided for every nonce");
            return results;
        }

        results = performSnapshotDeleteWork( paths, nonces);

        final long endTime = System.currentTimeMillis();
        final long duration = endTime -startTime;
        hostLog.info("Finished deleting snapshots. Took " + duration + " milliseconds");
        return results;
    }

    private final List<File> retrieveRelevantFiles(String filePath, String nonce) {
        final File path = new VoltFile(filePath);

        if (!path.exists()) {
            errorString = "Provided search path does not exist: " + filePath;
            return null;
        }

        if (!path.isDirectory()) {
            errorString = "Provided path exists but is not a directory: " + filePath;
            return null;
        }

        if (!path.canRead()) {
            if (!path.setReadable(true)) {
                errorString = "Provided path exists but is not readable: " + filePath;
                return null;
            }
        }

        if (!path.canWrite()) {
            if (!path.setWritable(true)) {
                errorString = "Provided path exists but is not writable: " + filePath;
                return null;
            }
        }

        return retrieveRelevantFiles(path, nonce);
    }

    private final List<File> retrieveRelevantFiles(File f, final String nonce) {
        assert(f.isDirectory());
        assert(f.canRead());
        assert(f.canWrite());
        return java.util.Arrays.asList(f.listFiles(new FileFilter() {

            @Override
            public boolean accept(File pathname) {
                if (pathname.isDirectory()) {
                    return false;
                }

                if (!pathname.getName().endsWith(".vpt") &&
                    !pathname.getName().endsWith(".digest") &&
                    !pathname.getName().endsWith(".jar")) {
                    return false;
                }

                if (pathname.getName().startsWith(nonce)) {
                    return true;
                }
                return false;
            }
        }));
    }

    private final VoltTable[] performSnapshotDeleteWork(String paths[], String nonces[])
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_snapshotDelete;
        pfs[0].outputDepId = DEP_snapshotDelete;
        pfs[0].multipartition = true;
        ParameterSet params = new ParameterSet();
        params.setParameters(paths, nonces);
        pfs[0].parameters = params;

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_snapshotDeleteResults;
        pfs[1].outputDepId = DEP_snapshotDeleteResults;
        pfs[1].inputDepIds  = new int[] { DEP_snapshotDelete };
        pfs[1].multipartition = false;
        pfs[1].parameters = new ParameterSet();


        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_snapshotDeleteResults);
        return results;
    }
}
