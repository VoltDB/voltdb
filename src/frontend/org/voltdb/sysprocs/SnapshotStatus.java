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

import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.io.StringWriter;
import java.io.PrintWriter;

import org.apache.log4j.Logger;
import org.voltdb.*;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot.Table;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot.TableIterator;
import org.voltdb.utils.VoltLoggerFactory;

@ProcInfo(singlePartition = false)
public class SnapshotStatus extends VoltSystemProcedure {
    private static final Logger TRACE_LOG =
        Logger.getLogger(SnapshotStatus.class.getName(),
                         VoltLoggerFactory.instance());

    private static final int DEP_scanSnapshotRegistries = (int)
        SysProcFragmentId.PF_scanSnapshotRegistries | DtxnConstants.MULTINODE_DEPENDENCY;

    private static final int DEP_scanSnapshotRegistriesResults = (int)
        SysProcFragmentId.PF_scanSnapshotRegistriesResults;

    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster)
    {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_scanSnapshotRegistries, this);
        site.registerPlanFragment(SysProcFragmentId.PF_scanSnapshotRegistriesResults, this);
    }

    @Override
    public DependencyPair
    executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies, long fragmentId, ParameterSet params,
                        final SystemProcedureExecutionContext context)
    {
        String hn = "";
        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            hn = localMachine.getHostName();
        } catch (java.net.UnknownHostException uhe) {
        }
        final String hostname = hn;
        final VoltTable results = constructFragmentResultsTable();
        if (fragmentId == SysProcFragmentId.PF_scanSnapshotRegistries)
        {
            TreeSet<Snapshot> snapshots = SnapshotRegistry.getSnapshotHistory();
            for (final Snapshot s : snapshots) {
                s.iterateTables(new TableIterator() {
                    @Override
                    public void next(Table t) {
                        results.addRow(
                                context.getSite().getHost().getTypeName(),
                                hostname,
                                t.name,
                                s.path,
                                s.nonce,
                                t.timeCreated,
                                t.timeClosed,
                                t.size,
                                t.error == null ? "SUCCESS" : "FAILURE",
                                t.error != null ? t.error.toString() : "");
                    }
                });
            }
            return new DependencyPair( DEP_scanSnapshotRegistries, results);
        } else if (fragmentId == SysProcFragmentId.PF_scanSnapshotRegistriesResults) {
            TRACE_LOG.trace("Aggregating Snapshot Status Scan  results");
            assert (dependencies.size() > 0);
            List<VoltTable> dep = dependencies.get(DEP_scanSnapshotRegistries);
            for (VoltTable table : dep)
            {
                while (table.advanceRow())
                {
                    // this will add the active row of table
                    results.add(table);
                }
            }
            return new
                DependencyPair( DEP_scanSnapshotRegistriesResults, results);
        }
        assert (false);
        return null;
    }

    private class SnapshotResult {
        public final String path;
        public final String nonce;
        public final long startTime;
        public long endTime;
        public long size;
        public boolean result = true;//true success, false failure

        public SnapshotResult(final VoltTableRow r) {
            path = r.getString("PATH");
            nonce = r.getString("NONCE");
            startTime = r.getLong("START_TIME");
            endTime = r.getLong("END_TIME");
            size = r.getLong("SIZE");
            if (!r.getString("RESULT").equals("SUCCESS")) {
                result = false;
            }
        }

        public void processRow(final VoltTableRow r) {
            endTime = Math.max(endTime, r.getLong("END_TIME"));
            size += r.getLong("SIZE");
            if (!r.getString("RESULT").equals("SUCCESS")) {
                result = false;
            }
        }
    }
    private VoltTable constructFragmentResultsTable() {

        ColumnInfo[] result_columns = new ColumnInfo[10];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo("HOST_ID", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("TABLE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("PATH", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("NONCE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("START_TIME", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("END_TIME", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("SIZE", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
        return new VoltTable(result_columns);
    }

    private VoltTable constructClientResultsTable() {
        ColumnInfo[] result_columns = new ColumnInfo[8];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo("PATH", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("NONCE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("START_TIME", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("END_TIME", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("SIZE", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("DURATION", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("THROUGHPUT", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
        return new VoltTable(result_columns);
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx) throws VoltAbortException
    {
        VoltTable scanResults = performScanWork()[0];
        VoltTable clientResults = constructClientResultsTable();
        HashMap<Long, SnapshotResult> aggregate = new HashMap<Long, SnapshotResult>();
        while (scanResults.advanceRow())
        {
            SnapshotResult snapshotResult = aggregate.get(scanResults.getLong("START_TIME"));
            if (snapshotResult == null) {
                aggregate.put(scanResults.getLong("START_TIME"), new SnapshotResult(scanResults));
            } else {
                snapshotResult.processRow(scanResults);
            }
        }
        for (SnapshotResult s : aggregate.values()) {
            final double duration = (s.endTime - s.startTime) / 1000.0;
            final double throughput =  (s.size / (1024.0 * 1024.0)) / duration;
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.printf("%.2f mb/sec", throughput);
            pw.flush();
            clientResults.addRow(
                    s.path,
                    s.nonce,
                    s.startTime,
                    s.endTime,
                    s.size,
                    s.endTime - s.startTime,
                    sw.toString(),
                    s.result ? "SUCCESS" : "FAILURE");
        }
        return new VoltTable[] { clientResults, scanResults };
    }

    private final VoltTable[] performScanWork()
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_scanSnapshotRegistries;
        pfs[0].outputDepId = DEP_scanSnapshotRegistries;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = true;
        ParameterSet params = new ParameterSet();
        params.setParameters();
        pfs[0].parameters = params;

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_scanSnapshotRegistriesResults;
        pfs[1].outputDepId = DEP_scanSnapshotRegistriesResults;
        pfs[1].inputDepIds = new int[] { DEP_scanSnapshotRegistries };
        pfs[1].multipartition = false;
        pfs[1].nonExecSites = false;
        pfs[1].parameters = new ParameterSet();


        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_scanSnapshotRegistriesResults);
        return results;
    }
}
