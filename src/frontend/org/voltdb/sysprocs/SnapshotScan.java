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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

import org.json_voltpatches.JSONArray;
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
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.logging.VoltLogger;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.TableSaveFile;
import org.voltdb.utils.VoltFile;

@ProcInfo(singlePartition = false)
public class SnapshotScan extends VoltSystemProcedure {
    private static final VoltLogger TRACE_LOG = new VoltLogger(SnapshotStatus.class.getName());

    private static final VoltLogger HOST_LOG = new VoltLogger("HOST");

    private static final int DEP_snapshotDigestScan = (int)
        SysProcFragmentId.PF_snapshotDigestScan | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_snapshotDigestScanResults = (int)
        SysProcFragmentId.PF_snapshotDigestScanResults;

    private static final int DEP_snapshotScan = (int)
        SysProcFragmentId.PF_snapshotScan | DtxnConstants.MULTIPARTITION_DEPENDENCY;

    private static final int DEP_snapshotScanResults = (int)
        SysProcFragmentId.PF_snapshotScanResults;

    private static final int DEP_hostDiskFreeScan = (int)
        SysProcFragmentId.PF_hostDiskFreeScan | DtxnConstants.MULTIPARTITION_DEPENDENCY;

    private static final int DEP_hostDiskFreeScanResults = (int)
        SysProcFragmentId.PF_hostDiskFreeScanResults;

    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster) {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_snapshotDigestScan, this);
        site.registerPlanFragment(SysProcFragmentId.PF_snapshotDigestScanResults, this);
        site.registerPlanFragment(SysProcFragmentId.PF_snapshotScan, this);
        site.registerPlanFragment(SysProcFragmentId.PF_snapshotScanResults, this);
        site.registerPlanFragment(SysProcFragmentId.PF_hostDiskFreeScan, this);
        site.registerPlanFragment(SysProcFragmentId.PF_hostDiskFreeScanResults, this);
    }

    private String errorString = null;

    @Override
    public DependencyPair
    executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies, long fragmentId, ParameterSet params,
                        final SystemProcedureExecutionContext context)
    {
        errorString = null;
        String hostname = ConnectionUtil.getHostnameOrAddress();
        if (fragmentId == SysProcFragmentId.PF_snapshotScan)
        {
            final VoltTable results = constructFragmentResultsTable();
            // Choose the lowest site ID on this host to do the file scan
            // All other sites should just return empty results tables.
            int host_id = context.getExecutionSite().getCorrespondingHostId();
            Integer lowest_site_id =
                VoltDB.instance().getCatalogContext().siteTracker.
                getLowestLiveExecSiteIdForHost(host_id);
            if (context.getExecutionSite().getSiteId() == lowest_site_id)
            {
                assert(params.toArray()[0] != null);
                assert(params.toArray()[0] instanceof String);
                final String path = (String)params.toArray()[0];
                List<File> relevantFiles = retrieveRelevantFiles(path);
                if (relevantFiles == null) {
                    results.addRow(
                                   Integer.parseInt(context.getSite().getHost().getTypeName()),
                                   hostname,
                                   "",
                                   "",
                                   0,
                                   0,
                                   "",
                                   "FALSE",
                                   0,
                                   "",
                                   "",
                                   0,
                                   "",
                                   "FAILURE",
                                   errorString);
                } else {
                    for (final File f : relevantFiles) {
                        if (f.getName().endsWith(".digest")) {
                            continue;
                        }
                        if (f.canRead()) {
                            try {
                                FileInputStream savefile_input = new FileInputStream(f);
                                try {
                                    TableSaveFile savefile =
                                        new TableSaveFile(
                                                          savefile_input.getChannel(),
                                                          1,
                                                          null);
                                    String partitions = "";

                                    for (int partition : savefile.getPartitionIds()) {
                                        partitions = partitions + "," + partition;
                                    }

                                    if (partitions.startsWith(",")) {
                                        partitions = partitions.substring(1);
                                    }

                                    results.addRow(
                                                   Integer.parseInt(context.getSite().getHost().getTypeName()),
                                                   hostname,
                                                   f.getParent(),
                                                   f.getName(),
                                                   savefile.getTxnId(),
                                                   org.voltdb.TransactionIdManager.getTimestampFromTransactionId(
                                                           savefile.getTxnId()),
                                                   savefile.getTableName(),
                                                   savefile.getCompleted() ? "TRUE" : "FALSE",
                                                                           f.length(),
                                                                           savefile.isReplicated() ? "TRUE" : "FALSE",
                                                                                                   partitions,
                                                                                                   savefile.getTotalPartitions(),
                                                                                                   f.canRead() ? "TRUE" : "FALSE",
                                                                                                               "SUCCESS",
                                                                                                               ""
                                    );
                                } catch (IOException e) {
                                    HOST_LOG.warn(e);
                                } finally {
                                    savefile_input.close();
                                }
                            } catch (IOException e) {
                                HOST_LOG.warn(e);
                            }
                        } else {
                            results.addRow(
                                           Integer.parseInt(context.getSite().getHost().getTypeName()),
                                           hostname,
                                           f.getParent(),
                                           f.getName(),
                                           0L,
                                           f.lastModified(),
                                           "",
                                           "FALSE",
                                           f.length(),
                                           "FALSE",
                                           "",
                                           -1,
                                           f.canRead() ? "TRUE" : "FALSE",
                                                       "SUCCESS",
                                                       ""
                            );
                        }
                    }
                }
            }
            return new DependencyPair( DEP_snapshotScan, results);
        } else if (fragmentId == SysProcFragmentId.PF_snapshotScanResults) {
            final VoltTable results = constructFragmentResultsTable();
            TRACE_LOG.trace("Aggregating Snapshot Scan  results");
            assert (dependencies.size() > 0);
            List<VoltTable> dep = dependencies.get(DEP_snapshotScan);
            for (VoltTable table : dep)
            {
                while (table.advanceRow())
                {
                    // this will add the active row of table
                    results.add(table);
                }
            }
            return new
                DependencyPair( DEP_snapshotScanResults, results);
        } else if (fragmentId == SysProcFragmentId.PF_snapshotDigestScan)
        {
            final VoltTable results = constructDigestResultsTable();
            // Choose the lowest site ID on this host to do the file scan
            // All other sites should just return empty results tables.
            int host_id = context.getExecutionSite().getCorrespondingHostId();
            Integer lowest_site_id =
                VoltDB.instance().getCatalogContext().siteTracker.
                getLowestLiveExecSiteIdForHost(host_id);
            if (context.getExecutionSite().getSiteId() == lowest_site_id)
            {
                assert(params.toArray()[0] != null);
                assert(params.toArray()[0] instanceof String);
                final String path = (String)params.toArray()[0];
                List<File> relevantFiles = retrieveRelevantFiles(path);
                if (relevantFiles == null) {
                    results.addRow(
                                   Integer.parseInt(context.getSite().getHost().getTypeName()),
                                   "",
                                   "",
                                   "",
                                   "FAILURE",
                                   errorString);
                } else {
                    for (final File f : relevantFiles) {
                        if (f.getName().endsWith(".vpt")) {
                            continue;
                        }
                        if (f.canRead()) {
                            try {
                                HashSet<String> tableNames = new HashSet<String>();
                                JSONArray tables = SnapshotUtil.CRCCheck(f).getJSONArray("tables");
                                for (int ii = 0; ii < tables.length(); ii++) {
                                    tableNames.add(tables.getString(ii));
                                }
                                final StringWriter sw = new StringWriter();
                                int ii = 0;
                                for (String name : tableNames) {
                                    sw.append(name);
                                    if (ii != tableNames.size() - 1) {
                                        sw.append(',');
                                    }
                                    ii++;
                                }
                                results.addRow(Integer.parseInt(context.getSite().getHost().getTypeName()),
                                               path,
                                               f.getName(),
                                               sw.toString(),
                                               "SUCCESS",
                                "");
                            } catch (Exception e) {
                                HOST_LOG.warn(e);
                            }
                        }
                    }
                }
            }
            return new DependencyPair( DEP_snapshotDigestScan, results);
        } else if (fragmentId == SysProcFragmentId.PF_snapshotDigestScanResults) {
            final VoltTable results = constructDigestResultsTable();
            TRACE_LOG.trace("Aggregating Snapshot Digest Scan  results");
            assert (dependencies.size() > 0);
            List<VoltTable> dep = dependencies.get(DEP_snapshotDigestScan);
            for (VoltTable table : dep)
            {
                while (table.advanceRow())
                {
                    // this will add the active row of table
                    results.add(table);
                }
            }
            return new
                DependencyPair( DEP_snapshotDigestScanResults, results);
        } else if (fragmentId == SysProcFragmentId.PF_hostDiskFreeScan)
        {
            final VoltTable results = constructDiskFreeResultsTable();
            // Choose the lowest site ID on this host to do the file scan
            // All other sites should just return empty results tables.
            int host_id = context.getExecutionSite().getCorrespondingHostId();
            Integer lowest_site_id =
                VoltDB.instance().getCatalogContext().siteTracker.
                getLowestLiveExecSiteIdForHost(host_id);
            if (context.getExecutionSite().getSiteId() == lowest_site_id)
            {
                assert(params.toArray()[0] != null);
                assert(params.toArray()[0] instanceof String);
                final String path = (String)params.toArray()[0];
                File dir = new VoltFile(path);

                if (dir.isDirectory()) {
                    final long free = dir.getUsableSpace();
                    final long total = dir.getTotalSpace();
                    final long used = total - free;
                    results.addRow(
                                   Integer.parseInt(context.getSite().getHost().getTypeName()),
                                   hostname,
                                   path,
                                   total,
                                   free,
                                   used,
                                   "SUCCESS",
                    "");
                } else {
                    results.addRow(
                                   Integer.parseInt(context.getSite().getHost().getTypeName()),
                                   hostname,
                                   path,
                                   0,
                                   0,
                                   0,
                                   "FAILURE",
                    "Path is not a directory");
                }
            }
            return new DependencyPair( DEP_hostDiskFreeScan, results);
        } else if (fragmentId == SysProcFragmentId.PF_hostDiskFreeScanResults) {
            final VoltTable results = constructDiskFreeResultsTable();
            TRACE_LOG.trace("Aggregating disk free results");
            assert (dependencies.size() > 0);
            List<VoltTable> dep = dependencies.get(DEP_hostDiskFreeScan);
            for (VoltTable table : dep)
            {
                while (table.advanceRow())
                {
                    // this will add the active row of table
                    results.add(table);
                }
            }
            return new
                DependencyPair( DEP_hostDiskFreeScanResults, results);
        }
        assert (false);
        return null;
    }

    private VoltTable constructFragmentResultsTable() {
        ColumnInfo[] result_columns = new ColumnInfo[15];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo(CNAME_HOST_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("PATH", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("NAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("TXNID", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("CREATED", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("TABLE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("COMPLETED", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("SIZE", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("IS_REPLICATED", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("PARTITIONS", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("TOTAL_PARTITIONS", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("READABLE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);

        return new VoltTable(result_columns);
    }

    private VoltTable constructDigestResultsTable() {
        ColumnInfo[] result_columns = new ColumnInfo[6];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo(CNAME_HOST_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("PATH", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("NAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("TABLES", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);

        return new VoltTable(result_columns);
    }

    public static final ColumnInfo clientColumnInfo[] = new ColumnInfo[] {
            new ColumnInfo("PATH", VoltType.STRING),
            new ColumnInfo("NONCE", VoltType.STRING),
            new ColumnInfo("TXNID", VoltType.BIGINT),
            new ColumnInfo("CREATED", VoltType.BIGINT),
            new ColumnInfo("SIZE", VoltType.BIGINT),
            new ColumnInfo("TABLES_REQUIRED", VoltType.STRING),
            new ColumnInfo("TABLES_MISSING", VoltType.STRING),
            new ColumnInfo("TABLES_INCOMPLETE", VoltType.STRING),
            new ColumnInfo("COMPLETE", VoltType.STRING)
    };

    private VoltTable constructClientResultsTable() {
        return new VoltTable(clientColumnInfo);
    }

    private VoltTable constructDiskFreeResultsTable() {
        ColumnInfo[] result_columns = new ColumnInfo[8];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo(CNAME_HOST_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("PATH", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("TOTAL", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("FREE", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("USED", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);

        return new VoltTable(result_columns);
    }

    private static class Table {
        private final int m_totalPartitions;
        private final HashSet<Long> m_partitionsSeen = new HashSet<Long>();
        private final long m_createTime;
        private final String m_name;
        private long m_size = 0;

        private Table(VoltTableRow r) {
            assert(r.getString("RESULT").equals("SUCCESS"));
            assert("TRUE".equals(r.getString("READABLE")));
            assert("TRUE".equals(r.getString("COMPLETED")));
            m_totalPartitions = (int)r.getLong("TOTAL_PARTITIONS");
            m_createTime = r.getLong("CREATED");
            m_name = r.getString("TABLE");
            String partitions[] = r.getString("PARTITIONS").split(",");
            for (String partition : partitions) {
                m_partitionsSeen.add(Long.parseLong(partition));
            }
            m_size += r.getLong("SIZE");
        }

        private void processRow(VoltTableRow r) {
            assert(r.getString("RESULT").equals("SUCCESS"));
            assert(m_totalPartitions == (int)r.getLong("TOTAL_PARTITIONS"));
            assert(m_createTime == r.getLong("CREATED"));
            assert("SUCCESS".equals(r.getString("RESULT")));
            assert("TRUE".equals(r.getString("COMPLETED")));
            m_size += r.getLong("SIZE");
            String partitions[] = r.getString("PARTITIONS").split(",");
            for (String partition : partitions) {
                m_partitionsSeen.add(Long.parseLong(partition));
            }
        }

        private boolean complete() {
            return m_partitionsSeen.size() == m_totalPartitions;
        }
    }

    private static class Snapshot {
        private final long m_txnId;
        private final long m_createTime;
        private final String m_path;
        private final String m_nonce;
        private final TreeMap<String, Table> m_tables = new TreeMap<String, Table>();
        private final HashSet<String> m_tableDigest = new HashSet<String>();

        private Snapshot(VoltTableRow r) {
            assert(r.getString("RESULT").equals("SUCCESS"));
            assert("TRUE".equals(r.getString("READABLE")));
            assert("TRUE".equals(r.getString("COMPLETED")));
            m_txnId = r.getLong("TXNID");
            m_createTime = r.getLong("CREATED");
            Table t = new Table(r);
            m_tables.put( t.m_name, t);
            m_nonce = r.getString("NAME").substring(0, r.getString("NAME").indexOf('-'));
            m_path = r.getString("PATH");
        }

        private void processRow(VoltTableRow r) {
            assert(r.getString("RESULT").equals("SUCCESS"));
            assert("TRUE".equals(r.getString("READABLE")));
            assert("TRUE".equals(r.getString("COMPLETED")));
            assert(r.getLong("CREATED") == m_createTime);
            Table t = m_tables.get(r.getString("TABLE"));
            if (t == null) {
                t = new Table(r);
                m_tables.put(t.m_name, t);
            } else {
                t.processRow(r);
            }
        }

        private void processDigest(String tablesString) {
            String tables[] = tablesString.split(",");
            for (String table : tables) {
                m_tableDigest.add(table);
            }
        }

        private Long size() {
            long size = 0;
            for (Table t : m_tables.values()) {
                size += t.m_size;
            }
            return size;
        }

        private String tablesRequired() {
            StringBuilder sb = new StringBuilder();
            for (String tableName : m_tableDigest) {
                sb.append(tableName);
                sb.append(',');
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            return sb.toString();
        }

        private String tablesMissing() {
            StringBuilder sb = new StringBuilder();
            for (String tableName : m_tableDigest) {
                if (!m_tables.containsKey(tableName)) {
                    sb.append(tableName);
                    sb.append(',');
                }
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            return sb.toString();
        }

        private String tablesIncomplete() {
            StringBuilder sb = new StringBuilder();
            for (Table t : m_tables.values()) {
                if (!t.complete()) {
                    sb.append(t.m_name);
                    sb.append(',');
                }
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            return sb.toString();
        }

        private String complete() {
            boolean complete = true;
            for (Table t : m_tables.values()) {
                if (!t.complete()) {
                    complete = false;
                    break;
                }
            }
            for (String tableName : m_tableDigest) {
                if (!m_tables.containsKey(tableName)) {
                    complete = false;
                }
            }
            return complete ? "TRUE" : "FALSE";
        }

        private Object[] asRow() {
            Object row[] = new Object[9];
            int ii = 0;
            row[ii++] = m_path;
            row[ii++] = m_nonce;
            row[ii++] = m_txnId;
            row[ii++] = m_createTime;
            row[ii++] = size();
            row[ii++] = tablesRequired();
            row[ii++] = tablesMissing();
            row[ii++] = tablesIncomplete();
            row[ii++] = complete();
            return row;
        }
    }

    private void hashToSnapshot(VoltTableRow r, HashMap<String, Snapshot> aggregates) {
        assert(r.getString("RESULT").equals("SUCCESS"));
        assert("TRUE".equals(r.getString("READABLE")));
        final String path = r.getString("PATH");
        final String nonce = r.getString("NAME").substring(0, r.getString("NAME").indexOf('-'));
        final String combined = path + File.separator + nonce;
        Snapshot s = aggregates.get(combined);
        if (s == null) {
            s = new Snapshot(r);
            aggregates.put(combined, s);
        } else {
            if (r.getLong("CREATED") != s.m_createTime){
                return;
            }
            s.processRow(r);
        }
    }

    private void hashDigestToSnapshot(VoltTableRow r, HashMap<String, Snapshot> aggregates) {
        assert(r.getString("RESULT").equals("SUCCESS"));
        final String path = r.getString("PATH");
        String name = r.getString("NAME");
        String nonce;
        /*
         * For compatibility with the pre 1.3 snapshots. Pre 1.3 all digests were the same at every
         * node so the name was the same. Now that the host id is embedded it is harder
         * to come up with the nonce used to create the Snapshot object. First check the filename
         * for a -, if it doesn't have one then the there is no -host_ because it is the old style.
         * If it has a - then the nonce will be before it.
         */
        if (name.indexOf("-") == -1) {
            nonce = r.getString("NAME").substring(0, r.getString("NAME").indexOf(".digest"));
        } else {
            nonce = r.getString("NAME").substring(0, r.getString("NAME").indexOf("-"));
        }
        final String combined = path + File.separator + nonce;
        Snapshot s = aggregates.get(combined);
        if (s == null) {
            return;
        } else {
            s.processDigest(r.getString("TABLES"));
        }
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx,
            String path) throws VoltAbortException
    {
        final long startTime = System.currentTimeMillis();
        if (path == null || path.equals("")) {
            ColumnInfo[] result_columns = new ColumnInfo[1];
            int ii = 0;
            result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
            VoltTable results[] = new VoltTable[] { new VoltTable(result_columns) };
            results[0].addRow("Provided path was null or the empty string");
            return results;
        }

        VoltTable scanResults = performSnapshotScanWork(path)[0];
        VoltTable clientResults = constructClientResultsTable();
        VoltTable diskFreeResults = performDiskFreeScanWork(path)[0];
        VoltTable digestScanResults = performSnapshotDigestScanWork(path)[0];
        HashMap<String, Snapshot> aggregates = new HashMap<String, Snapshot>();
        while (scanResults.advanceRow())
        {
            if (scanResults.getString("RESULT").equals("SUCCESS")
                    && scanResults.getString("READABLE").equals("TRUE") &&
                    scanResults.getString("COMPLETED").equals("TRUE")) {
                hashToSnapshot(scanResults, aggregates);
            }
        }

        while (digestScanResults.advanceRow()) {
            if (digestScanResults.getString("RESULT").equals("SUCCESS")) {
                hashDigestToSnapshot(digestScanResults, aggregates);
            }
        }

        for (Snapshot s : aggregates.values()) {
            clientResults.addRow(s.asRow());
        }

        final long endTime = System.currentTimeMillis();
        final long duration = endTime -startTime;
        HOST_LOG.info("Finished scanning snapshots. Took " + duration + " milliseconds");
        return new VoltTable[] { clientResults, diskFreeResults, scanResults };
    }

    private final List<File> retrieveRelevantFiles(String filePath) {
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

        return retrieveRelevantFiles(path, 0);
    }

    private final List<File> retrieveRelevantFiles(File f, int recursion) {
        assert(f.isDirectory());
        assert(f.canRead());

        ArrayList<File> retvals = new ArrayList<File>();

        if (recursion == 32) {
            return retvals;
        }

        for (File file : f.listFiles()) {
            if (file.isDirectory()) {
                if (!file.canRead()) {
                    if (!file.setReadable(true)) {
                        continue;
                    }
                }
                retvals.addAll(retrieveRelevantFiles(file, recursion++));
            } else {
                if (!file.getName().endsWith(".vpt") && !file.getName().endsWith(".digest")) {
                    continue;
                }
                if (!file.canRead()) {
                    file.setReadable(true);
                }
                retvals.add(file);
            }
        }
        return retvals;
    }

    private final VoltTable[] performSnapshotScanWork(String path)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_snapshotScan;
        pfs[0].outputDepId = DEP_snapshotScan;
        pfs[0].multipartition = true;
        ParameterSet params = new ParameterSet();
        params.setParameters(path);
        pfs[0].parameters = params;

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_snapshotScanResults;
        pfs[1].outputDepId = DEP_snapshotScanResults;
        pfs[1].inputDepIds  = new int[] { DEP_snapshotScan };
        pfs[1].multipartition = false;
        pfs[1].parameters = new ParameterSet();


        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_snapshotScanResults);
        return results;
    }

    private final VoltTable[] performSnapshotDigestScanWork(String path)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_snapshotDigestScan;
        pfs[0].outputDepId = DEP_snapshotDigestScan;
        pfs[0].multipartition = true;
        ParameterSet params = new ParameterSet();
        params.setParameters(path);
        pfs[0].parameters = params;

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_snapshotDigestScanResults;
        pfs[1].outputDepId = DEP_snapshotDigestScanResults;
        pfs[1].inputDepIds  = new int[] { DEP_snapshotDigestScan };
        pfs[1].multipartition = false;
        pfs[1].parameters = new ParameterSet();


        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_snapshotDigestScanResults);
        return results;
    }

    private final VoltTable[] performDiskFreeScanWork(String path)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_hostDiskFreeScan;
        pfs[0].outputDepId = DEP_hostDiskFreeScan;
        pfs[0].multipartition = true;
        ParameterSet params = new ParameterSet();
        params.setParameters(path);
        pfs[0].parameters = params;

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_hostDiskFreeScanResults;
        pfs[1].outputDepId = DEP_hostDiskFreeScanResults;
        pfs[1].inputDepIds  = new int[] { DEP_hostDiskFreeScan };
        pfs[1].multipartition = false;
        pfs[1].parameters = new ParameterSet();


        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_hostDiskFreeScanResults);
        return results;
    }
}
