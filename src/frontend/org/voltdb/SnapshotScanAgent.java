/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
package org.voltdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltdb.StatsProcInputTable.ProcInputRow;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.ClientResponse;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.TableSaveFile;
import org.voltdb.utils.VoltFile;

/**
 * Agent responsible for collecting SnapshotScan info on this host.
 *
 */
public class SnapshotScanAgent extends OpsAgent
{
    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    private final String m_hostname;
    private String m_errorString = "";

    public SnapshotScanAgent() {
        super("SnapshotScanAgent");
        m_hostname = CoreUtils.getHostnameOrAddress();
    }

    @Override
    protected void collectStatsImpl(Connection c, long clientHandle, OpsSelector selector,
            ParameterSet params) throws Exception
    {
        JSONObject obj = new JSONObject();
        obj.put("selector", "SNAPSHOTSCAN");
        String err = null;
        if (selector == OpsSelector.SNAPSHOTSCAN) {
            err = parseParams(params, obj);
        }
        else {
            err = "SnapshotScanAgent received non-SNAPSHOTSCAN selector: " + selector.name();
        }
        if (err != null) {
            // Maintain old @SnapshotScan behavior.
            ColumnInfo[] result_columns = new ColumnInfo[1];
            result_columns[0] = new ColumnInfo("ERR_MSG", VoltType.STRING);
            VoltTable results[] = new VoltTable[] { new VoltTable(result_columns) };
            results[0].addRow(err);
            ClientResponseImpl errorResponse = new ClientResponseImpl(ClientResponse.SUCCESS,
                    ClientResponse.UNINITIALIZED_APP_STATUS_CODE, null, results, err);
            errorResponse.setClientHandle(clientHandle);
            ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
            buf.putInt(buf.capacity() - 4);
            errorResponse.flattenToBuffer(buf).flip();
            c.writeStream().enqueue(buf);
            return;
        }
        String subselector = obj.getString("subselector");

        PendingOpsRequest psr =
            new PendingOpsRequest(
                    selector,
                    subselector,
                    c,
                    clientHandle,
                    System.currentTimeMillis(),
                    obj);
        distributeOpsWork(psr, obj);
    }

    // Parse the provided parameter set object and fill in subselector and interval into
    // the provided JSONObject.  If there's an error, return that in the String, otherwise
    // return null.  Yes, ugly.  Bang it out, then refactor later.
    private String parseParams(ParameterSet params, JSONObject obj) throws Exception
    {
        String path = null;
        if (params.toArray().length != 1) {
            return "Incorrect number of arguments to @SnapshotScan (expects 1, received " +
                    params.toArray().length + ")";
        }
        Object first = params.toArray()[0];
        if (!(first instanceof String)) {
            return "First argument to @SnapshotScan must be a valid STRING path, instead was " +
                first;
        }
        path = (String)first;
        if (path == null || path == "") {
            return "Provided path was null or the empty string";
        }
        // Dupe SNAPSHOTSCAN as the subselector in case we consolidate later
        obj.put("subselector", "SNAPSHOTSCAN");
        obj.put("interval", false);
        obj.put("path", path);

        return null;
    }

    @Override
    protected void dispatchFinalAggregations(PendingOpsRequest request)
    {
        // Need to post-aggregate the snapshot scan results and the digest results into
        // the client results.
        // Provided tables in the PendingOpsRequest are:
        // Digest results, disk free results, snapshot scan results
        // Returned tables are, in order:
        // Client results, disk free results, snapshot scan results
        VoltTable digestScanResults = request.aggregateTables[0];
        VoltTable scanResults = request.aggregateTables[2];
        Map<String, Snapshot> aggregates = new HashMap<String, Snapshot>();
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

        List<ClientResultRow> clientResults = new ArrayList<ClientResultRow>();
        for (Snapshot s : aggregates.values()) {
            clientResults.add(new ClientResultRow(
                    (String)s.asRow()[0],
                    (String)s.asRow()[1],
                    (long)s.asRow()[2],
                    (long)s.asRow()[3],
                    (long)s.asRow()[4],
                    (String)s.asRow()[5],
                    (String)s.asRow()[6],
                    (String)s.asRow()[7],
                    (String)s.asRow()[8]
                    ));
        }
        Collections.sort(clientResults, new Comparator<ClientResultRow>() {
            @Override
            public int compare(ClientResultRow r1, ClientResultRow r2) {
                return r1.compareTo(r2);
            }
        });
        VoltTable clientSortedResults = constructClientResultsTable();
        for (ClientResultRow row : clientResults) {
            clientSortedResults.addRow(
                    row.path,
                    row.nonce,
                    row.txnid,
                    row.created,
                    row.size,
                    row.tablesRequired,
                    row.tablesMissing,
                    row.tablesIncomplete,
                    row.complete
                    );
        }
        request.aggregateTables[0] = clientSortedResults;
    }

    @Override
    protected void handleJSONMessage(JSONObject obj) throws Exception {
        VoltTable[] results = null;

        OpsSelector selector = OpsSelector.valueOf(obj.getString("selector").toUpperCase());
        if (selector == OpsSelector.SNAPSHOTSCAN) {
            results = collectSnapshotScanTables(obj);
        }
        else {
            hostLog.warn("SnapshotScanAgent received a non-SNAPSHOTSCAN OPS selector: " + selector);
        }
        sendOpsResponse(results, obj);
    }

    private VoltTable[] collectSnapshotScanTables(JSONObject obj) throws JSONException
    {
        String path = obj.getString("path");
        VoltTable[] tables = new VoltTable[3];

        // get snapshot scan tables
        VoltTable scanResults = getSnapshotScanResults(path);
        // get snapshot digest scan tables
        VoltTable digestResults = getSnapshotDigestScanResults(path);
        // get disk free scan tables
        VoltTable diskFreeResults = getDiskFreeResults(path);

        tables[0] = digestResults;
        tables[1] = diskFreeResults;
        tables[2] = scanResults;

        return tables;
    }

    private VoltTable getSnapshotScanResults(String path)
    {
        List<SnapshotResultRow> results = new ArrayList<SnapshotResultRow>();

        List<File> relevantFiles = retrieveRelevantFiles(path);
        if (relevantFiles == null) {
            results.add(new SnapshotResultRow(
                    m_messenger.getHostId(),
                    m_hostname,
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
                    m_errorString));
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
                                        savefile_input,
                                        1,
                                        null);
                            String partitions = "";

                            for (int partition : savefile.getPartitionIds()) {
                                partitions = partitions + "," + partition;
                            }

                            if (partitions.startsWith(",")) {
                                partitions = partitions.substring(1);
                            }

                            results.add(new SnapshotResultRow(
                                    m_messenger.getHostId(),
                                    m_hostname,
                                    f.getParent(),
                                    f.getName(),
                                    savefile.getTxnId(),
                                    savefile.getTimestamp(),
                                    savefile.getTableName(),
                                    savefile.getCompleted() ? "TRUE" : "FALSE",
                                    f.length(),
                                    savefile.isReplicated() ? "TRUE" : "FALSE",
                                    partitions,
                                    savefile.getTotalPartitions(),
                                    f.canRead() ? "TRUE" : "FALSE",
                                    "SUCCESS",
                                    ""
                                    ));
                        } catch (IOException e) {
                            SNAP_LOG.warn(e);
                        } finally {
                            savefile_input.close();
                        }
                    } catch (IOException e) {
                        SNAP_LOG.warn(e);
                    }
                } else {
                    results.add(new SnapshotResultRow(
                            m_messenger.getHostId(),
                            m_hostname,
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
                            ));
                }
            }
        }
        Collections.sort(results, new Comparator<SnapshotResultRow>() {
            @Override
            public int compare(SnapshotResultRow r1, SnapshotResultRow r2) {
                return r1.compareTo(r2);
            }
        });
        VoltTable sortedResults = constructFragmentResultsTable();
        for (SnapshotResultRow row : results) {
            sortedResults.addRow(
                    row.hostId,
                    row.hostName,
                    row.path,
                    row.name,
                    row.txnid,
                    row.created,
                    row.table,
                    row.completed,
                    row.size,
                    row.isReplicated,
                    row.partitions,
                    row.totalPartitions,
                    row.readable,
                    row.result,
                    row.errMsg
                    );
        }
        return sortedResults;
    }

    private VoltTable getSnapshotDigestScanResults(String path)
    {
        VoltTable results = constructDigestResultsTable();
        List<File> relevantFiles = retrieveRelevantFiles(path);
        if (relevantFiles == null) {
            results.addRow(
                    m_messenger.getHostId(),
                    "",
                    "",
                    "",
                    "FAILURE",
                    m_errorString);
        } else {
            for (final File f : relevantFiles) {
                if (f.getName().endsWith(".vpt")) {
                    continue;
                }
                if (f.canRead()) {
                    try {
                        Set<String> tableNames = new HashSet<String>();
                        JSONObject digest = SnapshotUtil.CRCCheck(f, SNAP_LOG);
                        if (digest == null) continue;
                        JSONArray tables = digest.getJSONArray("tables");
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
                        results.addRow(
                                m_messenger.getHostId(),
                                path,
                                f.getName(),
                                sw.toString(),
                                "SUCCESS",
                                "");
                    } catch (Exception e) {
                        SNAP_LOG.warn(e);
                    }
                }
            }
        }
        return results;
    }

    private VoltTable getDiskFreeResults(String path)
    {
        VoltTable results = constructDiskFreeResultsTable();
        File dir = new VoltFile(path);

        if (dir.isDirectory()) {
            final long free = dir.getUsableSpace();
            final long total = dir.getTotalSpace();
            final long used = total - free;
            results.addRow(
                    m_messenger.getHostId(),
                    m_hostname,
                    path,
                    total,
                    free,
                    used,
                    "SUCCESS",
                    "");
        } else {
            results.addRow(
                    m_messenger.getHostId(),
                    m_hostname,
                    path,
                    0,
                    0,
                    0,
                    "FAILURE",
                    "Path is not a directory");
        }

        return results;
    }

    private final List<File> retrieveRelevantFiles(String filePath) {
        final File path = new VoltFile(filePath);

        if (!path.exists()) {
            m_errorString = "Provided search path does not exist: " + filePath;
            return null;
        }

        if (!path.isDirectory()) {
            m_errorString = "Provided path exists but is not a directory: " + filePath;
            return null;
        }

        if (!path.canRead()) {
            if (!path.setReadable(true)) {
                m_errorString = "Provided path exists but is not readable: " + filePath;
                return null;
            }
        }

        return retrieveRelevantFiles(path, 0);
    }

    private final List<File> retrieveRelevantFiles(File f, int recursion) {
        assert(f.isDirectory());
        assert(f.canRead());

        List<File> retvals = new ArrayList<File>();

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

    static class ClientResultRow implements Comparable<ClientResultRow>
    {
        String path;
        String nonce;
        long txnid;
        long created;
        long size;
        String tablesRequired;
        String tablesMissing;
        String tablesIncomplete;
        String complete;

        public ClientResultRow(String path, String nonce, long txnid, long created,
                long size, String tablesRequired, String tablesMissing, String tablesIncomplete,
                String complete)
        {
            this.path = path;
            this.nonce = nonce;
            this.txnid = txnid;
            this.created = created;
            this.size = size;
            this.tablesRequired = tablesRequired;
            this.tablesMissing = tablesMissing;
            this.tablesIncomplete = tablesIncomplete;
            this.complete = complete;
        }

        @Override
        public int compareTo(ClientResultRow other) {
            if (created > other.created) {
                return -1;
            }
            else if (created < other.created) {
                return 1;
            }
            else {
                return 0;
            }
        }
    }

    static class SnapshotResultRow implements Comparable<SnapshotResultRow>
    {
        int hostId;
        String hostName;
        String path;
        String name;
        long txnid;
        long created;
        String table;
        String completed;
        long size;
        String isReplicated;
        String partitions;
        int totalPartitions;
        String readable;
        String result;
        String errMsg;

        public SnapshotResultRow(int hostId, String hostName, String path, String name,
                long txnid, long created, String table, String completed, long size,
                String isReplicated, String partitions, int totalPartitions, String readable,
                String result, String errMsg)
        {
            this.hostId = hostId;
            this.hostName = hostName;
            this.path = path;
            this.name = name;
            this.txnid = txnid;
            this.created = created;
            this.table = table;
            this.completed = completed;
            this.size = size;
            this.isReplicated = isReplicated;
            this.partitions = partitions;
            this.totalPartitions = totalPartitions;
            this.readable = readable;
            this.result = result;
            this.errMsg = errMsg;
        }

        @Override
        public int compareTo(SnapshotResultRow other) {
            if (created > other.created) {
                return -1;
            }
            else if (created < other.created) {
                return 1;
            }
            else {
                return table.compareTo(other.table);
            }
        }
    }

    private VoltTable constructFragmentResultsTable() {
        ColumnInfo[] result_columns = new ColumnInfo[15];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID);
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
        result_columns[ii++] = new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("PATH", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("NAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("TABLES", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);

        return new VoltTable(result_columns);
    }

    private VoltTable constructDiskFreeResultsTable() {
        ColumnInfo[] result_columns = new ColumnInfo[8];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("PATH", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("TOTAL", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("FREE", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("USED", VoltType.BIGINT);
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
        private final Map<String, Table> m_tables = new TreeMap<String, Table>();
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

    private void hashToSnapshot(VoltTableRow r, Map<String, Snapshot> aggregates) {
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

    private void hashDigestToSnapshot(VoltTableRow r, Map<String, Snapshot> aggregates) {
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
}
