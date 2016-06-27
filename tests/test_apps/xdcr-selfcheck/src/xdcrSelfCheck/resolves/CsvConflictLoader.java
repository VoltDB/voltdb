/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package xdcrSelfCheck.resolves;

import com.google_voltpatches.common.collect.ArrayListMultimap;
import com.google_voltpatches.common.collect.ListMultimap;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class CsvConflictLoader {

    private Client client;
    private File csvConflictReportDir;

    static VoltLogger LOG = new VoltLogger(CsvConflictLoader.class.getSimpleName());

    public CsvConflictLoader(Client client, String csvConflictReportPath) {
        this.client = client;
        this.csvConflictReportDir = new File(csvConflictReportPath);
        if (!csvConflictReportDir.exists()) {
            throw new IllegalArgumentException("Files does not exist: " + csvConflictReportPath);
        }
    }

    public void load() throws Exception {
        File xdcrConflictFolder = new File(csvConflictReportDir + "/xdcr_conflicts");
        if (! xdcrConflictFolder.exists()) {
            throw new IllegalArgumentException("XDCR conflict export folder does not exist: " + xdcrConflictFolder);
        }

        boolean done = false;
        final long start = System.currentTimeMillis();
        File[] csvFiles = xdcrConflictFolder.listFiles((dir, name) -> {
            return name.endsWith(".csv");
        });

        while (!done && System.currentTimeMillis() - start < 60000) {
            TimeUnit.SECONDS.sleep(5);

            File[] newCsvFiles = xdcrConflictFolder.listFiles((dir, name) -> {
                return name.endsWith(".csv");
            });

            if (newCsvFiles.length > 0 && newCsvFiles.length == csvFiles.length) {
                File[] finalCsvFiles = csvFiles;
                done = IntStream.range(0, newCsvFiles.length)
                        .allMatch(i -> newCsvFiles[i].length() == finalCsvFiles[i].length());
            }

            csvFiles = newCsvFiles;
        }

        if (csvFiles.length == 0) {
            LOG.warn("No conflict CSV reports found in " + xdcrConflictFolder.getPath());
        }

        for (File csvFile : csvFiles) {
            ListMultimap<String, XdcrConflict> conflicts = loadConflictReport(csvFile);
            String insertProcName = getInsertProcName(csvFile.getName());
            persistConflictReport(client, insertProcName, conflicts);
        }
    }

    private ListMultimap<String, XdcrConflict> loadConflictReport(File csvFile) throws IOException, JSONException {
        ListMultimap<String, XdcrConflict> result = ArrayListMultimap.create();
        ICsvListReader listReader = null;
        try {
            listReader = new CsvListReader(new FileReader(csvFile), CsvPreference.STANDARD_PREFERENCE);

            List<Object> csvConflict;
            int cnt = 0;
            while( (csvConflict = listReader.read(getProcessors())) != null ) {
                // System.out.println(String.format("lineNo=%s, rowNo=%s", listReader.getLineNumber(), listReader.getRowNumber()));

                XdcrConflict xdcrConflict = new XdcrConflict();
                xdcrConflict.setRowType((String)csvConflict.get(0));
                xdcrConflict.setActionType((String)csvConflict.get(1));
                xdcrConflict.setConflictType((String)csvConflict.get(2));
                xdcrConflict.setConflictOnPrimaryKey((int)csvConflict.get(3));
                xdcrConflict.setDecision((String)csvConflict.get(4));
                xdcrConflict.setClusterId((int)csvConflict.get(5));
                xdcrConflict.setTimeStamp((String)csvConflict.get(6));
                xdcrConflict.setDivergenceType((String)csvConflict.get(7));
                xdcrConflict.setTableName((String)csvConflict.get(8));
                xdcrConflict.setCurrentClusterId((int)csvConflict.get(9));
                xdcrConflict.setCurrentTimestamp((String)csvConflict.get(10));
                String tupleStr = (String)csvConflict.get(11); // TBD: double check this
                xdcrConflict.setTuple(
                        tupleStr != null && !tupleStr.equalsIgnoreCase("null") ? new JSONObject(tupleStr) : null);
                result.put(xdcrConflict.getCurrentTimestamp(), xdcrConflict);
                cnt++;

                //System.out.println(xdcrConflict);
            }
            LOG.info("Loaded " + cnt + " conflict records");

            return result;
        }
        finally {
            if( listReader != null ) {
                listReader.close();
            }
        }
    }

    private void persistConflictReport(Client client, String insertProcName, ListMultimap<String, XdcrConflict> report)
            throws JSONException, IOException, ProcCallException {

        for (String conflictingTimestamp : report.keySet()) {
            List<XdcrConflict> conflicts = report.get(conflictingTimestamp);

            long clusterid = -1;
            long cid = -1;
            long rid = -1;

            for (XdcrConflict conflict : conflicts) {
                JSONObject tuple = conflict.getTuple();
                if (tuple != null) {
                    clusterid = tuple.getLong("CLUSTERID");
                    cid = tuple.getLong("CID");
                    rid = tuple.getLong("RID");

                }

                client.callProcedure(insertProcName,
                        cid,
                        rid,
                        clusterid,
                        conflict.getCurrentClusterId(),
                        conflict.getCurrentTimestamp(),
                        conflict.getRowType(),
                        conflict.getActionType(),
                        conflict.getConflictType(),
                        conflict.getConflictOnPrimaryKey(),
                        conflict.getDecision(),
                        conflict.getTimeStamp(),
                        conflict.getDivergenceType(),
                        tuple != null ? conflict.getTuple().toString().getBytes() : null);
            }
        }
    }

    private String getInsertProcName(String csvConflictFilepath) {
        if (csvConflictFilepath.contains("XDCR_CONFLICTS_PARTITIONED")) {
            return "InsertXdcrPartitionedActualSP";
        }

        if (csvConflictFilepath.contains("XDCR_CONFLICTS_REPLICATED")) {
            return "InsertXdcrReplicatedActualSP";
        }

        throw new IllegalArgumentException("Unrecognized conflict table: " + csvConflictFilepath);
    }

    private static CellProcessor[] getProcessors() {
        final CellProcessor[] processors = new CellProcessor[] {
                new NotNull(), // rowType
                new NotNull(), // actionType
                new NotNull(), // conflictType
                new ParseInt(), // conflictOnPrimaryKey
                new NotNull(), // decision
                new ParseInt(), // clusterId
                new NotNull(), // timeStamp
                new NotNull(), // divergenceType
                new NotNull(), // tableName
                new ParseInt(), // conflictingClusterId
                new NotNull(), // conflictTimestamp
                new NotNull(), // json tuple
        };

        return processors;
    }

    public static void main(String[] args) throws Exception {

        String csvConflictReportPath = args[0];

        ClientConfig clientConfig = new ClientConfig("", "");
        Client client = ClientFactory.createClient(clientConfig);
        client.createConnection("localhost", 21212);

        CsvConflictLoader csvConflictLoader = new CsvConflictLoader(client, csvConflictReportPath);
        csvConflictLoader.load();
    }
}
