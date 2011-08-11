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

package org.voltdb.sysprocs.saverestore;

import java.io.BufferedInputStream;
import java.io.CharArrayWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.CRC32;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltFile;

public class SnapshotUtil {

    /**
     * Create a digest for a snapshot
     * @param txnId   transaction ID when snapshot was initiated
     * @param path    path to which snapshot files will be written
     * @param nonce   nonce used to distinguish this snapshot
     * @param tables   List of tables present in this snapshot
     * @param hostId   Host ID where this is happening
     * @param exportSequenceNumbers  ???
     * @throws IOException
     */
    public static Runnable writeSnapshotDigest(
        long txnId,
        String path,
        String nonce,
        List<Table> tables,
        int hostId,
        Map<String, List<Pair<Integer, Long>>> exportSequenceNumbers)
    throws IOException
    {
        final File f = new VoltFile(path, constructDigestFilenameForNonce(nonce, hostId));
        if (f.exists()) {
            if (!f.delete()) {
                throw new IOException("Unable to write table list file " + f);
            }
        }
        final FileOutputStream fos = new FileOutputStream(f);
        StringWriter sw = new StringWriter();
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            stringer.key("version").value(1);
            stringer.key("txnId").value(txnId);
            stringer.key("tables").array();
            for (int ii = 0; ii < tables.size(); ii++) {
                stringer.value(tables.get(ii).getTypeName());
            }
            stringer.endArray();
            stringer.key("exportSequenceNumbers").array();
            for (Map.Entry<String, List<Pair<Integer, Long>>> entry : exportSequenceNumbers.entrySet()) {
                stringer.object();

                stringer.key("exportTableName").value(entry.getKey());

                stringer.key("sequenceNumberPerPartition").array();
                for (Pair<Integer, Long> sequenceNumber : entry.getValue()) {
                    stringer.object();
                    stringer.key("partition").value(sequenceNumber.getFirst());
                    stringer.key("exportSequenceNumber").value(sequenceNumber.getSecond());
                    stringer.endObject();
                }
                stringer.endArray();

                stringer.endObject();
            }
            stringer.endArray();
            stringer.key("catalogCRC").value(VoltDB.instance().getCatalogContext().catalogCRC);
            stringer.endObject();
        } catch (JSONException e) {
            throw new IOException(e);
        }

        sw.append(stringer.toString());

        final byte tableListBytes[] = sw.getBuffer().toString().getBytes("UTF-8");
        final CRC32 crc = new CRC32();
        crc.update(tableListBytes);
        ByteBuffer fileBuffer = ByteBuffer.allocate(tableListBytes.length + 4);
        fileBuffer.putInt((int)crc.getValue());
        fileBuffer.put(tableListBytes);
        fileBuffer.flip();
        fos.getChannel().write(fileBuffer);
        return new Runnable() {
            @Override
            public void run() {
                try {
                    fos.getChannel().force(true);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    try {
                        fos.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    public static List<JSONObject> retrieveDigests(String path,
            String nonce) throws Exception {
        VoltFile directoryWithDigest = new VoltFile(path);
        ArrayList<JSONObject> digests = new ArrayList<JSONObject>();
        for (File f : directoryWithDigest.listFiles()) {
            if ( f.getName().equals(nonce + ".digest") || //old style digest name
                    (f.getName().startsWith(nonce + "-host_") && f.getName().endsWith(".digest"))) {//new style
                digests.add(CRCCheck(f));
            }
        }
        return digests;
    }

    /**
     * Write the current catalog associated with the database snapshot
     * to the snapshot location
     */
    public static Runnable writeSnapshotCatalog(String path, String nonce)
    throws IOException
    {
        String filename = SnapshotUtil.constructCatalogFilenameForNonce(nonce);
        try
        {
            return VoltDB.instance().getCatalogContext().writeCatalogJarToFile(path, filename);
        }
        catch (IOException ioe)
        {
            throw new IOException("Unable to write snapshot catalog to file: " +
                                  path + File.separator + filename);
        }
    }

    /**
     *
     * This isn't just a CRC check. It also loads the file and returns it as
     * a JSON object.
     * Check if the CRC of the snapshot digest. Note that this only checks if
     * the CRC at the beginning of the digest file matches the CRC of the digest
     * file itself.
     *
     * @param f
     *            The snapshot digest file object
     * @return The table list as a string
     * @throws IOException
     *             If CRC does not match
     */
    public static JSONObject CRCCheck(File f) throws IOException {
        final FileInputStream fis = new FileInputStream(f);
        try {
            final BufferedInputStream bis = new BufferedInputStream(fis);
            ByteBuffer crcBuffer = ByteBuffer.allocate(4);
            if (4 != bis.read(crcBuffer.array())) {
                throw new EOFException(
                        "EOF while attempting to read CRC from snapshot digest " + f +
                        " on host " + ConnectionUtil.getHostnameOrAddress());
            }
            final int crc = crcBuffer.getInt();
            final InputStreamReader isr = new InputStreamReader(bis, "UTF-8");
            CharArrayWriter caw = new CharArrayWriter();
            while (true) {
                int nextChar = isr.read();
                if (nextChar == -1) {
                    break;
                }
                //The original format had a single newline terminate the file.
                //Still here for backwards compatiblity with previously generated
                //digests
                if (nextChar == '\n') {
                    break;
                }
                caw.write(nextChar);
            }

            /*
             * Try and parse the contents as a JSON object. If it succeeds then assume
             * it is a the new version of the digest file. It is unlikely the old version
             * will successfully parse as JSON because it starts with a number
             * instead of an open brace.
             */
            JSONObject obj = null;
            try {
                obj = new JSONObject(caw.toString());
            } catch (JSONException e) {
                //assume it is the old format
            }

            /*
             * Convert the old style file to a JSONObject so it can be presented
             * via a consistent interface.
             */
            if (obj == null) {
                String tableList = caw.toString();
                byte tableListBytes[] = tableList.getBytes("UTF-8");
                CRC32 tableListCRC = new CRC32();
                tableListCRC.update(tableListBytes);
                tableListCRC.update("\n".getBytes("UTF-8"));
                final int calculatedValue = (int)tableListCRC.getValue();
                if (crc != calculatedValue) {
                    throw new IOException("CRC of snapshot digest did not match digest contents");
                }

                String tableNames[] = tableList.split(",");
                long txnId = Long.valueOf(tableNames[0]);

                obj = new JSONObject();
                try {
                    obj.put("version", 0);
                    obj.put("txnId", txnId);
                    for (int ii = 1; ii < tableNames.length; ii++) {
                        obj.append("tables", tableNames[ii]);
                    }
                } catch (JSONException e) {
                    throw new IOException(e);
                }
                return obj;
            } else {
                /*
                 * Verify the CRC and then return the data as a JSON object.
                 */
                String tableList = caw.toString();
                byte tableListBytes[] = tableList.getBytes("UTF-8");
                CRC32 tableListCRC = new CRC32();
                tableListCRC.update(tableListBytes);
                final int calculatedValue = (int)tableListCRC.getValue();
                if (crc != calculatedValue) {
                    throw new IOException("CRC of snapshot digest did not match digest contents");
                }
                return obj;
            }
        } finally {
            try {
                if (fis != null)
                    fis.close();
            } catch (IOException e) {}
        }
    }

    /**
     * Storage for information about files that are part of a specific snapshot
     */
    public static class Snapshot {
        public final List<File> m_digests = new ArrayList<File>();
        public final List<Set<String>> m_digestTables = new ArrayList<Set<String>>();
        public final Map<String, TableFiles> m_tableFiles = new TreeMap<String, TableFiles>();
    }

    /**
     * Description of all the files for a specific table that is part of a specific snapshot
     */
    public static class TableFiles {
        public final boolean m_isReplicated;
        TableFiles(boolean isReplicated) {
            m_isReplicated = isReplicated;
        }

        public final List<File> m_files = new ArrayList<File>();
        public final List<Boolean> m_completed = new ArrayList<Boolean>();
        public final List<Set<Integer>> m_validPartitionIds = new ArrayList<Set<Integer>>();
        public final List<Set<Integer>> m_corruptParititionIds = new ArrayList<Set<Integer>>();
        public final List<Integer> m_totalPartitionCounts = new ArrayList<Integer>();
    }

    /**
     * Simple filter that includes directories and files that end in .digest or .vpt
     */
    public static class SnapshotFilter implements FileFilter {
        @Override
        public boolean accept(File pathname) {
            if (pathname.isDirectory()) {
                return true;
            }
            if (pathname.getName().endsWith(".digest") || pathname.getName().endsWith(".vpt")) {
                return true;
            }
            return false;
        }
    };

    /**
     * Filter that looks for files related to a specific snapshot.
     */
    public static class SpecificSnapshotFilter extends SnapshotFilter {
        private final Set<String> snapshotNames;

        public SpecificSnapshotFilter(Set<String> snapshotNames) {
            this.snapshotNames = snapshotNames;
        }

        @Override
        public boolean accept(File pathname) {
            if (!super.accept(pathname)) {
                return false;
            }

            if (pathname.isDirectory()) {
                return true;
            }

            for (String snapshotName : snapshotNames) {
                if (pathname.getName().startsWith(snapshotName + "-")  ||
                        pathname.getName().equals(snapshotName + ".digest")) {
                    return true;
                }
            }

            return false;
        }
    }

    /**
     * Spider the provided directory applying the provided FileFilter. Optionally validate snapshot
     * files. Return a summary of partition counts, partition information, files, digests etc.
     * that can be used to determine if a valid restore plan exists.
     * @param directory
     * @param snapshots
     * @param filter
     * @param recursion
     * @param validate
     */
    public static void retrieveSnapshotFiles(
            File directory,
            Map<Long, Snapshot> snapshots,
            FileFilter filter,
            int recursion,
            boolean validate) {
        if (recursion == 32) {
            return;
        }
        if (!directory.exists()) {
            System.err.println("Error: Directory " + directory.getPath() + " doesn't exist");
            return;
        }
        if (!directory.canRead()) {
            System.err.println("Error: Directory " + directory.getPath() + " is not readable");
            return;
        }
        if (!directory.canExecute()) {
            System.err.println("Error: Directory " + directory.getPath() + " is not executable");
            return;
        }

        for (File f : directory.listFiles(filter)) {
            if (f.isDirectory()) {
                if (!f.canRead() || !f.canExecute()) {
                    System.err.println("Warning: Skipping directory " + f.getPath()
                            + " due to lack of read permission");
                } else {
                    retrieveSnapshotFiles( f, snapshots, filter, recursion++, validate);
                }
                continue;
            }
            if (!f.canRead()) {
                System.err.println("Warning: " + f.getPath() + " is not readable");
                continue;
            }

            FileInputStream fis = null;
            try {
                fis = new FileInputStream(f);
            } catch (FileNotFoundException e1) {
                System.err.println(e1.getMessage());
                continue;
            }

            try {
                if (f.getName().endsWith(".digest")) {
                    JSONObject digest = CRCCheck(f);
                    Long snapshotTxnId = digest.getLong("txnId");
                    Snapshot s = snapshots.get(snapshotTxnId);
                    if (s == null) {
                        s = new Snapshot();
                        snapshots.put(snapshotTxnId, s);
                    }
                    TreeSet<String> tableSet = new TreeSet<String>();
                    JSONArray tables = digest.getJSONArray("tables");
                    for (int ii = 0; ii < tables.length(); ii++) {
                        tableSet.add(tables.getString(ii));
                    }
                    s.m_digestTables.add(tableSet);
                    s.m_digests.add(f);
                } else {
                    HashSet<Integer> partitionIds = new HashSet<Integer>();
                    TableSaveFile saveFile = new TableSaveFile(fis.getChannel(), 1, null, true);
                    try {
                        for (Integer partitionId : saveFile.getPartitionIds()) {
                            partitionIds.add(partitionId);
                        }
                        if (validate && saveFile.getCompleted()) {
                            while (saveFile.hasMoreChunks()) {
                                BBContainer cont = saveFile.getNextChunk();
                                if (cont != null) {
                                    cont.discard();
                                }
                            }
                        }
                        partitionIds.removeAll(saveFile.getCorruptedPartitionIds());
                        Snapshot s = snapshots.get(saveFile.getTxnId());
                        if (s == null) {
                            s = new Snapshot();
                            snapshots.put(saveFile.getTxnId(), s);
                        }

                        TableFiles tableFiles = s.m_tableFiles.get(saveFile.getTableName());
                        if (tableFiles == null) {
                            tableFiles = new TableFiles(saveFile.isReplicated());
                            s.m_tableFiles.put(saveFile.getTableName(), tableFiles);
                        }
                        tableFiles.m_files.add(f);
                        tableFiles.m_completed.add(saveFile.getCompleted());
                        tableFiles.m_validPartitionIds.add(partitionIds);
                        tableFiles.m_corruptParititionIds.add(saveFile.getCorruptedPartitionIds());
                        tableFiles.m_totalPartitionCounts.add(saveFile.getTotalPartitions());
                    } finally {
                        saveFile.close();
                    }
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
                System.err.println("Error: Unable to process " + f.getPath());
            } catch (JSONException e) {
                System.err.println(e.getMessage());
                System.err.println("Error: Unable to process " + f.getPath());
            }
            finally {
                try {
                    if (fis != null) {
                        fis.close();
                    }
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * Returns a detailed report and a boolean indicating whether the snapshot can be successfully loaded
     * @param snapshotTime
     * @param snapshot
     */
    public static Pair<Boolean, String> generateSnapshotReport(Long snapshotTxnId, Snapshot snapshot) {
        CharArrayWriter caw = new CharArrayWriter();
        PrintWriter pw = new PrintWriter(caw);
        boolean snapshotConsistent = true;
        String indentString = "";
        pw.println(indentString + "TxnId: " + snapshotTxnId);
        pw.println(indentString + "Date: " +
                new Date(
                        org.voltdb.TransactionIdManager.getTimestampFromTransactionId(snapshotTxnId)));
        pw.println(indentString + "Digests:");
        indentString = "\t";
        TreeSet<String> digestTablesSeen = new TreeSet<String>();

        if (snapshot.m_digests.isEmpty()) {
            pw.println(indentString + "No digests found.");
            snapshotConsistent = false;
        } else {
            boolean inconsistent = false;

            /*
             * Iterate over the digests and ensure that they all contain the same list of tables
             */
            Map<Integer, List<Integer>> inconsistentDigests = new HashMap<Integer, List<Integer>>();
            for (int ii = 0; ii < snapshot.m_digests.size(); ii++) {
                inconsistentDigests.put( ii, new ArrayList<Integer>());
                Set<String> tables = snapshot.m_digestTables.get(ii);
                for (int zz = 0; zz < snapshot.m_digests.size(); zz++) {
                    if (zz == ii) {
                        continue;
                    }
                    if (!tables.equals(snapshot.m_digestTables.get(zz))) {
                        snapshotConsistent = false;
                        inconsistent = true;
                        inconsistentDigests.get(ii).add(zz);
                    }
                }
            }

            /*
             * Summarize what was inconsistent/consistent
             */
            if (!inconsistent) {
                for (int ii = 0; ii < snapshot.m_digests.size(); ii++) {
                    pw.println(indentString + snapshot.m_digests.get(ii).getPath());
                }
            } else {
                pw.println(indentString + "Not all digests are consistent");
                indentString = indentString + "\t";
                for (Map.Entry<Integer, List<Integer>> entry : inconsistentDigests.entrySet()) {
                    File left = snapshot.m_digests.get(entry.getKey());
                    pw.println(indentString + left.getPath() + " is inconsistent with:");
                    indentString = indentString + "\t";
                    for (Integer id : entry.getValue()) {
                        File right = snapshot.m_digests.get(id);
                        pw.println(indentString + right.getPath());
                    }
                    indentString = indentString.substring(1);
                }
            }

            /*
             * Print the list of tables found in the digests
             */
            indentString = indentString.substring(1);
            pw.print(indentString + "Tables: ");
            int ii = 0;

            for (int jj = 0; jj < snapshot.m_digestTables.size(); jj++) {
                for (String table : snapshot.m_digestTables.get(jj)) {
                    digestTablesSeen.add(table);
                }
            }

            for (String table : digestTablesSeen) {
                if (ii != 0) {
                    pw.print(", ");
                }
                ii++;
                pw.print(table);
            }
            pw.print("\n");
        }

        /*
         * Check that the total partition count is the same in every table file
         */
        Integer totalPartitionCount = null;
        indentString = indentString + "\t";
        for (Map.Entry<String, TableFiles> entry : snapshot.m_tableFiles.entrySet()) {
            if (entry.getValue().m_isReplicated) {
                continue;
            }
            for (Integer partitionCount : entry.getValue().m_totalPartitionCounts){
                if (totalPartitionCount == null) {
                    totalPartitionCount = partitionCount;
                } else if (totalPartitionCount != partitionCount) {
                    snapshotConsistent = false;
                    pw.println(indentString + "Partition count is not consistent throughout snapshot files for "
                            + entry.getKey() + ". Saw "
                            + partitionCount + " and " + totalPartitionCount);
                }
            }
        }

        /*
         * Now check that each individual table has enough information to be restored.
         * It is possible for a valid partition set to be available and still have a restore
         * fail because the restore plan loads a save file with a corrupt partition.
         */
        TreeSet<String> consistentTablesSeen = new TreeSet<String>();
        for (Map.Entry<String, TableFiles> entry : snapshot.m_tableFiles.entrySet()) {
            TableFiles tableFiles = entry.getValue();

            /*
             * Calculate the set of visible partitions not corrupted partitions
             */
            TreeSet<Integer> partitionsAvailable = new TreeSet<Integer>();
            int kk = 0;
            for (Set<Integer> validPartitionIds : tableFiles.m_validPartitionIds) {
                if (tableFiles.m_completed.get(kk++)) {
                    partitionsAvailable.addAll(validPartitionIds);
                }
            }

            /*
             * Ensure the correct range of partition ids is present
             */
            boolean partitionsPresent = false;
            if ((partitionsAvailable.size() == (tableFiles.m_isReplicated ? 1 : totalPartitionCount)) &&
                (partitionsAvailable.first() == 0) &&
                (partitionsAvailable.last() == (tableFiles.m_isReplicated ? 1 : totalPartitionCount) - 1)) {
                partitionsPresent = true;
            }

            /*
             * Report if any of the files have corrupt partitions
             */
            boolean hasCorruptPartitions = false;
            for (Set<Integer> corruptIds : tableFiles.m_corruptParititionIds) {
                if (!corruptIds.isEmpty()) {
                    hasCorruptPartitions = true;
                    snapshotConsistent = false;
                }
            }

            pw.println(indentString + "Table name: " + entry.getKey());
            indentString = indentString + "\t";
            pw.println(indentString + "Replicated: " + entry.getValue().m_isReplicated);
            pw.println(indentString + "Valid partition set available: " + partitionsPresent);
            pw.println(indentString + "Corrupt partitions present: " + hasCorruptPartitions);

            /*
             * Print information about individual files such as the partitions present and whether
             * they are corrupted
             */
            pw.println(indentString + "Files: ");
            indentString = indentString + "\t";
            for (int ii = 0; ii < tableFiles.m_files.size(); ii++) {
                String corruptPartitionIdString = "";
                int zz = 0;
                for (Integer partitionId : tableFiles.m_corruptParititionIds.get(ii)) {
                    if (zz != 0) {
                        corruptPartitionIdString = corruptPartitionIdString + ", ";
                    }
                    zz++;
                    corruptPartitionIdString = corruptPartitionIdString + partitionId;
                }

                String validPartitionIdString = "";
                zz = 0;
                for (Integer partitionId : tableFiles.m_validPartitionIds.get(ii)) {
                    if (zz != 0) {
                        validPartitionIdString = validPartitionIdString + ", ";
                    }
                    zz++;
                    validPartitionIdString = validPartitionIdString + partitionId;
                }

                if (corruptPartitionIdString.isEmpty()) {
                    consistentTablesSeen.add(entry.getKey());
                    pw.println(indentString + tableFiles.m_files.get(ii).getPath() +
                            " Completed: " + tableFiles.m_completed.get(ii) + " Partitions: " +
                            validPartitionIdString);
                } else {
                    pw.println(indentString + tableFiles.m_files.get(ii).getPath() +
                            " Completed: " + tableFiles.m_completed.get(ii) +
                            " Valid Partitions: " +
                            validPartitionIdString +
                            " Corrupt Partitions: " +
                            corruptPartitionIdString);
                }
            }
            indentString = indentString.substring(2);
        }
        indentString = indentString.substring(1);

        StringBuilder missingTables = new StringBuilder(8192);
        if (!consistentTablesSeen.containsAll(digestTablesSeen)) {
            snapshotConsistent = false;
            missingTables.append("Missing tables: ");
            Set<String> missingTablesSet = new TreeSet<String>(digestTablesSeen);
            missingTablesSet.removeAll(consistentTablesSeen);
            int hh = 0;
            for (String tableName : missingTablesSet) {
                if (hh > 0) {
                    missingTables.append(", ");
                }
                missingTables.append(tableName);
                hh++;
            }
            missingTables.append('\n');
        }

        /*
         * Tack on a summary at the beginning to indicate whether a restore is guaranteed to succeed
         * with this file set.
         */
        if (snapshotConsistent) {
            return Pair.of( true, "Snapshot valid\n" + caw.toString());
        } else {
            StringBuilder sb = new StringBuilder(8192);
            sb.append("Snapshot corrupted\n").append(missingTables).append(caw.toCharArray());
            return Pair.of( false,  sb.toString());
        }
    }

    /**
     * Generates a Filename to the snapshot file for the given table.
     * @param table
     * @param fileNonce
     * @param hostId
     */
    public static final String constructFilenameForTable(Table table,
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
        return filename_builder.toString();
    }

    public static final File constructFileForTable(Table table,
            String filePath,
            String fileNonce,
            String hostId)
    {
        return new VoltFile(filePath, SnapshotUtil.constructFilenameForTable(
            table, fileNonce, hostId));
    }

    /**
     * Generates the digest filename for the given nonce.
     * @param nonce
     * @param hostId
     */
    public static final String constructDigestFilenameForNonce(String nonce, int hostId) {
        return (nonce + "-host_" + hostId + ".digest");
    }

    /**
     * Generates the catalog filename for the given nonce.
     * @param nonce
     */
    public static final String constructCatalogFilenameForNonce(String nonce) {
        return (nonce + ".jar");
    }

    public static final List<Table> getTablesToSave(Database database)
    {
        CatalogMap<Table> all_tables = database.getTables();
        ArrayList<Table> my_tables = new ArrayList<Table>();
        for (Table table : all_tables)
        {
            // Make a list of all non-materialized, non-export only tables
            if ((table.getMaterializer() != null) ||
                    (CatalogUtil.isTableExportOnly(database, table)))
            {
                continue;
            }
            my_tables.add(table);
        }
        return my_tables;
    }

    public static final int[] getPartitionsOnHost(
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
