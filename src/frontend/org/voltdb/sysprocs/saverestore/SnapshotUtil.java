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

import org.voltdb.utils.Pair;
import org.voltdb.utils.DBBPool.BBContainer;

public class SnapshotUtil {

    /**
     * Create a digest for a snapshot containing the time of the snapshot and the list of tables included.
     * The first item in the comma separated list is the time in milliseconds as a string.
     * @param snapshotTime
     * @param path
     * @param nonce
     * @param tables
     * @throws IOException
     */
    public static void
        recordSnapshotTableList(
            long snapshotTime,
            String path,
            String nonce,
            List<String> tables) throws IOException {
        final File f = new File(path, nonce + ".digest");
        if (f.exists()) {
            if (!f.delete()) {
                throw new IOException("Unable to write table list file " + f);
            }
        }
        FileOutputStream fos = new FileOutputStream(f);
        StringWriter sw = new StringWriter();
        sw.append(Long.toString(snapshotTime));
        if (!tables.isEmpty()) {
            sw.append(',');
        }
        for (int ii = 0; ii < tables.size(); ii++) {
            sw.append(tables.get(ii));
            if (!(ii == (tables.size() - 1))) {
                sw.append(',');
            } else {
                sw.append('\n');
            }
        }

        final byte tableListBytes[] = sw.getBuffer().toString().getBytes("UTF-8");
        final CRC32 crc = new CRC32();
        crc.update(tableListBytes);
        ByteBuffer fileBuffer = ByteBuffer.allocate(tableListBytes.length + 4);
        fileBuffer.putInt((int)crc.getValue());
        fileBuffer.put(tableListBytes);
        fileBuffer.flip();
        fos.getChannel().write(fileBuffer);
        fos.getFD().sync();
    }

    public static List<String> retrieveRelevantTableNames(String path,
            String nonce) throws Exception {
        return retrieveRelevantTableNamesAndTime(new File(path, nonce + ".digest")).getSecond();
    }

    /**
     * Retrieve a list of tables from a digest. Doesn't return the snapshot time
     * value that is stashed in the digest file.
     * @param f
     * @return
     * @throws Exception
     */
    public static Pair<Long, List<String>> retrieveRelevantTableNamesAndTime(File f) throws Exception {
        final FileInputStream fis = new FileInputStream(f);
        try {
            final BufferedInputStream bis = new BufferedInputStream(fis);
            ByteBuffer crcBuffer = ByteBuffer.allocate(4);
            if (4 != bis.read(crcBuffer.array())) {
                throw new EOFException("EOF while attempting to read CRC from snapshot digest");
            }
            final int crc = crcBuffer.getInt();
            final InputStreamReader isr = new InputStreamReader(bis, "UTF-8");
            CharArrayWriter caw = new CharArrayWriter();
            while (true) {
                int nextChar = isr.read();
                if (nextChar == -1) {
                    throw new EOFException("EOF while reading snapshot digest");
                }
                if (nextChar == '\n') {
                    break;
                }
                caw.write(nextChar);
            }
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
            String actualTableNames[] = new String[tableNames.length - 1];
            System.arraycopy( tableNames, 1, actualTableNames, 0, tableNames.length - 1);
            return Pair.of(
                    Long.valueOf(tableNames[0]),
                    java.util.Arrays.asList(actualTableNames));
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) {
            }
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
                if (pathname.getName().startsWith(snapshotName + "-") ||
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
                    Pair<Long, List<String>> result = null;
                    try {
                        result = retrieveRelevantTableNamesAndTime(f);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                        System.err.println("Error: Unable to process digest " + f.getPath());
                        continue;
                    }
                    Long snapshotTime = result.getFirst();
                    Snapshot s = snapshots.get(snapshotTime);
                    if (s == null) {
                        s = new Snapshot();
                        snapshots.put(snapshotTime, s);
                    }
                    TreeSet<String> tableSet = new TreeSet<String>();
                    tableSet.addAll(result.getSecond());
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
                        Snapshot s = snapshots.get(saveFile.getCreateTime());
                        if (s == null) {
                            s = new Snapshot();
                            snapshots.put(saveFile.getCreateTime(), s);
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
            } finally {
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
     * @return
     */
    public static Pair<Boolean, String> generateSnapshotReport(Long snapshotTime, Snapshot snapshot) {
        CharArrayWriter caw = new CharArrayWriter();
        PrintWriter pw = new PrintWriter(caw);
        boolean snapshotConsistent = true;
        String indentString = "";
        pw.println(indentString + "Date: " + new Date(snapshotTime));
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
         * Tack on a summary at the beginning to indicate whether a restore is guaranteed to succede
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
}
