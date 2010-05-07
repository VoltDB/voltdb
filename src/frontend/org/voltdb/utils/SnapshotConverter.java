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

package org.voltdb.utils;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.Snapshot;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.SpecificSnapshotFilter;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.TableFiles;
import org.voltdb.utils.CSVTableSaveFile.Escaper;
import org.voltdb.utils.CSVTableSaveFile.TSVEscaper;
import org.voltdb.utils.CSVTableSaveFile.CSVEscaper;
import java.util.*;

public class SnapshotConverter {

    /**
     * @param args
     */
    public static void main(String[] args) {
        int ii = 0;
        String snapshotName = null;
        File directories[] = null;
        String tables[] = null;
        File outdir = null;
        String type = null;
        Escaper escaper = null;
        char delimeter = ' ';
        for (String arg : args) {
            if (arg.equals("--help")) {
                printHelpAndQuit(0);
            } else if (arg.equals("--name")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --name");
                    printHelpAndQuit(-1);
                }
                snapshotName = args[ii + 1];
            } else if (arg.equals("--dirs")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --dirs");
                    printHelpAndQuit(-1);
                }
                boolean invalidDir = false;
                String dirs[] = args[ii + 1].split(",");
                directories = new File[dirs.length];
                int zz = 0;
                for (String dir : dirs) {
                    File f = new File(dir);
                    if (!f.exists()) {
                        System.err.println("Error: " + dir + " does not exist");
                        invalidDir = true;
                    }
                    if (!f.canRead()) {
                        System.err.println("Error: " + dir + " does not have read permission set");
                        invalidDir = true;
                    }
                    if (!f.canExecute()) {
                        System.err.println("Error: " + dir + " does not have execute permission set");
                        invalidDir = true;
                    }
                    directories[zz++] = f;
                }
                if (invalidDir) {
                    System.exit(-1);
                }
            } else if (arg.equals("--tables")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --tables");
                    printHelpAndQuit(-1);
                }
                tables = args[ii + 1].split(",");
                if (tables.length == 0) {
                    System.err.println("Error: No tables specified");
                    System.exit(-1);
                }
                for (int dd = 0; dd < tables.length; dd++) {
                    if (tables[dd].isEmpty()) {
                        System.err.println("Error: Empty table name specified");
                        System.exit(-1);
                    }
                    tables[dd] = tables[dd].toUpperCase();
                }
            } else if (arg.equals("--outdir")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --outdir");
                    printHelpAndQuit(-1);
                }
                boolean invalidDir = false;
                outdir = new File(args[ii + 1]);
                if (!outdir.exists()) {
                    System.err.println("Error: " + outdir.getPath() + " does not exist");
                    invalidDir = true;
                }
                if (!outdir.canRead()) {
                    System.err.println("Error: " + outdir.getPath() + " does not have read permission set");
                    invalidDir = true;
                }
                if (!outdir.canExecute()) {
                    System.err.println("Error: " + outdir.getPath() + " does not have execute permission set");
                    invalidDir = true;
                }
                if (!outdir.canWrite()) {
                    System.err.println("Error: " + outdir.getPath() + " does not have write permission set");
                    invalidDir = true;
                }
                if (invalidDir) {
                    System.exit(-1);
                }
            }  else if (arg.equals("--type")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --type");
                    printHelpAndQuit(-1);
                }
                type = args[ii + 1];
                if (type.equalsIgnoreCase("csv")) {
                    escaper = new CSVEscaper();
                    delimeter = ',';
                } else if (type.equalsIgnoreCase("tsv")) {
                    escaper = new TSVEscaper();
                    delimeter = '\t';
                } else {
                    System.err.println("Error: --type must be one of CSV or TSV");
                    printHelpAndQuit(-1);
                }
            }
            ii++;
        }
        boolean fail = false;
        if (snapshotName == null) {
            System.err.println("Error: No --name specified");
            fail = true;
        }
        if (directories == null) {
            directories = new File[] { new File(".") };
        }
        if (tables == null) {
            System.err.println("Error: No --tables specified");
            fail = true;
        }
        if (outdir == null) {
            outdir = new File(".");
        }
        if (type == null) {
            System.err.println("Error: No --type specified");
            fail = true;
        }

        if (fail) {
            printHelpAndQuit(-1);
        }

        TreeMap<Long, Snapshot> snapshots = new TreeMap<Long, Snapshot>();
        SpecificSnapshotFilter filter = new SpecificSnapshotFilter(snapshotName);
        for (File directory : directories) {
            SnapshotUtil.retrieveSnapshotFiles( directory, snapshots, filter, 0, false);
        }

        if (snapshots.size() > 1) {
            System.err.println("Error: Found " + snapshots.size() + " snapshots with specified name");
            ii = 0;
            for (Map.Entry<Long, Snapshot> entry : snapshots.entrySet()) {
                System.err.println("Snapshot " + ii + " taken " + new Date(entry.getKey()));
                System.err.println("Files: ");
                for (File digest : entry.getValue().m_digests) {
                    System.err.println("\t" + digest.getPath());
                }
                for (Map.Entry<String, TableFiles> e2 : entry.getValue().m_tableFiles.entrySet()) {
                    System.err.println("\t" + e2.getKey());
                    for (File tableFile : e2.getValue().m_files) {
                        System.err.println("\t\t" + tableFile.getPath());
                    }
                }
                ii++;
            }
            System.exit(-1);
        }

        if (snapshots.size() < 1) {
            System.err.println("Error: Did not find any snapshots with the specified name");
            System.exit(-1);
        }

        /*
         * Build a plan for what partitions to pull from which save file
         */
        final Snapshot snapshot = snapshots.values().iterator().next();
        Map<String, Map<File, Set<Integer>>> tableToFilesWithPartitions =
            new TreeMap<String, Map<File, Set<Integer>>>();
        for (String tableName : tables) {
            if (!snapshot.m_tableFiles.containsKey(tableName)) {
                System.err.println("Error: Snapshot does not contain table " + tableName);
                System.exit(-1);
            }
            TableFiles tableFiles = snapshot.m_tableFiles.get(tableName);

            if (!tableFiles.m_isReplicated) {
                TreeSet<Integer> partitionsIds = new TreeSet<Integer>();
                Map<File, Set<Integer>> partitionsFromFile = new TreeMap<File, Set<Integer>>();
                for (ii = 0; ii < tableFiles.m_files.size(); ii++) {
                    Set<Integer> validParititions = tableFiles.m_validPartitionIds.get(ii);
                    TreeSet<Integer> partitionsToTake = new TreeSet<Integer>(validParititions);
                    partitionsToTake.removeAll(partitionsIds);
                    partitionsIds.addAll(validParititions);
                    if (!partitionsToTake.isEmpty()) {
                        partitionsFromFile.put(tableFiles.m_files.get(ii), partitionsToTake);
                    }
                }
                int totalPartitionCount = tableFiles.m_totalPartitionCounts.get(0);
                if (!((partitionsIds.size() == totalPartitionCount) &&
                        (partitionsIds.first() == 0) &&
                        (partitionsIds.last() == totalPartitionCount - 1))) {
                    System.err.println("Error: Not all partitions present for table " + tableName);
                    fail = true;
                } else {
                    tableToFilesWithPartitions.put(tableName, partitionsFromFile);
                }
            } else {
                Map<File, Set<Integer>> partitionsFromFile = new TreeMap<File, Set<Integer>>();
                partitionsFromFile.put(tableFiles.m_files.get(0), null);
                tableToFilesWithPartitions.put(tableName, partitionsFromFile);
            }
        }

        if (fail) {
            System.exit(-1);
        }

        /*
         * For every output file that will be created attempt to instantiate and print an error
         * if the file already exists or couldn't be created.
         */
        for (Map.Entry<String, Map<File, Set<Integer>>> entry : tableToFilesWithPartitions.entrySet()) {
            String tableName = entry.getKey();
            File outfile = new File(outdir.getPath() + File.separator + tableName + "." + type.toLowerCase());
            try {
                if (!outfile.createNewFile()) {
                    System.err.println("Error: Failed to create output file "
                            + outfile.getPath() + " for table " + tableName + "\n File already exists");
                    fail = true;
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
                System.err.println("Error: Failed to create output file "
                        + outfile.getPath() + " for table " + tableName);
                fail = true;
            }
        }

        if (fail) {
            System.exit(-1);
        }

        /*
         * Actually convert the tables and write the data to the appropriate destination
         */
        for (Map.Entry<String, Map<File, Set<Integer>>> entry : tableToFilesWithPartitions.entrySet()) {
            String tableName = entry.getKey();
            File outfile = new File(outdir.getPath() + File.separator + tableName + "." + type.toLowerCase());

            Map<File, Set<Integer>> partitionsFromFile = entry.getValue();
            for (Map.Entry<File, Set<Integer>> e2 : partitionsFromFile.entrySet()) {
                File infile = e2.getKey();
                Set<Integer> partitionSet = e2.getValue();
                int partitions[] = null;
                if (partitionSet != null) {
                    partitions = new int[partitionSet.size()];
                    ii = 0;
                    for (Integer partition : partitionSet) {
                        partitions[ii++] = partition;
                    }
                }
                try {
                    CSVTableSaveFile.convertTableSaveFile(escaper, delimeter, partitions, outfile, infile);
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    System.err.println("Error: Failed to convert " + infile.getPath() + " to " + outfile.getPath());
                }
            }
        }

        if (fail) {
            System.exit(-1);
        }
    }

    private static void printHelpAndQuit( int code) {
        System.out.println("java -cp <classpath> -Djava.library.path=<library path> org.voltdb.utils.SnapshotConverter --help");
        System.out.println("java -cp <classpath> -Djava.library.path=<library path> org.voltdb.utils.SnapshotConverter --name full_snapshot_name --dirs dir1[,dir2[,dir3[..]]] " +
                "--tables table1[,table2[,table3[..]]] --type CSV|TSV --outdir dir ");
        System.exit(code);
    }
}
