/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;

import org.voltcore.logging.VoltLogger;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.Snapshot;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.SpecificSnapshotFilter;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.TableFiles;

public class SnapshotConverter {
    private static final VoltLogger CONSOLE_LOG = new VoltLogger("CONSOLE");

    /**
     * @param args
     */
    public static void main(String[] args) {
        String snapshotName = null;
        ArrayList<File> directories = new ArrayList<File>();
        ArrayList<String> tables = new ArrayList<String>();
        File outdir = null;
        String type = null;
        char delimiter = '\0';
        boolean filterHiddenColumns = false;

        for (int ii = 0; ii < args.length; ii++) {
            String arg = args[ii];
            if (arg.equals("--help")) {
                printHelpAndQuit(0);
            } else if (arg.equals("--dir")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --dirs");
                    printHelpAndQuit(-1);
                }
                boolean invalidDir = false;
                String dir = args[ii + 1];
                ii++;
                File f = new File(dir);
                if (!f.exists()) {
                    System.err.println("Error: " + dir + " does not exist");
                    invalidDir = true;
                } else {
                    if (!f.canRead()) {
                        System.err.println("Error: " + dir + " does not have read permission set");
                        invalidDir = true;
                    }
                    if (!f.canExecute()) {
                        System.err.println("Error: " + dir + " does not have execute permission set");
                        invalidDir = true;
                    }
                }
                directories.add(f);
                if (invalidDir) {
                    System.exit(-1);
                }
            } else if (arg.equals("--timezone")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --timezone");
                    printHelpAndQuit(-1);
                }
                String tzId = args[ii + 1];
                ii++;
                VoltTableUtil.tz = TimeZone.getTimeZone(tzId);
            } else if (arg.equals("--table")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --tables");
                    printHelpAndQuit(-1);
                }
                tables.add(args[ii +1].toUpperCase());
                ii++;
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
                } else {
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
                }
                if (invalidDir) {
                    System.exit(-1);
                }
                ii++;
            }  else if (arg.equals("--type")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --type");
                    printHelpAndQuit(-1);
                }
                type = args[ii + 1];
                if (type.equalsIgnoreCase("csv")) {
                    delimiter = ',';
                } else if (type.equalsIgnoreCase("tsv")) {
                    delimiter = '\t';
                } else {
                    System.err.println("Error: --type must be one of CSV or TSV");
                    printHelpAndQuit(-1);
                }
                ii++;
            } else if (arg.equals("--filter-hidden")) {
                filterHiddenColumns = true;
            } else {
                if (snapshotName != null) {
                    System.err.println("Error: Multiple snapshots specified for conversion. First - " + snapshotName + " second " + args[ii]);
                    printHelpAndQuit(-1);
                }
                snapshotName = args[ii];
            }
        }
        boolean fail = false;
        if (snapshotName == null) {
            System.err.println("Error: No --name specified");
            fail = true;
        }
        if (directories.isEmpty()) {
            directories.add(new File("."));
        }
        if (tables.isEmpty()) {
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

        Map<String, Snapshot> snapshots = new TreeMap<String, Snapshot>();
        HashSet<String> snapshotNames = new HashSet<String>();
        snapshotNames.add(snapshotName);
        SpecificSnapshotFilter filter = new SpecificSnapshotFilter(snapshotNames);
        for (File directory : directories) {
            SnapshotUtil.retrieveSnapshotFiles(directory, snapshots, filter, false, SnapshotPathType.SNAP_PATH, CONSOLE_LOG);
        }

        if (snapshots.size() > 1) {
            System.err.println("Error: Found " + snapshots.size() + " snapshots with specified name");
            int ii = 0;
            for (Snapshot entry : snapshots.values()) {
                System.err.println("Snapshot " + ii + " taken " + new Date(entry.getInstanceId().getTimestamp()));
                System.err.println("Files: ");
                for (File digest : entry.m_digests) {
                    System.err.println("\t" + digest.getPath());
                }
                for (Map.Entry<String, TableFiles> e2 : entry.m_tableFiles.entrySet()) {
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
                for (int ii = 0; ii < tableFiles.m_files.size(); ii++) {
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
                System.err.println("Error: Failed to create output file "
                        + outfile.getPath() + " for table " + tableName + ": " + e);
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
                Integer partitions[] = null;
                if (partitionSet != null) {
                    partitions = new Integer[partitionSet.size()];
                    int ii = 0;
                    for (Integer partition : partitionSet) {
                        partitions[ii++] = partition;
                    }
                }
                try {
                    CSVTableSaveFile.convertTableSaveFile(delimiter, partitions, outfile, infile, filterHiddenColumns);
                } catch (Exception e) {
                    System.err.println("Error: Failed to convert " + infile.getPath() + " to " + outfile.getPath());
                    e.printStackTrace();
                }
            }
        }

        if (fail) {
            System.exit(-1);
        }
    }

    private static void printHelpAndQuit( int code) {
        System.out.println("Usage: snapshotconverter --help");
        System.out.println("snapshotconverter --dir dir1 --dir dir2 --dir dir3 " +
                "--table table1 --table table2 --table table3 --type CSV|TSV --outdir dir snapshot_name --timezone GMT+0 [--filter-hidden]");
        System.exit(code);
    }
}
