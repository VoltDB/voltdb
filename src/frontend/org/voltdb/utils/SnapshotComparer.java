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

import static org.voltdb.RestoreAgent.checkSnapshotIsComplete;
import static org.voltdb.utils.SnapshotComparer.CONSOLE_LOG;
import static org.voltdb.utils.SnapshotComparer.DELIMITER;
import static org.voltdb.utils.SnapshotComparer.DIFF_FOLDER;
import static org.voltdb.utils.SnapshotComparer.REMOTE_SNAPSHOT_FOLDER;
import static org.voltdb.utils.SnapshotComparer.SNAPSHOT_LOG;
import static org.voltdb.utils.SnapshotComparer.STATUS_INCONSISTENCY;
import static org.voltdb.utils.SnapshotComparer.STATUS_INVALID_INPUT;
import static org.voltdb.utils.SnapshotComparer.STATUS_OK;
import static org.voltdb.utils.SnapshotComparer.TEMP_FOLDER;
import static org.voltdb.utils.SnapshotComparer.VPTFILE_PATTERN;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltdb.ElasticHashinator;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.RestoreAgent;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.VoltTable;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.Snapshot;
import org.voltdb.sysprocs.saverestore.TableSaveFile;

import com.google_voltpatches.common.collect.Lists;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;


/**
 * A command line utility for scanning and compare snapshots
 */

public class SnapshotComparer {
    public static int STATUS_OK = 0;
    public static int STATUS_INVALID_INPUT = -1;
    public static int STATUS_INCONSISTENCY = -2;
    public static int STATUS_UNKNOWN_ERROR = -3;

    public static int MAX_SNAPSHOT_DIFF_ROWS = Integer.getInteger("MAX_SNAPSHOT_DIFF_ROWS", 20);
    public static final VoltLogger CONSOLE_LOG = new VoltLogger("CONSOLE");
    public static final VoltLogger SNAPSHOT_LOG = new VoltLogger("SNAPSHOT");
    // A string builder to hold all snapshot validation errors, gets printed when no viable snapshot is found
    public static final StringBuilder m_ErrLogStr =
            new StringBuilder("The comparing process can not find a viable snapshot. "
                    + "Restore requires a complete, uncorrupted snapshot.");
    public static final String REMOTE_SNAPSHOT_FOLDER = "./remoteSnapshot/";
    public static final Pattern VPTFILE_PATTERN =
            Pattern.compile("host_(\\d)\\.vpt", Pattern.CASE_INSENSITIVE);
    // for temp result of csv/tsv file
    public static final char DELIMITER = '\t';
    public static final String TEMP_FOLDER = "./tempFolder/";
    public static final String DIFF_FOLDER = "./diffOutput/";


    public static void main(String[] args) {
        if (args.length == 0 || args[0].equals("--help")) {
            printHelpAndQuit(0);
        }

        Config config = new Config(args);
        if (!config.valid) {
            System.exit(STATUS_INVALID_INPUT);
        }

        SnapshotLoader source = new SnapshotLoader(config.local, config.username, config.sourceNonce, config.sourceDirs, config.sourceHosts, config.tables);
        System.err.println("main Snapshot configured orderLevel=" + config.orderLevel);
        if (config.selfCompare) {
            source.selfCompare(config.orderLevel);
        } else {
            SnapshotLoader target = new SnapshotLoader(config.local, config.username, config.targetNonce, config.targetDirs, config.targetHosts, config.tables);
            source.compareWith(target);
        }
    }

    private static void printHelpAndQuit(int code) {
        System.out.println("Usage: snapshotComparer --help");
        System.out.println("Self Comparision, verify data consistency among replicas of single snapshot: snapshotComparer --self nonce");
        System.out.println("for local snapshots, use --dirs for specify directories: snapshotComparer --self --nonce nonce1 --dirs dir1,dir2,dir3");
        System.out.println("for remote snapshots, use --paths and --hosts for specify remote directories: snapshotComparer --self --nonce nonce1 --paths path1,path2 --hosts host1,host2 --user username");
        System.out.println();
        System.out.println("Peer Comparision, verify data consistency among snapshots: snapshotComparer nonce1 nonce2");
        System.out.println("for local snapshots, use --dirs for specify directories: snapshotComparer  --nonce1 nonce1 --dirs1 dir1-1,dir1-2,dir1-3 nonce2 --dirs2 dir2-1,dir2-2,dir2-3");
        System.out.println("for remote snapshots, use --paths and --hosts for specify remote directories: snapshotComparer --nonce1 nonce1 --paths1 path1,path2 --hosts1 host1,host2 --nonce2 nonce2 --paths2 path1,path2 --hosts2 host1,host2 --user username");
        System.out.println();
        System.out.println("For fast integrity check only without row order, use --fastscan");
        System.out.println("For integrity check with full divergence report, use --deepscan");
        // System.out.println("For integrity check only with only chunk level row order, use --ignoreChunkOrder");
        System.exit(code);
    }

    static class Config {
        boolean selfCompare;
        // level of row order consistency
        // 0 for total order
        // 1 for chunk level order (there could be out of order within chunk, but not across chunk)
        // 2 for no order with checksum compare only (shallow scan)
        // 3 for no order with detail diff info (will reorder Lexicographically first then do the diff, deep scan)
        byte orderLevel = 0;
        Boolean local = null;
        String username = "";
        String password = "";

        String sourceNonce;
        String[] sourceDirs;
        String[] sourceHosts;

        String targetNonce;
        String[] targetDirs;
        String[] targetHosts;
        boolean cleanup = false;
        boolean valid = false;
        String[] tables;

        public Config(String[] args) {
            selfCompare = args[0].equalsIgnoreCase("--self");
            if (selfCompare) {
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (arg.equalsIgnoreCase("--nonce")) {
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --nonce");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        sourceNonce = args[i];
                    } else if (arg.equalsIgnoreCase("--ignoreChunkOrder")) {
                        orderLevel = 1;
                    } else if (arg.equalsIgnoreCase("--fastScan")) {
                        orderLevel = 2;
                    } else if (arg.equalsIgnoreCase("--deepScan")) {
                        orderLevel = 3;
                    } else if (arg.equalsIgnoreCase("--dirs")) {
                        if (local != null && !local) {
                            System.err.println("Error: already specify snapshot from remote");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        local = true;
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --dirs");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        sourceDirs = args[i].split(",");
                    } else if (arg.equalsIgnoreCase("--paths")) {
                        if (local != null && local) {
                            System.err.println("Error: already specify snapshot from local");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        local = false;
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --paths");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        sourceDirs = args[i].split(",");
                    } else if (arg.equalsIgnoreCase("--hosts")) {
                        if (local != null && local) {
                            System.err.println("Error: already specify snapshot from local");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        local = false;
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --hosts");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        sourceHosts = args[i].split(",");
                    } else if (arg.equalsIgnoreCase("--tables")) {
                            if (i + 1 >= args.length) {
                                System.err.println("Error: Not enough args following --tables");
                                printHelpAndQuit(STATUS_INVALID_INPUT);
                            }
                            i++;
                            tables = args[i].split(",");
                    } else if (arg.equalsIgnoreCase("--user")) {
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --user");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        username = args[i];
                    } else if (arg.equalsIgnoreCase("--cleanup")) {
                        cleanup = true;
                    }
                }
                if (sourceNonce == null || sourceNonce.isEmpty()) {
                    System.err.println("Error: Does not specify snapshot nonce.");
                    printHelpAndQuit(STATUS_INVALID_INPUT);
                }
                if (local == null) {
                    System.err.println("Error: Does not specify location of snapshot, either using --dirs for local or --paths for remote.");
                    printHelpAndQuit(STATUS_INVALID_INPUT);
                }
                if (!local && (
                        (sourceDirs == null) || (sourceHosts == null) || (sourceDirs.length == 0)
                                || (sourceDirs.length != sourceHosts.length))) {
                    System.err.println("Error: Directories and Host number does not match.");
                    printHelpAndQuit(STATUS_INVALID_INPUT);
                }
                if (!local && username.isEmpty()) {
                    System.err.println("Error: Does not specify username.");
                    printHelpAndQuit(STATUS_INVALID_INPUT);
                }
            } else {
                // TODO: better UI for specify target snapshot
                for (int i = 0; i < args.length; i++) {
                    String arg = args[i];
                    if (arg.equalsIgnoreCase("--nonce1")) {
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --nonce");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        sourceNonce = args[i];
                    } else if (arg.equalsIgnoreCase("--nonce2")) {
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --nonce");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        targetNonce = args[i];
                    } else if (arg.equalsIgnoreCase("--dirs1")) {
                        if (local != null && !local) {
                            System.err.println("Error: already specify snapshot from remote");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        local = true;
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --dirs");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        sourceDirs = args[i].split(",");
                    } else if (arg.equalsIgnoreCase("--dirs2")) {
                        if (local != null && !local) {
                            System.err.println("Error: already specify snapshot from remote");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        local = true;
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --dirs");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        targetDirs = args[i].split(",");
                    } else if (arg.equalsIgnoreCase("--paths1")) {
                        if (local != null && local) {
                            System.err.println("Error: already specify snapshot from local");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        local = false;
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --paths");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        sourceDirs = args[i].split(",");
                    } else if (arg.equalsIgnoreCase("--paths2")) {
                        if (local != null && local) {
                            System.err.println("Error: already specify snapshot from local");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        local = false;
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --paths");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        targetDirs = args[i].split(",");
                    } else if (arg.equalsIgnoreCase("--hosts1")) {
                        if (local != null && local) {
                            System.err.println("Error: already specify snapshot from local");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        local = false;
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --hosts");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        sourceHosts = args[i].split(",");
                    } else if (arg.equalsIgnoreCase("--hosts2")) {
                        if (local != null && local) {
                            System.err.println("Error: already specify snapshot from local");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        local = false;
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --hosts");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        targetHosts = args[i].split(",");
                    } else if (arg.equalsIgnoreCase("--user")) {
                        if (i + 1 >= args.length) {
                            System.err.println("Error: Not enough args following --user");
                            printHelpAndQuit(STATUS_INVALID_INPUT);
                        }
                        i++;
                        username = args[i];
                    } else if (arg.equalsIgnoreCase("--cleanup")) {
                        cleanup = true;
                    }
                }
                if (sourceNonce == null || sourceNonce.isEmpty()) {
                    System.err.println("Error: Does not specify source snapshot nonce.");
                    printHelpAndQuit(STATUS_INVALID_INPUT);
                }
                if (targetNonce == null || targetNonce.isEmpty()) {
                    System.err.println("Error: Does not specify comparing snapshot nonce.");
                    printHelpAndQuit(STATUS_INVALID_INPUT);
                }
                if (local == null) {
                    System.err.println("Error: Does not specify location of snapshot, either using --dirs for local or --paths for remote.");
                    printHelpAndQuit(STATUS_INVALID_INPUT);
                }
                if (!local && (
                        (sourceDirs == null) || (sourceHosts == null) || (sourceDirs.length == 0)
                                || (sourceDirs.length != sourceHosts.length)
                                || (targetDirs == null) || (targetHosts == null) || (targetDirs.length == 0)
                                || (targetDirs.length != targetHosts.length))) {
                    System.err.println("Error: Directories and Host number does not match.");
                    printHelpAndQuit(STATUS_INVALID_INPUT);
                }
                if (!local && username.isEmpty()) {
                    System.err.println("Error: Does not specify username.");
                    printHelpAndQuit(STATUS_INVALID_INPUT);
                }
            }
            if (!local && cleanup) {
                try {
                    FileUtils.deleteDirectory(new File(REMOTE_SNAPSHOT_FOLDER));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (cleanup) {
                try {
                    FileUtils.deleteDirectory(new File(TEMP_FOLDER));
                    FileUtils.deleteDirectory(new File(DIFF_FOLDER));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            valid = true;
        }
    }
    public static void compare(List<VoltTable> ftr, List<VoltTable> ftc, List<String> diff) {
        if (ftr.isEmpty() || ftc.isEmpty()) {
            return;
        }
        if (diff.size() >= MAX_SNAPSHOT_DIFF_ROWS) {
            return;
        }
        VoltTable tr = ftr.get(0);
        VoltTable tc = ftc.get(0);
        while (true) {
            boolean trMore = tr.advanceRow();
            if (!trMore) {
                ftr.remove(0);
            }
            boolean tcMore = tc.advanceRow();
            if (!tcMore) {
                ftc.remove(0);
            }
            if (!tcMore || !trMore) {
                if(trMore) {
                    // move cursor to previous position
                    int activeIndex = tr.getActiveRowIndex();
                    if (activeIndex > 0) {
                        tr.resetRowPosition();
                        tr.advanceToRow(0);
                        tr.advanceToRow(activeIndex - 1);
                    }
                }
                if(tcMore) {
                    // move cursor to previous position
                    int activeIndex = tc.getActiveRowIndex();
                    if (activeIndex > 0) {
                        tc.resetRowPosition();
                        tc.advanceToRow(0);
                        tc.advanceToRow(activeIndex - 1);
                    }
                }
                break;
            }
            if (diff.size() < SnapshotComparer.MAX_SNAPSHOT_DIFF_ROWS) {
                if (!tr.getRawRow().equals(tc.getRawRow())) {
                    diff.add("\nfile 1:" + tr.getRow() + "\nfile 2:" + tc.getRow());
                }
            }
        }
        if (!ftr.isEmpty() && !ftc.isEmpty()) {
           compare(ftr, ftc, diff);
        }
    }
}

class SnapshotLoader {
    final String nonce;
    final String[] dirs;
    final String[] hosts;
    InMemoryJarfile jar;
    ElasticHashinator hashinator;
    final Snapshot snapshot;
    final Map<String, Boolean> tables;
    int partitionCount, totalHost;

    public SnapshotLoader(boolean local, String username, String nonce, String[] dirs, String[] hosts, String[] verifyTables) {
        this.dirs = dirs;
        this.nonce = nonce;
        this.hosts = hosts;
        ArrayList<File> directories = new ArrayList<>();
        if (local) {
            boolean invalidDir = false;
            for (String path : dirs) {
                File f = new File(path);
                if (!f.exists()) {
                    System.err.println("Error: " + path + " does not exist");
                    invalidDir = true;
                }
                if (!f.canRead()) {
                    System.err.println("Error: " + path + " does not have read permission set");
                    invalidDir = true;
                }
                if (!f.canExecute()) {
                    System.err.println("Error: " + path + " does not have execute permission set");
                    invalidDir = true;
                }
                if (invalidDir) {
                    System.exit(STATUS_INVALID_INPUT);
                }
                directories.add(f);
            }
            if (directories.isEmpty()) {
                directories.add(new File("."));
            }
        } else {
            // if from remote, first fetch to local
            File localRootDir = new File(REMOTE_SNAPSHOT_FOLDER + nonce);
            localRootDir.mkdirs();
            for (int i = 0; i < hosts.length; i++) {
                File localDir = new File(localRootDir.getPath() + PATHSEPARATOR + hosts[i]);
                localDir.mkdirs();
                if (!downloadFiles(username, hosts[i], dirs[i], localDir.getPath(), nonce)) {
                    System.exit(STATUS_INVALID_INPUT);
                }
                directories.add(localDir);
            }
        }
        // Check snapshot completeness
        Map<String, SnapshotUtil.Snapshot> snapshots = new TreeMap<>();
        HashSet<String> snapshotNames = new HashSet<>();
        snapshotNames.add(nonce);
        SnapshotUtil.SpecificSnapshotFilter filter = new SnapshotUtil.SpecificSnapshotFilter(snapshotNames);
        for (File directory : directories) {
            SnapshotUtil.retrieveSnapshotFiles(directory, snapshots, filter, false, SnapshotPathType.SNAP_PATH, SNAPSHOT_LOG);
        }

        if (snapshots.size() > 1) {
            System.err.println("Error: Found " + snapshots.size() + " snapshots with specified name");
            int ii = 0;
            for (SnapshotUtil.Snapshot entry : snapshots.values()) {
                System.err.println("Snapshot " + ii + " taken " + new Date(entry.getInstanceId().getTimestamp()));
                System.err.println("Files: ");
                for (File digest : entry.m_digests) {
                    System.err.println("\t" + digest.getPath());
                }
                for (Map.Entry<String, SnapshotUtil.TableFiles> e2 : entry.m_tableFiles.entrySet()) {
                    System.err.println("\t" + e2.getKey());
                    for (File tableFile : e2.getValue().m_files) {
                        System.err.println("\t\t" + tableFile.getPath());
                    }
                }
                ii++;
            }
            System.exit(STATUS_INVALID_INPUT);
        }

        if (snapshots.size() < 1) {
            System.err.println("Error: Did not find any snapshots with the specified name");
            System.exit(STATUS_INVALID_INPUT);
        }
        snapshot = snapshots.values().iterator().next();
        RestoreAgent.SnapshotInfo info = checkSnapshotIsComplete(snapshot.getTxnId(), snapshot, SnapshotComparer.m_ErrLogStr, 0);
        if (info == null) {
            System.exit(STATUS_INVALID_INPUT);
        }
        partitionCount = info.partitionCount;

        // load the in memory jar
        try {
            jar = getInMemoryJarFileBySnapShotName(directories.get(0).getPath(), nonce);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(STATUS_INVALID_INPUT);
        }

        // Deserialize hashinator from .hash file
        // Use HASH_EXTENSION
        // ElasticHashinator hashinator = getHashinatorFromFile(dirs[0], nonce, Integer.parseInt(hosts.get(0)));
        // Validate SnapshotName
        // Collect tables
        tables = new HashMap<>();
        // check all tables, get from catalog
        Set<String> tableSet = new HashSet<>();
        if (verifyTables != null) {
            for (String tablename: verifyTables) {
                tableSet.add(tablename.toUpperCase());
            }
        }
        for (SnapshotTableInfo sti : SnapshotUtil.getTablesToSave(CatalogUtil.getDatabaseFrom(jar), t-> true, t->false)) {
            if (verifyTables!= null && tableSet.contains(sti.getName().toUpperCase())) {
                tables.put(sti.getName(), sti.isReplicated());
                tableSet.remove(sti.getName().toUpperCase());
            }
            if (verifyTables == null) {
                tables.put(sti.getName(), sti.isReplicated());
            }
        }
        if (verifyTables != null && !tableSet.isEmpty()) {
            System.err.println("Error: Cannot find snapshot for these tables:" + tableSet);
            System.exit(STATUS_INVALID_INPUT);
        }
    }

    /**
     * Validate the data consistency within the snapshot
     */
    // Todo: now is 1-1 comparing, implement m-way comparing
    public void selfCompare(byte orderLevel) {
        boolean fail = false;
        CONSOLE_LOG.info("selfCompare Snapshot orderLevel=" + orderLevel);
        // Build a plan for which save file as the baseline for each partition
        Map<String, List<List<File>>> tableToCopies = new HashMap<>();
        for (String tableName : tables.keySet()) {
            if (!snapshot.m_tableFiles.containsKey(tableName)) {
                System.err.println("Error: Snapshot does not contain table " + tableName);
                System.exit(STATUS_INVALID_INPUT);
            }
            SnapshotUtil.TableFiles tableFiles = snapshot.m_tableFiles.get(tableName);
            if (!tableFiles.m_isReplicated) {
                TreeSet<Integer> partitionsIds = new TreeSet<>();
                List<List<File>> partitionToFiles = new ArrayList<>();
                for (int i = 0; i < partitionCount; i++) {
                    partitionToFiles.add(new ArrayList<>());
                }
                for (int ii = 0; ii < tableFiles.m_files.size(); ii++) {
                    Set<Integer> validPartitions = tableFiles.m_validPartitionIds.get(ii);
                    partitionsIds.addAll(validPartitions);
                    for (int p : validPartitions) {
                        partitionToFiles.get(p).add(tableFiles.m_files.get(ii));
                    }
                }
                int totalPartitionCount = tableFiles.m_totalPartitionCounts.get(0);
                if (!((partitionsIds.size() == totalPartitionCount) &&
                        (partitionsIds.first() == 0) &&
                        (partitionsIds.last() == totalPartitionCount - 1))) {
                    System.err.println("Error: Not all partitions present for table " + tableName);
                    fail = true;
                } else {
                    tableToCopies.put(tableName, partitionToFiles);
                }
            } else {
                List<List<File>> partitionToFiles = new ArrayList<>();
                partitionToFiles.add(new ArrayList<>());
                partitionToFiles.get(0).addAll(tableFiles.m_files);
                tableToCopies.put(tableName, partitionToFiles);
            }
        }
        if (fail) {
            System.exit(STATUS_INVALID_INPUT);
        }
        if (orderLevel == 3) {
            File tempFolder = new File(TEMP_FOLDER);
            tempFolder.mkdir();
            tempFolder = new File(DIFF_FOLDER);
            tempFolder.mkdir();
        }

        // based on the plan, retrieve the file and compare
        Set<String> inconsistentTable = new HashSet<>();
        for (String tableName : tables.keySet()) {
            List<List<File>> partitionToFiles = tableToCopies.get(tableName);
            boolean isReplicated = tables.get(tableName);
            for (int p = 0; p < partitionToFiles.size(); p++) {
                Integer[] relevantPartition = isReplicated ? null : new Integer[]{p};
                int partitionid = isReplicated ? 16383 : p;
                // figure out real hostId;
                int baseHostId = 0;
                Matcher matcher = VPTFILE_PATTERN.matcher(partitionToFiles.get(p).get(0).getName());
                if (matcher.find()) {
                    baseHostId = Integer.parseInt(matcher.group(1));
                }
                for (int target = 1; target < partitionToFiles.get(p).size(); target++) {
                    boolean isConsistent = true;
                    // figure out real comparing hostId
                    int compareHostId = target;
                    Matcher matcher2 = VPTFILE_PATTERN.matcher(partitionToFiles.get(p).get(target).getName());
                    if (matcher2.find()) {
                        compareHostId = Integer.parseInt(matcher2.group(1));
                    }

                    long refCheckSum = 0l, compCheckSum = 0l;

                    try (
                        TableSaveFile referenceSaveFile = new TableSaveFile(
                                    new FileInputStream(partitionToFiles.get(p).get(0)), 1, relevantPartition);
                        TableSaveFile compareSaveFile = new TableSaveFile(
                                    new FileInputStream(partitionToFiles.get(p).get(target)), 1, relevantPartition)) {
                        DBBPool.BBContainer cr = null, cc = null;

                        List<VoltTable> ftr = Lists.newArrayList();
                        List<VoltTable> ftc = Lists.newArrayList();
                        List<String> diff = Lists.newArrayList();
                        long ftrCount = 0;
                        long ftcCount = 0;
                        while (referenceSaveFile.hasMoreChunks() || compareSaveFile.hasMoreChunks()) {
                            // skip chunk for irrelevant partition
                            cr = referenceSaveFile.getNextChunk();
                            cc = compareSaveFile.getNextChunk();
                            if (cr == null && cc == null) { // both reached EOF
                                break;
                            }
                            try {
                                if (orderLevel >= 2) {
                                    if (cr != null) {
                                        refCheckSum += PrivateVoltTableFactory.createVoltTableFromBuffer(cr.b(), true)
                                                .getTableCheckSum(false);
                                    }
                                    if (cc != null) {
                                        compCheckSum += PrivateVoltTableFactory.createVoltTableFromBuffer(cc.b(), true)
                                                .getTableCheckSum(false);
                                    }
                                    if (CONSOLE_LOG.isDebugEnabled()) {
                                        CONSOLE_LOG.debug("Checksum for " + tableName + " partition " + partitionid
                                                + " on host" + baseHostId + " is " + refCheckSum + " on host"
                                                + compareHostId + " is " + compCheckSum);
                                    }
                                } else {
                                    if (cr != null && cc == null) {
                                        break;
                                    }
                                    VoltTable tr = null;
                                    if (cr != null) {
                                        tr = PrivateVoltTableFactory.createVoltTableFromBuffer(cr.b(), true);
                                        ftrCount += tr.getRowCount();
                                        ftr.add(tr);
                                    }
                                    VoltTable tc = null;
                                    if (cc != null) {
                                        tc = PrivateVoltTableFactory.createVoltTableFromBuffer(cc.b(), true);
                                        ftcCount += tc.getRowCount();
                                        ftc.add(tc);
                                    }

                                    // compare them now, continue checking total counts even after they are found inconsistent
                                    if (orderLevel == 0) {
                                        if (diff.size() < SnapshotComparer.MAX_SNAPSHOT_DIFF_ROWS) {
                                            SnapshotComparer.compare(ftr, ftc, diff);
                                            if (!diff.isEmpty()) {
                                                isConsistent = false;
                                            }
                                        }
                                    } else {
                                        // Now we compare whole table that we built.
                                        if (tr != null && tc != null && !tr.hasSameContents(tc, orderLevel == 1)) {
                                            // seek to find where discrepancy happened
                                            if (SNAPSHOT_LOG.isDebugEnabled()) {
                                                SNAPSHOT_LOG.debug(
                                                        "table from file: " + partitionToFiles.get(p).get(0) + " : " + ftr);
                                                SNAPSHOT_LOG.debug("table from file: " + partitionToFiles.get(p).get(target)
                                                        + " : " + ftc);
                                            }

                                            StringBuilder output = new StringBuilder().append("Order level:" + orderLevel + " Diffs between file 1:")
                                                    .append(partitionToFiles.get(p).get(0)).append(" and file 2: ")
                                                    .append(partitionToFiles.get(p).get(target)).append(" \n");
                                            diff(tr, tc, output);
                                            CONSOLE_LOG.info(output.toString());
                                            isConsistent = false;
                                            break;
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                if (cr != null) {
                                    cr.discard();
                                }
                                if (cc != null) {
                                    cc.discard();
                                }
                            }
                        }

                        if (orderLevel == 0) {
                            if (diff.size() < SnapshotComparer.MAX_SNAPSHOT_DIFF_ROWS) {
                                SnapshotComparer.compare(ftr, ftc, diff);
                                if (!diff.isEmpty()) {
                                    isConsistent = false;
                                }
                            }
                            if (!isConsistent) {
                                StringBuilder sb = new StringBuilder();
                                sb.append(tableName + " inconsistent between file 1 " + partitionToFiles.get(p).get(0) + " total row count:" + ftrCount + " and file 2 " + partitionToFiles.get(p).get(target)
                                        + " total row count: " + ftcCount + " on partition:" + partitionid + "\n");
                                sb.append("Difference:\n");
                                for (String s : diff) {
                                    sb.append(s);
                                }
                                CONSOLE_LOG.warn(sb.toString());
                            }
                        }
                        if (orderLevel >= 2) {
                            isConsistent = (refCheckSum == compCheckSum);
                            // for orderLevel 3. drill down the discrepancies by reorder the whole table to csv then diff
                            if (!isConsistent && orderLevel == 3) {
                                CONSOLE_LOG.warn("Checksum for " + tableName + " partition " + partitionid + " on host"
                                        + baseHostId + " is " + refCheckSum + " on host" + compareHostId + " is "
                                        + compCheckSum);
                                // For every output file that will be created attempt to instantiate and print an error  couldn't be created.
                                String baseOutfileName = (isReplicated ? "Replicated" : "Partitioned") + "-Table-" + tableName + "-host" + baseHostId + "-partition" + partitionid;
                                convertTableToCSV(baseOutfileName, tableName, partitionid, partitionToFiles.get(p).get(0), TEMP_FOLDER, DELIMITER, isReplicated, true);

                                String compareOutfileName = (isReplicated ? "Replicated" : "Partitioned") + "-Table-" + tableName + "-host" + compareHostId + "-partition" + partitionid;
                                convertTableToCSV(compareOutfileName, tableName, partitionid, partitionToFiles.get(p).get(target), TEMP_FOLDER, DELIMITER, isReplicated, false);

                                String diffFileName = tableName + "-partition" + partitionid + "-host" + baseHostId + "-vs-host"+ compareHostId + ".diff";
                                generateDiffOutput(TEMP_FOLDER, baseOutfileName, compareOutfileName, diffFileName);
                            }
                        }
                        if (isConsistent) {
                            SNAPSHOT_LOG.info((isReplicated ? "Replicated" : "Partitioned") + " Table " + tableName
                                    + " is consistent between host" + baseHostId + " with host" + compareHostId + " on partition "
                                    + partitionid);
                        } else {
                            inconsistentTable.add(tableName);
                            SNAPSHOT_LOG.warn((isReplicated ? "Replicated" : "Partitioned") + " Table " + tableName
                                    + " is inconsistent between host" + baseHostId + " with host" + compareHostId + " on partition "
                                    + partitionid);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            CONSOLE_LOG.info("Finished comparing table " + tableName + ".\n");
        }
        CONSOLE_LOG.info("Finished comparing all tables.");
        if (!inconsistentTable.isEmpty()) {
            CONSOLE_LOG.info("The inconsistent tables are: " + inconsistentTable);
            System.exit(STATUS_INCONSISTENCY);
        } else {
            System.exit(STATUS_OK);
        }
    }

    private static boolean generateDiffOutput(String localTempFolder, String baseOutfileName, String compareOutfileName, String diffFileName) {
        try {
            // invoke external diff command
            ProcessBuilder pb = new ProcessBuilder("diff", "-u",  baseOutfileName+"-sorted.tsv", compareOutfileName+"-sorted.tsv");
            pb.directory(new File(localTempFolder));
            pb.redirectErrorStream(true);
            pb.redirectOutput(new File(DIFF_FOLDER + diffFileName));
            Process p = pb.start();
            //Check result
            if (p.waitFor() == 0) {
                return true;
            }
        } catch (Exception e) {
            CONSOLE_LOG.error("Failed to diff between" + baseOutfileName + " and " + compareOutfileName, e);
        }
        return false;
    }

    private static boolean convertTableToCSV(String outfileName, String tableName, int partitionid, File infile, String outputFolder, char delimiter, boolean isReplicated, boolean skipExist) {
        File outfile = new File(outputFolder + outfileName + ".tsv");

        try {
            if (outfile.exists()) {
                if (skipExist) {
                    return true;
                }
                System.err.println("Error: Failed to create output file "
                        + outfile.getPath() + " for table " + tableName + "\n File already exists");
                return false;
            }
            if (!outfile.createNewFile()) {
                return false;
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.err.println("Error: Failed to create output file "
                    + outfile.getPath() + " for table " + tableName);
            return false;

        }

        // Actually convert the tables and write the data to the appropriate destination
        try {
            SNAPSHOT_LOG.debug("Converting table " + tableName + " to " + outfileName);
            Integer[] relevantPartition = isReplicated ? null : new Integer[]{partitionid};
            CSVTableSaveFile.convertTableSaveFile(delimiter, relevantPartition, outfile, infile, false);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println("Error: Failed to convert " + infile.getPath() + " to " + outfile.getPath());
        }

        try {
            // invoke external sort command for sorting
            ProcessBuilder pb = new ProcessBuilder("sort", "-n", "-o" , outfileName+"-sorted.tsv", outfile.getName());
            pb.directory(new File(outputFolder));
            pb.redirectErrorStream(true);
            Process p = pb.start();
            //Read output
            java.io.InputStreamReader reader = new java.io.InputStreamReader(p.getInputStream());
            java.io.BufferedReader br = new java.io.BufferedReader(reader);
            StringBuilder out = new StringBuilder();
            String line = null, previous = null;
            while ((line = br.readLine()) != null) {
                if (!line.equals(previous)) {
                    previous = line;
                    out.append(line).append('\n');
                    System.out.println(line);
                }
            }

            //Check result
            if (p.waitFor() == 0) {
                return true;
            }
        } catch (Exception e) {
            CONSOLE_LOG.error("Failed to sort " + outfileName, e);
        }

        return false;
    }

    private static void diff(VoltTable t1, VoltTable t2, StringBuilder sb) {
        if (t1.getRowCount() != t2.getRowCount()) {
            sb.append("The number of rows differs. File 1:" + t1.getRowCount() + " File 2:" + t2.getRowCount());
            return;
        }
        t1.resetRowPosition();
        t2.resetRowPosition();
        long maxCount = SnapshotComparer.MAX_SNAPSHOT_DIFF_ROWS;
        long index = 0;
        while (t1.advanceRow() && t2.advanceRow()) {
            if (!t1.getRawRow().equals(t2.getRawRow())) {
                sb.append("\n");
                sb.append("file 1:" + t1.getRow());
                sb.append("file 2:" + t2.getRow());
                index++;
                if (index == maxCount) {
                    break;
                }
            }
        }
    }

    public void compareWith(SnapshotLoader another) {
        boolean fail = false;
        // Build a plan for which save file as the baseline for each partition
        Map<String, List<List<File>>> tableToCopies = new HashMap<>();
        for (String tableName : tables.keySet()) {
            if (!snapshot.m_tableFiles.containsKey(tableName)) {
                System.err.println("Error: Snapshot does not contain table " + tableName);
                System.exit(STATUS_INVALID_INPUT);
            }
            SnapshotUtil.TableFiles tableFiles = snapshot.m_tableFiles.get(tableName);
            if (!tableFiles.m_isReplicated) {
                TreeSet<Integer> partitionsIds = new TreeSet<>();
                List<List<File>> partitionToFiles = new ArrayList<>();
                for (int i = 0; i < partitionCount; i++) {
                    partitionToFiles.add(new ArrayList<>());
                }
                for (int ii = 0; ii < tableFiles.m_files.size(); ii++) {
                    Set<Integer> validPartitions = tableFiles.m_validPartitionIds.get(ii);
                    partitionsIds.addAll(validPartitions);
                    for (int p : validPartitions) {
                        partitionToFiles.get(p).add(tableFiles.m_files.get(ii));
                    }
                }
                int totalPartitionCount = tableFiles.m_totalPartitionCounts.get(0);
                if (!((partitionsIds.size() == totalPartitionCount) &&
                        (partitionsIds.first() == 0) &&
                        (partitionsIds.last() == totalPartitionCount - 1))) {
                    System.err.println("Error: Not all partitions present for table " + tableName);
                    fail = true;
                } else {
                    tableToCopies.put(tableName, partitionToFiles);
                }
            } else {
                List<List<File>> partitionToFiles = new ArrayList<>();
                partitionToFiles.add(new ArrayList<>());
                partitionToFiles.get(0).addAll(tableFiles.m_files);
                tableToCopies.put(tableName, partitionToFiles);
            }
        }
        if (fail) {
            System.exit(STATUS_INVALID_INPUT);
        }
        // based on the plan, retrieve the file and compare
        Set<String> inconsistentTable = new HashSet<>();

        CONSOLE_LOG.info("Finished comparing all tables.");
        if (!inconsistentTable.isEmpty()) {
            CONSOLE_LOG.info("The inconsistent tables are: " + inconsistentTable);
            System.exit(STATUS_INCONSISTENCY);
        } else {
            System.exit(STATUS_OK);
        }
    }

    /**
     * return InMemoryJarfile of a snapshot jar file
     */
    private InMemoryJarfile getInMemoryJarFileBySnapShotName(String location, String nonce) throws IOException {
        final File file = new VoltSnapshotFile(location, nonce + ".jar");
        byte[] bytes = MiscUtils.fileToBytes(file);
        return CatalogUtil.loadInMemoryJarFile(bytes);
    }

    /**
     * using publickey for auth
     */
    private ChannelSftp setupJsch(String username, String remoteHost) throws JSchException {
        JSch jsch = new JSch();
        jsch.setKnownHosts("~/.ssh/known_hosts");
        jsch.addIdentity("~/.ssh/id_rsa");

        Session session = jsch.getSession(username, remoteHost);
        session.setConfig("PreferredAuthentications", "publickey");
        session.setConfig("StrictHostKeyChecking", "no");

        // jschSession.setPassword(password);
        session.connect(30000);
        return (ChannelSftp) session.openChannel("sftp");
    }

    /**
     * download remote files through ssh
     */
    static String PATHSEPARATOR = "/";

    private boolean downloadFiles(String username, String remoteHost, String sourcePath, String destinationPath,
            String nonce) {
        ChannelSftp channelSftp = null;
        try {
            channelSftp = setupJsch(username, remoteHost);
            channelSftp.connect();
            channelSftp.cd(sourcePath);
            // list of folder content
            Vector<ChannelSftp.LsEntry> fileAndFolderList = channelSftp.ls(sourcePath);
            //Iterate through list of folder content
            for (ChannelSftp.LsEntry item : fileAndFolderList) {
                // Check if it is a file (not a directory).and starts with the desired nonce
                if (!item.getAttrs().isDir() && item.getFilename().startsWith(nonce)) {
                    File file = new File(destinationPath + PATHSEPARATOR + item.getFilename());
                    if (!file.exists() || item.getAttrs().getMTime() > file.lastModified() / 1000) {
                        // Download only if changed later.
                        // Download file from source (source filename, destination filename).
                        channelSftp.get(sourcePath + PATHSEPARATOR + item.getFilename(), file.getPath());
                    }
                }
            }
        } catch (JSchException | SftpException ex) {
            ex.printStackTrace();
            return false;
        } finally {
            if (channelSftp != null) {
                channelSftp.disconnect();
            }
        }
        return true;
    }
}
