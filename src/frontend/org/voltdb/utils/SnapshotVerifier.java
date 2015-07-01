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

package org.voltdb.utils;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.Snapshot;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.SnapshotFilter;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.SpecificSnapshotFilter;

/**
 * A command line utility for scanning and validating snapshots. Provides detailed information about the files
 * that make up a snapshot, what partitions they contains, and whether they are corrupted or intact. In the event
 * that a table file is corrupted it will also specify what partitions can still be salvaged.
 *
 */
public class SnapshotVerifier {
    private static final VoltLogger CONSOLE_LOG = new VoltLogger("CONSOLE");

    public static void main(String args[]) {
        if (args.length == 0) {
            //printHelpAndQuit(0);
        } else if (args[0].equals("--help")) {
            printHelpAndQuit(0);
        }

        HashSet<String> snapshotNames = new HashSet<String>();
        for (int ii = 0; ii < args.length; ii++) {
            if (args[ii].equals("--dir")) {
                ii++;
                continue;
            }
            snapshotNames.add(args[ii]);
        }

        List<String> directories = new ArrayList<String>();
        for (int ii = 0; ii < args.length; ii++) {
            if (args[ii].equals("--dir")) {
                if (ii + 1 >= args.length) {
                    System.err.println("Error: No directories specified after --dir");
                    printHelpAndQuit(-1);
                    break;
                }
                directories.add(args[ii + 1]);
                ii++;
            }
        }

        if (directories.isEmpty()) {
            directories.add(".");
        }

        verifySnapshots(directories, snapshotNames, false);
    }

    /**
     * Perform snapshot verification.
     * @param directories list of directories to search for snapshots
     * @param snapshotNames set of snapshot names/nonces to verify
     */
    public static void verifySnapshots(
            final List<String> directories, final Set<String> snapshotNames, boolean expectHashinator) {

        FileFilter filter = new SnapshotFilter();
        if (!snapshotNames.isEmpty()) {
            filter = new SpecificSnapshotFilter(snapshotNames);
        }

        Map<String, Snapshot> snapshots = new HashMap<String, Snapshot>();
        for (String directory : directories) {
            SnapshotUtil.retrieveSnapshotFiles( new File(directory), snapshots, filter, true, CONSOLE_LOG);
        }

        if (snapshots.isEmpty()) {
            System.out.println("Snapshot corrupted");
            System.out.println("No files found");
        }

        for (Snapshot s : snapshots.values()) {
            System.out.println(SnapshotUtil.generateSnapshotReport(s.getTxnId(), s, expectHashinator).getSecond());
        }
    }

    private static void printHelpAndQuit( int code) {
        System.out.println("Usage\nSpecific snapshot: java -cp <classpath> -Djava.library.path=<library path> org.voltdb.utils.SnapshotVerifier snapshot_name --dir dir1 --dir dir2 --dir dir3");
        System.out.println("All snapshots: java -cp <classpath> -Djava.library.path=<library path> org.voltdb.utils.SnapshotVerifier --dir dir1 --dir dir2 --dir dir3");
        System.exit(code);
    }
}
