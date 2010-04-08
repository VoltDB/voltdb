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

import java.util.*;
import java.io.*;

import org.voltdb.sysprocs.saverestore.*;
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

    public static void main(String args[]) {
        if (args[0].equals("--help")) {
            printHelpAndQuit(0);
        }

        boolean specifiedSingle = false;
        boolean specifiedAll = false;
        for (int ii = 0; ii < args.length; ii++) {
            if (args[ii].equals("--single")) {
                specifiedSingle = true;
            } else if (args[ii].equals("--all")) {
                specifiedAll = true;
            }
        }

        if (specifiedSingle && specifiedAll) {
            System.err.println("Error: Can only specify one option of --single or --all");
            printHelpAndQuit(-1);
        }

        if (!specifiedSingle && !specifiedAll) {
            System.err.println("Error: Must specify one of --single or --all");
            printHelpAndQuit(-1);
        }

        List<String> directories = null;
        for (int ii = 0; ii < args.length; ii++) {
            if (args[ii].equals("--dirs")) {
                if (ii + 1 >= args.length) {
                    System.err.println("Error: No directories specified after --dirs");
                    printHelpAndQuit(-1);
                }
                directories = Arrays.asList(args[ii + 1].split(","));
            }
        }
        if (directories == null) {
            System.err.println("Error: No directories specified using --dirs");
            printHelpAndQuit(-1);
        }

        FileFilter filter = new SnapshotFilter();

        if (specifiedSingle) {
            String snapshotName = null;
            for (int ii = 0; ii < args.length; ii++) {
                if (args[ii].equals("--name")) {
                    if (ii + 1 >= args.length) {
                        System.err.println("Error: No name specified after --name");
                        printHelpAndQuit(-1);
                    }
                    snapshotName = args[ii + 1];
                    break;
                }
            }

            if (snapshotName == null) {
                System.err.println("Error: No snapshot name specified using --name even though --single was specified");
                printHelpAndQuit(-1);
            }

            filter = new SpecificSnapshotFilter(snapshotName);
        }

        TreeMap<Long, Snapshot> snapshots = new TreeMap<Long, Snapshot>();
        for (String directory : directories) {
            SnapshotUtil.retrieveSnapshotFiles( new File(directory), snapshots, filter, 0, true);
        }

        for (Map.Entry<Long, Snapshot> s : snapshots.entrySet()) {
            System.out.println(SnapshotUtil.generateSnapshotReport(s.getKey(), s.getValue()).getSecond());
        }

        System.exit(0);
    }

    private static void printHelpAndQuit( int code) {
        System.out.println("Usage: snapshotcheck --single --name full_snapshot_name --dirs dir1[,dir2[,dir3[..]]] ");
        System.out.println("snapshotcheck --all --dirs dir1[,dir2[,dir3[..]]] ");
        System.exit(code);
    }
}
