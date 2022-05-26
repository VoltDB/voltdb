/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltcore.logging.VoltLogger;

/**
 * This class supports archiving of existing snapshot directories
 * during an 'init --force' operation.  Generally, we rename the
 * existing snapshots directory (from the previous deployment) to
 * snapshots.1 to serve as an archive, and create a new empty
 * snapshots directory.
 *
 * Existing archived snapshots direcories are renamed by incrementing
 * their suffix (so snapshots.1 to .2, etc). The caller can specify
 * a limit on how many of these we retain. The excess will be
 * removed.
 *
 */
class SnapshotArchiver {

    static class ArchiverException extends RuntimeException {
        private ArchiverException(Exception cause, String format, Object... args) {
            super(message(cause, format, args));
        }
        private static String message(Exception cause, String format, Object... args) {
            String s = String.format(format, args);
            if (cause != null) {
                s += ": " + cause;
            }
            return s;
        }
    }

    /**
     * Archives existing snapshot directories. Entered with some number
     * of existing archived snapshot dirs and possibly a populated
     * snapshot dir from the previous deployment. On return, there
     * are no more than 'retainCount' archived dirs, and an empty
     * snapshot dir for use in the new deployment.
     *
     * @param snapshotPath absolute path to snapshot directory
     * @param retainCount limit on archived snapshot directories
     */
    static void archiveSnapshotDirectory(String snapshotPath, int retainCount) {
        VoltLogger hostLog = new VoltLogger("HOST");
        VoltLogger consoleLog = new VoltLogger("CONSOLE");

        File snapshotDir = new File(snapshotPath);
        String snapshotName = snapshotDir.getName();

        // Archived snapshot directories have a suffix of 1 to 3 digits
        retainCount = Math.min(Math.max(retainCount, 0), 999);
        Pattern archPattern = Pattern.compile(snapshotName + "\\.(\\d\\d?\\d?)");

        // For existing archives, build map from integer suffix to path
        TreeMap<Integer,File> archDirs = new TreeMap<>();
        snapshotDir.getParentFile().listFiles(new FileFilter() {
            @Override
            public boolean accept(File path) {
                Matcher matcher = archPattern.matcher(path.getName());
                if (path.isDirectory() && matcher.matches()) {
                    Integer seq = Integer.valueOf(matcher.group(1));
                    File prev = archDirs.put(seq, path);
                    if (prev != null) { // snapshots.9 and snapshots.009?
                        throw new ArchiverException(null, "Conflicting archived snapshot directories with sequence %s: '%s' and '%s'",
                                                    seq, prev, path);
                    }
                    return true;
                }
                return false;
            }
        });

        // Are there existing snapshots?  If not, we can skip
        // doing anything to the base snapshot directory.
        String[] snapshots = snapshotDir.list();
        boolean renameExisting = false, deleteExisting = false;
        if (snapshots != null && snapshots.length != 0) {
            renameExisting = (retainCount > 0);
            deleteExisting = (retainCount == 0);
        }
        snapshots = null; // don't need to keep this list

        // Remove sufficient older snapshot archives to get us to
        // our target count (including the existing snapshot directory
        // that will shortly become an archive)
        int adjustedRetainCount = retainCount - (renameExisting ? 1 : 0);
        while (archDirs.size() > adjustedRetainCount) {
            Map.Entry<Integer,File> lastEntry = archDirs.lastEntry();
            File dir = lastEntry.getValue();
            try {
                removeDirectory(dir);
                hostLog.infoFmt("Removed archived snapshot directory '%s' (retain count is %d)",
                                dir, retainCount);
            } catch (IOException ex) {
                throw new ArchiverException(ex, "Failed to remove archived snapshot directory '%s'", dir);
            }
            archDirs.remove(lastEntry.getKey());
        }

        // If we're going to rename the existing snapshots directory
        // then we need slide everything upwards.
        if (renameExisting) {

            // Find first gap in numbering. There should not be one
            // but let's be fussy about it, no point in renaming dirs
            // above the gap in numbering, and over time maybe we'll
            // squeeze out all gaps that should not be there.
            int prev = 0, vacant = archDirs.size() + 1;
            for (Integer key : archDirs.keySet()) {
                if (key != prev + 1) {
                    vacant = key;
                    break;
                }
                prev = key;
            }

            // Rename everything up one
            for (Map.Entry<Integer,File> ent : archDirs.headMap(vacant, false).descendingMap().entrySet()) {
                File fromDir = ent.getValue();
                File toDir = new File(snapshotPath + "." + (ent.getKey() + 1));
                try {
                    Files.move(fromDir.toPath(), toDir.toPath());
                    hostLog.infoFmt("Renamed archived snapshot directory '%s' to '%s'", fromDir, toDir);
                } catch (IOException ex) {
                    throw new ArchiverException(ex, "Failed to rename archived snapshot directory '%s' to '%s'", fromDir, toDir);
                }
            }

            // And now we can rename the existing snapshots directory
            File fromDir = snapshotDir;
            File toDir = new File(snapshotDir.getPath() + ".1");
            try {
                Files.move(fromDir.toPath(), toDir.toPath());
                String mess = String.format("Archived previous snapshot directory '%s' to '%s'", fromDir, toDir);
                hostLog.info(mess);
                consoleLog.info(mess);
            } catch (IOException ex) {
                throw new ArchiverException(ex, "Failed to archive previous snapshot directory '%s' to '%s'", fromDir, toDir);
            }
        }

        // A retainCount of zero is a special case; we must delete the
        // existing base dir.
        else if (deleteExisting) {
            try {
                removeDirectory(snapshotDir);
                String mess = String.format("Removed previous snapshot directory '%s' (retain count is %d)",
                                            snapshotDir, retainCount);
                hostLog.info(mess);
                consoleLog.info(mess);
            } catch (IOException ex) {
                throw new ArchiverException(ex, "Failed to remove previous snapshot directory '%s'", snapshotDir);
            }
        }

        // Lastly, make a new snapshots directory if we disposed of the old one
        if (deleteExisting | renameExisting) {
            if (!snapshotDir.mkdir()) {
                throw new ArchiverException(null, "Failed to create snapshot directory '%s'", snapshotDir);
            }
        }
    }

    private static void removeDirectory(File file) throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                removeDirectory(f);
            }
        }
        Files.delete(file.toPath());
    }
}
