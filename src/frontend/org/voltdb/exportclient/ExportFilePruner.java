/* This file is part of VoltDB.
 * Copyright (C) 2021-2022 Volt Active Data Inc.
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

package org.voltdb.exportclient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;

/**
 * Helper class used to prune export files when that is
 * configured for an export-to-file target.
 */
class ExportFilePruner {

    private static final VoltLogger logger = new VoltLogger("ExportClient");
    private static final int LOG_RATE_LIMIT = 60 * 60; // seconds

    private Pattern m_namePattern;
    private SimpleDateFormat m_dateFormat;

    /**
     * Constructs an ExportFilePruner.
     *
     * @param pattern a pattern that can be used to accept a file/dir name,
     *                including a group named TS that is the timestamp
     * @param dateFormat a format that can be used to parse the timestamp
     *                   extracted from the file/dir name
     */
    ExportFilePruner(Pattern pattern, String dateFormat) {
        m_namePattern = pattern;
        m_dateFormat = new SimpleDateFormat(dateFormat);
    }

    /**
     * Prunes export files and directories by age.
     *
     * Batched mode: 'dir' is a directory containing batch directories.
     * For each one that is older than the retention time, the directory
     * and its contents are deleted. The age of the contents is immaterial.
     *
     * Non-batched mode: 'dir' is a directory containing export files.
     * Each one is considered individually, and deleted if its age exceeds
     * the specified retention.
     *
     * Age is determined based on the timestamp that is conventionally
     * embedded in the export file/directory name.
     *
     * @param dir directory containing the named files/dirs
     * @param retention retention period in milliseconds
     */
    void pruneByAge(File dir, long retention) {
        String[] names = selectFiles(dir);
        if (names == null || names.length == 0) {
            return;
        }
        SortedSet<NameAndTime> sorted = sortFiles(dir, names);
        long now = System.currentTimeMillis();
        for (NameAndTime entry : sorted) {
            long age = now - entry.time;
            if (age < retention) {
                break;
            }
            removeOldFile(new File(dir, entry.name), age);
        }
    }

    /**
     * Prunes export files and directories in age order based
     * on a target retention count.
     *
     * Batched mode: 'dir' is a directory containing batch directories.
     * Non-batched mode: 'dir' is a directory containing export files.
     *
     * In either case, the directory is searched for applicable files/dirs,
     * the resulting names are sorted based on the timestamp that is
     * embedded in the export file/directory name. We parse this into
     * an actual time rather than assuming chronological order and lexical
     * order are identical. Case in point: USA mm-dd-yyyy dates.
     *
     * @param dir directory containing the named files/dirs
     * @param retainCount target retention count.
     */
    void pruneByCount(File dir, int retainCount) {
        String[] names = selectFiles(dir);
        if (names == null || names.length < retainCount) {
            return; // no need to do more work
        }
        SortedSet<NameAndTime> sorted = sortFiles(dir, names);
        int deleteCount = sorted.size() - retainCount;
        if (deleteCount > 0) {
            logger.infoFmt("Found %d export files/dirs, retain count is %d, so deleting %d",
                           sorted.size(), retainCount, deleteCount);
            long now = System.currentTimeMillis();
            int deleted = 0;
            for (NameAndTime entry : sorted) {
                if (deleted >= deleteCount) {
                    break;
                }
                removeOldFile(new File(dir, entry.name), now - entry.time);
                deleted++;
            }
        }
    }

    /*
     * Select files/dirs in a specified parent directory that match
     * the name pattern we're using.
     */
    private String[] selectFiles(File parent) {
        return parent.list((dir, name) -> m_namePattern.matcher(name).matches());
    }

    /*
     * Class representing filenames with timestamp. Natural order
     * is by timestamp and then (if the unlikely happens) by name.
     */
    private static class NameAndTime implements Comparable<NameAndTime> {
        final long time;
        final String name;

        NameAndTime(long t, String n) {
            time = t;
            name = n;
        }

        public int compareTo(NameAndTime other) {
            int cmp = Long.compare(time, other.time);
            if (cmp == 0) {
                cmp = name.compareTo(other.name);
            }
            return cmp;
        }
    }

    /*
     * Build a sorted set of names and times from the provided
     * list of filenames (and the directory they are contained in).
     *
     * We ignore files for which we cannot determine a suitable
     * timestamp (though this may be logged).
     */
    private SortedSet<NameAndTime> sortFiles(File dir, String[] names) {
        SortedSet<NameAndTime> out = new TreeSet<>();
        for (String name : names) {
            long ts = fileTimeFromName(name);
            if (ts < 0) { // parse failed, use fallback
                ts = fileTimeFromFS(new File(dir, name));
            }
            if (ts > 0) {
                out.add(new NameAndTime(ts, name));
            }
        }
        if (!out.isEmpty() && logger.isDebugEnabled()) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssXXX");
            logger.debug("Export files/dirs sorted by date:");
            for (NameAndTime entry : out) {
                logger.debugFmt("  %s  %s", sdf.format(new Date(entry.time)), entry.name);
            }
        }
        return out;
    }

    /*
     * Parse timestamp from name of file, if we can.
     * Returns -1 on error.
     *
     * If a parse error occurs at all, it's likely to
     * affect all files (probably related to the user's
     * weird choice of date format), so we use rate-
     * lmited logging.
     */
    private long fileTimeFromName(String name) {
        long ts = -1;
        String err = "parse failed";
        try {
            Matcher matcher = m_namePattern.matcher(name);
            if (matcher.matches()) {
                String tim = matcher.group("TS");
                ParsePosition pp = new ParsePosition(0);
                Date date = m_dateFormat.parse(tim, pp);
                if (date != null && pp.getIndex() == tim.length()) {
                    ts = date.getTime();
                }
            }
        }
        catch (Exception ex) {
            err = ex.toString();
        }
        if (ts < 0) {
            logger.rateLimitedWarn(LOG_RATE_LIMIT,
                                   "Unable to extract timestamp from '%s' using pattern '%s' and date format '%s' - %s",
                                   name, m_namePattern, m_dateFormat.toPattern(), err);
        }
        return ts;
    }

    /*
     * Get timestamp from file system, as last modification time.
     * Returns -1 on error, 0 if file system told us 1970-01-01.
     *
     * If we can't determine a last-mod time, it's likely to be
     * a systemic problem, so we use rate-limited logging.
     */
    private long fileTimeFromFS(File file) {
        long ts = -1;
        try {
            ts = Files.getLastModifiedTime(file.toPath()).toMillis();
        }
        catch (IOException ex) {
            logger.rateLimitedWarn(LOG_RATE_LIMIT, "Unable to determine last-modification time of '%s' - %s",
                                   file, ex.toString());
        }
        return ts;
    }

    /*
     * Remove file or directory recursively.
     * Logs success or failure message including reason.
     */
    private void removeOldFile(File file, long age) {
        String what = file.isDirectory() ? "directory" : "file";
        long mins = TimeUnit.MILLISECONDS.toMinutes(age);
        try {
            removeRec(file);
            logger.infoFmt("Pruned %s %s, age %d mins", what, file, mins);
        }
        catch (IOException ex) {
            logger.warnFmt("Unable to prune %s %s - %s", what, file, ex);
        }
    }

    private void removeRec(File file) throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                removeRec(f);
            }
        }
        Files.delete(file.toPath());
    }
}
