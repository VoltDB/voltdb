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

package org.voltdb.client;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Utility methods for use with {@link ClientStats} objects.
 */
public final class ClientStatsUtil {

    /**
     * Append a single line of comma-separated values to the file specified.
     * Used mainly for collecting results from benchmarks.
     * <p>
     * The format of this output is subject to change between versions.
     * As of V11.0 there are 13 fields. The format is:
     * <ol>
     * <li>Timestamp (ms) of creation of the given {@link ClientStats} instance, stats.</li>
     * <li>Duration from first procedure call within the given {@link ClientStats} instance
     *    until this call.</li>
     * <li>Count of invocations completed.</li>
     * <li>Minimum round trip latency estimate.</li>
     * <li>Maximum round trip latency estimate.</li>
     * <li>95-percentile round trip latency estimate.</li>
     * <li>99-percentile round trip latency estimate.</li>
     * <li>99.9-percentile round trip latency estimate.</li>
     * <li>99.99-percentile round trip latency estimate.</li>
     * <li>99.999-percentile round trip latency estimate.</li>
     * <li>Count of invocation errors.</li>
     * <li>Count of invocation aborts.</li>
     * <li>Count of invocation timeouts.</li>
     * </ol>
     * <p>
     * All times are given in milliseconds.
     *
     * @param stats {@link ClientStats} instance with relevant stats
     * @param path path to write to, passed to {@link FileWriter#FileWriter(String)}
     * @throws IOException on any file write error
     */
    public static void writeSummaryCSV(ClientStats stats, String path) throws IOException {
        writeSummaryCSV(null, stats, path);
    }

    /**
     * Write a single line of comma-separated values to the file specified.
     * Used mainly for collecting results from benchmarks.
     * <p>
     * The format of this output is subject to change between versions.
     * See {@link #writeSummaryCSV(ClientStats, String)} for the format.
     * <p>
     * This variation on <code>writeSummaryCSV</code> inserts an initial
     * column containing a user-specified string; this can be used to
     * identify the row. Users should not use a mix of the two forms
     * of <code>writeSummaryCSV</code> on the same file, since the format
     * will be inconsistent.
     *
     * @param statsRowName name to be inserted as first column
     * @param stats {@link ClientStats} instance with relevant stats
     * @param path path to write to, passed to {@link FileWriter#FileWriter(String)}
     * @throws IOException on any file write error
     */
    public static void writeSummaryCSV(String statsRowName, ClientStats stats, String path) throws IOException {
        if (stats == null) {
            throw new IllegalArgumentException("stats required");
        }
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path required");
        }

        String prefix = "";
        if (statsRowName != null && !statsRowName.isEmpty()) {
            prefix = statsRowName + ",";
        }

        String row = String.format("%s%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d,%d\n",
                                   prefix,
                                   stats.getStartTimestamp(),
                                   stats.getDuration(),
                                   stats.getInvocationsCompleted(),
                                   stats.kPercentileLatencyAsDouble(0.0),
                                   stats.kPercentileLatencyAsDouble(1.0),
                                   stats.kPercentileLatencyAsDouble(0.95),
                                   stats.kPercentileLatencyAsDouble(0.99),
                                   stats.kPercentileLatencyAsDouble(0.999),
                                   stats.kPercentileLatencyAsDouble(0.9999),
                                   stats.kPercentileLatencyAsDouble(0.99999),
                                   stats.getInvocationErrors(),
                                   stats.getInvocationAborts(),
                                   stats.getInvocationTimeouts());

        try (FileWriter fw = new FileWriter(path, true)) {
            fw.append(row);
        }
    }
}
