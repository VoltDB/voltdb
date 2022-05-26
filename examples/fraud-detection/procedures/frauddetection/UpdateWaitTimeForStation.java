/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package frauddetection;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;


public class UpdateWaitTimeForStation extends VoltProcedure {
    // Assuming we use the same timestamp for all stations in the wait_time table.

    // Get the timestamp of last departure time at last calculation for each station
    public static final SQLStmt getLastDeparts = new SQLStmt(
    "SELECT last_depart " +
    "FROM wait_time " +
    "WHERE station_id = ? " +
    "ORDER BY update_time DESC " +
    "LIMIT 1;"
    );

    // Get train departure times after the last calculation
    public static final SQLStmt getTrainDeparts = new SQLStmt(
    "SELECT * FROM train_activity WHERE station_id = ? AND time > ? ORDER BY time;"
    );

    // Get average wait time for each train departure by station
    public static final SQLStmt getWaitTimeForTrain = new SQLStmt(
    "SELECT SUM(SINCE_EPOCH(SECOND, ?) - SINCE_EPOCH(SECOND, date_time)) AS wait_sum, COUNT(*) AS entries " +
    "FROM activity " +
    "WHERE station_id = ? AND date_time > ? AND date_time <= ? AND accept = 1 AND activity_code = 1;"
    );

    // Update wait time by station
    public static final SQLStmt updateWaitTime = new SQLStmt(
    "UPSERT INTO wait_time (station_id, last_depart, total_time, entries) VALUES (?, ?, ?, ?);"
    );

    // Delete old wait time calculations
    public static final SQLStmt deleteOldData = new SQLStmt(
    "DELETE FROM wait_time WHERE update_time < DATEADD(SECOND, ?, NOW);"
    );

    private static class StationStats {
        public final short station;
        public final TimestampType lastDepartTime;
        public final long totalTime;
        public final long totalEntries;
        private StationStats(short station, TimestampType lastDepartTime, long totalTime, long totalEntries)
        {
            this.station = station;
            this.lastDepartTime = lastDepartTime;
            this.totalTime = totalTime;
            this.totalEntries = totalEntries;
        }

        @Override
        public String toString()
        {
            return "StationStats{" +
                   "station=" + station +
                   ", lastDepartTime=" + lastDepartTime +
                   ", totalTime=" + totalTime +
                   ", totalEntries=" + totalEntries +
                   '}';
        }
    }

    /**
     * Calculate the current average wait time for each station and prune old wait time calculations.
     * @param ttl_seconds Time-to-live for wait time calculations, results older than this will be deleted.
     * @return
     * @throws VoltAbortException
     */
    public VoltTable[] run(short station, long ttl_seconds) throws VoltAbortException {
        voltQueueSQL(getLastDeparts, station);

        // For each station, calculate the wait time between departures
        final StationStats stats = getStationStats(station, voltExecuteSQL());
        voltQueueSQL(updateWaitTime,
                        stats.station,
                        stats.lastDepartTime,
                        stats.totalTime,
                        stats.totalEntries);
        // Prune old data
        voltQueueSQL(deleteOldData, -ttl_seconds);

        return voltExecuteSQL(true);
    }

    /**
     * Get the wait time statistics for each station
     * @param stations         All stations
     * @param lastDepartResult Last departure time since last calculation for each station.
     *                        The order must match the order in the stations list.
     * @return The wait time statistics.
     */
    private StationStats getStationStats(short station, VoltTable[] lastDepartResult) {
        TimestampType lastDepartTime;
        if (lastDepartResult[0].advanceRow()) {
            lastDepartTime = lastDepartResult[0].getTimestampAsTimestamp("last_depart");
        } else {
            lastDepartTime = new TimestampType(0);
        }
        voltQueueSQL(getTrainDeparts, station, lastDepartTime);
        final VoltTable departResult = voltExecuteSQL()[0];

        while (departResult.advanceRow()) {
            final TimestampType departTime = departResult.getTimestampAsTimestamp("time");
            voltQueueSQL(getWaitTimeForTrain, departTime, station, lastDepartTime, departTime);
            lastDepartTime = departTime;
        }

        long totalWaitTime = 0;
        long totalEntries = 0;
        if (departResult.getRowCount() > 0) {
            final VoltTable[] waitTimeResult = voltExecuteSQL();
            for (VoltTable res : waitTimeResult) {
                if (res.advanceRow()) {
                    final long entries = res.getLong("entries");
                    final long waitTime = res.getLong("wait_sum");
                    // Don't record empty train
                    if (entries > 0) {
                        totalEntries += entries;
                        totalWaitTime += waitTime;
                    }
                }
            }
        }

        return new StationStats(station, lastDepartTime, totalWaitTime, totalEntries);
    }
}
