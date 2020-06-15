/* This file is part of VoltDB.
 * Copyright (C) 2020 VoltDB Inc.
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

package org.voltdb.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltdb.DRConsumerStatsBase;
import org.voltdb.DRProducerStatsBase;
import org.voltdb.DRRoleStats;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.StatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

/**
 * The XDCRReadinessStats statistics provide a summary of DRRole,
 * DRConsumer and DRProducer that can be used to determine the cluster
 * XDCR readiness.
 *
 * The result of the statistics request is a table with one
 * row per host, and a summary of readiness in various categories.
 * Readiness is summarized in the 'READY' column; if desired,
 * then other columns can be used to determine the status of each category.
 */
public class XDCRReadinessStats extends StatsSource {
    private static final VoltLogger logger = new VoltLogger("HOST");

    private enum ColumnName {
        IS_READY,         // Quick summary of XDCR readiness, true if
                          //      1) DRROLE_STATE == "ACTIVE",
                          //      2) DRPROD_STATE == "ACTIVE",
                          //      3) DRPROD_ISSYNCED == true,
                          //      4) DRPROD_CNXSTS == "UP",
                          //      5) DRCONS_STATE == "RECEIVE",
                          //      6) DRCONS_ISCOVERED == true,
                          //      7) DRCONS_ISPAUSED == false.
        DRROLE_STATE,     // "DISABLED", "PENDING", "STOPPED", "ACTIVE"
        DRPROD_STATE,     // "OFF", "PENDING", "ACTIVE"
        DRPROD_ISSYNCED,  // aggregated for all partitions, false if any partition is out of sync.
        DRPROD_CNXSTS,    // connection status, aggregated for all connections, "DOWN" if any connection is down, else "UP".
        DRCONS_STATE,     // "UNINITIALIZED", "INITIALIZE", "DISABLE", "SYNC", "RECEIVE"
        DRCONS_ISCOVERED, // aggregated for all partitions, false if any partition isn't covered.
        DRCONS_ISPAUSED,  // aggregated for all partitions, true if any partition is paused.
    };

    public XDCRReadinessStats() {
        super(false);
    }

    private class ReadinessHelper {
        // Readiness check
        String ready;
        String drroleState;
        String drprodState;
        String drprodIsSynced;
        String drprodIsCnxUp;
        String drconsState;
        String drconsIsCovered;
        String drconsIsPaused;
        /*
         * Main stats collection method. Stats values
         * are saved as member variables.
         *
         * @param types  ordered list of stats types
         * @return active/inactive summary flag
         */
        void collect() {
            boolean ready = true;
            ready &= checkDrConsumerReadiness();
            ready &= checkDrProducerReadiness();
            ready &= checkDrRole();
            this.ready = ready ? "true" : "false";
        }

        /*
         * XDCR drconsumer readiness check
         */
        private boolean checkDrConsumerReadiness() {
            try {
                StatsSource ss = getStatsSource(StatsSelector.DRCONSUMERNODE);
                if (ss != null) {
                    int idxState = getIndex(ss, DRConsumerStatsBase.Columns.STATE);
                    for (Object[] row : ss.getStatsRows(false, System.currentTimeMillis())) { // only one row
                        drconsState = String.valueOf(row[idxState]);
                    }
                }
                ss = getStatsSource(StatsSelector.DRCONSUMERPARTITION);
                if (ss != null) {
                    int idxIsCovered = getIndex(ss, DRConsumerStatsBase.Columns.IS_COVERED);
                    int idxIsPaused = getIndex(ss, DRConsumerStatsBase.Columns.IS_PAUSED);
                    boolean isCovered = true;
                    boolean isPaused = false;
                    Object[][] rows = ss.getStatsRows(false, System.currentTimeMillis());
                    for (Object[] row : rows) {
                        isCovered &= asBoolean(row[idxIsCovered]); // false if any partition isn't covered.
                        isPaused |= asBoolean(row[idxIsPaused]);   // true if any partition is paused
                    }
                    if (rows.length != 0) {
                        drconsIsCovered = String.valueOf(isCovered);
                        drconsIsPaused = String.valueOf(isPaused);
                    }
                }
            }
            catch (Exception ex) {
                logger.warn("Unexpected exception in checking DRConsumer Statistics", ex);
            }
            // Ready - in "RECEIVE" state, every partition is covered and no one is paused.
            return drconsState != null && drconsState.equalsIgnoreCase("RECEIVE") &&
                    drconsIsCovered != null && drconsIsCovered.equalsIgnoreCase("true") &&
                    drconsIsPaused != null && drconsIsPaused.equalsIgnoreCase("false");
        }

        /*
         * XDCR drproducer readiness check
         */
        private boolean checkDrProducerReadiness() {
            try {
                StatsSource ss = getStatsSource(StatsSelector.DRPRODUCERNODE);
                if (ss != null) {
                    int idxState = getIndex(ss, DRProducerStatsBase.Columns.STATE);
                    for (Object[] row : ss.getStatsRows(false, System.currentTimeMillis())) { // only one row
                        drprodState = String.valueOf(row[idxState]);
                    }
                }
                ss = getStatsSource(StatsSelector.DRPRODUCERPARTITION);
                if (ss != null) {
                    int idxIsSynced = getIndex(ss, DRProducerStatsBase.Columns.IS_SYNCED);
                    int idxCStatus = getIndex(ss, DRProducerStatsBase.Columns.CONNECTION_STATUS);
                    boolean isSynced = true;
                    boolean isUp = true;
                    Object[][] rows = ss.getStatsRows(false, System.currentTimeMillis());
                    for (Object[] row : rows) {
                        isSynced &= asBoolean(row[idxIsSynced]); // false if any partition isn't sync'ed.
                        isUp &= String.valueOf(row[idxCStatus]).equalsIgnoreCase("UP"); // false if any connection is down
                    }
                    if (rows.length != 0) {
                        drprodIsSynced = String.valueOf(isSynced);
                        drprodIsCnxUp = isUp ? "UP" : "DOWN";
                    }
                }
            }
            catch (Exception ex) {
                logger.warn("Unexpected exception in checking DRProducer Statistics", ex);
            }
            // Ready - in "ACTIVE" state, every partition is sync'ed and every connection is up.
            return drprodState != null && drprodState.equalsIgnoreCase("ACTIVE") &&
                    drprodIsSynced != null && drprodIsSynced.equalsIgnoreCase("true") &&
                    drprodIsCnxUp != null && drprodIsCnxUp.equalsIgnoreCase("UP");
        }

        private boolean checkDrRole() {
            try {
                StatsSource ss = getStatsSource(StatsSelector.DRROLE);
                if (ss != null) {
                    int idxState = getIndex(ss, DRRoleStats.CN_STATE);
                    for (Object[] row : ss.getStatsRows(false, System.currentTimeMillis())) { // only one row
                        drroleState = String.valueOf(row[idxState]);
                    }
                }
            }
            catch (Exception ex) {
                logger.warn("Unexpected exception in checking DRRole Statistics", ex);
            }
            // Ready - in "ACTIVE" state.
            return drroleState != null && drroleState.equalsIgnoreCase("ACTIVE");
        }

        /*
         * Find source for someone else's stats
         */
        private StatsSource getStatsSource(StatsSelector selector) {
            StatsSource ss = null;
            StatsAgent sa = VoltDB.instance().getStatsAgent();
            if (sa != null) {
                Set<StatsSource> sss = sa.lookupStatsSource(selector, 0);
                if (sss != null && !sss.isEmpty()) {
                    assert sss.size() == 1;
                    ss = sss.iterator().next();
                }
            }
            return ss;
        }
    }

    /*
     * Constructs a description of the columns we'll return
     * in our stats table.
     */
    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        for (ColumnName col : ColumnName.values()) {
            columns.add(new ColumnInfo(col.name(), VoltType.STRING));
        }
    }

    /*
     * Iterator through the single row of stats we make available.
     */
    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new ActivityHelper.OneShotIterator();
    }

    @Override
    protected void updateStatsRow(Object key, Object[] row) {
        try {
            ReadinessHelper helper = new ReadinessHelper();
            helper.collect();
            setValue(row, ColumnName.IS_READY, helper.ready);
            setValue(row, ColumnName.DRROLE_STATE, helper.drroleState);
            setValue(row, ColumnName.DRPROD_STATE, helper.drprodState);
            setValue(row, ColumnName.DRPROD_ISSYNCED, helper.drprodIsSynced); // unfortunately volt table doesn't support boolean column
            setValue(row, ColumnName.DRPROD_CNXSTS, helper.drprodIsCnxUp);
            setValue(row, ColumnName.DRCONS_STATE, helper.drconsState);
            setValue(row, ColumnName.DRCONS_ISCOVERED, helper.drconsIsCovered);
            setValue(row, ColumnName.DRCONS_ISPAUSED, helper.drconsIsPaused);
        }
        catch (Exception ex) {
            logger.error("Unexpected exception in XDCR_READINESS Stats: " + ex);
        }
        super.updateStatsRow(key, row);
    }

    /*
     * Utilities to set a value in a row.
     */
    private void setValue(Object[] row, ColumnName col, String val) {
        row[columnNameToIndex.get(col.name())] = val;
    }

    /*
     * Get index for a column in someone else's stats table.
     */
    private static int getIndex(StatsSource ss, String name) {
        return ss.getStatsColumnIndex(name);
    }

    /*
     * Convert object in stats row to boolean.
     */
    private static boolean asBoolean(Object obj) {
        return String.valueOf(obj).equalsIgnoreCase("true");
    }
}
