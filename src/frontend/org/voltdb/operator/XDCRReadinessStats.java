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
import org.voltdb.DRProducerStatsBase.DRProducerNodeStatsBase.DRProducerNode;
import org.voltdb.DRProducerStatsBase.DRProducerPartitionStatsBase.DRProducerPartition;
import org.voltdb.DRRoleStats.DRRole;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.StatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.dr2.DRConsumerStatsBase.DRConsumerNodeStatsBase.DRConsumerNode;
import org.voltdb.dr2.DRConsumerStatsBase.DRConsumerPartitionStatsBase.DRConsumerPartition;

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

    public enum XDCRReadiness {
        IS_READY                (VoltType.STRING),
        DRROLE_STATE            (VoltType.STRING),
        DRPROD_STATE            (VoltType.STRING),
        DRPROD_ISSYNCED         (VoltType.STRING),
        DRPROD_CNXSTS           (VoltType.STRING),
        DRCONS_STATE            (VoltType.STRING),
        DRCONS_ISCOVERED        (VoltType.STRING),
        DRCONS_ISPAUSED         (VoltType.STRING);

        public final VoltType m_type;
        XDCRReadiness(VoltType type) { m_type = type; }
    }

    private static final VoltLogger logger = new VoltLogger("HOST");

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
                    int idxState = getIndex(ss, DRConsumerNode.STATE.name());
                    for (Object[] row : ss.getStatsRows(false, System.currentTimeMillis())) { // only one row
                        drconsState = String.valueOf(row[idxState]);
                    }
                }
                ss = getStatsSource(StatsSelector.DRCONSUMERPARTITION);
                if (ss != null) {
                    int idxIsCovered = getIndex(ss, DRConsumerPartition.IS_COVERED.name());
                    int idxIsPaused = getIndex(ss, DRConsumerPartition.IS_PAUSED.name());
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
                    int idxState = getIndex(ss, DRProducerNode.STATE.name());
                    for (Object[] row : ss.getStatsRows(false, System.currentTimeMillis())) { // only one row
                        drprodState = String.valueOf(row[idxState]);
                    }
                }
                ss = getStatsSource(StatsSelector.DRPRODUCERPARTITION);
                if (ss != null) {
                    int idxIsSynced = getIndex(ss, DRProducerPartition.ISSYNCED.name());
                    int idxCStatus = getIndex(ss, DRProducerPartition.CONNECTION_STATUS.name());
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
                    int idxState = getIndex(ss, DRRole.STATE.name());
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
        super.populateColumnSchema(columns, XDCRReadiness.class);
        // Quick summary of XDCR readiness, true if
        //      1) DRROLE_STATE == "ACTIVE",
        //      2) DRPROD_STATE == "ACTIVE",
        //      3) DRPROD_ISSYNCED == true,
        //      4) DRPROD_CNXSTS == "UP",
        //      5) DRCONS_STATE == "RECEIVE",
        //      6) DRCONS_ISCOVERED == true,
        //      7) DRCONS_ISPAUSED == false.
    }

    /*
     * Iterator through the single row of stats we make available.
     */
    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new ActivityHelper.OneShotIterator();
    }

    @Override
    protected int updateStatsRow(Object key, Object[] row) {
        int offset = super.updateStatsRow(key, row);
        try {
            ReadinessHelper helper = new ReadinessHelper();
            helper.collect();
            row[offset + XDCRReadiness.IS_READY.ordinal()] = helper.ready;
            row[offset + XDCRReadiness.DRROLE_STATE.ordinal()] = helper.drroleState;
            row[offset + XDCRReadiness.DRPROD_STATE.ordinal()] = helper.drprodState;
            row[offset + XDCRReadiness.DRPROD_ISSYNCED.ordinal()] = helper.drprodIsSynced; // unfortunately volt table doesn't support boolean column
            row[offset + XDCRReadiness.DRPROD_CNXSTS.ordinal()] = helper.drprodIsCnxUp;
            row[offset + XDCRReadiness.DRCONS_STATE.ordinal()] = helper.drconsState;
            row[offset + XDCRReadiness.DRCONS_ISCOVERED.ordinal()] = helper.drconsIsCovered;
            row[offset + XDCRReadiness.DRCONS_ISPAUSED.ordinal()] = helper.drconsIsPaused;
        }
        catch (Exception ex) {
            logger.error("Unexpected exception in XDCR_READINESS Stats: " + ex);
        }
        return offset + XDCRReadiness.values().length;
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
