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

import org.voltcore.logging.VoltLogger;
import org.voltdb.StatsSource;
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
        READY, // 0 if all other gauges are in desired state, which happen to be the lowest display priority. Otherwise 1.
        DRROLE_STATE,    // display priority (same as below): "DISABLED" > "PENDING" > "STOPPED" > "ACTIVE"
        DRPROD_STATE,    // aggregated for all hosts, "OFF" > "PENDING" > "ACTIVE"
        DRPROD_ISSYNCED, // aggregated for all partitions, "false" > "true"
        DRPROD_CSTATUS,  // connection status, aggregated for all connections, "DOWN" > "UP"
        DRCONS_STATE,    // aggregated for all hosts, "UNINITIALIZED" > "INITIALIZE" > "DISABLE" > "SYNC" > "RECEIVE"
        DRCONS_ISCOVERED, // aggregated for all partitions, "false" > "true"
        DRCONS_ISPAUSED,  // aggregated for all partitions, "true" > "false"
    };

    public XDCRReadinessStats() {
        super(false);
    }

    /*
     * Constructs a description of the columns we'll return
     * in our stats table.
     */
    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        for (ColumnName col : ColumnName.values()) {
            VoltType type = (col == ColumnName.READY ? VoltType.TINYINT : VoltType.STRING);
            columns.add(new ColumnInfo(col.name(), type));
        }
    }

    /*
     * Iterator through the single row of stats we make available.
     */
    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new ActivityHelper.OneShotIterator();
    }

    /*
     * Main stats collection. We return a single row, and
     * the key is irrelevant.
     */
    private static final ActivityHelper.Type[] statsList = {
        ActivityHelper.Type.DRROLE, ActivityHelper.Type.DRPROD_RDY, ActivityHelper.Type.DRCONS_RDY
    };

    @Override
    protected void updateStatsRow(Object key, Object[] row) {
        boolean notReady = false;
        try {
            ActivityHelper helper = new ActivityHelper();
            notReady = helper.collect(statsList);
            setValue(row, ColumnName.DRROLE_STATE, helper.drroleState);
            setValue(row, ColumnName.DRPROD_STATE, helper.drprodState);
            setValue(row, ColumnName.DRPROD_ISSYNCED, helper.drprodIsSynced ? "true" : "false"); // unfortunately volt table doesn't support boolean column
            setValue(row, ColumnName.DRPROD_CSTATUS, helper.drprodCStatus);
            setValue(row, ColumnName.DRCONS_STATE, helper.drconsState);
            setValue(row, ColumnName.DRCONS_ISCOVERED, helper.drconsIsCovered ? "true" : "false");
            setValue(row, ColumnName.DRCONS_ISPAUSED, helper.drconsIsPaused ? "true" : "false");
        }
        catch (Exception ex) {
            logger.error("Unhandled exception in XDCRReadinessStats: " + ex);
        }
        setValue(row, ColumnName.READY, notReady);
        super.updateStatsRow(key, row);
    }

    /*
     * Utilities to set a value in a row.
     */
    private void setValue(Object[] row, ColumnName col, String val) {
        row[columnNameToIndex.get(col.name())] = val;
    }

    private void setValue(Object[] row, ColumnName col, boolean val) {
        row[columnNameToIndex.get(col.name())] = (val ? 1 : 0);  // Align with other activity checks, ready - 0
    }
}
