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
 * The PauseActivityStats statistics provide a summary of current
 * cluster activity that can be used to determine when the cluster
 * activity has subsided after a call to @Pause.
 *
 * The result of the statistics request is a table with one
 * row per host, and a summary of activity in various categories.
 * Activity is summarized in the 'ACTIVE' column; if desired,
 * then other columns can be used to determine what the activity
 * relates to, and perhaps whether forward progress is being made.
 */
public class PauseActivityStats extends StatsSource {
    private static final VoltLogger logger = new VoltLogger("HOST");

    private enum ColumnName {
        ACTIVE, // 0 if all other gauges 0, else 1
        EXPORTS_PENDING,
        DRPROD_ROWS, DRPROD_BYTES,
    };

    public PauseActivityStats() {
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
            VoltType type = (col == ColumnName.ACTIVE ? VoltType.TINYINT : VoltType.BIGINT);
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
     * the key is irrelevant. Stats collected match those
     * used by 'voltadmin pause --wait'.
     */
    private static final ActivityHelper.Type[] statsList = {
        ActivityHelper.Type.EXPORT, ActivityHelper.Type.DRPROD,
    };

    @Override
    protected void updateStatsRow(Object key, Object[] row) {
        boolean active = false;
        try {
            ActivityHelper helper = new ActivityHelper();
            active = helper.collect(statsList);
            setValue(row, ColumnName.EXPORTS_PENDING, helper.exportPend);
            setValue(row, ColumnName.DRPROD_ROWS, helper.drprodRowsPend);
            setValue(row, ColumnName.DRPROD_BYTES, helper.drprodBytesPend);
        }
        catch (Exception ex) {
            logger.error("Unhandled exception in PauseActivityStats: " + ex);
        }
        setValue(row, ColumnName.ACTIVE, active);
        super.updateStatsRow(key, row);
    }

    /*
     * Utilities to set a value in a row.
     */
    private void setValue(Object[] row, ColumnName col, long val) {
        row[columnNameToIndex.get(col.name())] = val;
    }

    private void setValue(Object[] row, ColumnName col, boolean val) {
        row[columnNameToIndex.get(col.name())] = (val ? 1 : 0);
    }
}
