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

package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltcore.logging.VoltLogger;

/**
 * The ShutdownActivityStats statistics provide a summary of
 * cluster activity that can be used to determine when cluster
 * shutdown can safely proceed.
 *
 * The intended use is that an admin client executes the
 * two pre-shutdown procedures @PrepareShutdown and @Quiesce,
 * and then monitors '@Statistics activity-summary' until all
 * work is drained.
 *
 * The result of the statistics request is a table with one
 * row per host, and a summary of activity in various categories.
 * Activity is summarized in the 'ACTIVE' column; if desired,
 * then other columns can be used to determine what the activity
 * relates to, and perhaps whether forward progress is being made.
 */
public class ShutdownActivityStats extends StatsSource {
    private static final VoltLogger logger = new VoltLogger("HOST");

    private enum ColumnName {
        ACTIVE, // 0 if all other gauges 0, else 1
        CLIENT_TXNS, CLIENT_REQ_BYTES, CLIENT_RESP_MSGS,
        CMDLOG_TXNS, CMDLOG_BYTES,
        IMPORTS_PENDING, EXPORTS_PENDING,
        DRPROD_ROWS, DRPROD_BYTES, DRCONS_PARTS,
    };

    public ShutdownActivityStats() {
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
     * Iterator through the one row of stats we make available.
     */
    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new ActivityHelper.OneShotIterator();
    }

    /*
     * Main stats collection. We return a single row, and
     * the key is irrelevant.
     *
     * We have two distinct cases depending on whether
     * command-logging is in use. In particular, DR is
     * only of interest if we do not have command-logging.
     * The assumption here is that the client is going
     * to make a shutdown snapshot, thus needs DR copies
     * to be stable.
     *
     * Ordering reflects that used by 'voltadmin shutdown',
     * on which this is based, and is loosely characterized
     * as 'inputs before outputs'. Probably this no longer
     * matters, since from the client point of view we are
     * now checking in parallel rather than sequentially.
     */
    private static final ActivityHelper.Type[] statsWithCmdLog = {
        ActivityHelper.Type.CLIENT, ActivityHelper.Type.IMPORT,
        ActivityHelper.Type.CMDLOG, ActivityHelper.Type.EXPORT,
    };

    private static final ActivityHelper.Type[] statsWithoutCmdLog = {
        ActivityHelper.Type.CLIENT, ActivityHelper.Type.IMPORT,
        ActivityHelper.Type.DRCONS, ActivityHelper.Type.EXPORT,
        ActivityHelper.Type.DRPROD,
    };

    @Override
    protected void updateStatsRow(Object key, Object[] row) {
        boolean active = false;
        try {
            ActivityHelper helper = new ActivityHelper();
            if (usingCommandLog()) {
                active = helper.collect(statsWithCmdLog);
                setValue(row, ColumnName.CLIENT_TXNS, helper.clientTxns);
                setValue(row, ColumnName.CLIENT_REQ_BYTES, helper.clientReqBytes);
                setValue(row, ColumnName.CLIENT_RESP_MSGS, helper.clientRespMsgs);
                setValue(row, ColumnName.IMPORTS_PENDING, helper.importPend);
                setValue(row, ColumnName.CMDLOG_TXNS, helper.cmdlogTxns);
                setValue(row, ColumnName.CMDLOG_BYTES, helper.cmdlogBytes);
                setValue(row, ColumnName.EXPORTS_PENDING, helper.exportPend);
            }
            else {
                active = helper.collect(statsWithoutCmdLog);
                setValue(row, ColumnName.CLIENT_TXNS, helper.clientTxns);
                setValue(row, ColumnName.CLIENT_REQ_BYTES, helper.clientReqBytes);
                setValue(row, ColumnName.CLIENT_RESP_MSGS, helper.clientRespMsgs);
                setValue(row, ColumnName.IMPORTS_PENDING, helper.importPend);
                setValue(row, ColumnName.DRCONS_PARTS, helper.drconsPend);
                setValue(row, ColumnName.EXPORTS_PENDING, helper.exportPend);
                setValue(row, ColumnName.DRPROD_ROWS, helper.drprodRowsPend);
                setValue(row, ColumnName.DRPROD_BYTES, helper.drprodBytesPend);
            }
        }
        catch (Exception ex) {
            logger.error("Unhandled exception in ShutdownActivityStats: " + ex);
        }
        setValue(row, ColumnName.ACTIVE, active);
        super.updateStatsRow(key, row);
    }

    /*
     * Are we running with command logging enabled?
     */
    private boolean usingCommandLog() {
        CommandLog cl = VoltDB.instance().getCommandLog();
        return cl != null && cl.isEnabled();
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
