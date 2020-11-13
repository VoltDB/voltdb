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
import org.voltdb.CommandLog;
import org.voltdb.StatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

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

    public enum ShutdownActivity {
        ACTIVE                  (VoltType.TINYINT),
        CLIENT_TXNS             (VoltType.BIGINT),
        CLIENT_REQ_BYTES        (VoltType.BIGINT),
        CLIENT_RESP_MSGS        (VoltType.BIGINT),
        CMDLOG_TXNS             (VoltType.BIGINT),
        CMDLOG_BYTES            (VoltType.BIGINT),
        IMPORTS_PENDING         (VoltType.BIGINT),
        EXPORTS_PENDING         (VoltType.BIGINT),
        DRPROD_ROWS             (VoltType.BIGINT),
        DRPROD_BYTES            (VoltType.BIGINT),
        DRCONS_PARTS            (VoltType.BIGINT);

        public final VoltType m_type;
        ShutdownActivity(VoltType type) { m_type = type; }
    }

    public ShutdownActivityStats() {
        super(false);
    }

    /*
     * Constructs a description of the columns we'll return
     * in our stats table.
     */
    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, ShutdownActivity.class);
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
    protected int updateStatsRow(Object key, Object[] row) {
        int offset = super.updateStatsRow(key, row);
        boolean active = false;
        try {
            ActivityHelper helper = new ActivityHelper();
            if (usingCommandLog()) {
                active = helper.collect(statsWithCmdLog);
                row[offset + ShutdownActivity.CLIENT_TXNS.ordinal()] = helper.clientTxns;
                row[offset + ShutdownActivity.CLIENT_REQ_BYTES.ordinal()] = helper.clientReqBytes;
                row[offset + ShutdownActivity.CLIENT_RESP_MSGS.ordinal()] = helper.clientRespMsgs;
                row[offset + ShutdownActivity.IMPORTS_PENDING.ordinal()] = helper.importPend;
                row[offset + ShutdownActivity.CMDLOG_TXNS.ordinal()] = helper.cmdlogTxns;
                row[offset + ShutdownActivity.CMDLOG_BYTES.ordinal()] = helper.cmdlogBytes;
                row[offset + ShutdownActivity.EXPORTS_PENDING.ordinal()] = helper.exportPend;
            }
            else {
                active = helper.collect(statsWithoutCmdLog);
                row[offset + ShutdownActivity.CLIENT_TXNS.ordinal()] = helper.clientTxns;
                row[offset + ShutdownActivity.CLIENT_REQ_BYTES.ordinal()] = helper.clientReqBytes;
                row[offset + ShutdownActivity.CLIENT_RESP_MSGS.ordinal()] = helper.clientRespMsgs;
                row[offset + ShutdownActivity.IMPORTS_PENDING.ordinal()] = helper.importPend;
                row[offset + ShutdownActivity.DRCONS_PARTS.ordinal()] = helper.drconsPend;
                row[offset + ShutdownActivity.EXPORTS_PENDING.ordinal()] = helper.exportPend;
                row[offset + ShutdownActivity.DRPROD_ROWS.ordinal()] = helper.drprodRowsPend;
                row[offset + ShutdownActivity.DRPROD_BYTES.ordinal()] = helper.drprodBytesPend;
            }
        }
        catch (Exception ex) {
            logger.error("Unhandled exception in ShutdownActivityStats: " + ex);
        }
        row[offset + ShutdownActivity.ACTIVE.ordinal()] = active;
        return offset + ShutdownActivity.values().length;
    }

    /*
     * Are we running with command logging enabled?
     */
    private boolean usingCommandLog() {
        CommandLog cl = VoltDB.instance().getCommandLog();
        return cl != null && cl.isEnabled();
    }
}
