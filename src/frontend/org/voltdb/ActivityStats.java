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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.importer.ImportManager;
import org.voltdb.export.ExportManagerInterface;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;

/**
 * The ActivityStats statistics provide a summary of current
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
public class ActivityStats extends StatsSource
{
    private static final VoltLogger logger = new VoltLogger("HOST");

    private enum ColumnName {
        ACTIVE, // 0 if all other gauges 0, else 1
        CLIENT_TXNS, CLIENT_REQ_BYTES, CLIENT_RESP_MSGS,
        CMDLOG_TXNS, CMDLOG_BYTES,
        IMPORTS_PENDING, EXPORTS_PENDING,
        DRPROD_SEGS, DRPROD_BYTES, DRCONS_PARTS,
    };

    public ActivityStats() {
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
     * Iterator through the rows of stats we make available. In fact
     * we have a single row, so we fake out the iterator.
     */
    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval)
    {
        return new DummyIterator();
    }

    private class DummyIterator implements Iterator<Object> {
        private boolean hasNext = true;
        @Override
        public boolean hasNext() {
            return hasNext;
        }
        @Override
        public Object next() {
            Object obj = null;
            if (hasNext) {
                hasNext = false;
                obj = "THE_ROW";
            }
            return obj;
        }
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
    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        boolean active = false;
        try {
            if (usingCommandLog()) {
                active |= checkClients(rowValues);
                active |= checkImporter(rowValues);
                active |= checkCommandLog(rowValues);
                active |= checkExporter(rowValues);
            }
            else {
                active |= checkClients(rowValues);
                active |= checkImporter(rowValues);
                active |= checkDrConsumer(rowValues);
                active |= checkExporter(rowValues);
                active |= checkDrProducer(rowValues);
            }
            if (!active) { // if active, we already logged details
                logger.info("ActivityStats, no activity");
            }
        }
        catch (Exception ex) {
            logger.error("Unhandled exception in ActivityStats: " + ex);
        }
        setValue(rowValues, ColumnName.ACTIVE, active);
        super.updateStatsRow(rowKey, rowValues);
    }

    /*
     * Are we running with command logging enabled?
     */
    private boolean usingCommandLog() {
        CommandLog cl = VoltDB.instance().getCommandLog();
        return cl != null && cl.isEnabled();
    }

    /*
     * Client interface.
     * Check for outstanding requests, responses, transactions.
     * Counters are all zero if nothing outstanding.
     */
    private boolean checkClients(Object[] out) {
        long reqBytes = 0, respMsgs = 0, txns = 0;
        try {
            ClientInterface ci = VoltDB.instance().getClientInterface();
            if (ci != null) {
                for (Pair<String,long[]> ent : ci.getLiveClientStats().values()) {
                    long[] val = ent.getSecond(); // order of values is assumed
                    reqBytes += val[1];
                    respMsgs += val[2];
                    txns += val[3];
                }
            }
        }
        catch (Exception ex) {
            warn("checkClients", ex);
        }
        setValue(out, ColumnName.CLIENT_TXNS, txns);
        setValue(out, ColumnName.CLIENT_REQ_BYTES, reqBytes);
        setValue(out, ColumnName.CLIENT_RESP_MSGS, respMsgs);
        return isActive("client interface: outstanding txns %d, request bytes %d, responses %d",
                        txns, reqBytes, respMsgs);
    }

    /*
     * Command log.
     * Check for outstanding logging: bytes and transactions.
     * Counters are zero if nothing outstanding.
     */
    private boolean checkCommandLog(Object[] out) {
        long bytes = 0, txns = 0;
        try {
            CommandLog cl = VoltDB.instance().getCommandLog();
            if (cl != null) {
                long[] temp = new long[2];
                temp[0] = temp[1] = 0;
                cl.getCommandLogOutstanding(temp);
                bytes = temp[0];
                txns = temp[1];
            }
        }
        catch (Exception ex) {
            warn("checkCommandLog", ex);
        }
        setValue(out, ColumnName.CMDLOG_TXNS, txns);
        setValue(out, ColumnName.CMDLOG_BYTES, bytes);
        return isActive("command log: outstanding txns %d, bytes %d", txns, bytes);
    }

    /*
     * Importer.
     * Check for unprocessed import request.
     * Count is of outstanding requests across all importers,
     * and is zero if there's nothing outstanding.
     */
    private boolean checkImporter(Object[] out) {
        long pend = 0;
        try {
            ImportManager im = ImportManager.instance();
            if (im != null) {
                pend = im.statsCollector().getTotalPendingCount();
            }
        }
        catch (Exception ex) {
            warn("checkImporter", ex);
        }
        setValue(out, ColumnName.IMPORTS_PENDING, pend);
        return isActive("importer: %d pending", pend);
    }

    /*
     * Exporter.
     * Check for unprocessed exports.
     * Count is of outstanding tuples across all exporters,
     * and is zero if there's nothing outstanding.
     */
    private boolean checkExporter(Object[] out) {
        long pend = 0;
        try {
            ExportManagerInterface em = ExportManagerInterface.instance();
            if (em != null) {
                pend = em.getTotalPendingCount();
            }
        }
        catch (Exception ex) {
            warn("checkExporter", ex);
        }
        setValue(out, ColumnName.EXPORTS_PENDING, pend);
        return isActive("exporter: %d pending", pend);
    }

    /*
     * DR producer.
     * Returns details of outstanding data, expressed in bytes and message
     * segments, summed across all partitions. Zero when none outstanding.
     */
    private boolean checkDrProducer(Object[] out) {
        long bytesPend = 0, segsPend = 0;
        try {
            StatsSource ss = getStatsSource(StatsSelector.DRPRODUCERPARTITION);
            if (ss != null) {
                int ix_totalBytes = getIndex(ss, DRProducerStatsBase.Columns.TOTAL_BYTES);
                int ix_lastQueued = getIndex(ss, DRProducerStatsBase.Columns.LAST_QUEUED_DRID);
                int ix_lastAcked = getIndex(ss, DRProducerStatsBase.Columns.LAST_ACK_DRID);
                for (Object[] row : ss.getStatsRows(false, System.currentTimeMillis())) {
                    long totalBytes = asLong(row[ix_totalBytes]);
                    long lastQueuedDrId = asLong(row[ix_lastQueued]);
                    long lastAckedDrId = asLong(row[ix_lastAcked]);
                    bytesPend += totalBytes;
                    if (lastQueuedDrId > lastAckedDrId) {
                        segsPend += lastQueuedDrId - lastAckedDrId;
                    }
                }
            }
        }
        catch (Exception ex) {
            warn("checkDrProducer", ex);
        }
        setValue(out, ColumnName.DRPROD_SEGS, segsPend);
        setValue(out, ColumnName.DRPROD_BYTES, bytesPend);
        return isActive("DR producer: outstanding segments %d, bytes %d", segsPend, bytesPend);
    }

    /*
     * DR consumer.
     * Returns count of partitions for which there is data not
     * yet successfully applied. Zero when nothing outstanding.
     */
    private boolean checkDrConsumer(Object[] out) {
        long pend = 0;
        try {
            StatsSource ss = getStatsSource(StatsSelector.DRCONSUMERPARTITION);
            if (ss != null) {
                int ix_timeRcvd = getIndex(ss, DRConsumerStatsBase.Columns.LAST_RECEIVED_TIMESTAMP);
                int ix_timeAppl = getIndex(ss, DRConsumerStatsBase.Columns.LAST_APPLIED_TIMESTAMP);
                for (Object[] row : ss.getStatsRows(false, System.currentTimeMillis())) {
                    long timeLastRcvd = asLong(row[ix_timeRcvd]);
                    long timeLastApplied = asLong(row[ix_timeAppl]);
                    if (timeLastRcvd != timeLastApplied) {
                        pend++;
                    }
                }
            }
        }
        catch (Exception ex) {
            warn("checkDrConsumer", ex);
        }
        setValue(out, ColumnName.DRCONS_PARTS, pend);
        return isActive("DR consumer: %d partitions with pending data", pend);
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

    /*
     * Convert object in stats row to long integer
     */
    private static long asLong(Object obj) {
        return ((Long)obj).longValue();
    }

    /*
     * Find source for someone else's stats (used for DR activity check)
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

    /*
     * Get index for a column in someone else's stats table.
     */
    private static int getIndex(StatsSource ss, String name) {
        return ss.columnNameToIndex.get(name);
    }

    /*
     * Summarize activity based on any count being non-zero.
     * If active, write a log message using the supplied format string.
     * No logging done in the inactive case.
     */
    private static boolean isActive(String fmt, long... counts) {
        boolean actv = false;
        for (long count : counts) {
            actv |= (count != 0);
        }
        if (actv) {
            Long[] args = new Long[counts.length];
            for (int i=0; i<counts.length; i++) {
                args[i] = counts[i]; // boxing
            }
            logger.info("ActivityStats, " + String.format(fmt, args));
        }
        return actv;
    }

    /*
     * Standardized logging for exception handling. The exception
     * is otherwise ignored.
     */
    private static void warn(String func, Exception ex) {
        logger.warn(String.format("Unexpected exception in ActivityStats.%s: %s", func, ex));
    }
}
