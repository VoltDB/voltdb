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

import java.util.Iterator;
import java.util.Set;

import org.voltdb.importer.ImportManager;
import org.voltdb.export.ExportManagerInterface;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;

/**
 * Activity stats helper class. This holds methods that collect
 * specific statistics for the purpose of determining whether
 * cluster activity has quiesced, for example preparatory to
 * cluster shutdown.
 *
 * The stats class provides a list of the stats it is interested
 * in; we fetch those stats, and leave them in member data for
 * the stats class to pick up by name. This is not a particularly
 * clean interface, but that's ok. This class is just for use by
 * two or three stats classes under our control. We have this
 * class just to avoid a lot of cut and paste, and inheritance
 * hierarchies would be going too far.
 */
class ActivityHelper {

    private static final VoltLogger logger = new VoltLogger("HOST");

    /*
     * The types of stats for which we offer help.
     */
    enum Type {
        CLIENT,
        CMDLOG,
        IMPORT,
        EXPORT,
        DRCONS,
        DRPROD,
    }

    /*
     * Output values. Package access intentionally; there is
     * little point in adding a dozen or so simple-minded
     * getters for internal-only use. Obviously these are
     * named with prefixes similar to the Type names above.
     */
    long client_reqBytes, client_respMsgs, client_txns;
    long cmdlog_bytes, cmdlog_txns;
    long import_pend;
    long export_pend;
    long drcons_pend;
    long drprod_bytesPend, drprod_segsPend;

    /*
     * Main stats collection method. Stats values
     * are saved as member variables.
     *
     * @param types  ordered list of stats types
     * @return active/inactive summary flag
     */
    boolean collect(Type[] types) {
        boolean active = false;
        for (Type type : types) {
            switch (type) {
            case CLIENT:
                active |= checkClients();
                break;
            case CMDLOG:
                active |= checkCommandLog();
                break;
            case IMPORT:
                active |= checkImporter();
                break;
            case EXPORT:
                active |= checkExporter();
                break;
            case DRCONS:
                active |= checkDrConsumer();
                break;
            case DRPROD:
                active |= checkDrProducer();
                break;
            }
        }
        if (!active) { // if active, we already logged details
            logger.info("Activity check: no activity");
        }
        return active;
    }

    /*
     * Client interface.
     * Check for outstanding requests, responses, transactions.
     * Counters are all zero if nothing outstanding.
     */
    private boolean checkClients() {
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
        client_reqBytes = reqBytes;
        client_respMsgs = respMsgs;
        client_txns = txns;
        return isActive("client interface: outstanding txns %d, request bytes %d, responses %d",
                        txns, reqBytes, respMsgs);
    }

    /*
     * Command log.
     * Check for outstanding logging: bytes and transactions.
     * Counters are zero if nothing outstanding.
     */
    private boolean checkCommandLog() {
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
        cmdlog_bytes = bytes;
        cmdlog_txns = txns;
        return isActive("command log: outstanding txns %d, bytes %d", txns, bytes);
    }

    /*
     * Importer.
     * Check for unprocessed import request.
     * Count is of outstanding requests across all importers,
     * and is zero if there's nothing outstanding.
     */
    private boolean checkImporter() {
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
        import_pend = pend;
        return isActive("importer: %d pending", pend);
    }

    /*
     * Exporter.
     * Check for unprocessed exports.
     * Count is of outstanding tuples across all exporters,
     * and is zero if there's nothing outstanding.
     */
    private boolean checkExporter() {
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
        export_pend = pend;
        return isActive("exporter: %d pending", pend);
    }

    /*
     * DR producer.
     * Returns details of outstanding data, expressed in bytes and message
     * segments, summed across all partitions. Zero when none outstanding.
     */
    private boolean checkDrProducer() {
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
        drprod_bytesPend = bytesPend;
        drprod_segsPend = segsPend;
        return isActive("DR producer: outstanding segments %d, bytes %d", segsPend, bytesPend);
    }

    /*
     * DR consumer.
     * Returns count of partitions for which there is data not
     * yet successfully applied. Zero when nothing outstanding.
     */
    private boolean checkDrConsumer() {
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
        drcons_pend = pend;
        return isActive("DR consumer: %d partitions with pending data", pend);
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
    private static StatsSource getStatsSource(StatsSelector selector) {
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
            logger.info("Activity check: " + String.format(fmt, (Object[])args));
        }
        return actv;
    }

    /*
     * Standardized logging for exception handling. The exception
     * is otherwise ignored.
     */
    private static void warn(String func, Exception ex) {
        logger.warn(String.format("Unexpected exception in ActivityHelper.%s: %s", func, ex));
    }

    /*
     * Iterator through the rows of stats we make available. In fact
     * we have a single row, so we fake out the iterator.
     */
    static class OneShotIterator implements Iterator<Object> {
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
}
