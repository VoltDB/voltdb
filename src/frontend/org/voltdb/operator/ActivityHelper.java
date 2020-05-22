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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.voltdb.ClientInterface;
import org.voltdb.CommandLog;
import org.voltdb.DRConsumerStatsBase;
import org.voltdb.DRProducerStatsBase;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.StatsSource;
import org.voltdb.VoltDB;
import org.voltdb.importer.ImportManager;
import org.voltdb.iv2.Cartographer;
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
        LEADER,
        IMPORT,
        EXPORT,
        EXMAST,
        DRCONS,
        DRPROD,
    }

    /*
     * Output values. Package access intentionally; there is
     * little point in adding a dozen or so simple-minded
     * getters for internal-only use. Obviously these are
     * named with prefixes similar to the Type names above.
     */
    long clientReqBytes, clientRespMsgs, clientTxns;
    long cmdlogBytes, cmdlogTxns;
    long leaderCount;
    long importPend;
    long exportPend;
    long exportMasters;
    long drconsPend;
    long drprodBytesPend, drprodRowsPend;

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
            case LEADER:
                active |= checkPartitionLeadership();
                break;
            case IMPORT:
                active |= checkImporter();
                break;
            case EXPORT:
                active |= checkExporter();
                break;
            case EXMAST:
                active |= checkExportMastership();
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
        clientReqBytes = reqBytes;
        clientRespMsgs = respMsgs;
        clientTxns = txns;
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
        cmdlogBytes = bytes;
        cmdlogTxns = txns;
        return isActive("command log: outstanding txns %d, bytes %d", txns, bytes);
    }

    /*
     * Partition leaderhip.
     * Check for partitions for which the local host
     * is the leader.
     */
    private boolean checkPartitionLeadership() {
        int leaders = 0;
        try {
            int hostId = VoltDB.instance().getMyHostId();
            Cartographer cart = VoltDB.instance().getCartographer();
            if (cart != null) {
                leaders = cart.getMasterCount(hostId);
            }
        }
        catch (Exception ex) {
            warn("checkPartitionLeadership", ex);
        }
        leaderCount = leaders;
        return isActive("partitions: %d led by this host", leaders);

    }

    /*
     * Importer.
     * Check for unprocessed import requests by counting
     * outstanding requests across all importers.
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
        importPend = pend;
        return isActive("importer: %d pending", pend);
    }

    /*
     * Exporter.
     * Check for unprocessed exports by looking at the
     * count of outstanding tuples across all exporters.
     */
    private boolean checkExporter() {
        long pend = 0;
        try {
            ExportManagerInterface em = VoltDB.getExportManager();
            if (em != null) {
                pend = em.getTotalPendingCount();
            }
        }
        catch (Exception ex) {
            warn("checkExporter", ex);
        }
        exportPend = pend;
        return isActive("exporter: %d pending", pend);
    }

    /*
     * Export mastership for current host.
     * Counts locally active streams.
     */
    private boolean checkExportMastership() {
        int masters = 0;
        try {
            ExportManagerInterface em = VoltDB.getExportManager();
            if (em != null) {
                masters = em.getMastershipCount();
            }
        }
        catch (Exception ex) {
            warn("checkExportMastership", ex);
        }
        exportMasters = masters;
        return isActive("exports: %d mastered on this host", masters);
    }

    /*
     * DR producer.
     * Counts outstanding data, expressed in bytes and table rows,
     * summed across all partitions.
     */
    private boolean checkDrProducer() {
        long bytesPend = 0, rowsPend = 0;
        try {
            StatsSource ss = getStatsSource(StatsSelector.DRPRODUCERPARTITION);
            if (ss != null) {
                int ixTotalBytes = getIndex(ss, DRProducerStatsBase.Columns.TOTAL_BYTES);
                int ixLastQueued = getIndex(ss, DRProducerStatsBase.Columns.LAST_QUEUED_DRID);
                int ixLastAcked = getIndex(ss, DRProducerStatsBase.Columns.LAST_ACK_DRID);
                for (Object[] row : ss.getStatsRows(false, System.currentTimeMillis())) {
                    long totalBytes = asLong(row[ixTotalBytes]);
                    long lastQueuedDrId = asLong(row[ixLastQueued]);
                    long lastAckedDrId = asLong(row[ixLastAcked]);
                    bytesPend += totalBytes;
                    if (lastQueuedDrId > lastAckedDrId) {
                        rowsPend += lastQueuedDrId - lastAckedDrId;
                    }
                }
            }
        }
        catch (Exception ex) {
            warn("checkDrProducer", ex);
        }
        drprodBytesPend = bytesPend;
        drprodRowsPend = rowsPend;
        return isActive("DR producer: outstanding rows %d, bytes %d", rowsPend, bytesPend);
    }

    /*
     * DR consumer.
     * Counts partitions for which there is data not yet
     * successfully applied.
     */
    private boolean checkDrConsumer() {
        long pend = 0;
        try {
            StatsSource ss = getStatsSource(StatsSelector.DRCONSUMERPARTITION);
            if (ss != null) {
                int ixTimeRcvd = getIndex(ss, DRConsumerStatsBase.Columns.LAST_RECEIVED_TIMESTAMP);
                int ixTimeAppl = getIndex(ss, DRConsumerStatsBase.Columns.LAST_APPLIED_TIMESTAMP);
                for (Object[] row : ss.getStatsRows(false, System.currentTimeMillis())) {
                    long timeLastRcvd = asLong(row[ixTimeRcvd]);
                    long timeLastApplied = asLong(row[ixTimeAppl]);
                    if (timeLastRcvd != timeLastApplied) {
                        pend++;
                    }
                }
            }
        }
        catch (Exception ex) {
            warn("checkDrConsumer", ex);
        }
        drconsPend = pend;
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
        return ss.getStatsColumnIndex(name);
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
        logger.warn(String.format("Unexpected exception in ActivityHelper.%s", func), ex);
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
            if (!hasNext) {
                throw new NoSuchElementException("no more rows");
            }
            hasNext = false;
            return "THE_ROW";
        }
    }
}
