/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

package org.voltdb.sysprocs;

import java.util.Arrays;
import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.ClientInterface;
import org.voltdb.CommandLog;
import org.voltdb.DRProducerStatsBase;
import org.voltdb.DRConsumerStatsBase;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.StatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.importer.ImportManager;
import org.voltdb.export.ExportManagerInterface;

/**
 * Execute activity checks on a single host, preparatory
 * to shutting down the cluster. Does not complete until
 * there is no activity (or the waiting times out).
 *
 * This is intended only for use from @OpShutdown and
 * therefore the interface may change at no notice.
 *
 * Response is a table with a single row, two columns:
 * execution status (0 for success) and bitmask of
 * remaining activity, if any. Status and activity
 * values are defined in the OpShutdown class.
 *
 * Two timeouts are implemented:
 * - A progress timeout; each source is required to
 *   make some progress (assumed to be towards quiescence)
 *   within this limit.
 * - An overall timeout; the entire cluster is required
 *   to become inactive within this limit.
 * Either or both timeouts can be set to infinity, which
 * is represented as -1. Otherwise the value is expressed
 * in msec (unlike @OpShutdown's API).
 *
 * We monitor the flag set by @PrepareShutdown and
 * cleared by @CancelShutdown to determine if we should
 * exit prematurely.
 */
public class OpShutdownWait extends VoltNTSystemProcedure {

    private static final VoltLogger log = new VoltLogger("HOST");

    // Minimum allowed value for checkInterval
    static final int MIN_CHECK_INTERVAL = 1000;

    // Option flags on input
    static final int DRAIN_DR = 1;

    // For tracking progress while we wait for activity
    // to drain (one per source of activity)
    private ProgressTracker clientProgress;
    private ProgressTracker cmdLogProgress;
    private ProgressTracker importProgress;
    private ProgressTracker exportProgress;
    private ProgressTracker drProdProgress;
    private ProgressTracker drConsProgress;
    private int timedOutSummary;
    private int progressTmo;

     /**
     * Main entry point. Stalls until there is no
     * activity.  TODO: is there a better way?
     *
     * @param options bit-encoded options for this run
     * @param progressTmo time limit on lack of progress on one source
     * @param waitTmo time limit on waiting for all activity to drain
     * @param checkInterval time between activity checks (>= 1000)
     * @return single table containing single row (status, activity)
     *
     * All times expressed in msec. Timeouts can be null or negative
     * if we should try indefinitely. Check interval is quietly forced
     * to be at least some minimum value. Options are optional.
     */
    public VoltTable[] run(Integer options, Integer progressTmo, Integer waitTmo, Integer checkInterval) {
        if (options == null) options = 0;
        if (progressTmo == null) progressTmo = -1;
        if (waitTmo == null) waitTmo = -1;
        if (checkInterval == null || checkInterval < MIN_CHECK_INTERVAL) checkInterval = MIN_CHECK_INTERVAL;

        info("Waiting for activity on this host to complete");
        int status = -1; // not yet set
        int activity = 0;
        initTracking(progressTmo);

        try {
            boolean drainDr = (options & DRAIN_DR) != 0;
            long start = System.currentTimeMillis();
            long elapsed = 0;
            Thread.sleep(Math.min(1000, checkInterval)); // short initial delay
            while (status < 0 && (activity = hostActivity(drainDr)) != 0) {
                elapsed = System.currentTimeMillis() - start;
                if (waitTmo >= 0 && elapsed >= waitTmo || timedOutSummary != 0) {
                    warn("Wait timed out after %d msec", elapsed);
                    status = OpShutdown.TIMED_OUT;
                }
                else if (shutdownCancelled()) {
                    warn("Wait cancelled by client after %d msec", elapsed);
                    status = OpShutdown.CANCELLED;
                }
                else {
                    Thread.sleep(checkInterval);
                }
            }
            if (status < 0) {
                status = OpShutdown.SUCCESS;
                elapsed = System.currentTimeMillis() - start;
                info("All activity on this host drained after %d msec", elapsed);
            }
        }
        catch (InterruptedException ex) {
            status = OpShutdown.CANCELLED;
            warn("Wait interrupted");
        }
        catch (Exception ex) {
            status = OpShutdown.FAILED;
            warn("Unexpected exception in OpShutdownWait: %s", ex);
        }

        dropTracking();
        ColumnInfo[] cols = new ColumnInfo[] { new ColumnInfo("STATUS", VoltType.INTEGER),
                                               new ColumnInfo("ACTIVITY", VoltType.INTEGER) };
        VoltTable vt = new VoltTable(cols);
        vt.addRow(Integer.valueOf(status), Integer.valueOf(activity));
        return new VoltTable[] { vt };
    }

    /*
     * Allocate/deallocate progress-tracking mechanism
     */
    private void initTracking(int tmo) {
        clientProgress = new ProgressTracker(OpShutdown.CLIENT);
        cmdLogProgress = new ProgressTracker(OpShutdown.CMDLOG);
        importProgress = new ProgressTracker(OpShutdown.IMPORT);
        exportProgress = new ProgressTracker(OpShutdown.EXPORT);
        drProdProgress = new ProgressTracker(OpShutdown.DRPROD);
        drConsProgress = new ProgressTracker(OpShutdown.DRCONS);
        progressTmo = tmo;
        timedOutSummary = 0;
    }

    private void dropTracking() {
        clientProgress = null;
        cmdLogProgress = null;
        importProgress = null;
        exportProgress = null;
        drProdProgress = null;
        drConsProgress = null;
    }

    /*
     * Note that we timed out waiting for a particular source
     * to quiesce. We'll complete the current pass before we
     * fail.
     */
    private void noProgressTimeout(int flag) {
        timedOutSummary |= flag;
    }

    /*
     * Check whether @CancelShutdown has been called (assumes
     * the flag was appropriately set at start of shutdown).
     */
    private boolean shutdownCancelled() {
        return !VoltDB.instance().isPreparingShuttingdown();
    }

    /*
     * Check for any local activity that needs to drain before we can
     * start to shut down in earnest. Return is a bitmask for active
     * components, zero if all is quiet.
     *
     * Checking on DR is optional, as in the previous implementation
     * of this wait code in the voltadmin script.
     *
     * TODO: is it necessary to sync with site threads?
     */
    private int hostActivity(boolean incDr) {
        int activity = 0;
        activity |= checkClients();
        activity |= checkImporter();
        activity |= checkCommandLog();
        if (incDr) {
            activity |= checkDrConsumer();
            activity |= checkExporter();
            activity |= checkDrProducer();
        }
        else {
            activity |= checkExporter();
        }
        return activity;
    }

    /*
     * Client interface.
     * Check for outstanding requests, responses, transactions.
     * Counters are all zero if nothing outstanding.
     */
    private int checkClients() {
        long reqBytes = 0, respMsgs = 0, txns = 0;
        ClientInterface ci = VoltDB.instance().getClientInterface();
        if (ci != null) {
            for (Pair<String,long[]> ent : ci.getLiveClientStats().values()) {
                long[] val = ent.getSecond(); // order of values is assumed
                reqBytes += val[1];
                respMsgs += val[2];
                txns += val[3];
            }
            if (txns > 0) txns--; // one of these is me
        }
        return clientProgress.track("client interface: outstanding txns %d, bytes %d, responses %d",
                                    txns, reqBytes, respMsgs);
    }

    /*
     * Command log.
     * Check for outstanding logging: bytes and transactions.
     * Counters are zero if nothing outstanding.
     */
    private int checkCommandLog() {
        long txns = 0, bytes = 0;
        CommandLog cl = VoltDB.instance().getCommandLog();
        if (cl != null) {
            long[] temp = new long[2]; // bytes, txns
            temp[0] = temp[1] = 0;
            cl.getCommandLogOutstanding(temp);
            bytes = temp[0]; txns = temp[1];
        }
        return cmdLogProgress.track("command log: outstanding txns %d, bytes %d", txns, bytes);
    }

    /*
     * Importer.
     * Check for unprocessed import request.
     * Count is of outstanding requests across all importers,
     * and is zero if there's nothing outstanding.
     */
    private int checkImporter() {
        long pend = 0;
        ImportManager im = ImportManager.instance();
        if (im != null) {
            pend = im.statsCollector().getTotalPendingCount();
        }
        return importProgress.track("importer: %d pending", pend);
    }

    /*
     * Exporter.
     * Check for unprocessed exports.
     * Count is of outstanding tuples across all exporters,
     * and is zero if there's nothing outstanding.
     */
    private int checkExporter() {
        long pend = 0;
        ExportManagerInterface em = ExportManagerInterface.instance();
        if (em != null) {
            pend = em.getTotalPendingCount();
        }
        return exportProgress.track("exporter: %d pending", pend);
    }

    /*
     * DR producer.
     * Checks details of outstanding data, expressed in bytes and message
     * segments, summed across all partitions. Zero when none outstanding.
     */
    private int checkDrProducer() {
        long bytesPend = 0, segsPend = 0;
        StatsAgent sa = VoltDB.instance().getStatsAgent();
        if (sa != null) {
            Set<StatsSource> sss = sa.lookupStatsSource(StatsSelector.DRPRODUCERPARTITION, 0);
            if (sss != null && !sss.isEmpty()) {
                assert sss.size() == 1;
                StatsSource ss = sss.iterator().next();
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
        return drProdProgress.track("DR producer: outstanding segments %d, bytes %d", segsPend, bytesPend);
    }

    /*
     * DR consumer.
     * Checks count of partitions for which there is data not
     * yet successfully applied. Zero when nothing outstanding.
     */
    private int checkDrConsumer() {
        long pend = 0;
        StatsAgent sa = VoltDB.instance().getStatsAgent();
        if (sa != null) {
            Set<StatsSource> sss = sa.lookupStatsSource(StatsSelector.DRCONSUMERPARTITION, 0);
            if (sss != null && !sss.isEmpty()) {
                assert sss.size() == 1;
                StatsSource ss = sss.iterator().next();
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
        return drConsProgress.track("DR consumer: %d partitions with pending data", pend);
    }

    /*
     * Convert object in stats row to long integer
     */
    private static long asLong(Object obj) {
        return ((Long)obj).longValue();
    }

    /*
     * Get index for a column in someone else's stats table.
     */
    private static int getIndex(StatsSource ss, String name) {
        return ss.getStatsColumnIndex(name);
    }

    /*
     * Logging wrappers
     */
    private static void warn(String str, Object... args) {
        if (args != null && args.length != 0)
            str = String.format(str, args);
        log.warn(str);
    }

    private static void info(String str, Object... args) {
        if (args != null && args.length != 0)
            str = String.format(str, args);
        log.info(str);
    }

    /**
     * Generic progress tracker. Given a set of 'counts' for
     * a particular source, we determine whether the source
     * is now quiescent, indicated by all counts being zero.
     * If so, return zero.
     *
     * Otherwise, check for progress being made since the previous
     * call; this is a simple-minded check that the current counts
     * differ from the previous counts. If there is progress, note
     * the time and current counts, and return non-zero (a source-
     * specific value, opaque to us).
     *
     * If there is no progress and we've been without progress
     * for too long, then indicate that in our containing instance
     * of OpShutdownWait; we don't fail yet since we want to make
     * a complete pass on all activity sources. Return the usual
     * activity flag, but don't reset the time since we last made
     * progress.
     */
    private class ProgressTracker {
        private final int activityFlag;
        private long lastChangeTime;
        private long[] prevCounts;

        ProgressTracker(int flag) {
            activityFlag = flag;
        }

        int track(String format, long... counts) {
            long currTime = System.currentTimeMillis();
            if (inactive(counts)) {
                lastChangeTime = 0; // reset in case source goes active again
                save(counts);
                return 0;
            }
            else {
                logActivity(format, counts);
                if (lastChangeTime == 0 || !Arrays.equals(counts, prevCounts)) {
                    lastChangeTime = currTime;
                    save(counts);
                }
                else if (progressTmo >= 0 && currTime - lastChangeTime >= progressTmo) {
                    info("No progress made in the last %d msec", currTime - lastChangeTime);
                    noProgressTimeout(activityFlag);
                }
                return activityFlag;
            }
        }

        private boolean inactive(long[] counts) {
            boolean inact = true;
            for (long count : counts) {
                inact &= (count == 0);
            }
            return inact;
        }

        private void save(long[] counts) {
            if (prevCounts == null) {
                prevCounts = new long[counts.length];
            }
            for (int i=0; i<counts.length; i++) {
                prevCounts[i] = counts[i];
            }
        }

        private void logActivity(String format, long[] counts) {
            Long[] args = new Long[counts.length];
            for (int i=0; i<counts.length; i++) {
                args[i] = counts[i];
            }
            info(format, (Object[])args);
        }
    }
}
