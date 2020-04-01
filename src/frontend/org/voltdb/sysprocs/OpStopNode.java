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

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;

/**
 * Gracefully stop a node. This proceeds in 4 phases:
 * 1. Pause the database by @PrepareStopNode.
 * 2. Wait until partition leadership has migrated away.
 * 3. Wait until export mastership has migrated away.
 * 4. Invoke the @StopNode procedure to stop the node.
 *
 * This has to be executed on an admin connection, not least
 * because some of the procedures it executes do not operate
 * without admin-level access. We explicitly check for admin
 * since otherwise the results can be confusing.
 *
 * This procedure must be executed on some host which is not
 * the node that is being stopped.
 */
public class OpStopNode extends VoltNTSystemProcedure {

    private final static VoltLogger log = new VoltLogger("HOST");
    private final static int SYSPROC_CALL_TIMEOUT = 10 * 1000; // 10 seconds
    private final static int PROGRESS_TIMEOUT = 2 * 60 * 1000; // default, 2 minutes
    private final static int WAIT_TIMEOUT = 10 * 60 * 1000; // default, 10 minutes
    private final static int CHECK_INTERVAL = 10 * 1000; // 10 seconds

    /**
     * Shut down a node gracefully. This is to be executed on
     * a host that is not the one being shut down. The procedure
     * is intended for use in operator automation scenarios, and
     * is not currently exposed to end users.
     *
     * @param hostId the host that is to be stopped
     * @param progressTmo time limit on lack of progress on one source
     * @param waitTmo time limit on waiting for all activity to drain
     * @return an empty VoltTable
     *
     * Timeouts are specified in seconds in this external API.
     * Either timeout can be made infinite by setting it to -1.
     */
    public VoltTable[] run(Integer hostId, Integer progressTmo, Integer waitTmo) {
        if (hostId == null) {
             throw new VoltAbortException("@OpStopNode requires a host id");
        }
        if (!isAdminConnection()) {
            throw new VoltAbortException("@OpStopNode requires an admin connection");
        }

        info("Graceful stopping of host %d requested", hostId);
        callProcGetResult("@PrepareStopNode", hostId);

        info("Waiting for host %d to quiesce", hostId);
        int progTmoMs = toMs(progressTmo, PROGRESS_TIMEOUT);
        int waitTmoMs = toMs(waitTmo, WAIT_TIMEOUT);
        drain("partition leadership", hostId, this::countLeadership, true, progTmoMs, waitTmoMs);
        drain("export mastership", hostId, this::countExportMastership, false, progTmoMs, waitTmoMs);

        info("Stopping host %d", hostId);
        callProcGetResult("@StopNode", hostId);

        info("Host %d is stopped", hostId);
        return new VoltTable[0];
    }

    /*
     * Seconds to milliseconds, handling null, 'infinite', and
     * avoiding integer overflow.
     */
    private int toMs(Integer secs, int deflt) {
        return secs == null ? deflt
            : secs < 0 ? -1 // means infinite timeout
            : secs < Integer.MAX_VALUE/1000 ? secs*1000
            : Integer.MAX_VALUE;
    }

    /*
     * Waits for some countable condition to become zero; the count is
     * reported by countFunc, which we repeatedly evaluate. There are
     * optional timeouts (in msec) for the total time we can wait to drain,
     * and on the time that can pass without progress being made.
     */
    private void drain(String what, int hostId, IntUnaryOperator countFunc,
                       boolean failOnTmo, int progressTmo, int waitTmo) {
        info("Waiting for migration of %s to complete", what);
        try {
            long startTime = System.currentTimeMillis();
            long lastChangeTime = startTime;
            int lowCount = Integer.MAX_VALUE, currCount = 0, negWarn = 0;
            while ((currCount = countFunc.applyAsInt(hostId)) != 0) {
                long currTime = System.currentTimeMillis();
                if (currCount < lowCount) { // we're moving in the right direction (currCount decreasing)
                    lastChangeTime = currTime;
                    lowCount = currCount;
                    negWarn = currCount;
                }
                else { // currCount >= lowCount
                    if (currCount > lowCount && currCount > negWarn) {
                        info("Negative progress made on migration of %s: count was %d, now %d", what, negWarn, currCount);
                        negWarn = currCount; // track so we warn again only if it gets worse
                    }
                    if (progressTmo >= 0 && currTime - lastChangeTime >= progressTmo) {
                        timedOut(failOnTmo, "No progress made on migration of %s in the last %d msec", what, currTime - lastChangeTime);
                        return;
                    }
                }
                if (waitTmo >= 0 && currTime - startTime >= waitTmo) {
                    timedOut(failOnTmo, "Migration of %s has not completed after %d msec", what, currTime - startTime);
                    return;
                }
                Thread.sleep(CHECK_INTERVAL);
            }
            long elapsed = System.currentTimeMillis() - startTime;
            info("Migration of %s complete after %d msec", what, elapsed);
        }
        catch (InterruptedException ex) {
            warn("Wait interrupted");
            throw new GeneralException("Wait interrupted");
        }
        catch (Exception ex) {
            warn("Unexpected exception: %s", ex);
            throw new GeneralException("Unexpected exception: " + ex);
        }
    }

    /*
     * Process a timeout; caller specifies whether it's a fatality
     */
    private static void timedOut(boolean failOnTmo, String form, Object... args) {
        String msg = String.format(form, args);
        if (failOnTmo) {
            warn(msg);
            throw new GeneralException(msg);
        }
        else {
            warn("%s (but proceeding to stop node)", msg);
        }
    }

    /*
     * Counts the number of partitions for which the specified host
     * is the partition leader. Queries TOPO stats to get the data.
     * Partition numbers are in the range [0,16383], but 16383 has
     * special meaning, which we ignore.
     */
    private int countLeadership(int hostId) {
        int leaders = 0;
        VoltTable vt = callProcGetResult("@Statistics", "TOPO", Integer.valueOf(0));
        while (vt != null && vt.advanceRow()) {
            long partition = vt.getLong(0);
            String leader = vt.getString(2);
            if (partition < 16383 && leader != null && !leader.isEmpty()) {
                int leaderHost = Integer.parseInt(leader.split(":")[0]);
                if (leaderHost == hostId) {
                    info("Host %d is still leader for partition %d", leaderHost, partition);
                    leaders++;
                }
            }
        }
        return leaders;
    }

    /*
     * Counts the number of export for which the specified host
     * is the master. Queries EXPORT stats to get the data.
     */
    private int countExportMastership(int hostId) {
        int masters = 0;
        VoltTable vt = callProcGetResult("@Statistics", "EXPORT", Integer.valueOf(0));
        while (vt != null && vt.advanceRow()) {
            long masterHost = vt.getLong(1);
            String source = vt.getString(5);
            String active = vt.getString(7);
            if (masterHost == hostId && "TRUE".equals(active)) {
                info("Host %d is still master for export '%s'", masterHost, source);
                masters++;
            }
        }
        return masters;
    }

    /*
     * Execute a single sysproc, wait for completion, and check for
     * success status in the response. Return first VoltTable from
     * the response, or null if there isn't one.
     */
    private VoltTable callProcGetResult(String procName, Object... args) {
        CompletableFuture<ClientResponse> future = callProcedure(procName, args);
        ClientResponse resp = awaitResponse(future, procName, SYSPROC_CALL_TIMEOUT);
        if (resp.getStatus() != ClientResponse.SUCCESS) {
            String detail = resp.getStatusString();
            if (detail == null) detail = "no detail";
            String msg = String.format("Internal call to system procedure %s %s failed, status %d, %s",
                                       procName, args[0], resp.getStatus(), detail);
            warn(msg);
            throw new GeneralException(msg);
        }
        VoltTable[] vtarr = resp.getResults();
        if (vtarr == null || vtarr.length == 0 || vtarr[0] == null) {
            info("Call to %s %s returned no data", procName, args[0]);
            return null;
        }
        return vtarr[0];
    }

    /*
     * Wait on a future with a specified timeout, and get the response.
     * Convert any exception to a runtime exception, unwrapping any of
     * the wrapper exceptions to get the true cause.
     */
    private static ClientResponse awaitResponse(CompletableFuture<ClientResponse> future, String procName, long timeout) {
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex) {
            String s = String.format("Internal call to system procedure %s failed to complete after %d msec", procName, timeout);
            warn(s);
            throw new GeneralException(s);
        }
        catch (Throwable th) {
            while ((th instanceof InterruptedException ||
                    th instanceof CancellationException ||
                    th instanceof ExecutionException) && th.getCause() != null) {
                th = th.getCause();
            }
            String s = String.format("Internal call to system procedure %s failed", procName);
            warn("%s, %s", s, th);
            throw new GeneralException(s);
        }
    }

    /**
     * General runtime exception class
     */
    public static class GeneralException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public GeneralException(String msg) {
            super(msg);
        }
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
}
