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

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;

/**
 * Graceful shutdown. This proceeds in 4 phases:
 * 1. Pause the database by @PrepareShutdown.
 * 2. Start to flush exports by @Quiesce.
 * 3. Wait for ongoing activity to cease on all hosts.
 * 4. Invoke the @Shutdown procedure to actually shut down,
 *    requesting a snapshot if caller wanted or we think
 *    it necessary.
 *
 * This has to be executed on an admin connection, not least
 * because some of the procedures it executes do not operate
 * without admin-level access. We explicitly check for admin
 * since otherwise the results can be confusing.
 */
public class OpShutdown extends VoltNTSystemProcedure {

    private final static VoltLogger log = new VoltLogger("HOST");
    private final static AtomicBoolean lock = new AtomicBoolean();
    private final static int PROGRESS_TIMEOUT = 2 * 60 * 1000; // default, 2 minutes
    private final static int WAIT_TIMEOUT = 10 * 60 * 1000; // default, 10 minutes
    private final static int CHECK_INTERVAL = 10 * 1000; // 10 seconds
    private final static int EXTRA_TIMEOUT = 3 * CHECK_INTERVAL; // 30 seconds

    /**
     * Option flags on input
     */
    public static final int SAVE_SNAPSHOT = 1;
    public static final int FORCE_SHUT_AFTER_TIMEOUT = 2;

    /**
     * Resultant status values
     */
    public static final int SUCCESS = 0; // activity bitmap will be zero
    public static final int TIMED_OUT = 1; // activity bitmap will have result of last check
    public static final int CANCELLED = 2; // activity bitmap indeterminate
    public static final int FAILED = 3; // activity bitmap indeterminate

    /**
     * Activity bitmap (enum/EnumSet would be better but we
     * want to send these through a VoltTable as an integer)
     */
    public static final int CLIENT = 1;
    public static final int CMDLOG = 2;
    public static final int IMPORT = 4;
    public static final int EXPORT = 8;
    public static final int DRPROD = 16;
    public static final int DRCONS = 32;

    /**
     * Execute a graceful shutdown. The procedure is intended for
     * use in operator automation scenarios, and is not currently
     * exposed to end users.
     *
     * @param options bit-encoded options for this run
     * @param progressTmo time limit on lack of progress on one source
     * @param waitTmo time limit on waiting for all activity to drain
     * @return on failure to quiesce: table of (host id, status, activity)
     *         on successful shutdown: does not return
     *
     * Timeouts are specified in seconds in this external API.
     * Either timeout can be made infinite by setting it to -1.
     */
    public VoltTable[] run(Integer options, Integer progressTmo, Integer waitTmo) {
        if (!isAdminConnection()) {
            throw new VoltAbortException("@OpShutdown requires an admin connection");
        }
        if (lock.getAndSet(true)) {
            throw new VoltAbortException("@OpShutdown is already running");
        }
        try {
            return runShutdown(options == null ? 0 : options.intValue(),
                               toMs(progressTmo, PROGRESS_TIMEOUT),
                               toMs(waitTmo, WAIT_TIMEOUT));
        }
        finally {
            if (!lock.getAndSet(false)) {
                warn("Internal error, lock mishandled in @OpShutdown");
            }
        }
    }

    /*
     * Seconds to milliseconds, handling null, 'infinite', and
     * avoiding integer overflow.
     */
    private int toMs(Integer secs, int deflt) {
        return secs == null ? deflt
            : secs < 0 ? -1
            : secs < Integer.MAX_VALUE/1000 ? secs*1000
            : Integer.MAX_VALUE;
    }

    /*
     * Implementation, separate method to avoid clutter from the
     * lock management in the public method.
     *
     * Note that from here on, times are always handled in msec.
     */
    private VoltTable[] runShutdown(int options, int progressTmo, int waitTmo) {
        info("Graceful shutdown of database requested");
        info("  with options:%d, progressTmo:%d, waitTmo:%d", options, progressTmo, waitTmo);

        ClientResponse resp1 = callProcCheckResponse("@PrepareShutdown");
        if (VoltDB.instance().getMode() != OperationMode.PAUSED) {
            warn("Internal error: expected @PrepareShutdown to pause the database");
            throw new GeneralException("Database did not pause as expected");
        }

        boolean save = (options & SAVE_SNAPSHOT) != 0;
        if (VoltDB.instance().isMasterOnly()) {
            info("Cluster is operating in reduced-safety mode; shutdown snapshot will be taken");
            save = true;
        }

        Long zkPauseTxn = null;
        if (save) {
            zkPauseTxn = getPauseTxnId(resp1);
            if (zkPauseTxn == null) {
                warn("Internal error: response from @PrepareShutdown did not yield the expected txn id");
                throw new GeneralException("Could not get the txn id needed for shutdown snapshot");
            }
        }

        info("Quiescing exports");
        ClientResponse resp2 = callProcCheckResponse("@Quiesce");

        info("Waiting for in-progress activity to complete on all hosts");
        boolean force = (options & FORCE_SHUT_AFTER_TIMEOUT) != 0;
        int waitOptions = save ? OpShutdownWait.DRAIN_DR : 0;
        int waitRespTmo = waitTmo >= 0 ? waitTmo+EXTRA_TIMEOUT : -1;
        Map<Integer,ClientResponse> resp3 = awaitResponse(callNTProcedureOnAllHosts("@OpShutdownWait",
                                                                                    waitOptions, progressTmo,
                                                                                    waitTmo, CHECK_INTERVAL),
                                                          "@OpShutdownWait", waitRespTmo);
        if (resp3 == null) { // @OpShutdownWait is wedged
            warn("Cannot recover from previous error; forcing shutdown");
        }
        else if (allQuiet(resp3)) {
            info("In-progress activity is complete on all hosts");
        }
        else if (force) {
            warn("Forcing shutdown despite possible activity");
        }
        else {
            return new VoltTable[] { makeResponseTable(resp3) };
        }

        if (save) {
            info("Taking snapshot prior to shutdown");
            callProcCheckResponse("@Shutdown", zkPauseTxn);
        }
        else {
            info("Proceeding with database shutdown");
            callProcCheckResponse("@Shutdown");
        }

        info("Shutdown is complete"); // @Shutdown does not return
        return new VoltTable[0]; // so this is never actually executed
    }

    /*
     * Execute a single sysproc, wait for completion, then check for
     * success status in the response.
     */
    private ClientResponse callProcCheckResponse(String procName) {
        CompletableFuture<ClientResponse> future = callProcedure(procName);
        return checkResponse(awaitResponse(future, procName, -1), procName);
    }

    private ClientResponse callProcCheckResponse(String procName, Object arg) {
        CompletableFuture<ClientResponse> future = callProcedure(procName, arg);
        return checkResponse(awaitResponse(future, procName, -1), procName);
    }

    private static ClientResponse checkResponse(ClientResponse resp, String procName) {
        if (resp.getStatus() != ClientResponse.SUCCESS) {
            String detail = resp.getStatusString();
            if (detail == null) detail = "no detail";
            String msg = String.format("Internal call to system procedure %s failed, status %d, %s",
                                       procName, resp.getStatus(), detail);
            warn(msg);
            throw new GeneralException(msg);
        }
        return resp;
    }

    /*
     * Wait on a future, get the response. Convert any exception to
     * a runtime exception, unwrapping any of the wrapper exceptions
     * to get the true cause.
     *
     * In the case of a timeout from 'get', the procedure may still be
     * running. Our usage is such that this should not happen: the
     * procedure is given its own time limit and we expect it to return
     * within that limit, finished or not. We use a slightly longer time
     * on the 'get' to make doubly sure. The return will be null in
     * this case only.
     */
    private static <T> T awaitResponse(CompletableFuture<T> future, String procName, long timeout) {
        try {
            if (timeout < 0) {
                return future.get();
            }
            else {
                return future.get(timeout, TimeUnit.MILLISECONDS);
            }
        }
        catch (TimeoutException ex) {
            String s = String.format("Internal call to system procedure %s failed to complete after %d msec", procName, timeout);
            warn(s);
            if (timeout < 0) { // timeout exception with no timeout requested?
                throw new GeneralException(s);
            }
            return null;
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

    /*
     * Extract transaction id from @PrepareShutdown response.
     * We may need this to create a snapshot.
     */
    private static Long getPauseTxnId(ClientResponse resp) {
        Long txnId = null;
        VoltTable vt = resp.getResults()[0];
        if (vt.advanceRow()) {
            txnId = vt.getLong(0);
        }
        return txnId;
    }

    /*
     * Examine collected responses from execution of @OpShutdownWait.
     * Did everything successfully quiesce? We treat any reported errors
     * or unexpected results as if the host still has activity.
     */
    private static boolean allQuiet(Map<Integer,ClientResponse> respMap) {
        boolean quiet = true;
        for (Map.Entry<Integer,ClientResponse> ent : respMap.entrySet()) {
            ClientResponse resp = ent.getValue();
            if (resp.getStatus() == ClientResponse.SUCCESS) {
                VoltTable wt = resp.getResults()[0];
                if (wt.advanceRow()) {
                    long status = wt.getLong(0);
                    long activity = wt.getLong(1);
                    if (status != 0) {
                        warn("Host id %d has not quiesced (status %d, activity %#x)",
                             ent.getKey(), status, activity);
                        quiet = false; // actual
                    }
                }
                else {
                    warn("Host id %d returned empty result table", ent.getKey());
                    quiet = false; // assumed
                }
            }
            else {
                String msg = resp.getStatusString();
                if (msg == null) msg = "no detail";
                warn("Call to host id %d failed with status %d, %s", ent.getKey(), resp.getStatus(), msg);
                quiet = false; // assumed
            }
        }
        return quiet;
    }

    /*
     * Build response table with all hosts and activity flag; this is
     * currently used only in the case of failure to get to a quiet place.
     *
     * This could be combined with allQuiet, but is separate because
     * we only need it on failure (which we don't expect) and because
     * error-handling design is still in flux.
     */
    private static VoltTable makeResponseTable(Map<Integer,ClientResponse> respMap) {
        ColumnInfo[] cols = new ColumnInfo[] { new ColumnInfo("HOST_ID", VoltType.INTEGER),
                                               new ColumnInfo("STATUS", VoltType.INTEGER),
                                               new ColumnInfo("ACTIVITY", VoltType.INTEGER) };
        VoltTable vt = new VoltTable(cols);
        for (Map.Entry<Integer,ClientResponse> ent : respMap.entrySet()) {
            long status = -1, activity = -1; // jam in -1 if table empty
            ClientResponse resp = ent.getValue();
            if (resp.getStatus() == ClientResponse.SUCCESS) {
                VoltTable wt = resp.getResults()[0];
                if (wt.advanceRow()) {
                    status = wt.getLong(0);
                    activity = wt.getLong(1);
                }
            }
            vt.addRow(ent.getKey(), status, activity);
        }
        return vt;
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
