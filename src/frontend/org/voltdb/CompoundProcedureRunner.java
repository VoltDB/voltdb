/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import static org.voltdb.NTProcedureService.LOG;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.voltcore.messaging.Mailbox;
import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltcore.logging.Level;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.VoltCompoundProcedure.CompoundProcAbortException;
import org.voltdb.VoltCompoundProcedure.Stage;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;
import org.voltdb.compiler.deploymentfile.CompoundProcPolicyType;

/**
 * A variation on ProcedureRunnerNT, specialized
 * for running VoltCompoundProcedures structured as
 * a sequence of stages.
 */
public class CompoundProcedureRunner extends ProcedureRunnerNT {

    // Timeout value is a constant, in microseconds, that can be overridden in SPI.
    // The default is 150s, which is slightly higher than the default timeout of the VoltDB client.
    private static final int COMPOUND_PROCEDURE_TIMEOUT = 150_000_000;

    // Per-stage limit on queued procedure calls. May be overridden at
    // initialization time based on deployment file.
    private static int QUEUED_PROC_LIMIT = 10;

    // Limit for rate-limited logging (user procedure failures are apt to
    // be repetitive in nature). Value in seconds.
    private static final int LOG_RATE_LIMIT = 600;

    // Link to our procedure service
    private final NTProcedureService ntProcService;

    // Stats specific to compound procedure execution
    private final CompoundProcCallStats compoundProcStats;

    CompoundProcedureRunner(long id, AuthUser user, Connection ccxn, boolean isAdmin, long ciHandle,
                            long clientHandle, int timeout, VoltCompoundProcedure procedure,
                            String procName, Method procMethod, Class<?>[] paramTypes,
                            ExecutorService responseService, NTProcedureService procService,
                            Mailbox mailbox,  ProcedureStatsCollector statsCollector,
                            CompoundProcCallStats compoundStats) {
        super(id, user, ccxn, isAdmin, ciHandle,
              clientHandle, timeout, procedure,
              procName, procMethod, paramTypes,
              responseService, procService,
              mailbox, statsCollector);
        this.ntProcService = procService;
        this.compoundProcStats = compoundStats;
        procedure.setRunner(this);
    }

    private final boolean debugging = LOG.isDebugEnabled();
    private final CompletableFuture<?> completion = new CompletableFuture<>();

    // Access is synchronized(procQueue)
    private final List<Proc> procQueue = new ArrayList<>();

    // Access is synchronized(stages)
    private final List<Stage> stages = new ArrayList<>();
    private Iterator<Stage> stageIterator;
    private int stageNumber;

    // Volatile for inter-thread visibility
    private volatile boolean stopExecution;
    private volatile boolean abortedOrFailed;

    // Holds proc-call details from queueProcedureCall
    // until the calls are actually issued
    private class Proc {
        String name;
        Object[] params;
        Proc(String n, Object[] p) {
            name = n; params = p;
        }
    }

    @Override
    final int getTimeout() {
        return COMPOUND_PROCEDURE_TIMEOUT;
    }

    static void setExecutionPolicy(CompoundProcPolicyType pol) {
        if (pol.getCallsperstage() != null) {
            QUEUED_PROC_LIMIT = pol.getCallsperstage();
        }
    }

    /**
     * This method, entered from coreCall in our superclass,
     * initiates processing of the compound procedure. We
     * call the 'run' method of the procedure, which is
     * expected to set up a list of execution stages by
     * a call to 'setStageList'.
     *
     * The 'run' method can optionally queue up some
     * procedure calls, which we will issue on return.
     * Otherwise, we immediately enter the first stage.
     *
     * The return value from 'run' is ignored, but the
     * Volt compiler insists on it being one of the types
     * allowed for ordinary VoltProcedures.
     *
     * The procedure's monitor is acquired across the call;
     * see header comments for 'execNextStage' for rationale.
     */
    @Override
    protected Object invokeRunMethod(Object[] paramList) {
        try {
            debug("starting");
            synchronized (m_procedure) {
                m_procMethod.invoke(m_procedure, paramList);
            }
            if (stages.isEmpty()) {
                failProcedure("'run' method failed to set up execution stages");
            }
        }
        catch (CompoundProcAbortException ex) {
            abortProcedure(ex);
        }
        catch (InvocationTargetException ex) {
            failProcedure(ex.getCause());
        }
        catch (Throwable th) {
            failProcedure(th);
        }
        execNextStage(new ClientResponse[0]);
        return completion;
    }

    void setStageList(List<Stage> list) {
        synchronized (stages) {
            if (!stages.isEmpty()) {
                throw new RuntimeException("Cannot change stage-list");
            }
            stages.addAll(list); // copy so there's no funny stuff
            stageIterator = stages.iterator();
            stageNumber = 0;
        }
    }

    /*
     * On completion of requests issued by a stage (if any)
     * we will enter the next stage, and on return, issue
     * any procedure calls queued by that stage.
     *
     * We hold the procedure's monitor lock across the call
     * to the procedure. The reason for this is that the
     * procedure may store intermediate data in its own member
     * variables, successive stages may execute in different
     * threads, and the Java memory model requires proper
     * synchronization to guarantee data visibility.
     *
     * Note that none of our own locks can be held at the
     * point at which we call the procedure, otherwise we
     * risk deadlock in calls from the procedure to us.
     */
    private void execNextStage(ClientResponse[] respList) {
        Stage next = null;
        while (!stopExecution && (next = nextStage()) != null) {
            try {
                synchronized (m_procedure) {
                    next.execute(respList);
                }
            }
            catch (CompoundProcAbortException ex) {
                abortProcedure(ex);
            }
            catch (Throwable th) {
                failProcedure(th);
            }
            if (!stopExecution && callQueuedProcedures() != 0) {
                return; // async completion propagates execution
            }
            next = null;
            respList = new ClientResponse[0];
        }
        if (!stopExecution) {
            failProcedure("end of stage list reached and procedure not completed");
        }
        else if (!abortedOrFailed) {
            checkProcQueueEmpty();
        }
    }

    private Stage nextStage() {
        Stage next = null;
        synchronized (stages) {
            if (stageIterator.hasNext()) {
                next = stageIterator.next();
                stageNumber++;
                debug("exec stage %d", stageNumber);
            }
        }
        return next;
    }

    /**
     * Add a procedure call to the pending queue, to be sent
     * when the stage returns to this runner. Called via
     * a wrapper method in VoltCompoundProcedure.
     *
     * The eventual completion of those calls is what will
     * drive us into the next state.
     */
    void queueProcedureCall(String procName, Object... params) {
        synchronized (procQueue) {
            if (procQueue.size() >= QUEUED_PROC_LIMIT) {
                String err = "Too many pending procedure calls, limit is " + QUEUED_PROC_LIMIT;
                throw new RuntimeException(err);
            }
            if (ntProcService.isCompoundProc(procName)) {
                String err = "Rejecting nested call to compound procedure " + procName;
                throw new RuntimeException(err);
            }
            procQueue.add(new Proc(procName, params));
        }
    }

    private int callQueuedProcedures() {
        int count = 0;
        List<Proc> procs = null;
        synchronized (procQueue) {
            count = procQueue.size();
            if (count != 0) {
                procs = new ArrayList<>(procQueue);
                procQueue.clear();
            }
        }
        if (count != 0) {
            debug("issuing %d proc calls", count);
            trackCalledProcedures(procs);
            final CompletableFuture<?>[] fut = new CompletableFuture<?>[count];
            for (int i=0; i<count; i++) {
                Proc p = procs.get(i);
                fut[i] = super.callProcedure(p.name, p.params);
            }
            CompletableFuture.allOf(fut).whenComplete((nil, th) -> batchComplete(fut));
        }
        return count;
    }

    private void trackCalledProcedures(List<Proc> procs) {
        for (Proc p : procs) {
            compoundProcStats.trackCallTo(p.name);
        }
    }

    private void batchComplete(CompletableFuture<?>[] fut) {
        debug("all %d proc calls complete", fut.length);
        ClientResponse[] resp = new ClientResponse[fut.length];
        for (int i=0; i<fut.length; i++) {
            resp[i] = extractResponse(fut[i]);
        }
        execNextStage(resp);
    }

    private ClientResponse extractResponse(CompletableFuture<?> fut) {
        try {
            return (ClientResponse)fut.get();
        }
        catch (Exception ex) {
            ex = unwrapException(ex);
            String msg = ex.getMessage();
            if (msg == null || msg.isEmpty()) msg = ex.getClass().getName();
            return new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE, new VoltTable[0], msg);
        }
    }

    private Exception unwrapException(Exception ex) {
        while ((ex instanceof ExecutionException || ex instanceof CompletionException) &&
               ex.getCause() != null &&
               ex.getCause() instanceof Exception) {
            ex = (Exception) ex.getCause();
        }
        return ex;
    }

    private void resetProcedureQueue() {
        synchronized (procQueue) {
            procQueue.clear();
        }
    }

    private void checkProcQueueEmpty() {
        int n;
        synchronized (procQueue) {
            n = procQueue.size();
        }
        if (n > 0) {
            info("completed; %d procedure call%s queued in last stage will not be sent",
                 n, n == 1 ? "" : "s");
        }
    }

    /*
     * Called from the executing compound procedure to indicate
     * completion, via wrappers in VoltCompoundProcedure.
     *
     * On failure/abort, we always build the exact ClientResponse
     * we need, rather than expecting exceptional-completion
     * handling in ProcedureRunnerNT. Fortunately, the normal
     * completion code can handle getting either the expected
     * result (Long, VoltTable, VoltTable[]) or a ready-made
     * ClientResponse.
     *
     * In various places, we catch Throwables rather than Exceptions,
     * because we want to treat some Errors as only being reasons to
     * fail the procedure, not fatal to the server. We follow the
     * pattern used elsewhere, which is to catch all Throwables
     * and then use a utility method to sort them out.
     */
    @SuppressWarnings("unchecked")
    <T> void completeProcedure(Class<T> type, T result) {
        stopExecution = true;
        debug("completed");
        ((CompletableFuture<T>) completion).complete(result);
    }

    void abortProcedure(String reason) {
        stopExecution = abortedOrFailed = true;
        String abmsg = String.format("User abort: %s", reason);
        debug("aborted, \\\\%s\\\\", abmsg);
        completeError(ClientResponse.COMPOUND_PROC_USER_ABORT, abmsg);
    }

    private void abortProcedure(Exception ex) {
        stopExecution = abortedOrFailed = true;
        String abmsg = String.format("User abort: %s", ex.getMessage());
        debug("abort exception, \\\\%s\\\\", abmsg);
        completeError(ClientResponse.COMPOUND_PROC_USER_ABORT, abmsg);
    }

    private void failProcedure(String reason) {
        stopExecution = abortedOrFailed = true;
        String errmsg = String.format("Unexpected failure: %s", reason);
        info("failed, \\\\%s\\\\", errmsg);
        completeError(ClientResponse.UNEXPECTED_FAILURE, errmsg);
    }

    private void failProcedure(Throwable th) {
        stopExecution = abortedOrFailed = true;
        if (CoreUtils.isStoredProcThrowableFatalToServer(th)) {
            throw (Error)th; // That's not my department, says Werner Von Braun
        }
        String errmsg = String.format("Unexpected failure: %s", th.toString());
        info("failed, \\\\%s\\\\", errmsg);
        completeError(ClientResponse.UNEXPECTED_FAILURE, errmsg);
    }

    private void completeError(byte status, String statusStr) {
        ClientResponse resp = new ClientResponseImpl(status,
                                                     m_appStatusCode, m_appStatusString,
                                                     new VoltTable[0],
                                                     "COMPOUND PROC ERROR: " + statusStr,
                                                     m_clientHandle);
        ((CompletableFuture<ClientResponse>) completion).complete(resp);
    }

    /*
     * Called from the compound procedure to set the application
     * status code and string that will be returned to the
     * client as part of the ClientResponse.
     */
    void setResponseAppStatusCode(byte status) {
        super.setAppStatusCode(status);
    }

    void setResponseAppStatusString(String message) {
        super.setAppStatusString(message);
    }

    /*
     * Logging. Non-debug logging is rate-limited. Since procedure runners
     * are ephemeral, this needs careful balance. The 'format' given to the
     * rate-limiting logger must include the procedure name (to avoid cross-
     * talk) and the 'format' used for the message itself, but not include the
     * variable args for the message. Also, it's imperative that the underlying
     * logger be statically allocated (it is, in NTProcedureService)
     */
    private void debug(String fmt, Object... args) {
        if (debugging) {
            String fmt2 = String.format("Compound proc %s; %s", m_procedureName, fmt);
            LOG.debugFmt(fmt, args);
        }
    }

    private void info(String fmt, Object... args) {
        if (debugging) {
            String fmt2 = String.format("Compound proc %s; %s", m_procedureName, fmt);
            LOG.infoFmt(fmt2, args);
        }
        else {
            String fmt2 = String.format("Compound proc %s; %s\n\t(this message is rate-limited to once per %d sec)",
                                        m_procedureName, fmt, LOG_RATE_LIMIT);
            LOG.rateLimitedInfo(LOG_RATE_LIMIT, fmt2, args);
        }
    }

    /*
     * Unsupported NT operations. Calls to the similarly-named methods
     * in VoltNonTransactionalProcedure end up here in the runner, at
     * which point we will refuse them.
     */
    @Override
    protected CompletableFuture<ClientResponse> callProcedure(String procName, Object... params) {
        CompletableFuture<ClientResponse> fut = new CompletableFuture<>();
        fut.completeExceptionally(new UnsupportedOperationException("callProcedure"));
        return fut;
    }

    @Override
    protected CompletableFuture<ClientResponseWithPartitionKey[]> callAllPartitionProcedure(String procedureName, Object... params) {
        CompletableFuture<ClientResponseWithPartitionKey[]> fut = new CompletableFuture<>();
        fut.completeExceptionally(new UnsupportedOperationException("callAllPartitionProcedure"));
        return fut;
    }

    @Override
    protected CompletableFuture<Map<Integer,ClientResponse>> callAllNodeNTProcedure(String procName, Object... params) {
        CompletableFuture<Map<Integer,ClientResponse>> fut = new CompletableFuture<>();
        fut.completeExceptionally(new UnsupportedOperationException("callAllNodeNTProcedure"));
        return fut;
    }
}
