/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

/**
 * Support class non-transactional procedures that runs 1-1
 * with an instance of VoltNTProcedure.
 *
 * Much like regular ProcedureRunner, this NT version lets you run
 * the run method and manages the API of the user-supplied proc.
 *
 */
public class ProcedureRunnerNT {

    // m_responseService is used for handling ClientResponse callbacks
    private final ExecutorService m_responseService;
    // m_ntProcService is used for procedure calls from this runner
    private final NTProcedureService m_ntProcService;
    // client interface mailbox
    private final Mailbox m_mailbox;
    // shared between all concurrent calls to the same procedure
    private ProcedureStatsCollector m_statsCollector;
    // generated for each call
    private StatementStats.SingleCallStatsToken m_perCallStats = null;

    // unique for each call
    protected final long m_id;

    // regular call support stuff
    protected final AuthUser m_user;
    protected final Connection m_ccxn;
    protected final long m_ciHandle;
    protected final long m_clientHandle;
    protected final int m_timeout;
    protected final String m_procedureName;
    protected final VoltNonTransactionalProcedure m_procedure;
    protected final Method m_procMethod;
    protected final Class<?>[] m_paramTypes;
    protected final boolean m_isAdmin;
    protected byte m_statusCode = ClientResponse.SUCCESS;
    protected String m_statusString = null;
    protected byte m_appStatusCode = ClientResponse.UNINITIALIZED_APP_STATUS_CODE;
    protected String m_appStatusString = null;

    // gate to only allow one all-host call at a time
    private final AtomicBoolean m_outstandingAllHostProc = new AtomicBoolean(false);

    // Track the outstanding all host procedures
    private final Object m_allHostCallbackLock = new Object();
    private Set<Integer> m_outstandingAllHostProcedureHostIds;
    private Map<Integer, ClientResponse> m_allHostResponses;
    private CompletableFuture<Map<Integer, ClientResponse>> m_allHostFut;

    ProcedureRunnerNT(
            long id, AuthUser user, Connection ccxn, boolean isAdmin, long ciHandle, long clientHandle, int timeout,
            VoltNonTransactionalProcedure procedure, String procName, Method procMethod, Class<?>[] paramTypes,
            ExecutorService responseService, NTProcedureService procService, Mailbox mailbox,
            ProcedureStatsCollector statsCollector) {
        m_id = id;
        m_user = user;
        m_ccxn = ccxn;
        m_isAdmin = isAdmin;
        m_ciHandle = ciHandle;
        m_clientHandle = clientHandle;
        m_timeout = timeout;
        m_procedure = procedure;
        m_procedureName = procName;
        m_procMethod = procMethod;
        m_paramTypes = paramTypes;
        m_responseService = responseService;
        m_ntProcService = procService;
        m_mailbox = mailbox;
        m_statsCollector = statsCollector;
    }

    /**
     * Complete the future when we get a traditional ProcedureCallback.
     * (Package access for LightweightNTClientResponseAdapter)
     */
    class NTNestedProcedureCallback implements ProcedureCallback {
        final CompletableFuture<ClientResponse> m_fut = new CompletableFuture<>();

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            // the future needs to be completed in the right executor service
            // so any follow on work will be in the right executor service
            m_responseService.submit(() -> {
                m_fut.complete(clientResponse);
            });
        }

        CompletableFuture<ClientResponse> fut() {
            return m_fut;
        }

        String getHostnameOrIP() {
            return m_ccxn.getHostnameOrIP();
        }

        long getConnectionId(long clientHandle) {
            return m_ccxn.connectionId(m_clientHandle);
        }
    }

    /**
     * This is called when an all-host proc responds from a particular node.
     * It completes the future when all of the
     *
     * It uses a dumb hack that the hostid is stored in the appStatusString.
     * Since this is just for sysprocs, VoltDB devs making sysprocs should know
     * that string app status doesn't work.
     */
    public void allHostNTProcedureCallback(ClientResponse clientResponse) {
        synchronized(m_allHostCallbackLock) {
            int hostId = Integer.parseInt(clientResponse.getAppStatusString());
            assert (m_outstandingAllHostProcedureHostIds != null);
            boolean removed = m_outstandingAllHostProcedureHostIds.remove(hostId);
            // log this for now... I don't expect it to ever happen, but will be interesting to see...
            if (!removed) {
                LOG.errorFmt("ProcedureRunnerNT.allHostNTProcedureCallback for procedure %s received late or unexepected response from hostID %d.",
                          m_procedureName, hostId);
                return;
            }

            m_allHostResponses.put(hostId, clientResponse);
            if (m_outstandingAllHostProcedureHostIds.size() == 0) {
                m_outstandingAllHostProc.set(false);
                m_allHostFut.complete(m_allHostResponses);
            }
        }
    }

    /**
     * Call a procedure (either txn or NT) and complete the returned future when done.
     */
    protected CompletableFuture<ClientResponse> callProcedure(String procName, Object... params) {
        NTNestedProcedureCallback cb = new NTNestedProcedureCallback();
        m_ntProcService.m_internalNTClientAdapter.callProcedure(m_user, isAdminConnection(), m_timeout, cb, procName, params);
        return cb.fut();
    }

    protected CompletableFuture<ClientResponseWithPartitionKey[]> callAllPartitionProcedure(final String procedureName, final Object... params) {
        final Object[] args = new Object[params.length + 1];
        System.arraycopy(params, 0, args, 1, params.length);

        // get the partition keys
        VoltTable keys = TheHashinator.getPartitionKeys(VoltType.INTEGER);

        @SuppressWarnings("unchecked")
        final CompletableFuture<ClientResponse>[] futureList = (CompletableFuture<ClientResponse>[]) new CompletableFuture<?>[keys.getRowCount()];
        final int[] keyList = new int[keys.getRowCount()];

        // get a list of all keys and call the procedure for each
        keys.resetRowPosition();
        for (int i = 0; keys.advanceRow(); i++) {
            keyList[i] = (int) keys.getLong(1);
            args[0] = keyList[i];
            futureList[i] = callProcedure(procedureName, args);
        }

        // create the block to handle the procedure responses
        Function<Void, ClientResponseWithPartitionKey[]> processResponses = v -> {
            final ClientResponseWithPartitionKey[] crs = new ClientResponseWithPartitionKey[futureList.length];
            for (int j = 0; j < futureList.length; ++j) {
                ClientResponse cr2 = null;
                try {
                    cr2 = futureList[j].get();
                } catch (InterruptedException | ExecutionException e) {
                    cr2 = new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE, new VoltTable[0], e.toString());
                }

                crs[j] = new ClientResponseWithPartitionKey(keyList[j], cr2);
            }
            return crs;
        };

        // make a meta-future that waits on all individual responses
        CompletableFuture<Void> gotAllResponsesCF = CompletableFuture.allOf(futureList);

        // block until all responses have been received then call the block to process them
        return gotAllResponsesCF.thenApply(processResponses).exceptionally(t -> {
            // exception handling code should be called very rarely

            // this checks for really bad things and tries not to swallow them
            // it's probably overkill here because we're not directly calling user code
            if (CoreUtils.isStoredProcThrowableFatalToServer(t)) {
                throw (Error) t;
            }

            // get an appropriate error response
            ClientResponse cr = ProcedureRunner.getErrorResponse(
                    m_procedureName, true, 0, m_appStatusCode, m_appStatusString,
                    null, t);
            // wrap this with the right type for the response
            ClientResponseWithPartitionKey crwpk = new ClientResponseWithPartitionKey(0, cr);
            return new ClientResponseWithPartitionKey[] { crwpk };
        });
    }

    /**
     * Send an invocation directly to each host's CI mailbox.
     * This ONLY works for NT procedures.
     * Track responses and complete the returned future when they're all accounted for.
     */
    protected CompletableFuture<Map<Integer,ClientResponse>> callAllNodeNTProcedure(String procName, Object... params) {
        // only one of these at a time
        if (!m_outstandingAllHostProc.compareAndSet(false, true)) {
            throw new VoltAbortException(new IllegalStateException("Only one AllNodeNTProcedure operation can be running at a time."));
        }

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName(procName);
        invocation.setParams(params);
        invocation.setClientHandle(m_id);

        final Iv2InitiateTaskMessage workRequest = new Iv2InitiateTaskMessage(
                m_mailbox.getHSId(), m_mailbox.getHSId(), TransactionInfoBaseMessage.UNUSED_TRUNC_HANDLE,
                m_id, m_id, true, false, false, invocation, m_id, ClientInterface.NT_REMOTE_PROC_CID,
                false);

        m_allHostFut = new CompletableFuture<>();
        m_allHostResponses = new HashMap<>();

        // hold this lock while getting the count of live nodes
        // also held when
        Set<Long> hsids;
        synchronized(m_allHostCallbackLock) {
            // collect the set of live client interface mailbox ids
            hsids = VoltZK.getMailBoxesForNT(VoltDB.instance().getHostMessenger().getZK());
            m_outstandingAllHostProcedureHostIds =
                    hsids.stream().map(hsid->CoreUtils.getHostIdFromHSId(hsid)).collect(Collectors.toSet());
        }

        // send the invocation to all live nodes
        // n.b. can't combine this step with above because sometimes the callbacks comeback so fast
        //  you get a concurrent modification exception
        for (long hsid : hsids) {
            m_mailbox.send(hsid, workRequest);
        }

        return m_allHostFut;
    }

    /**
     * Synchronous call to NT procedure run(..) method.
     * We are executing in the context of a task running
     * from an executor service owned by the NTProcedureService.
     *
     * Wraps coreCall with statistics.
     *
     * @return True if done and false if there is an
     * async task still running.
     */
    protected boolean call(Object... paramListIn) {
        m_perCallStats = m_statsCollector.beginProcedure();

        // if we're keeping track, calculate parameter size
        if (m_perCallStats.samplingProcedure()) {
            ParameterSet params = ParameterSet.fromArrayNoCopy(paramListIn);
            m_perCallStats.setParameterSize(params.getSerializedSize());
        }

        ClientResponseImpl response = coreCall(paramListIn);

        // null response means this procedure isn't over and has some async component
        if (response == null) {
            return false;
        }

        // if the whole call is done (no async bits)

        // if we're keeping track, calculate result size
        if (m_perCallStats.samplingProcedure()) {
            m_perCallStats.setResultSize(response.getResults());
        }
        m_statsCollector.endProcedure(response.aborted(), response.failed(), m_perCallStats);

        // allow the GC to collect per-call stats if this proc isn't called for a while
        m_perCallStats = null;

        // send the response to caller
        // must be done as IRM to CI mailbox for backpressure accounting
        response.setClientHandle(m_clientHandle);
        InitiateResponseMessage irm = InitiateResponseMessage.messageForNTProcResponse(
                m_ciHandle, m_ccxn.connectionId(), response);
        m_mailbox.deliver(irm);

        // remove record of this procedure in NTPS
        // only done if procedure is really done
        m_ntProcService.handleNTProcEnd(this);
        return true;
    }

    /**
     * Send a response back to the proc caller. Refactored out of coreCall for both
     * regular and exceptional paths.
     *
     * TODO - why doesn't above call() method use this?
     */
    private void completeCall(ClientResponseImpl response) {

        // if we're keeping track, calculate result size
        if (m_perCallStats.samplingProcedure()) {
            m_perCallStats.setResultSize(response.getResults());
        }
        m_statsCollector.endProcedure(response.aborted(), response.failed(), m_perCallStats);

        // allow the GC to collect per-call stats if this proc isn't called for a while
        m_perCallStats = null;
        respond(response);
    }

    private void respond(ClientResponseImpl response) {
        // send the response to the caller
        // must be done as IRM to CI mailbox for backpressure accounting
        response.setClientHandle(m_clientHandle);
        InitiateResponseMessage irm = InitiateResponseMessage.messageForNTProcResponse(
                m_ciHandle, m_ccxn.connectionId(), response);

        m_mailbox.deliver(irm);
        m_ntProcService.handleNTProcEnd(ProcedureRunnerNT.this);
    }

    /**
     * Send a timeout response and end the procedure call. Leave the stats around
     * to handle a possible late completion.
     *
     * @param tos   timeout, in microseconds
     */
    public void timeoutCall(int tos) {
        String statusString = String.format("Procedure %s timed out after taking more than %d milliseconds",
                m_procedureName, tos / 1000);
        LOG.warn(statusString);
        respond(new ClientResponseImpl(ClientResponse.COMPOUND_PROC_TIMEOUT, ClientResponse.COMPOUND_PROC_TIMEOUT,
                statusString, new VoltTable[0], statusString));
    }

    /**
     * Core synchronous call to NT procedure run(..) method.
     *
     * @return ClientResponseImpl non-null if done and null
     *         if there is an async task still running.
     */
    private ClientResponseImpl coreCall(Object... paramListIn) {
        final VoltTable[] results;

        // use local var to avoid warnings about reassigning method argument
        Object[] paramList = paramListIn;

        try {
            if ((m_paramTypes.length > 0) && (m_paramTypes[0] == ParameterSet.class)) {
                assert(m_paramTypes.length == 1);
                paramList = new Object[] { ParameterSet.fromArrayNoCopy(paramListIn) };
            }

            if (paramList.length != m_paramTypes.length) {
                String msg = "PROCEDURE " + m_procedureName + " EXPECTS " + m_paramTypes.length +
                    " PARAMS, BUT RECEIVED " + paramList.length;
                m_statusCode = ClientResponse.GRACEFUL_FAILURE;
                return ProcedureRunner.getErrorResponse(m_statusCode, m_appStatusCode, m_appStatusString, msg, null);
            }

            for (int i = 0; i < m_paramTypes.length; i++) {
                try {
                    paramList[i] = ParameterConverter.tryToMakeCompatible(m_paramTypes[i], paramList[i]);
                    // check the result type in an assert
                    assert(ParameterConverter.verifyParameterConversion(paramList[i], m_paramTypes[i]));
                } catch (Exception e) {
                    String msg = "PROCEDURE " + m_procedureName + " TYPE ERROR FOR PARAMETER " + i + ": " + e.toString();
                    m_statusCode = ClientResponse.GRACEFUL_FAILURE;
                    return ProcedureRunner.getErrorResponse(m_statusCode, m_appStatusCode, m_appStatusString, msg, null);
                }
            }

            try {
                m_procedure.m_runner = this;
                Object rawResult = invokeRunMethod(paramList);

                if (rawResult instanceof CompletableFuture<?>) {
                    final CompletableFuture<?> fut = (CompletableFuture<?>) rawResult;

                    fut.thenRun(() -> {
                        //
                        // Happy path. No exceptions thrown. Procedure work is complete.
                        //
                        Object innerRawResult = null;
                        ClientResponseImpl response = null;
                        try {
                            innerRawResult = fut.get();
                        } catch (InterruptedException | ExecutionException e) {
                            assert(false);
                            // this is a bad place to be, but it's hard to know if it's crash bad...
                            innerRawResult = new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                                    new VoltTable[0],
                                    "Future returned from NTProc " + m_procedureName + " failed to complete.",
                                    m_clientHandle);
                        }

                        if (innerRawResult instanceof ClientResponseImpl) {
                            response = (ClientResponseImpl) innerRawResult;
                        } else {
                            try {
                                VoltTable[] r = ParameterConverter.getResultsFromRawResults(m_procedureName, innerRawResult);
                                response = responseFromTableArray(r);
                            } catch (Exception e) {
                                // this is a bad place to be, but it's hard to know if it's crash bad...
                                response = new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE, // TODO: this status is unsuitable
                                        new VoltTable[0],
                                        "Type " + innerRawResult.getClass().getName() +
                                            " returned from NTProc \"" + m_procedureName +
                                            "\" was not an acceptible VoltDB return type.",
                                        m_clientHandle);
                            }
                        }
                        completeCall(response);
                    }).exceptionally(e -> {
                        //
                        // Exception path. Some bit of async work threw something.
                        //
                        if ((e instanceof ExecutionException || e instanceof CompletionException) &&
                            e.getCause() != null) {
                            e = e.getCause();
                        }

                        SerializableException se = null;
                        if (e instanceof SerializableException) {
                            se = (SerializableException) e;
                        }

                        String msg = "PROCEDURE " + m_procedureName + " THREW EXCEPTION: ";
                        if (se != null) {
                            msg += se.getMessage();
                        } else {
                            msg += e.toString();
                        }
                        m_statusCode = ClientResponse.GRACEFUL_FAILURE; // TODO: this seems like an overly-optimistic claim
                        completeCall(ProcedureRunner.getErrorResponse(m_statusCode, m_appStatusCode, m_appStatusString, msg, se));
                        return null;
                    });

                    return null;
                }
                results = ParameterConverter.getResultsFromRawResults(m_procedureName, rawResult);
            } catch (IllegalAccessException e) { // If reflection fails, invoke the same error handling that other exceptions do
                throw new InvocationTargetException(e);
            }
        } catch (InvocationTargetException itex) {
            //itex.printStackTrace();
            Throwable ex = itex.getCause();
            if (CoreUtils.isStoredProcThrowableFatalToServer(ex)) {
                // If the stored procedure attempted to do something other than linklibrary or instantiate
                // a missing object that results in an error, throw the error and let the server deal with
                // the condition as best as it can (usually a crashLocalVoltDB).
                throw (Error)ex;
            } else {
                return ProcedureRunner.getErrorResponse(
                        m_procedureName, true, 0, m_appStatusCode, m_appStatusString,
                        null, ex);
            }
        }
        return responseFromTableArray(results);
    }

    protected Object invokeRunMethod(Object[] paramList)
            throws IllegalAccessException, InvocationTargetException {
        return m_procMethod.invoke(m_procedure, paramList);
    }

    private ClientResponseImpl responseFromTableArray(VoltTable[] results) {
        // don't leave empty handed
        if (results == null) {
            results = new VoltTable[0];
        } else if (results.length > Short.MAX_VALUE) {
            String statusString = "Stored procedure returns too much data. Exceeded maximum number of VoltTables: " + Short.MAX_VALUE;
            return new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE, ClientResponse.GRACEFUL_FAILURE, statusString, new VoltTable[0], // TODO: status
                    statusString);
        }
        return new ClientResponseImpl(
                    m_statusCode, m_appStatusCode, m_appStatusString, results, m_statusString);
    }

    /**
     * For all-host NT procedures, use site failures to call callbacks for hosts
     * that will obviously never respond.
     *
     * ICH and the other plumbing should handle regular, txn procs.
     */
    public void processAnyCallbacksFromFailedHosts(Set<Integer> failedHosts) {
        if (m_outstandingAllHostProcedureHostIds != null) {
            synchronized (m_allHostCallbackLock) {
                failedHosts.forEach(i -> {
                    if (m_outstandingAllHostProcedureHostIds.contains(i)) {
                        ClientResponseImpl cri = new ClientResponseImpl(
                                ClientResponse.CONNECTION_LOST,
                                new VoltTable[0],
                                "Host " + i + " failed, connection lost");
                        // embed the hostid as a string in app status string
                        // because the recipient expects this hack
                        cri.setAppStatusString(String.valueOf(i));

                        allHostNTProcedureCallback(cri);
                    }
                });
            }
        }
    }

    /*
     * Cluster id is immutable and is persisted across snapshot/recover events
     */
    public int getClusterId() {
        return VoltDB.instance().getCatalogContext().cluster.getDrclusterid();
    }

    public void setAppStatusCode(byte statusCode) {
        m_appStatusCode = statusCode;
    }

    public void setAppStatusString(String statusString) {
        m_appStatusString = statusString;
    }

    int getTimeout() {
        return 0;
    }

    public String getProcedureName() {
        return m_procedureName;
    }

    // BELOW TO SUPPORT NT SYSPROCS

    protected String getHostname() {
        return m_ccxn.getHostnameOrIP(m_clientHandle);
    }

    protected boolean isAdminConnection() {
        return m_isAdmin;
    }

    protected long getClientHandle() {
        return m_clientHandle;
    }

    protected String getUsername() {
        return m_user.m_name;
    }

    protected boolean isRestoring() {
        return m_ntProcService.isRestoring;
    }

    protected void noteRestoreCompleted() {
        m_ntProcService.isRestoring = false;
    }

    public String getConnectionIPAndPort() {
        return m_ccxn.getHostnameAndIPAndPort();
    }

    public InetSocketAddress getRemoteAddress() {
        return m_ccxn.getRemoteSocketAddress();
    }

    public boolean isUserAuthEnabled() {
        return m_user.isAuthEnabled();
    }

    @Override
    public String toString() {
        return String.format("id:%d proc:%s sts:%d", m_id, m_procedureName, m_statusCode);
    }
}
