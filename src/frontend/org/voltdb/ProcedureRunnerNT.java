/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
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

    private static final VoltLogger tmLog = new VoltLogger("TM");

    // this is priority service for follow up work
    protected final ExecutorService m_executorService;
    protected final NTProcedureService m_ntProcService;
    // client interface mailbox
    protected final Mailbox m_mailbox;
    // shared between all concurrent calls to the same procedure
    ProcedureStatsCollector m_statsCollector;
    // generated for each call
    StatementStats.SingleCallStatsToken m_perCallStats = null;

    // unique for each call
    protected final long m_id;
    // gate to only allow one all-host call at a time
    protected final AtomicBoolean m_outstandingAllHostProc = new AtomicBoolean(false);

    // regular call support stuff
    protected final AuthUser m_user;
    protected final Connection m_ccxn;
    protected final long m_ciHandle;
    protected final long m_clientHandle;
    protected final String m_procedureName;
    protected final VoltNonTransactionalProcedure m_procedure;
    protected final Method m_procMethod;
    protected final Class<?>[] m_paramTypes;
    protected byte m_statusCode = ClientResponse.SUCCESS;
    protected String m_statusString = null;
    protected byte m_appStatusCode = ClientResponse.UNINITIALIZED_APP_STATUS_CODE;
    protected String m_appStatusString = null;

    ProcedureRunnerNT(long id,
                      AuthUser user,
                      Connection ccxn,
                      long ciHandle,
                      long clientHandle,
                      VoltNonTransactionalProcedure procedure,
                      String procName,
                      Method procMethod,
                      Class<?>[] paramTypes,
                      ExecutorService executorService,
                      NTProcedureService procSet,
                      Mailbox mailbox,
                      ProcedureStatsCollector statsCollector)
    {
        m_id = id;
        m_user = user;
        m_ccxn = ccxn;
        m_ciHandle = ciHandle;
        m_clientHandle = clientHandle;
        m_procedure = procedure;
        m_procedureName = procName;
        m_procMethod = procMethod;
        m_paramTypes = paramTypes;
        m_executorService = executorService;
        m_ntProcService = procSet;
        m_mailbox = mailbox;
        m_statsCollector = statsCollector;
    }

    /**
     * Complete the future when we get a traditional ProcedureCallback.
     */
    class MyProcedureCallback implements ProcedureCallback {
        final CompletableFuture<ClientResponse> fut = new CompletableFuture<>();
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            // the future needs to be completed in the right executor service
            // so any follow on work will be in the right executor service
            m_executorService.submit(new Runnable() {
                @Override
                public void run() {
                    fut.complete(clientResponse);
                }
            });
        }
    }

    Object m_allHostCallbackLock = new Object();
    Set<Integer> m_outstandingAllHostProcedureHostIds;
    Map<Integer,ClientResponse> m_allHostResponses;
    CompletableFuture<Map<Integer,ClientResponse>> m_allHostFut;

    /**
     * This is called when an all-host proc responds from a particular node.
     * It completes the future when all of the
     *
     * It uses a dumb hack that the hostid is stored in the appStatusString.
     * Since this is just for sysprocs, VoltDB devs making sysprocs should know
     * that string app status doesn't work.
     */
    public synchronized void allHostNTProcedureCallback(ClientResponse clientResponse) {
        int hostId = Integer.parseInt(clientResponse.getAppStatusString());
        boolean removed = m_outstandingAllHostProcedureHostIds.remove(hostId);
        // log this for now... I don't expect it to ever happen, but will be interesting to see...
        if (!removed) {
            tmLog.error(String.format(
                      "ProcedureRunnerNT.allHostNTProcedureCallback for procedure %s received late or unexepected response from hostID %d.",
                      m_procedureName, hostId));
            return;
        }

        final Map<Integer,ClientResponse> allHostResponses = m_allHostResponses;

        m_allHostResponses.put(hostId, clientResponse);
        if (m_outstandingAllHostProcedureHostIds.size() == 0) {
            m_outstandingAllHostProc.set(false);
            // the future needs to be completed in the right executor service
            // so any follow on work will be in the right executor service
            m_executorService.submit(new Runnable() {
                @Override
                public void run() {
                    m_allHostFut.complete(allHostResponses);
                }
            });
        }
    }

    /**
     * Call a procedure (either txn or NT) and complete the returned future when done.
     */
    protected CompletableFuture<ClientResponse> callProcedure(String procName, Object... params) {
        MyProcedureCallback cb = new MyProcedureCallback();
        boolean success = m_ntProcService.m_ich.callProcedure(m_user, false, 1000 * 120, cb, true, null, procName, params);
        assert(success);
        return cb.fut;
    }

    /**
     * Send an invocation directly to each host's CI mailbox.
     * This ONLY works for NT procedures.
     * Track responses and complete the returned future when they're all accounted for.
     */
    protected CompletableFuture<Map<Integer,ClientResponse>> callAllNodeNTProcedure(String procName, Object... params) {
        // only one of these at a time
        if (m_outstandingAllHostProc.get()) {
            throw new VoltAbortException(new IllegalStateException("Only one AllNodeNTProcedure operation can be running at a time."));
        }
        m_outstandingAllHostProc.set(true);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName(procName);
        invocation.setParams(params);
        invocation.setClientHandle(m_id);

        final Iv2InitiateTaskMessage workRequest =
                new Iv2InitiateTaskMessage(m_mailbox.getHSId(),
                                           m_mailbox.getHSId(),
                                           Iv2InitiateTaskMessage.UNUSED_TRUNC_HANDLE,
                                           m_id,
                                           m_id,
                                           true,
                                           false,
                                           invocation,
                                           m_id,
                                           ClientInterface.NT_REMOTE_PROC_CID,
                                           false);

        m_allHostFut = new CompletableFuture<>();
        m_allHostResponses = new HashMap<>();

        Set<Integer> liveHostIds = null;

        // hold this lock while getting the count of live nodes
        // also held when
        synchronized(m_allHostCallbackLock) {
            // collect the set of live client interface mailbox ids
            liveHostIds = VoltDB.instance().getHostMessenger().getLiveHostIds();
            m_outstandingAllHostProcedureHostIds = liveHostIds;
        }

        // send the invocation to all live nodes
        liveHostIds.stream()
                .map(hostId -> CoreUtils.getHSIdFromHostAndSite(hostId, HostMessenger.CLIENT_INTERFACE_SITE_ID))
                .forEach(hsid -> {
                    m_mailbox.send(hsid, workRequest);
                });

        return m_allHostFut;
    }

    /**
     * Synchronous call to NT procedure run(..) method.
     *
     * Wraps coreCall with statistics.
     *
     * @return True if done and null if there is an
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
        m_statsCollector.endProcedure(response.getStatus() == ClientResponse.USER_ABORT,
                                      (response.getStatus() != ClientResponse.USER_ABORT) &&
                                      (response.getStatus() != ClientResponse.SUCCESS),
                                      m_perCallStats);

        // send the response to caller
        // must be done as IRM to CI mailbox for backpressure accounting
        response.setClientHandle(m_clientHandle);
        InitiateResponseMessage irm = InitiateResponseMessage.messageForNTProcResponse(m_ciHandle,
                                                                                       m_ccxn.connectionId(),
                                                                                       response);
        m_mailbox.deliver(irm);

        // remove record of this procedure in NTPS
        // only done if procedure is really done
        m_ntProcService.handleNTProcEnd(this);

        return true;
    }

    /**
     * Core Synchronous call to NT procedure run(..) method.
     * @return ClientResponseImpl non-null if done and null if there is an
     * async task still running.
     */
    private ClientResponseImpl coreCall(Object... paramListIn) {
        VoltTable[] results = null;

        // use local var to avoid warnings about reassigning method argument
        Object[] paramList = paramListIn;

        try {
            if (paramList.length != m_paramTypes.length) {
                String msg = "PROCEDURE " + m_procedureName + " EXPECTS " + String.valueOf(m_paramTypes.length) +
                    " PARAMS, BUT RECEIVED " + String.valueOf(paramList.length);
                m_statusCode = ClientResponse.GRACEFUL_FAILURE;
                return ProcedureRunner.getErrorResponse(m_statusCode, m_appStatusCode, m_appStatusString, msg, null);
            }

            for (int i = 0; i < m_paramTypes.length; i++) {
                try {
                    paramList[i] = ParameterConverter.tryToMakeCompatible(m_paramTypes[i], paramList[i]);
                    // check the result type in an assert
                    assert(ParameterConverter.verifyParameterConversion(paramList[i], m_paramTypes[i]));
                } catch (Exception e) {
                    String msg = "PROCEDURE " + m_procedureName + " TYPE ERROR FOR PARAMETER " + i +
                            ": " + e.toString();
                    m_statusCode = ClientResponse.GRACEFUL_FAILURE;
                    return ProcedureRunner.getErrorResponse(m_statusCode, m_appStatusCode, m_appStatusString, msg, null);
                }
            }

            try {
                m_procedure.m_runner = this;
                Object rawResult = m_procMethod.invoke(m_procedure, paramList);
                if (rawResult instanceof CompletableFuture<?>) {
                    final CompletableFuture<?> fut = (CompletableFuture<?>) rawResult;
                    fut.thenRun(new Runnable() {
                        @Override
                        public void run() {
                            Object rawResult = null;
                            ClientResponseImpl response = null;
                            try {
                                rawResult = fut.get();
                            } catch (InterruptedException | ExecutionException e) {
                                // this is a bad place to be, but it's hard to know if it's crash bad...
                                rawResult = new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                                        new VoltTable[0],
                                        "Future returned from NTProc " + m_procedureName + " failed to complete.",
                                        m_clientHandle);
                            }

                            if (rawResult instanceof ClientResponseImpl) {
                                response = (ClientResponseImpl) rawResult;
                            }
                            else {
                                VoltTable[] results = null;
                                try {
                                    results = ParameterConverter.getResultsFromRawResults(m_procedureName, rawResult);
                                    response = responseFromTableArray(results);
                                } catch (Exception e) {
                                    // this is a bad place to be, but it's hard to know if it's crash bad...
                                    response = new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                                            new VoltTable[0],
                                            "Type " + rawResult.getClass().getName() +
                                                " returned from NTProc \"" + m_procedureName +
                                                "\" was not an acceptible VoltDB return type.",
                                            m_clientHandle);
                                }
                            }

                            // if we're keeping track, calculate result size
                            if (m_perCallStats.samplingProcedure()) {
                                m_perCallStats.setResultSize(response.getResults());
                            }
                            m_statsCollector.endProcedure(response.getStatus() == ClientResponse.USER_ABORT,
                                                          (response.getStatus() != ClientResponse.USER_ABORT) &&
                                                          (response.getStatus() != ClientResponse.SUCCESS),
                                                          m_perCallStats);

                            // send the response to the caller
                            // must be done as IRM to CI mailbox for backpressure accounting
                            response.setClientHandle(m_clientHandle);
                            InitiateResponseMessage irm = InitiateResponseMessage.messageForNTProcResponse(m_ciHandle,
                                                                                                           m_ccxn.connectionId(),
                                                                                                           response);
                            m_mailbox.deliver(irm);

                            m_ntProcService.handleNTProcEnd(ProcedureRunnerNT.this);
                        }
                    });

                    return null;
                }
                results = ParameterConverter.getResultsFromRawResults(m_procedureName, rawResult);
            }
            catch (IllegalAccessException e) {
                // If reflection fails, invoke the same error handling that other exceptions do
                throw new InvocationTargetException(e);
            }
        }
        catch (InvocationTargetException itex) {
            //itex.printStackTrace();
            Throwable ex = itex.getCause();
            if (CoreUtils.isStoredProcThrowableFatalToServer(ex)) {
                // If the stored procedure attempted to do something other than linklibrary or instantiate
                // a missing object that results in an error, throw the error and let the server deal with
                // the condition as best as it can (usually a crashLocalVoltDB).
                throw (Error)ex;
            }
            return ProcedureRunner.getErrorResponse(m_procedureName,
                                                    true,
                                                    0,
                                                    m_appStatusCode,
                                                    m_appStatusString,
                                                    null,
                                                    ex);
        }

        return responseFromTableArray(results);
    }

    ClientResponseImpl responseFromTableArray(VoltTable[] results) {
        // don't leave empty handed
        if (results == null) {
            results = new VoltTable[0];
        }
        else if (results.length > Short.MAX_VALUE) {
            String statusString = "Stored procedure returns too much data. Exceeded maximum number of VoltTables: " + Short.MAX_VALUE;
            return new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE,
                    ClientResponse.GRACEFUL_FAILURE,
                    statusString,
                    new VoltTable[0],
                    statusString);
        }

        return new ClientResponseImpl(
                    m_statusCode,
                    m_appStatusCode,
                    m_appStatusString,
                    results,
                    m_statusString);
    }

    /**
     * For all-host NT procs, use site failures to call callbacks for hosts
     * that will obviously never respond.
     *
     * ICH and the other plumbing should handle regular, txn procs.
     */
    public void processAnyCallbacksFromFailedHosts(Set<Integer> failedHosts) {
        synchronized(m_allHostCallbackLock) {
            failedHosts.stream()
                .forEach(i -> {
                    if (m_outstandingAllHostProcedureHostIds.contains(i)) {
                        ClientResponseImpl cri = new ClientResponseImpl(
                                ClientResponse.CONNECTION_LOST,
                                new VoltTable[0],
                                "");
                        // embed the hostid as a string in app status string
                        // because the recipient expects this hack
                        cri.setAppStatusString(String.valueOf(i));

                        allHostNTProcedureCallback(cri);
                    }
                });
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
}
