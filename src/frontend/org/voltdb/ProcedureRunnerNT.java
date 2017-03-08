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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.SpecifiedException;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class ProcedureRunnerNT {

    protected final ExecutorService m_executorService;
    protected final NTProcedureService m_procSet;
    protected final Mailbox m_mailbox;

    protected final long m_id;
    protected final AtomicBoolean m_outstandingAllHostProc = new AtomicBoolean(false);

    protected final AuthUser m_user;
    protected final Connection m_ccxn;
    protected final long m_clientHandle;

    protected final String m_procedureName;
    protected final VoltNTProcedure m_procedure;
    protected final Method m_procMethod;
    protected final Class<?>[] m_paramTypes;

    protected byte m_statusCode = ClientResponse.SUCCESS;
    protected String m_statusString = null;
    // Status code that can be set by stored procedure upon invocation that will be returned with the response.
    protected byte m_appStatusCode = ClientResponse.UNINITIALIZED_APP_STATUS_CODE;
    protected String m_appStatusString = null;

    ProcedureRunnerNT(long id,
                      AuthUser user,
                      Connection ccxn,
                      long clientHandle,
                      VoltNTProcedure procedure,
                      String procName,
                      Method procMethod,
                      Class<?>[] paramTypes,
                      ExecutorService executorService,
                      NTProcedureService procSet,
                      Mailbox mailbox)
    {
        m_id = id;
        m_user = user;
        m_ccxn = ccxn;
        m_clientHandle = clientHandle;
        m_procedure = procedure;
        m_procedureName = procName;
        m_procMethod = procMethod;
        m_paramTypes = paramTypes;
        m_executorService = executorService;
        m_procSet = procSet;
        m_mailbox = mailbox;
    }

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

    public synchronized void allHostNTProcedureCallback(ClientResponse clientResponse) {
        int hostId = Integer.parseInt(clientResponse.getAppStatusString());
        boolean removed = m_outstandingAllHostProcedureHostIds.remove(hostId);
        assert(removed); // just while developing -- should handle this

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

    protected CompletableFuture<ClientResponse> callProcedure(String procName, Object... params) {
        MyProcedureCallback cb = new MyProcedureCallback();
        boolean success = m_procSet.m_ich.callProcedure(m_user, false, 1000 * 120, cb, procName, params);
        assert(success);
        return cb.fut;
    }

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

        liveHostIds.stream()
                .map(hostId -> CoreUtils.getHSIdFromHostAndSite(hostId, HostMessenger.CLIENT_INTERFACE_SITE_ID))
                .forEach(hsid -> {
                    m_mailbox.send(hsid, workRequest);
                });

        return m_allHostFut;
    }

    protected ClientResponseImpl call(Object... paramListIn) {

        VoltTable[] results = null;

        // use local var to avoid warnings about reassigning method argument
        Object[] paramList = paramListIn;

        try {
            if (paramList.length != m_paramTypes.length) {
                String msg = "PROCEDURE " + m_procedureName + " EXPECTS " + String.valueOf(m_paramTypes.length) +
                    " PARAMS, BUT RECEIVED " + String.valueOf(paramList.length);
                m_statusCode = ClientResponse.GRACEFUL_FAILURE;
                return getErrorResponse(m_statusCode, msg, null);
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
                    return getErrorResponse(m_statusCode, msg, null);
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
                                    results = ParameterConverter.getResultsFromRawResults(rawResult);
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

                            // send the response to the caller
                            response.setClientHandle(m_clientHandle);
                            ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
                            buf.putInt(buf.capacity() - 4);
                            response.flattenToBuffer(buf).flip();
                            m_ccxn.writeStream().enqueue(buf);

                            m_procSet.handleNTProcEnd(ProcedureRunnerNT.this);
                        }
                    });

                    return null;
                }
                results = ParameterConverter.getResultsFromRawResults(rawResult);
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
            return getErrorResponse(ex);
        }

        return responseFromTableArray(results);
    }

    public ClientResponseImpl responseFromTableArray(VoltTable[] results) {
        // don't leave empty handed
        if (results == null) {
            results = new VoltTable[0];
        }
        else if (results.length > Short.MAX_VALUE) {
            String statusString = "Stored procedure returns too much data. Exceeded  maximum number of VoltTables: " + Short.MAX_VALUE;
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
    *
    * @param e
    * @return A ClientResponse containing error information
    */
   protected ClientResponseImpl getErrorResponse(Throwable eIn) {
       // use local var to avoid warnings about reassigning method argument
       Throwable e = eIn;
       boolean expected_failure = true;
       boolean hideStackTrace = false;
       StackTraceElement[] stack = e.getStackTrace();
       ArrayList<StackTraceElement> matches = new ArrayList<StackTraceElement>();
       for (StackTraceElement ste : stack) {
           if (isProcedureStackTraceElement(ste)) {
               matches.add(ste);
           }
       }

       byte status = ClientResponse.UNEXPECTED_FAILURE;
       StringBuilder msg = new StringBuilder();

       if (e.getClass() == VoltAbortException.class) {
           status = ClientResponse.USER_ABORT;
           msg.append("USER ABORT\n");
       }
       else if (e.getClass() == org.voltdb.exceptions.ConstraintFailureException.class) {
           status = ClientResponse.GRACEFUL_FAILURE;
           msg.append("CONSTRAINT VIOLATION\n");
       }
       else if (e.getClass() == org.voltdb.exceptions.SQLException.class) {
           status = ClientResponse.GRACEFUL_FAILURE;
           msg.append("SQL ERROR\n");
       }
       // Interrupt exception will be thrown when the procedure is killed by a user
       // or by a timeout in the middle of executing.
       else if (e.getClass() == org.voltdb.exceptions.InterruptException.class) {
           status = ClientResponse.GRACEFUL_FAILURE;
           msg.append("Transaction Interrupted\n");
       }
       else if (e.getClass() == org.voltdb.ExpectedProcedureException.class) {
           String backendType = "HSQL";
           msg.append(backendType);
           msg.append("-BACKEND ERROR\n");
           if (e.getCause() != null) {
               e = e.getCause();
           }
       }
       else if (e.getClass() == org.voltdb.exceptions.TransactionRestartException.class) {
           status = ClientResponse.TXN_RESTART;
           msg.append("TRANSACTION RESTART\n");
       }
       // SpecifiedException means the dev wants control over status and message
       else if (e.getClass() == SpecifiedException.class) {
           SpecifiedException se = (SpecifiedException) e;
           status = se.getStatus();
           expected_failure = true;
           hideStackTrace = true;
       }
       else {
           msg.append("UNEXPECTED FAILURE:\n");
           expected_failure = false;
       }

       // ensure the message is returned if we're not going to hit the verbose condition below
       if (expected_failure || hideStackTrace) {
           msg.append("  ").append(e.getMessage());
       }

       // Rarely hide the stack trace.
       // Right now, just for SpecifiedException, which is usually from sysprocs where the error is totally
       // known and not helpful to the user.
       if (!hideStackTrace) {
           // If the error is something we know can happen as part of normal operation,
           // reduce the verbosity.
           // Otherwise, generate more output for debuggability
           if (expected_failure) {
               for (StackTraceElement ste : matches) {
                   msg.append("\n    at ");
                   msg.append(ste.getClassName()).append(".").append(ste.getMethodName());
                   msg.append("(").append(ste.getFileName()).append(":");
                   msg.append(ste.getLineNumber()).append(")");
               }
           }
           else {
               Writer result = new StringWriter();
               PrintWriter pw = new PrintWriter(result);
               e.printStackTrace(pw);
               msg.append("  ").append(result.toString());
           }
       }

       return getErrorResponse(
               status, msg.toString(),
               e instanceof SerializableException ? (SerializableException)e : null);
    }

    protected ClientResponseImpl getErrorResponse(byte status, String msg, SerializableException e) {
        return new ClientResponseImpl(
                status,
                m_appStatusCode,
                m_appStatusString,
                new VoltTable[0],
                "VOLTDB ERROR: " + msg);
    }

    /**
     * Test whether or not the given stack frame is within a procedure invocation
     * @param stel a stack trace element
     * @return true if it is, false it is not
     */
    protected boolean isProcedureStackTraceElement(StackTraceElement stel) {
        int lastPeriodPos = stel.getClassName().lastIndexOf('.');

        if (lastPeriodPos == -1) {
            lastPeriodPos = 0;
        } else {
            ++lastPeriodPos;
        }

        // Account for inner classes too. Inner classes names comprise of the parent
        // class path followed by a dollar sign
        String simpleName = stel.getClassName().substring(lastPeriodPos);
        return simpleName.equals(m_procedureName)
            || (simpleName.startsWith(m_procedureName) && simpleName.charAt(m_procedureName.length()) == '$');
    }

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
}
