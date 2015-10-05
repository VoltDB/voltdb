/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import static org.voltdb.ClientInterface.getPartitionForProcedure;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.network.Connection;
import org.voltcore.network.NIOReadStream;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.AsyncCompilerResult;
import org.voltdb.compiler.AsyncCompilerWork;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.CatalogChangeWork;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.MiscUtils;

/**
 * A very simple adapter for import handler that deserializes bytes into client responses.
 * For each partition it creates a single thread executor to sequence per partition transaction submission.
 * Responses are also written on a single thread executor to avoid bottlenecking on callback doing heavy work.
 * It calls crashLocalVoltDB() if the deserialization fails, which should only happen if there's a bug.
 */
public class InternalClientResponseAdapter implements Connection, WriteStream {
    private static final VoltLogger m_logger = new VoltLogger("IMPORT");
    public static final long MAX_PENDING_TRANSACTIONS_PER_PARTITION = Integer.getInteger("INTERNAL_MAX_PENDING_TRANSACTION_PER_PARTITION", 500);

    public interface Callback {
        public void handleResponse(ClientResponse response) throws Exception;
        public String getProcedureName();
        public int getPartitionId();
        public InternalConnectionContext getInternalContext();
    }

    private final long m_connectionId;
    private final AtomicLong m_handles = new AtomicLong();
    private final AtomicLong m_failures = new AtomicLong(0);
    private final Map<Long, InternalCallback> m_callbacks = Collections.synchronizedMap(new HashMap<Long, InternalCallback>());
    private final ConcurrentMap<Integer, ExecutorService> m_partitionExecutor = new ConcurrentHashMap<>();
    // Maintain internal connection ids per caller id. This is useful when collecting statistics
    // so that information can be grouped per user of this Connection.
    private final ConcurrentMap<String, Long> m_internalConnectionIds = new ConcurrentHashMap<>();

    private InternalConnectionContext m_context;
    private ProcedureCallback m_proccb;

    private class InternalCallback implements Callback {

        private final ProcedureCallback m_cb;
        private final int m_partition;
        private final InternalConnectionContext m_context;
        private final StoredProcedureInvocation m_task;
        private final Procedure m_proc;

        public InternalCallback(final InternalConnectionContext context, Procedure proc, StoredProcedureInvocation task, String procName, int partition, ProcedureCallback cb, long id) {
            m_context = context;
            m_task = task;
            m_proc = proc;
            m_cb = cb;
            m_partition = partition;
        }

        @Override
        public void handleResponse(ClientResponse response) throws Exception {
            if (m_cb != null) {
                m_cb.clientCallback(response);
            }
            if (response.getStatus() == ClientResponse.RESPONSE_UNKNOWN) {
                //Handle failure of transaction due to node kill
                createTransaction(m_context, m_task.getProcName(), m_proc, m_cb, m_task, m_partition, System.nanoTime());
            }
        }

        @Override
        public String getProcedureName() {
            return m_task.getProcName();
        }

        @Override
        public int getPartitionId() {
            return m_partition;
        }

        @Override
        public InternalConnectionContext getInternalContext() {
            return m_context;
        }
    }

    public long getPendingCount() {
        return m_callbacks.size();
    }

    public boolean hasBackPressure() {
        // 500 default per partition.
        return (m_callbacks.size() > (m_partitionExecutor.size() * MAX_PENDING_TRANSACTIONS_PER_PARTITION));
    }

    public ClientInterface getClientInterface() {
        return VoltDB.instance().getClientInterface();
    }

    private final  AsyncCompilerWork.AsyncCompilerWorkCompletionHandler m_adhocCompletionHandler = new AsyncCompilerWork.AsyncCompilerWorkCompletionHandler() {
        @Override
        public void onCompletion(AsyncCompilerResult result) {
            final Connection c = (Connection)result.clientData;
            if (result instanceof CatalogChangeResult) {
                final CatalogChangeResult changeResult = (CatalogChangeResult) result;

                // if the catalog change is a null change
                if (changeResult.encodedDiffCommands.trim().isEmpty()) {
                    ClientResponseImpl response =
                            new ClientResponseImpl(
                                    ClientResponseImpl.SUCCESS,
                                    new VoltTable[0], "Catalog update with no changes was skipped.",
                                    result.clientHandle);
                    ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
                    buf.putInt(buf.capacity() - 4);
                    response.flattenToBuffer(buf);
                    buf.flip();
                    c.writeStream().enqueue(buf);
                }
                else {
                    StoredProcedureInvocation task = getClientInterface().getUpdateCatalogExecutionTask(changeResult);

                    /*
                     * Round trip the invocation to initialize it for command logging
                     */
                    try {
                        task = MiscUtils.roundTripForCL(task);
                    } catch (IOException e) {
                        VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                    }

                    Procedure catProc = getClientInterface().getProcedureFromName(task.procName, VoltDB.instance().getCatalogContext());
                    int partition = -1;
                    try {
                        partition = getPartitionForProcedure(catProc, task);
                    } catch (Exception e) {
                        String fmt = "Can not invoke procedure %s from streaming interface %s. Partition not found.";
                        m_logger.rateLimitedLog(InternalConnectionHandler.SUPPRESS_INTERVAL, Level.ERROR, e, fmt, task.procName, m_context);
                        return;
                    }

                    // initiate the transaction. These hard-coded values from catalog
                    // procedure are horrible, horrible, horrible.
                    createTransaction(m_context, task.procName, catProc, m_proccb, task, partition, System.nanoTime());
                }
            } else {
                throw new RuntimeException(
                        "Should not be able to get here (ClientInterface.checkForFinishedCompilerWork())");
            }
        }
    };

    public boolean dispatchUpdateApplicationCatalog(StoredProcedureInvocation task, AuthSystem.AuthUser user, InternalConnectionContext context,
            ProcedureCallback proccb) {
        m_context = context;
        m_proccb = proccb;
        Object[] params = task.getParams().toArray();
        // default catalogBytes to null, when passed along, will tell the
        // catalog change planner that we want to use the current catalog.
        byte[] catalogBytes = null;
        Object catalogObj = params[0];
        if (catalogObj != null) {
            if (catalogObj instanceof String) {
                // treat an empty string as no catalog provided
                String catalogString = (String) catalogObj;
                if (!catalogString.isEmpty()) {
                    catalogBytes = Encoder.hexDecode(catalogString);
                }
            } else if (catalogObj instanceof byte[]) {
                // treat an empty array as no catalog provided
                byte[] catalogArr = (byte[]) catalogObj;
                if (catalogArr.length != 0) {
                    catalogBytes = catalogArr;
                }
            }
        }
        String deploymentString = (String) params[1];
        LocalObjectMessage work = new LocalObjectMessage(
                new CatalogChangeWork(
                    getClientInterface().m_siteId,
                    task.clientHandle, connectionId(), this.getHostnameAndIPAndPort(),
                    false, this, catalogBytes, deploymentString,
                    task.procName, task.type, task.originalTxnId, task.originalUniqueId,
                    VoltDB.instance().getReplicationRole() == ReplicationRole.REPLICA,
                    false, m_adhocCompletionHandler, user));

        getClientInterface().m_mailbox.send(getClientInterface().m_plannerSiteId, work);
        return true;
    }

    public boolean createTransaction(final InternalConnectionContext context,
            final String procName,
            final Procedure catProc,
            final ProcedureCallback proccb,
            final StoredProcedureInvocation task,
            final int partition, final long nowNanos) {

        if (!m_partitionExecutor.containsKey(partition)) {
            m_partitionExecutor.putIfAbsent(partition, CoreUtils.getSingleThreadExecutor("InternalHandlerExecutor - " + partition));
        }

        ExecutorService executor = m_partitionExecutor.get(partition);
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    context.setBackPressure(hasBackPressure());
                    if (!m_internalConnectionIds.containsKey(context.getName())) {
                        m_internalConnectionIds.putIfAbsent(context.getName(), VoltProtocolHandler.getNextConnectionId());
                    }
                    submitTransaction();
                }
                public boolean submitTransaction() {
                    final long handle = nextHandle();
                    task.setClientHandle(handle);
                    final InternalCallback cb = new InternalCallback(context, catProc, task, procName, partition, proccb, handle);
                    m_callbacks.put(handle, cb);

                    //Submit the transaction.
                    boolean bval = getClientInterface().createTransaction(connectionId(), task,
                            catProc.getReadonly(), catProc.getSinglepartition(), catProc.getEverysite(), partition,
                            task.getSerializedSize(), nowNanos);
                    if (!bval) {
                        // Supposedly this will never happen and is OK to ignore from stats collection perspective.
                        // Hence it is OK that this is not getting reported to callbacks.
                        m_logger.error("Failed to submit transaction.");
                        m_callbacks.remove(handle);
                    }
                    return bval;
                }
            });
        } catch (RejectedExecutionException ex) {
            m_logger.error("Failed to submit transaction to the partition queue.", ex);
            return false;
        }

        return true;
    }

    /**
     * @param connectionId    The connection ID for this adapter, needs to be unique for this
     *                        node.
     * @param name            Human readable name identifying the adapter, will stand in for hostname
     */
    public InternalClientResponseAdapter(long connectionId, String name) {
        m_connectionId = connectionId;
    }

    public long nextHandle() {
        return m_handles.incrementAndGet();
    }

    @Override
    public void queueTask(Runnable r) {
        // Called when node failure happens
        r.run();
    }

    @Override
    public void fastEnqueue(DeferredSerialization ds) {
        enqueue(ds);
    }

    @Override
    public void enqueue(DeferredSerialization ds) {
        try {
            ByteBuffer buf = null;
            synchronized(this) {
                final int serializedSize = ds.getSerializedSize();
                if (serializedSize <= 0) {
                    //Bad ignored transacton.
                    return;
                }
                buf = ByteBuffer.allocate(serializedSize);
                ds.serialize(buf);
            }
            enqueue(buf);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB("enqueue() in InternalClientResponseAdapter throw an exception", true, e);
        }
    }

    @Override
    public void enqueue(final ByteBuffer b) {

        final ClientResponseImpl resp = new ClientResponseImpl();
        b.position(4);
        try {
            resp.initFromBuffer(b);
        } catch (IOException ex) {
            VoltDB.crashLocalVoltDB("enqueue() in InternalClientResponseAdapter throw an exception", true, ex);
        }
        final Callback callback = m_callbacks.get(resp.getClientHandle());
        if (!m_partitionExecutor.containsKey(callback.getPartitionId())) {
            m_logger.error("Invalid partition response recieved for sending internal client response.");
            return;
        }
        ExecutorService executor = m_partitionExecutor.get(callback.getPartitionId());
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    callback.getInternalContext().setBackPressure(hasBackPressure());
                    handle();
                }

                public void handle() {
                    try {
                        if (resp.getStatus() != ClientResponse.SUCCESS) {
                            String fmt = "InternalClientResponseAdapter stored procedure failed: %s Error: %s failures: %d";
                            rateLimitedLog(Level.WARN, null, fmt, callback.getProcedureName(), resp.getStatusString(), m_failures.incrementAndGet());
                        }
                        callback.handleResponse(resp);
                    } catch (Exception ex) {
                        m_logger.error("Failed to process callback.", ex);
                    } finally {
                        m_callbacks.remove(resp.getClientHandle());
                    }
                }
            });
        } catch (RejectedExecutionException ex) {
            m_logger.error("Failed to submit callback to the response processing queue.", ex);
        }
    }

    @Override
    public void enqueue(ByteBuffer[] b)
    {
        if (b.length == 1) {
            enqueue(b[0]);
        } else {
            throw new UnsupportedOperationException("Buffer chains not supported in internal invocation adapter");
        }
    }

    //Do rate limited logging for messages.
    private void rateLimitedLog(Level level, Throwable cause, String format, Object...args) {
        RateLimitedLogger.tryLogForMessage(
                EstTime.currentTimeMillis(),
                ImportHandler.SUPPRESS_INTERVAL, TimeUnit.SECONDS,
                m_logger, level,
                cause, format, args
                );
    }

    @Override
    public int calculatePendingWriteDelta(long now) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hadBackPressure() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getOutstandingMessageCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WriteStream writeStream() {
        return this;
    }

    @Override
    public NIOReadStream readStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void disableReadSelection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enableReadSelection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getHostnameAndIPAndPort() {
        return "InternalAdapter";
    }

    @Override
    public String getHostnameOrIP() {
        return "InternalAdapter";
    }

    @Override
    public String getHostnameOrIP(long clientHandle) {
        InternalCallback callback = m_callbacks.get(clientHandle);
        if (callback==null) {
            m_logger.rateLimitedLog(ImportHandler.SUPPRESS_INTERVAL, Level.WARN, null,
                    "Could not find caller details for client handle %d. Using internal adapter name", clientHandle);
            return getHostnameOrIP();
        } else {
            return callback.getInternalContext().getName();
        }
    }

    @Override
    public int getRemotePort() {
        return -1;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return null;
    }

    @Override
    public long connectionId() {
        return m_connectionId;
    }

    @Override
    public long connectionId(long clientHandle) {
        InternalCallback callback = m_callbacks.get(clientHandle);
        if (callback==null) {
            m_logger.rateLimitedLog(ImportHandler.SUPPRESS_INTERVAL, Level.WARN, null,
                    "Could not find caller details for client handle %d. Using internal adapter level connection id", clientHandle);
            return connectionId();
        }

        Long internalId = m_internalConnectionIds.get(callback.getInternalContext().getName());
        if (internalId==null) {
            m_logger.rateLimitedLog(ImportHandler.SUPPRESS_INTERVAL, Level.WARN, null,
                "Could not find internal connection id for client handle %d. Using internal adapter level connection id", clientHandle);
            return connectionId();
        } else {
            return internalId;
        }
    }

    @Override
    public Future<?> unregister() {
        return null;
    }
}
