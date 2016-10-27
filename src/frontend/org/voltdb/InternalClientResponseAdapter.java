/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashMap;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
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

/**
 * A very simple adapter for import handler that deserializes bytes into client responses.
 * For each partition it creates a single thread executor to sequence per partition transaction submission.
 * Responses are also written on a single thread executor to avoid bottlenecking on callback doing heavy work.
 * It calls crashLocalVoltDB() if the deserialization fails, which should only happen if there's a bug.
 */
public class InternalClientResponseAdapter implements Connection, WriteStream {

    private static final VoltLogger m_logger = new VoltLogger("HOST");
    public final static long SUPPRESS_INTERVAL = 120;
    public static final long MAX_PENDING_TRANSACTIONS_PER_PARTITION =
            Integer.getInteger("INTERNAL_MAX_PENDING_TRANSACTION_PER_PARTITION", 500);

    public interface Callback {
        public void handleResponse(ClientResponse response) throws Exception;
        public String getProcedureName();
        public int getPartitionId();
        public InternalConnectionContext getInternalContext();
    }

    private final long m_connectionId;
    private final AtomicLong m_handles = new AtomicLong();
    private final AtomicLong m_failures = new AtomicLong(0);
    private final ConcurrentMap<Long, InternalCallback> m_callbacks = new ConcurrentHashMap<>(2048, .75f, 128);
    private final ConcurrentMap<Integer, ExecutorService> m_partitionExecutor = new NonBlockingHashMap<>();
    // Maintain internal connection ids per caller id. This is useful when collecting statistics
    // so that information can be grouped per user of this Connection.
    private final ConcurrentMap<String, Long> m_internalConnectionIds = new NonBlockingHashMap<>();

    private class InternalCallback implements Callback {

        private final ProcedureCallback m_cb;
        private final InternalConnectionStatsCollector m_statsCollector;
        private final int m_partition;
        private final InternalAdapterTaskAttributes m_kattrs;
        private final StoredProcedureInvocation m_task;
        private final Procedure m_proc;
        private final AuthSystem.AuthUser m_user;
        private final String m_procName;
        public InternalCallback(
                final InternalAdapterTaskAttributes kattrs,
                Procedure proc,
                StoredProcedureInvocation task,
                String procName,
                int partition,
                ProcedureCallback cb,
                InternalConnectionStatsCollector statsCollector,
                AuthSystem.AuthUser user,
                long id)
        {
            m_kattrs = kattrs;
            m_task = task;
            m_proc = proc;
            m_cb = cb;
            m_statsCollector = statsCollector;
            m_partition = partition;
            m_user = user;
            m_procName = procName;
        }

        @Override
        public void handleResponse(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS && m_kattrs.isImporter()) {
                String fmt = "Stored procedure failed: %s Error: %s failures: %d";
                rateLimitedLog(Level.WARN, null, fmt, m_procName, response.getStatusString(), m_failures.incrementAndGet());
            }
            if (m_cb != null) {
                m_cb.clientCallback(response);
            }

            if (m_statsCollector != null) {
                m_statsCollector.reportCompletion(m_kattrs.getName(), m_task.getProcName(), response);
            }

            if (response.getStatus() == ClientResponse.RESPONSE_UNKNOWN) {
                //Handle failure of transaction due to node kill
                createTransaction(
                        m_kattrs,
                        m_task.getProcName(),
                        m_proc, m_cb,
                        m_statsCollector,
                        m_task,
                        m_user,
                        m_partition,
                        System.nanoTime());
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
            return m_kattrs;
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

    public boolean createTransaction(final InternalAdapterTaskAttributes kattrs,
            final String procName,
            final Procedure catProc,
            final ProcedureCallback proccb,
            final InternalConnectionStatsCollector statsCollector,
            final StoredProcedureInvocation task,
            final AuthSystem.AuthUser user,
            final int partition, final long nowNanos) {

        if (!m_partitionExecutor.containsKey(partition)) {
            m_partitionExecutor.putIfAbsent(partition, CoreUtils.getSingleThreadExecutor("InternalHandlerExecutor - " + partition));
        }

        final InvocationDispatcher dispatcher = getClientInterface().getDispatcher();

        ExecutorService executor = m_partitionExecutor.get(partition);
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    kattrs.setBackPressure(hasBackPressure());
                    if (!m_internalConnectionIds.containsKey(kattrs.getName())) {
                        m_internalConnectionIds.putIfAbsent(kattrs.getName(), VoltProtocolHandler.getNextConnectionId());
                    }
                    submitTransaction();
                }
                public boolean submitTransaction() {
                    final long handle = nextHandle();
                    task.setClientHandle(handle);
                    final InternalCallback cb = new InternalCallback(
                            kattrs, catProc, task, procName, partition, proccb, statsCollector, user, handle);
                    m_callbacks.put(handle, cb);

                    ClientResponseImpl r = dispatcher.dispatch(task, kattrs, InternalClientResponseAdapter.this, user, null);
                    boolean bval = r == null || r.getStatus() == ClientResponse.SUCCESS;
                    if (r != null) {
                        try {
                            cb.handleResponse(r);
                        } catch (Exception e) {
                            m_logger.error("failed to process dispatch response " + r.getStatusString(), e);
                        } finally {
                            m_callbacks.remove(handle);
                        }
                        return bval;
                    }

                    //Submit the transaction.
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
     */
    public InternalClientResponseAdapter(long connectionId) {
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
            synchronized (this) {
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
                SUPPRESS_INTERVAL, TimeUnit.SECONDS,
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
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.WARN, null,
                    "Could not find caller details for client handle %d. Using internal adapter level connection id", clientHandle);
            return connectionId();
        }

        Long internalId = m_internalConnectionIds.get(callback.getInternalContext().getName());
        if (internalId==null) {
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.WARN, null,
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
