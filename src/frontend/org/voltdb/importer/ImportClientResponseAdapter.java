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

package org.voltdb.importer;

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
import org.voltcore.network.Connection;
import org.voltcore.network.NIOReadStream;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.ClientInterface;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ImportHandler;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

/**
 * A very simple adapter for import handler that deserializes bytes into client responses.
 * For each partition it creates a single thread executor to sequence per partition transaction submission.
 * Responses are also written on a single thread executor to avoid bottlenecking on callback doing heavy work.
 * It calls
 * crashLocalVoltDB() if the deserialization fails, which should only happen if there's a bug.
 */
public class ImportClientResponseAdapter implements Connection, WriteStream {

    private static final VoltLogger m_logger = new VoltLogger("IMPORT");
    public static final long MAX_PENDING_TRANSACTIONS_PER_PARTITION = Integer.getInteger("IMPORTER_MAX_PENDING_TRANSACTION_PER_PARTITION", 500);

    public interface Callback {
        public void handleResponse(ClientResponse response) throws Exception;
        public String getProcedureName();
        public int getPartitionId();
        public ImportContext getImportContext();
    }

    private final long m_connectionId;
    private final AtomicLong m_handles = new AtomicLong();
    private final AtomicLong m_failures = new AtomicLong(0);
    private final Map<Long, Callback> m_callbacks = Collections.synchronizedMap(new HashMap<Long, Callback>());
    private final ConcurrentMap<Integer, ExecutorService> m_partitionExecutor = new ConcurrentHashMap<>();

    private class ImportCallback implements Callback {

        private DBBPool.BBContainer m_cont;
        private final long m_id;
        private final ProcedureCallback m_cb;
        private final String m_procedure;
        private final int m_partition;
        private final ImportContext m_context;

        public ImportCallback(final ImportContext context, final DBBPool.BBContainer cont, String proc, int partition, ProcedureCallback cb, long id) {
            m_context = context;
            m_cont = cont;
            m_cb = cb;
            m_id = id;
            m_procedure = proc;
            m_partition = partition;
        }

        public void discard() {
            if (m_cont != null) {
                m_cont.discard();
                m_cont = null;
            }
        }

        @Override
        public void handleResponse(ClientResponse response) throws Exception {
            try {
                if (m_cb != null) {
                    m_cb.clientCallback(response);
                }
            } finally {
                discard();
            }
        }

        @Override
        public String getProcedureName() {
            return m_procedure;
        }

        @Override
        public int getPartitionId() {
            return m_partition;
        }

        @Override
        public ImportContext getImportContext() {
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

    public boolean createTransaction(final ImportContext context, final String procName, final Procedure catProc, final ProcedureCallback proccb, final StoredProcedureInvocation task,
            final DBBPool.BBContainer tcont, final int partition, final long nowNanos) {

        if (!m_partitionExecutor.containsKey(partition)) {
            m_partitionExecutor.putIfAbsent(partition, CoreUtils.getSingleThreadExecutor("ImportHandlerExecutor - " + partition));
        }
        ExecutorService executor = m_partitionExecutor.get(partition);
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    context.hasBackPressure(hasBackPressure());
                    submitTransaction();
                }
                public boolean submitTransaction() {
                    final long handle = nextHandle();
                    task.setClientHandle(handle);
                    final ImportCallback cb = new ImportCallback(context, tcont, procName, partition, proccb, handle);
                    m_callbacks.put(handle, cb);

                    //Submit the transaction.
                    boolean bval = getClientInterface().createTransaction(connectionId(), task,
                            catProc.getReadonly(), catProc.getSinglepartition(), catProc.getEverysite(), partition,
                            task.getSerializedSize(), nowNanos);
                    if (!bval) {
                        m_logger.error("Failed to submit transaction.");
                        cb.discard();
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
    public ImportClientResponseAdapter(long connectionId, String name) {
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
                buf = ByteBuffer.allocate(serializedSize);
                ds.serialize(buf);
            }
            enqueue(buf);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB("enqueue() in ImportClientResponseAdapter throw an exception", true, e);
        }
    }

    @Override
    public void enqueue(final ByteBuffer b) {

        final ClientResponseImpl resp = new ClientResponseImpl();
        b.position(4);
        try {
            resp.initFromBuffer(b);
        } catch (IOException ex) {
            VoltDB.crashLocalVoltDB("enqueue() in ImportClientResponseAdapter throw an exception", true, ex);
        }
        final Callback callback = m_callbacks.get(resp.getClientHandle());
        if (!m_partitionExecutor.containsKey(callback.getPartitionId())) {
            m_logger.error("Invalid partition response recieved for sending importer response.");
            return;
        }
        ExecutorService executor = m_partitionExecutor.get(callback.getPartitionId());
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    callback.getImportContext().hasBackPressure(hasBackPressure());
                    handle();
                }

                public void handle() {
                    try {
                        if (resp.getStatus() != ClientResponse.SUCCESS) {
                            String fmt = "Importer stored procedure failed: %s Error: %s failures: %d";
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
            throw new UnsupportedOperationException("Buffer chains not supported in Import invocation adapter");
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
        return "ImportAdapter";
    }

    @Override
    public String getHostnameOrIP() {
        return "ImportAdapter";
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
    public Future<?> unregister() {
        return null;
    }
}
