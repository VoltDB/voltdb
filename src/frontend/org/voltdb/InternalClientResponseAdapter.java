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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.voltcore.network.Connection;
import org.voltcore.network.NIOReadStream;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

/**
 * A very simple adapter for internal txn requests that deserializes bytes into client responses. It calls
 * crashLocalVoltDB() if the deserialization fails, which should only happen if there's a bug.
 */
public class InternalClientResponseAdapter implements Connection, WriteStream {
    public static interface Callback {
        public void handleResponse(ClientResponse response) throws Exception;
    }

    private final long m_connectionId;
    private final AtomicLong m_handles = new AtomicLong();
    private final Map<Long, Callback> m_callbacks = Collections.synchronizedMap(new HashMap<Long, Callback>());
    private final Map<Long, InternalCallback> m_pendingCallbacks = Collections.synchronizedMap(new HashMap<Long, InternalCallback>());
    private final ScheduledExecutorService m_es;
    private final ConcurrentMap<Integer, ExecutorService> m_partitionExecutor = new ConcurrentHashMap<>();

    private class InternalCallback implements Callback {

        private DBBPool.BBContainer m_cont;
        private long m_id;
        private final ProcedureCallback m_cb;

        public InternalCallback(final DBBPool.BBContainer cont, ProcedureCallback cb) {
            m_cont = cont;
            m_cb = cb;
        }

        public void setId(long id) {
            m_id = id;
        }

        public void discard() {
            if (m_cont != null) {
                m_cont.discard();
                m_cont = null;
            }
        }

        @Override
        public void handleResponse(ClientResponse response) throws Exception {
            discard();
            m_pendingCallbacks.remove(m_id);
            if (m_cb != null) {
                m_cb.clientCallback(response);
            }
        }
    }

    public long getPendingCount() {
        return m_pendingCallbacks.size();
    }

    //Similar to distributer drain.
    public void drain() {
        long sleep = 500;
        do {
            if (m_pendingCallbacks.isEmpty()) {
                break;
            }
            /*
             * Back off to spinning at five millis. Try and get drain to be a little
             * more prompt. Spinning sucks!
             */
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(sleep));
            if (sleep < 5000) {
                sleep += 500;
            }
        } while(true);
    }

    public boolean createTransaction(final Procedure catProc,
            final ProcedureCallback proccb,
            final StoredProcedureInvocation task,
            final DBBPool.BBContainer tcont,
            final int partition,
            final long nowNanos) {
        if (!m_partitionExecutor.containsKey(partition)) {
            ThreadFactory factory = CoreUtils.getThreadFactory(null, "ImportHandlerExecutor",
                    CoreUtils.SMALL_STACK_SIZE, false, null);
            m_partitionExecutor.putIfAbsent(partition, new ScheduledThreadPoolExecutor(1, factory));
        }
        ExecutorService executor = m_partitionExecutor.get(partition);
        executor.submit(new Runnable()
        {
            @Override
            public void run() {
                InternalCallback cb = new InternalCallback(tcont, proccb);
                long cbhandle = registerCallback(cb);
                cb.setId(cbhandle);
                task.setClientHandle(cbhandle);
                m_pendingCallbacks.put(cbhandle, cb);

                //Submit the transaction.
                VoltDB.instance().getClientInterface().createTransaction(connectionId(), task,
                        catProc.getReadonly(), catProc.getSinglepartition(), catProc.getEverysite(), partition,
                        task.getSerializedSize(), nowNanos);
            }
        });

        return true;
    }

    /**
     * @param connectionId    The connection ID for this adapter, needs to be unique for this
     *                        node.
     * @param name            Human readable name identifying the adapter, will stand in for hostname
     */
    public InternalClientResponseAdapter(long connectionId, String name) {
        m_connectionId = connectionId;
        ThreadFactory factory = CoreUtils.getThreadFactory(null, "ImportResponseHandler", CoreUtils.SMALL_STACK_SIZE,
                false, null);
        m_es = new ScheduledThreadPoolExecutor(1, factory);
    }

    public long registerCallback(Callback c) {
        final long handle = m_handles.incrementAndGet();
        m_callbacks.put( handle, c);
        return handle;
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
            final int serializedSize = ds.getSerializedSize();
            buf = ByteBuffer.allocate(serializedSize);
            ds.serialize(buf);
            enqueue(buf);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB("enqueue() in IClientResponseAdapter throw an exception", true, e);
        }
    }

    @Override
    public void enqueue(ByteBuffer b) {
        final ClientResponseImpl resp = new ClientResponseImpl();
        try {
            b.position(4);
            resp.initFromBuffer(b);

            final Callback callback = m_callbacks.remove(resp.getClientHandle());
            if (callback != null) {
                m_es.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            callback.handleResponse(resp);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                });

            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to deserialize ClientResponse in InternalClientResponseAdapter", e);
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
