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

import org.voltdb.*;
import org.voltcore.network.Connection;
import org.voltcore.network.NIOReadStream;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.client.ClientResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.voltcore.utils.DBBPool;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ProcedureCallback;

/**
 * A very simple adapter for import handler that deserializes bytes into client responses. It calls
 * crashLocalVoltDB() if the deserialization fails, which should only happen if there's a bug.
 */
public class ImportClientResponseAdapter implements Connection, WriteStream {
    public static interface Callback {
        public void handleResponse(ClientResponse response) throws Exception;
    }

    private final long m_connectionId;
    private final AtomicLong m_handles = new AtomicLong();
    private final Map<Long, Callback> m_callbacks = Collections.synchronizedMap(new HashMap<Long, Callback>());
    private final Map<Long, ImportCallback> m_pendingCallbacks = Collections.synchronizedMap(new HashMap<Long, ImportCallback>());

    private class ImportCallback implements Callback {

        private DBBPool.BBContainer m_cont;
        private long m_id;
        private final ProcedureCallback m_cb;

        public ImportCallback(final DBBPool.BBContainer cont, ProcedureCallback cb) {
            m_cont = cont;
            m_cb = cb;
        }

        public void setId(long id) {
            m_id = id;
        }

        public synchronized void discard() {
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

    public boolean createTransaction(Procedure catProc, ProcedureCallback proccb, StoredProcedureInvocation task,
            DBBPool.BBContainer tcont, int partition, long nowNanos) {
            ImportCallback cb = new ImportCallback(tcont, proccb);
            long cbhandle = registerCallback(cb);
            cb.setId(cbhandle);
            task.setClientHandle(cbhandle);
            m_pendingCallbacks.put(cbhandle, cb);

            //Submmit the transaction.
            return VoltDB.instance().getClientInterface().createTransaction(connectionId(), task,
                    catProc.getReadonly(), catProc.getSinglepartition(), catProc.getEverysite(), partition,
                    task.getSerializedSize(), nowNanos);
    }

    /**
     * @param connectionId    The connection ID for this adapter, needs to be unique for this
     *                        node.
     * @param name            Human readable name identifying the adapter, will stand in for hostname
     */
    public ImportClientResponseAdapter(long connectionId, String name) {
        m_connectionId = connectionId;
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
            synchronized(this) {
                int sz = ds.getSerializedSize();
                buf = ByteBuffer.allocate(sz);
                ds.serialize(buf);
            }
            enqueue(buf);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB("enqueue() in ImportClientResponseAdapter throw an exception", true, e);
        }
    }

    @Override
    public void enqueue(ByteBuffer b) {
        ClientResponseImpl resp = new ClientResponseImpl();
        try {
            b.position(4);
            resp.initFromBuffer(b);

            Callback callback = m_callbacks.remove(resp.getClientHandle());
            if (callback != null) {
                callback.handleResponse(resp);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to deserialize ClientResponse in ImportClientResponseAdapter", e);
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
