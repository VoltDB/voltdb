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
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;
import org.voltcore.network.Connection;
import org.voltcore.network.NIOReadStream;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.Pair;
import org.voltdb.client.ClientResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A very simple adapter for import handler that deserializes bytes into client responses. It calls
 * crashLocalVoltDB() if the deserialization fails, which should only happen if there's a bug.
 */
public class ImportClientResponseAdapter implements Connection, WriteStream {
    public static interface Callback {
        public void handleResponse(ClientResponse response);
    }

    private final long m_connectionId;
    private final AtomicLong m_handles = new AtomicLong();
    private final Map<Long, Callback> m_callbacks = Collections.synchronizedMap(new HashMap<Long, Callback>());
    private final Map<Long, ImportHandler> m_handlers = new HashMap<Long, ImportHandler>();
    private final boolean m_leaveCallback;
    private boolean m_stopped = false;

    /**
     * @param connectionId    The connection ID for this adapter, needs to be unique for this
     *                        node.
     * @param name            Human readable name identifying the adapter, will stand in for hostname
     */
    public ImportClientResponseAdapter(long connectionId, String name) {
        this(connectionId, name, false);
    }


    /**
     * @param connectionId    The connection ID for this adapter, needs to be unique for this
     *                        node.
     * @param name            Human readable name identifying the adapter, will stand in for hostname
     * @param leaveCallback   Don't remove callbacks when invoking them, they are reused
     */
    public ImportClientResponseAdapter(long connectionId, String name, boolean leaveCallback) {
        m_connectionId = connectionId;
        m_leaveCallback = leaveCallback;
    }

    //None of the values matter (other than the future) just using this as a shortcut to collect stats internally
    public ImportClientResponseAdapter(SettableFuture<ClientResponseImpl> fut) {
        m_leaveCallback = false;
        m_connectionId = 0;
    }

    public synchronized void registerHandler(ImportHandler handler) {
        m_stopped = false;
        m_handlers.put(handler.getId(), handler);
    }

    public void unregisterHandler(ImportHandler handler) {
        m_stopped = true;
        m_handlers.remove(handler.getId());
    }

    public static
        Pair<ImportClientResponseAdapter, ListenableFuture<ClientResponseImpl>>  getAsListenableFuture() {
        final SettableFuture<ClientResponseImpl> fut = SettableFuture.create();
        return Pair.of(new ImportClientResponseAdapter(fut), (ListenableFuture<ClientResponseImpl>)fut);
    }

    public void registerCallback(long handle, Callback c) {
        m_handles.set(handle + 1);//just in case make them not match
        m_callbacks.put(handle, c);
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
    public boolean hadBackPressure() {
        //TODO: Notify of backpressure for the ImportHandler
        for (ImportHandler handler : m_handlers.values()) {
            handler.hadBackPressure();
        }
        return true;
    }

    @Override
    public void fastEnqueue(DeferredSerialization ds) {
        enqueue(ds);
    }

    @Override
    public void enqueue(DeferredSerialization ds) {
        if (m_stopped) {
            return;
        }
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

            Callback callback = null;
            if (m_leaveCallback) {
                callback = m_callbacks.get(resp.getClientHandle());
            } else {
                callback = m_callbacks.remove(resp.getClientHandle());
            }
            if (callback != null) {
                callback.handleResponse(resp);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to deserialize ClientResponse in ImportClientResponseAdapter", e);
        }
    }

    @Override
    public void enqueue(ByteBuffer[] b)
    {
        if (b.length != 1) {
            throw new RuntimeException("Can't use chained ByteBuffers to enqueue");
        }
        enqueue(b[0]);
    }

    @Override
    public int calculatePendingWriteDelta(long now) {
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
