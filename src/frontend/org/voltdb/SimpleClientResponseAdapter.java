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

import com.google_voltpatches.common.base.Supplier;
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
 * A very simple adapter that deserializes bytes into client responses. It calls
 * crashLocalVoltDB() if the deserialization fails, which should only happen if there's a bug.
 */
public class SimpleClientResponseAdapter implements Connection, WriteStream {
    public static interface Callback {
        public void handleResponse(ClientResponse response);
    }

    public static final Callback NULL_CALLBACK = new Callback() {
        @Override
        public void handleResponse(ClientResponse response)
        {}
    };

    public static final class SyncCallback implements Callback {
        private final SettableFuture<ClientResponse> m_responseFuture = SettableFuture.create();

        public ClientResponse getResponse(long timeoutMs) throws InterruptedException
        {
            try {
                return m_responseFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                return null;
            } catch (ExecutionException e) {
                VoltDB.crashLocalVoltDB("Should never happen", true, e);
                return null;
            }
        }

        @Override
        public void handleResponse(ClientResponse response)
        {
            m_responseFuture.set(response);
        }
    }

    private final long m_connectionId;
    private final AtomicLong m_handles = new AtomicLong();
    private Map<Long, Callback> m_callbacks = Collections.synchronizedMap(new HashMap<Long, Callback>());
    private final String m_name;
    public static volatile AtomicLong m_testConnectionIdGenerator;
    private final boolean m_leaveCallback;
    private final SettableFuture<ClientResponseImpl> m_retFuture;

    /**
     * @param connectionId    The connection ID for this adapter, needs to be unique for this
     *                        node.
     * @param name            Human readable name identifying the adapter, will stand in for hostname
     */
    public SimpleClientResponseAdapter(long connectionId, String name) {
        this(connectionId, name, false);
    }


    /**
     * @param connectionId    The connection ID for this adapter, needs to be unique for this
     *                        node.
     * @param name            Human readable name identifying the adapter, will stand in for hostname
     * @param leaveCallback   Don't remove callbacks when invoking them, they are reused
     */
    public SimpleClientResponseAdapter(long connectionId, String name, boolean leaveCallback) {
        if (m_testConnectionIdGenerator != null) {
            m_connectionId = m_testConnectionIdGenerator.incrementAndGet();
        } else {
            m_connectionId = connectionId;
        }
        m_name = name;
        m_leaveCallback = leaveCallback;
        m_retFuture = null;
    }

    //None of the values matter (other than the future) just using this as a shortcut to collect stats internally
    public SimpleClientResponseAdapter(SettableFuture<ClientResponseImpl> fut) {
        m_retFuture = fut;
        m_leaveCallback = false;
        m_name = "";
        m_connectionId = 0;
    }

    public static
        Pair<SimpleClientResponseAdapter, ListenableFuture<ClientResponseImpl>>  getAsListenableFuture() {
        final SettableFuture<ClientResponseImpl> fut = SettableFuture.create();
        return Pair.of(new SimpleClientResponseAdapter(fut), (ListenableFuture<ClientResponseImpl>)fut);
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

    public Supplier<Pair<Long, SyncCallback>> getSyncCallbackSupplier() {
        return new Supplier<Pair<Long, SyncCallback>>() {
            @Override
            public Pair<Long, SyncCallback> get() {
                final SyncCallback callback = new SyncCallback();
                final long handle = registerCallback(callback);
                return Pair.of(handle, callback);
            }
        };
    }

    @Override
    public void queueTask(Runnable r) {
        // Called when node failure happens
        r.run();
    }

    @Override
    public boolean hadBackPressure() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fastEnqueue(DeferredSerialization ds) {
        enqueue(ds);
    }

    @Override
    public void enqueue(DeferredSerialization ds) {
        try {
            // serialize() touches not-threadsafe state around Initiator
            // stats.  In the normal code path, this is protected by a lock
            // in NIOWriteStream.enqueue().
            ByteBuffer buf = null;
            synchronized(this) {
                buf = ByteBuffer.allocate(ds.getSerializedSize());
                ds.serialize(buf);
            }
            if (buf == null) {
                throw new UnsupportedOperationException();
            }
            enqueue(buf);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB("enqueue() in SimpleClientResponseAdapter throw an exception", true, e);
        }
    }

    @Override
    public void enqueue(ByteBuffer b) {
        ClientResponseImpl resp = new ClientResponseImpl();
        try {
            b.position(4);
            resp.initFromBuffer(b);

            //Go for a different behavior if we are using this adapter as a gussied up future for
            //an internal request
            if (m_retFuture != null) {
                m_retFuture.set(resp);
                return;
            }

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
            throw new RuntimeException("Unable to deserialize ClientResponse in SimpleClientResponseAdapter", e);
        }
    }

    @Override
    public void enqueue(ByteBuffer[] b)
    {
        if (b.length != 1)
        {
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
        return m_name;
    }

    @Override
    public String getHostnameOrIP() {
        return m_name;
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
