/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import org.voltcore.network.Connection;
import org.voltcore.network.NIOReadStream;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.client.ClientResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Exchanger;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A very simple adapter that deserializes bytes into client responses. It calls
 * crashLocalVoltDB() if the deserialization fails, which should only happen if there's a bug.
 */
public class SimpleClientResponseAdapter implements Connection, WriteStream {
    public static interface Callback {
        public void handleResponse(ClientResponse response);
    }

    public static final class SyncCallback implements Callback {
        private final Exchanger<ClientResponse> m_responseExchanger = new Exchanger<ClientResponse>();

        public ClientResponse getResponse(long timeoutMs) throws InterruptedException
        {
            try {
                return m_responseExchanger.exchange(null, timeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                return null;
            }
        }

        @Override
        public void handleResponse(ClientResponse response)
        {
            try {
                m_responseExchanger.exchange(response);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private final long m_connectionId;
    private volatile Callback m_callback = null;
    private final String m_name;
    public static volatile AtomicLong m_testConnectionIdGenerator;


    /**
     * @param connectionId    The connection ID for this adapter, needs to be unique for this
     *                        node.
     * @param name            Human readable name identifying the adapter, will stand in for hostname
     */
    public SimpleClientResponseAdapter(long connectionId, String name) {
        if (m_testConnectionIdGenerator != null) {
            m_connectionId = m_testConnectionIdGenerator.incrementAndGet();
        } else {
            m_connectionId = connectionId;
        }
        m_name = name;
    }

    public void setCallback(Callback callback) {
        m_callback = callback;
    }

    @Override
    public void queueTask(Runnable r) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hadBackPressure() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enqueue(DeferredSerialization ds) {
        try {
            ByteBuffer[] serialized;
            // serialize() touches not-threadsafe state around Initiator
            // stats.  In the normal code path, this is protected by a lock
            // in NIOWriteStream.enqueue().
            synchronized(this) {
                serialized = ds.serialize();
            }
            if (serialized.length != 1) {
                throw new UnsupportedOperationException();
            }
            enqueue(serialized[0]);
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

            Callback callback = m_callback;
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
