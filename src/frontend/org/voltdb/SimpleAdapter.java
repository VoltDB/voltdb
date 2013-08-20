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
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

/**
 * A very simple adapter that deserializes bytes into client responses. It calls
 * crashLocalVoltDB() if the deserialization fails, which should only happen if there's a bug.
 */
public class SimpleAdapter implements Connection, WriteStream {
    public static interface Callback {
        public void handleResponse(ClientResponse response);
    }

    private final long m_connectionId;
    private final Callback m_callback;

    /**
     * @param connectionId    The connection ID for this adapter, needs to be unique for this
     *                        node.
     * @param callback        A callback to take the client response, null is accepted.
     */
    public SimpleAdapter(long connectionId, Callback callback) {
        m_connectionId = connectionId;
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
            VoltDB.crashLocalVoltDB("enqueue() in SimpleAdapter throw an exception", true, e);
        }
    }

    @Override
    public void enqueue(ByteBuffer b) {
        ClientResponseImpl resp = new ClientResponseImpl();
        try {
            b.position(4);
            resp.initFromBuffer(b);

            if (m_callback != null) {
                m_callback.handleResponse(resp);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to deserialize ClientResponse in SimpleAdapter", e);
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
    public String getHostnameOrIP() {
        return "";
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
