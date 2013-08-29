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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltcore.network.NIOReadStream;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.client.ClientResponse;

/**
 * A dummy connection to provide to the DTXN. It routes ClientResponses back
 * to the restore agent.
 */
public class RestoreAdapter implements Connection, WriteStream {
    private final static VoltLogger LOG = new VoltLogger("HOST");

    public static volatile AtomicLong m_testConnectionIdGenerator;

    private final Runnable m_doneNotifier;
    private final long m_connectionId;
    public RestoreAdapter(Runnable doneNotfifier) {
        m_doneNotifier = doneNotfifier;
        if (m_testConnectionIdGenerator != null) {
            m_connectionId = m_testConnectionIdGenerator.incrementAndGet();
        } else {
            m_connectionId = Long.MIN_VALUE + 1;
        }
    }

    @Override
    public boolean hadBackPressure() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enqueue(DeferredSerialization ds) {
        try {
            ByteBuffer[] bbArray = ds.serialize();
            if (bbArray.length != 1) {
                throw new UnsupportedOperationException();
            }
            enqueue(bbArray[0]);
        } catch (IOException error) {
            throw new UnsupportedOperationException(error.getMessage());
        }
    }

    @Override
    public void enqueue(ByteBuffer b) {
        ClientResponseImpl resp = new ClientResponseImpl();
        try
        {
            b.position(4);
            resp.initFromBuffer(b);
        }
        catch (IOException ioe)
        {
            LOG.error("Unable to deserialize ClientResponse from snapshot",
                    ioe);
            return;
        }
        handleResponse(resp);
    }

    private void handleResponse(ClientResponse res) {
        boolean failure = false;
        if (res.getStatus() != ClientResponse.SUCCESS) {
            failure = true;
        }

        VoltTable[] results = res.getResults();
        if (results == null || results.length != 1) {
            failure = true;
        }

        while (!failure && results[0].advanceRow()) {
            String resultStatus = results[0].getString("RESULT");
            if (!resultStatus.equalsIgnoreCase("success")) {
                failure = true;
            }
        }

        if (failure) {
            for (VoltTable result : results) {
                LOG.fatal(result);
            }
            VoltDB.crashGlobalVoltDB("Failed to restore from snapshot: " +
                                     res.getStatusString(), false, null);
        } else {
            Thread networkHandoff = new Thread() {
                @Override
                public void run() {
                    m_doneNotifier.run();
                }
            };
            networkHandoff.start();
        }
    }

    @Override
    public void enqueue(ByteBuffer[] b) {
        if (b.length == 1)
        {
            enqueue(b[0]);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
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
        return "RestoreAdapter";
    }

    @Override
    public String getHostnameOrIP() {
        return "RestoreAdapter";
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

    @Override
    public void queueTask(Runnable r) {
        throw new UnsupportedOperationException();
    }
}

