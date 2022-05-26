/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltcore.network.NIOReadStream;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.ProcedureRunnerNT.NTNestedProcedureCallback;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.utils.MiscUtils;

/**
 * This is a fork of InternalClientResponseAdapter that removes a lot of extra code like
 * backpressure, per-partition queues and an executor service with context switches. It's
 * used by non-transactional procedures to call other volt procedures Inception-style.
 *
 * It doesn't need backpressure because NT procs already have backpressure. This is a slight
 * tradeoff. An NT proc could call a bazillion sub-procs and flood the system. On the other
 * hand, the more we cut out, the faster we go. Ditto for per-partition queues.
 *
 * It doesn't need it's own exec service because NT procs already have lots of threads
 * that don't block EEs, so we're cool.
 */
public class LightweightNTClientResponseAdapter implements Connection, WriteStream {

    private final static String DEFAULT_INTERNAL_ADAPTER_NAME = "+!_NTInternalAdapter_!+";

    private final static VoltLogger m_logger = new VoltLogger("HOST");
    private final static long SUPPRESS_INTERVAL = 120;

    private final long m_connectionId;
    private final AtomicLong m_handles = new AtomicLong();
    private final ConcurrentMap<Long, ProcedureCallback> m_callbacks = new ConcurrentHashMap<>(2048, .75f, 128);

    private final InvocationDispatcher m_dispatcher;

    private void createTransaction(final InternalAdapterTaskAttributes kattrs,
            final ProcedureCallback cb,
            final StoredProcedureInvocation task,
            final AuthSystem.AuthUser user) {
        assert(m_dispatcher != null);

        final long handle = nextHandle();
        task.setClientHandle(handle);

        assert(m_callbacks.get(handle) == null);
        m_callbacks.put(handle, cb);

        // ntPriority (last dispatcher arg) is only significant if task is an NT procedure
        ClientResponseImpl r = m_dispatcher.dispatch(task, kattrs, this, user, null, true);
        if (r != null) {
            try {
                cb.clientCallback(r);
            } catch (Exception e) {
                m_logger.error("failed to process dispatch response " + r.getStatusString(), e);
            } finally {
                m_callbacks.remove(handle);
            }
        }
    }

    /**
     * @param connectionId    The connection ID for this adapter, needs to be unique for this
     *                        node.
     */
    public LightweightNTClientResponseAdapter(long connectionId, InvocationDispatcher dispatcher) {
        m_connectionId = connectionId;
        m_dispatcher = dispatcher;
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

        final ProcedureCallback callback = m_callbacks.remove(resp.getClientHandle());
        if (callback == null) {
            assert(false);
            throw new IllegalStateException("Callback was null?");
        }

        try {
            callback.clientCallback(resp);
        } catch (Exception ex) {
            assert(false);
            m_logger.error("Failed to process callback.", ex);
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
    public void disableWriteSelection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enableWriteSelection() {
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
        ProcedureCallback callback = m_callbacks.get(clientHandle);
        if (callback == null || callback instanceof NTNestedProcedureCallback == false) {
           return getHostnameOrIP();
        }
        return ((NTNestedProcedureCallback)callback).getHostnameOrIP();
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
        ProcedureCallback callback = m_callbacks.get(clientHandle);
        if (callback == null) {
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.WARN, null,
                    "Could not find caller details for client handle %d. Using internal adapter level connection id", clientHandle);
            return connectionId();
        }

        if (callback instanceof NTNestedProcedureCallback == false) {
            return connectionId();
        }

        return ((NTNestedProcedureCallback)callback).getConnectionId(clientHandle);
    }

    @Override
    public Future<?> unregister() {
        return null;
    }

    /**
     * Used to call a procedure from NTPRocedureRunner
     * Calls createTransaction with the proper params
     */
    public void callProcedure(AuthUser user,
                              boolean isAdmin,
                              int timeout,
                              ProcedureCallback cb,
                              String procName,
                              Object[] args)
    {
        // since we know the caller, this is safe
        assert(cb != null);

        StoredProcedureInvocation task = new StoredProcedureInvocation();
        task.setProcName(procName);
        task.setParams(args);
        if (timeout != BatchTimeoutOverrideType.NO_TIMEOUT) {
            task.setBatchTimeout(timeout);
        }

        InternalAdapterTaskAttributes kattrs = new InternalAdapterTaskAttributes(
                DEFAULT_INTERNAL_ADAPTER_NAME, isAdmin, connectionId());

        assert(m_dispatcher != null);

        // JHH: I have no idea why we need to do this, but CL crashes if we don't. Sigh.
        try {
            task = MiscUtils.roundTripForCL(task);
        } catch (Exception e) {
            String msg = String.format("Cannot invoke procedure %s. failed to create task: %s",
                    procName, e.getMessage());
            m_logger.rateLimitedLog(SUPPRESS_INTERVAL, Level.ERROR, null, msg);
            ClientResponseImpl cri = new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE, new VoltTable[0], msg);
            try {
                cb.clientCallback(cri);
            } catch (Exception e1) {
                throw new IllegalStateException(e1);
            }
        }

        createTransaction(kattrs, cb, task, user);
    }
}
