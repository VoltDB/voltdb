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

package org.voltcore.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.voltcore.logging.VoltLogger;

/** Encapsulates a socket registration for a VoltNetwork */
public class VoltPort implements Connection
{
    /** The network this port participates in */
    protected final VoltNetwork m_network;

    protected static final VoltLogger networkLog = new VoltLogger("NETWORK");

    public static final int MAX_MESSAGE_LENGTH = 52428800;

    protected final NetworkDBBPool m_pool;

    /** The currently selected operations on this port. */
    private int m_readyOps = 0;

    /** The operations the port wishes to have installed */
    protected volatile int m_interestOps = 0;

    /** True when the port is executing  VoltNetwork dispatch */
    volatile boolean m_running = false;

    protected volatile boolean m_isDead = false;

    protected volatile boolean m_isShuttingDown = false;

    /** Used internally to make operation changes atomic.
     *  External writers (like a foreign host, for example),
     *  must be able to register network writes to a port
     *  that's running. This requires synchronization. */
    protected final Object m_lock = new Object();

    // BUG: should be final but currently VoltNetwork.register(), which
    // generates the selection key, takes the port as a parameter. catch-22.
    // Currently, only assigned in VoltNetwork.register(), which shouldn't
    // be called on an already registered Port. As long as writes aren't
    // queued to unregistered ports, this is thread safe.
    protected SelectionKey m_selectionKey;

    /** The channel this port wraps */
    protected SocketChannel m_channel;

    protected final InputHandler m_handler;

    protected NIOReadStream m_readStream;
    protected VoltNIOWriteStream m_writeStream;
    protected long m_messagesRead = 0;
    private long m_lastMessagesRead = 0;

    /*
     * This variable will be changed to the actual hostname some time later. It
     * is not guaranteed on how long it will take to do the reverse DNS lookup.
     * It is not recommended to use the value of this variable as a key. Use
     * m_remoteIP if you need to identify a host.
     */
    volatile String m_remoteHostname = null;
    final InetSocketAddress m_remoteSocketAddress;
    final String m_remoteSocketAddressString;
    private volatile String m_remoteHostAndAddressAndPort;
    private String m_toString = null;

    /** Wrap a socket with a VoltPort */
    public VoltPort(
            VoltNetwork network,
            InputHandler handler,
            InetSocketAddress remoteAddress,
            NetworkDBBPool pool) {
        m_network = network;
        m_handler = handler;
        m_remoteSocketAddress = remoteAddress;
        m_remoteSocketAddressString = remoteAddress.getAddress().getHostAddress();
        m_pool = pool;
        m_remoteHostAndAddressAndPort = "/" + m_remoteSocketAddressString + ":" + m_remoteSocketAddress.getPort();
        m_toString = super.toString() + ":" + m_remoteHostAndAddressAndPort;
    }

    /**
     * Do a reverse DNS lookup of the remote end. Done in a separate thread unless synchronous is specified.
     * If asynchronous lookup is requested the task may be dropped and resolution may never occur
     */
    void resolveHostname(boolean synchronous) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                String remoteHost = ReverseDNSCache.hostnameOrAddress(m_remoteSocketAddress.getAddress());
                if (!remoteHost.equals(m_remoteSocketAddress.getAddress().getHostAddress())) {
                    m_remoteHostname = remoteHost;
                    m_remoteHostAndAddressAndPort = remoteHost + m_remoteHostAndAddressAndPort;
                    m_toString = VoltPort.this.toString() + ":" + m_remoteHostAndAddressAndPort;
                }
            }
        };
        if (synchronous) {
            r.run();
        } else {
            /*
             * Start the reverse DNS lookup in background because it might be
             * very slow if the hostname is not specified in local /etc/hosts.
             */
            try {
                ReverseDNSCache.submit(r);
            } catch (RejectedExecutionException e) {
                networkLog.debug(
                        "Reverse DNS lookup for " + m_remoteSocketAddress + " rejected because the queue was full");
            }
        }
    }

    protected void setKey (SelectionKey key) {
        m_selectionKey = key;
        m_channel = (SocketChannel)key.channel();
        m_readStream = new NIOReadStream();
        m_writeStream = new VoltNIOWriteStream(
                this,
                m_handler.offBackPressure(),
                m_handler.onBackPressure(),
                m_handler.writestreamMonitor());
        m_interestOps = key.interestOps();
    }

    /**
     * Lock the VoltPort for running by the VoltNetwork executor service. This prevents anything from sneaking in a messing with
     * the selector set until the executor service has had a chance to handle all the I/O.
     */
    void lockForHandlingWork() {
        synchronized(m_lock) {
            assert m_running == false;
            m_running = true;
            m_readyOps = 0;
            m_readyOps = m_selectionKey.readyOps();      // runnable.run() doesn't accept parameters
        }
    }

    public void run() throws IOException {
        try {
            /*
             * Have the read stream fill from the network
             */
            if (readyForRead()) {
                final int maxRead = m_handler.getMaxRead();
                if (maxRead > 0) {
                    int read = fillReadStream(maxRead);
                    if (read > 0) {
                        try {
                            ByteBuffer message;
                            while ((message = m_handler.retrieveNextMessage(readStream())) != null) {
                                m_handler.handleMessage(message, this);
                                m_messagesRead++;
                            }
                        } catch (VoltProtocolHandler.BadMessageLength e) {
                            String err = String.format("Bad message length, from %s%s", m_remoteHostAndAddressAndPort,
                                                       m_messagesRead != 0 ? "" : "; this may indicate mismatched SSL/TLS setting or non-VoltDB sender");
                            networkLog.error(err, e);
                            throw e;
                        }
                    }
                }
            }

            /*
             * On readiness selection, optimistically assume that write will succeed,
             * in the common case it will
             */
            drainWriteStream();
        } finally {
            synchronized(m_lock) {
                assert(m_running == true);
                m_running = false;
            }
        }
    }

    protected int fillReadStream(int maxBytes) throws IOException {
        if ( maxBytes == 0 || m_isShuttingDown)
            return 0;

        // read from network, copy data into read buffers, which from thread local memory pool
        final int read = m_readStream.read(m_channel, maxBytes, m_pool);

        if (read == -1) {
            handleReadStreamEOF();
        }
        return read;
    }

    protected void handleReadStreamEOF() throws IOException {
        disableReadSelection();

        if (m_channel.socket().isConnected()) {
            try {
                m_channel.socket().shutdownInput();
            } catch (SocketException e) {
                //Safe to ignore to these
            }
        }

        m_isShuttingDown = true;
        m_handler.stopping(this);

        /*
         * Allow the write queue to drain if possible
         */
        enableWriteSelection();
    }

    protected final void drainWriteStream() throws IOException {
        //Safe to do this with a separate embedded synchronization because no interest ops are modded
        m_writeStream.serializeQueuedWrites(m_pool);

        /*
         * All interactions with write stream must be protected
         * with a lock to ensure that interests ops are consistent with
         * the state of writes queued to the stream. This prevent
         * lost queued writes where the write is queued
         * but the write interest op is not set.
         */
        synchronized (m_writeStream) {
            /*
             * If there is something to write always give it a whirl.
             */
            if (!m_writeStream.isEmpty())
            {
                m_writeStream.drainTo(m_channel);
            }

            // Write selection is turned on when output data in enqueued,
            // turn it off when the queue becomes empty.
            if (m_writeStream.isEmpty()) {
                disableWriteSelection();

                if (m_isShuttingDown) {
                    m_channel.close();
                    //m_handler.stopped(this);
                    unregistered();
                }
            }
        }
    }

    public void enableWriteSelection() {
        setInterests(SelectionKey.OP_WRITE, 0);
    }

    public void disableWriteSelection() {
        setInterests(0, SelectionKey.OP_WRITE);
    }

    @Override
    public void disableReadSelection() {
        setInterests(0, SelectionKey.OP_READ);
    }

    @Override
    public void enableReadSelection() {
        setInterests( SelectionKey.OP_READ, 0);
    }

    /** Report the operations the network should next select */
    int interestOps() {
        return m_interestOps;
    }

    /** Change the desired interest key set */
    public void setInterests(int opsToAdd, int opsToRemove) {
        // must be done atomically with changes to m_running
        synchronized(m_lock) {
            int oldInterestOps = m_interestOps;
            m_interestOps = (m_interestOps | opsToAdd) & (~opsToRemove);

            if (oldInterestOps != m_interestOps && !m_running) {
                /*
                 * If this is a write, optimistically assume the write
                 * will succeed and try it without using the selector
                 */
                m_network.addToChangeList(this, (opsToAdd & SelectionKey.OP_WRITE) != 0);
            }
        }
    }

    /** Return the nio selection key underlying this port. */
    public SelectionKey getKey() {
        return m_selectionKey;
    }

    /** Return the operations that were last selected by the network */
    int readyOps() {
        return m_readyOps;
    }

    boolean readyForRead() {
        return (readyOps() & SelectionKey.OP_READ) != 0 && (m_interestOps & SelectionKey.OP_READ) != 0;
    }

    boolean isRunning() {
        return m_running;
    }

    void die() {
        m_isDead = true;
    }

    boolean isDead() {
        return m_isDead;
    }

    @Override
    public NIOReadStream readStream() {
        assert(m_readStream != null);
        return m_readStream;
    }

    @Override
    public VoltNIOWriteStream writeStream() {
        assert(m_writeStream != null);
        return m_writeStream;
    }

    /**
     * Invoked when the InputHandler is registering but not active
     */
    void registering() {
        m_handler.starting(this);
    }

    /**
     * Invoked before the first message is sent/received
     */
    void registered() {
        m_handler.started(this);
    }

    /**
     * Called when the unregistration has been requested. Could be do
     * to app code or due to the client hanging up or some other error
     * Spin until the port is no longer running.
     */
    void unregistering() {
        m_handler.stopping(this);
    }

    private boolean m_alreadyStopped = false;

    /**
     * Called when unregistration is complete and the Connection can no
     * longer be interacted with.
     *
     * Various error paths fall back to unregistering so it can happen multiple times and is really
     * annoying. Suppress it here with a flag
     */
    void unregistered() {
        try {
            if (!m_alreadyStopped) {
                m_alreadyStopped = true;
                try {
                    m_handler.stopped(this);
                } finally {
                    try {
                        m_writeStream.shutdown();
                    } finally {
                        if (m_readStream != null) {
                            m_readStream.shutdown();
                        }
                    }
                }
            }
        } finally {
            networkLog.debug("Closing channel " + m_toString);
            try {
                m_channel.close();
            } catch (IOException e) {
                networkLog.warn(e);
            }
        }
    }

    @Override
    public String toString() {
        if (m_toString == null) {
            return super.toString();
        } else {
            return m_toString;
        }
    }

    long getMessagesRead(boolean interval) {
        if (interval) {
            final long messagesRead = m_messagesRead;
            final long messagesReadThisTime = messagesRead - m_lastMessagesRead;
            m_lastMessagesRead = messagesRead;
            return messagesReadThisTime;
        } else {
            return m_messagesRead;
        }
    }

    @Override
    public String getHostnameOrIP() {
        if (m_remoteHostname != null) {
            return m_remoteHostname;
        } else {
            return m_remoteSocketAddressString;
        }
    }

    @Override
    public String getHostnameOrIP(long clientHandle) {
        return getHostnameOrIP();
    }

    @Override
    public String getHostnameAndIPAndPort() {
        return m_remoteHostAndAddressAndPort;
    }

    @Override
    public int getRemotePort() {
        return m_remoteSocketAddress.getPort();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return m_remoteSocketAddress;
    }

    @Override
    public long connectionId() {
        return m_handler.connectionId();
    }

    @Override
    public long connectionId(long clientHandle) {
        return connectionId();
    }

    @Override
    public Future<?> unregister() {
        return m_network.unregisterChannel(this);
    }

    @Override
    public void queueTask(Runnable r) {
        m_network.queueTask(r);
    }

}
