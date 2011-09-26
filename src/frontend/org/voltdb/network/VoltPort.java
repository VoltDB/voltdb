/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
/** Encapsulates a socket registration for a VoltNetwork */
public class VoltPort implements Callable<VoltPort>, Connection
{
    /** The network this port participates in */
    private final VoltNetwork m_network;

    private static final VoltLogger networkLog = new VoltLogger("NETWORK");

    /*
     * Thread pool for doing reverse DNS lookups. It will create new threads on
     * demand, until the maximum of 16 threads is reached, in which case it will
     * queue new works.
     */
    private static final ThreadPoolExecutor m_es =
            new ThreadPoolExecutor(0, 16, 1, TimeUnit.SECONDS,
                                   new LinkedBlockingQueue<Runnable>(),
                                   new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "VoltPort DNS Reverse Lookup");
                }
            });

    /** The currently selected operations on this port. */
    private int m_readyOps = 0;

    /** The operations the port wishes to have installed */
    private volatile int m_interestOps = 0;

    /** True when the port is executing  VoltNetwork dispatch */
    volatile boolean m_running = false;

    private volatile boolean m_isDead = false;

    private boolean m_isShuttingDown = false;

    /** Used internally to make operation changes atomic.
     *  External writers (like a foreign host, for example),
     *  must be able to register network writes to a port
     *  that's running. This requires synchronization. */
    private final Object m_lock = new Object();

    // BUG: should be final but currently VoltNetwork.register(), which
    // generates the selection key, takes the port as a parameter. catch-22.
    // Currently, only assigned in VoltNetwork.register(), which shouldn't
    // be called on an already registered Port. As long as writes aren't
    // queued to unregistered ports, this is thread safe.
    protected SelectionKey m_selectionKey;

    /** The channel this port wraps */
    private SocketChannel m_channel;

    private final InputHandler m_handler;

    private NIOReadStream m_readStream;
    private NIOWriteStream m_writeStream;
    private final AtomicLong m_messagesRead = new AtomicLong(0);
    private long m_lastMessagesRead = 0;
    final int m_expectedOutgoingMessageSize;

    /*
     * This variable will be changed to the actual hostname some time later. It
     * is not guaranteed on how long it will take to do the reverse DNS lookup.
     * It is not recommended to use the value of this variable as a key. Use
     * m_remoteIP if you need to identify a host.
     */
    volatile String m_remoteHost = null;
    final String m_remoteIP;
    private String m_toString = null;

    /**
     * Package private so that the thread factory in VoltNetwork can clear the pool when the threads exit.
     */
    static final ThreadLocal<DBBPool> m_pool =
        new ThreadLocal<DBBPool>() {
        @Override
        protected DBBPool initialValue() {
            return new DBBPool();
        }
    };

    /** Wrap a socket with a VoltPort */
    public VoltPort(
            VoltNetwork network,
            InputHandler handler,
            final int expectedOutgoingMessageSize,
            String remoteIP) {
        m_network = network;
        m_handler = handler;
        m_expectedOutgoingMessageSize = expectedOutgoingMessageSize;
        m_remoteIP = remoteIP;
    }

    /**
     * If the port is still alive, start a thread in background to do a reverse
     * DNS lookup of the remote hostname.
     */
    void resolveHostname() {
        synchronized (m_lock) {
            if (!m_running) {
                return;
            }

            /*
             * Start the reverse DNS lookup in background because it might be
             * very slow if the hostname is not specified in local /etc/hosts.
             */
            m_es.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        m_remoteHost = InetAddress.getByName(m_remoteIP).getHostName();
                    } catch (UnknownHostException e) {
                        networkLog.warn("Unable to resolve hostname of host "
                                        + m_remoteIP);
                    }
                }
            });
        }
    }

    void setKey (SelectionKey key) {
        m_selectionKey = key;
        m_channel = (SocketChannel)key.channel();
        java.net.SocketAddress remoteAddress = m_channel.socket().getRemoteSocketAddress();
        m_toString = super.toString() + ":" + (remoteAddress == null ? "null" : remoteAddress.toString());
        m_readStream = new NIOReadStream();
        m_writeStream = new NIOWriteStream(
                this,
                m_handler.offBackPressure(),
                m_handler.onBackPressure(),
                m_handler.writestreamMonitor());
        m_interestOps = key.interestOps();
    }

    /**
     * Lock the VoltPort for running by the VoltNetwork executor service. This prevents anything from sneaking in a messing with
     * the selector set until the executor service has had a chance to handle all the I/O.
     * @param selectedOps
     */
    void lockForHandlingWork() {
        synchronized(m_lock) {
            assert m_running == false;
            m_running = true;
            m_readyOps = 0;
            m_readyOps = m_selectionKey.readyOps();      // runnable.run() doesn't accept parameters
        }
    }

    /** VoltNetwork invokes this to prepare and invoke run() */
    @Override
    public VoltPort call() throws IOException {
        try {
            final DBBPool pool = m_pool.get();

            try {
                /*
                 * Have the read stream fill from the network
                 */
                if (readyForRead()) {
                    final int maxRead = m_handler.getMaxRead();
                    if (maxRead > 0) {
                        fillReadStream( maxRead, pool);
                        ByteBuffer message;

                        /*
                         * Process all the buffered bytes and retrieve as many messages as possible
                         * and pass them off to the input handler.
                         */
                        while ((message = m_handler.retrieveNextMessage( this )) != null) {
                            m_handler.handleMessage( message, this);
                            m_messagesRead.incrementAndGet();
                        }
                    }
                }

                drainWriteStream(pool);
            } finally {

                /*
                 * m_lock is used to protect the m_scheduledRunnables structure and doesn't need to be held
                 * while scheduled runnables are invoked. It can't be held because a scheduled runnable might
                 * need to acquire other locks such as the NIOWriteStream's lock for shutdown.
                 */
                if (m_hasQueuedRunnables) {
                    while (true) {
                        Runnable r;
                        synchronized (m_lock) {
                            if (m_scheduledRunnables.isEmpty()) {
                                m_hasQueuedRunnables = false;
                                break;
                            }
                            r = m_scheduledRunnables.poll();
                        }
                        r.run();
                    }
                }
            }
        } finally {
            synchronized(m_lock) {
                assert(m_running == true);
                m_running = false;
            }
        }

        return this;
    }

    private final int fillReadStream(int maxBytes, final DBBPool pool) throws IOException {
        if ( maxBytes == 0 || m_isShuttingDown)
            return 0;

        final int read = m_readStream.read(m_channel, maxBytes, pool);

        if (read == -1) {
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
        return read;
    }

    private final void drainWriteStream(final DBBPool pool) throws IOException {
        //Safe to do this with a separate embedded synchronization because no interest ops are modded
        final BBContainer results[] = m_writeStream.swapAndSerializeQueuedWrites(pool);

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
            if (!m_writeStream.isEmpty() || results != null)
            {
                m_writeStream.drainTo(m_channel, results);
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

    private void enableWriteSelection() {
        setInterests(SelectionKey.OP_WRITE, 0);
    }

    private void disableWriteSelection() {
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
                m_network.addToChangeList (this);
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
        return (readyOps() & SelectionKey.OP_READ) != 0;
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
    public NIOWriteStream writeStream() {
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

    /**
     * Called when unregistration is complete and the Connection can no
     * longer be interacted with.
     */
    void unregistered() {
        try {
            try {
                m_handler.stopped(this);
            } finally {
                try {
                    m_writeStream.shutdown();
                } finally {
                    m_readStream.shutdown();
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

    public synchronized long getMessagesRead(boolean interval) {
        if (interval) {
            final long messagesRead = m_messagesRead.get();
            final long messagesReadThisTime = messagesRead - m_lastMessagesRead;
            m_lastMessagesRead = messagesRead;
            return messagesReadThisTime;
        } else {
            return m_messagesRead.get();
        }
    }

    @Override
    public String getHostnameOrIP() {
        if (m_remoteHost != null) {
            return m_remoteHost;
        } else {
            return m_remoteIP;
        }
    }

    @Override
    public long connectionId() {
        return m_handler.connectionId();
    }

    private volatile boolean m_hasQueuedRunnables = false;

    /**
     * Has actions waiting to be executed
     */
    public boolean hasQueuedRunnables() {
        return m_hasQueuedRunnables;
    }

    private final ArrayDeque<Runnable> m_scheduledRunnables = new ArrayDeque<Runnable>();

    @Override
    public void scheduleRunnable(Runnable r) {
        synchronized (m_lock) {
            m_hasQueuedRunnables = true;
            m_scheduledRunnables.offer(r);
        }
        m_network.addToChangeList(this);
    }

    @Override
    public void unregister() {
        scheduleRunnable(new Runnable() {
            @Override
            public void run() {
                m_network.unregisterChannel(VoltPort.this);
            }
        });
    }

}
