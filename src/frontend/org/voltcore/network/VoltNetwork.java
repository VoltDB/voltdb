/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

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
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLEngine;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.VoltNetworkPool.IOStatsIntf;
import org.voltcore.utils.LatencyWatchdog;
import org.voltcore.utils.Pair;

import com.google_voltpatches.common.util.concurrent.SettableFuture;

import io.netty_voltpatches.NinjaKeySet;
import jsr166y.ThreadLocalRandom;

/** Produces work for registered ports that are selected for read, write */
class VoltNetwork implements Runnable, IOStatsIntf
{
    private final Selector m_selector;
    private static final VoltLogger m_logger = new VoltLogger(VoltNetwork.class.getName());
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");
    private final ConcurrentLinkedQueue<Runnable> m_tasks = new ConcurrentLinkedQueue<Runnable>();
    private volatile boolean m_shouldStop = false;//volatile boolean is sufficient
    private final Thread m_thread;
    private final HashSet<VoltPort> m_ports = new HashSet<VoltPort>();
    private final AtomicInteger m_numPorts = new AtomicInteger();
    final NetworkDBBPool m_pool = new NetworkDBBPool();
    private final String m_coreBindId;
    final String networkThreadName;

    private final NinjaKeySet m_ninjaSelectedKeys;

    /**
     * Start this VoltNetwork's thread;
     */
    void start() {
        m_thread.start();
    }

    /**
     * Initialize a m_selector and become ready to perform real work
     * If the network is not going to provide any threads provideOwnThread should be false
     * and runOnce should be called periodically
     **/
    VoltNetwork(int networkId, String coreBindId, String networkName) {
        m_thread = new Thread(this, "Volt " + networkName + " Network - " + networkId);
        networkThreadName = new String("Volt " + networkName + " Network - " + networkId);
        m_thread.setDaemon(true);
        m_coreBindId = coreBindId;
        try {
            m_selector = Selector.open();
        } catch (IOException ex) {
            m_logger.fatal(null, ex);
            throw new RuntimeException(ex);
        }
        m_ninjaSelectedKeys = NinjaKeySet.instrumentSelector(m_selector);
    }

    VoltNetwork( Selector s) {
        m_thread = null;
        m_selector = s;
        m_coreBindId = null;
        networkThreadName = new String("Test Selector Thread");
        m_ninjaSelectedKeys = NinjaKeySet.instrumentSelector(m_selector);
    }

    /** Instruct the network to stop after the current loop */
    void shutdown() throws InterruptedException {
        m_shouldStop = true;
        if (m_thread != null) {
            m_selector.wakeup();
            m_thread.join();
        }
    }

    /**
     * Helps {@link VoltPort} discern cases when the network is shutting down
     */
    boolean isStopping() {
        return m_shouldStop;
    }

    /**
     * Register a channel with the selector and create a Connection that will pass incoming events
     * to the provided handler.
     * @param channel
     * @param handler
     * @throws IOException
     */
    Connection registerChannel(
            final SocketChannel channel,
            final InputHandler handler,
            final int interestOps,
            final ReverseDNSPolicy dns,
            final CipherExecutor cipherService,
            final SSLEngine sslEngine) throws IOException {

        synchronized(channel.blockingLock()) {
            channel.configureBlocking (false);
            channel.socket().setKeepAlive(true);
        }

        Callable<Connection> registerTask = new Callable<Connection>() {
            @Override
            public Connection call() throws Exception {
                final VoltPort port = VoltPortFactory.createVoltPort(
                                channel,
                                VoltNetwork.this,
                                handler,
                                (InetSocketAddress)channel.socket().getRemoteSocketAddress(),
                                m_pool,
                                cipherService,
                                sslEngine);
                port.registering();

                /*
                 * This means we are used by a client. No need to wait then, trigger
                 * the reverse DNS lookup now.
                 */
                if (dns != ReverseDNSPolicy.NONE) {
                    port.resolveHostname(dns == ReverseDNSPolicy.SYNCHRONOUS);
                }

                try {
                    SelectionKey key = channel.register (m_selector, interestOps, null);

                    port.setKey (key);
                    port.registered();

                    //Fix a bug witnessed on the mini where the registration lock and the selector wakeup contained
                    //within was not enough to prevent the selector from returning the port after it was registered,
                    //but before setKey was called. Suspect a bug in the selector.wakeup() or register() implementation
                    //on the mac.
                    //The null check in invokeCallbacks will catch the null attachment, continue, and do the work
                    //next time through the selection loop
                    key.attach(port);

                    return port;
                } finally {
                    m_ports.add(port);
                    m_numPorts.incrementAndGet();
                }
            }
        };

        FutureTask<Connection> ft = new FutureTask<Connection>(registerTask);
        m_tasks.offer(ft);
        m_selector.wakeup();

        try {
            return ft.get();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private Runnable getUnregisterRunnable(final Connection c) {
        return new Runnable() {
            @Override
            public void run() {
                VoltPort port = (VoltPort)c;
                assert(c != null);
                SelectionKey selectionKey = port.getKey();

                try {
                    if (!m_ports.contains(port)) {
                        return;
                    }
                    try {
                        port.unregistering();
                    } finally {
                        try {
                            selectionKey.attach(null);
                            selectionKey.cancel();
                        } finally {
                            m_ports.remove(port);
                            m_numPorts.decrementAndGet();
                        }
                    }
                } finally {
                    port.unregistered();
                }
            }
        };
    }

    /**
     * Unregister a channel. The connections streams are not drained before finishing.
     * @param c
     */
    Future<?> unregisterChannel (Connection c) {
        FutureTask<Object> ft = new FutureTask<Object>(getUnregisterRunnable(c), null);
        m_tasks.offer(ft);
        m_selector.wakeup();
        return ft;
    }

    void addToChangeList(final VoltPort port) {
        addToChangeList( port, false);
    }

    /** Set interest registrations for a port */
    void addToChangeList(final VoltPort port, final boolean runFirst) {
        if (runFirst) {
            m_tasks.offer(new Runnable() {
                @Override
                public void run() {
                    callPort(port);
                }
            });
        } else {
            m_tasks.offer(new Runnable() {
                @Override
                public void run() {
                    installInterests(port);
                }
            });
        }
        m_selector.wakeup();
    }

    @Override
    public void run() {
        final ThreadLocalRandom r = ThreadLocalRandom.current();
        if (m_coreBindId != null) {
            // Remove Affinity for now to make this dependency dissapear from the client.
            // Goal is to remove client dependency on this class in the medium term.
            //PosixJNAAffinity.INSTANCE.setAffinity(m_coreBindId);
        }
        try {
            while (m_shouldStop == false) {
                try {
                    while (m_shouldStop == false) {
                        LatencyWatchdog.pet();

                        final int readyKeys = m_selector.select();

                        /*
                         * Run the task queue immediately after selection to catch
                         * any tasks that weren't a result of readiness selection
                         */
                        Runnable task = null;
                        while ((task = m_tasks.poll()) != null) {
                            task.run();
                        }

                        if (readyKeys > 0) {
                            if (NinjaKeySet.supported) {
                                optimizedInvokeCallbacks(r);
                            } else {
                                invokeCallbacks(r);
                            }
                        }

                        /*
                         * Poll the task queue again in case new tasks were created
                         * by invoking callbacks.
                         */
                        task = null;
                        while ((task = m_tasks.poll()) != null) {
                            task.run();
                        }
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                    m_logger.error(null, ex);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            try {
                p_shutdown();
            } catch (Throwable t) {
                m_logger.error("Error shutting down Volt Network", t);
                t.printStackTrace();
            }
        }
    }

    private void p_shutdown() {
        Set<SelectionKey> keys = m_selector.keys();

        for (SelectionKey key : keys) {
            VoltPort port = (VoltPort) key.attachment();
            if (port != null) {
                try {
                    getUnregisterRunnable(port).run();
                } catch (Throwable e) {
                    networkLog.error("Exception unregistering port " + port, e);
                }
            }
        }

        m_pool.clear();

        try {
            m_selector.close();
        } catch (IOException e) {
            m_logger.error(null, e);
        }
    }

    void installInterests(VoltPort port) {
        try {
            if (port.isRunning()) {
                assert(false) : "Shouldn't be running since it is all single threaded now?";
                return;
            }

            if (port.isDead()) {
                getUnregisterRunnable(port).run();
                try {
                    port.m_selectionKey.channel().close();
                } catch (IOException e) {}
            } else {
                resumeSelection(port);
            }
        } catch (java.nio.channels.CancelledKeyException e) {
            networkLog.warn(
                    "Had a cancelled key exception while processing queued runnables for port "
                    + port, e);
        }
    }

    private void resumeSelection( VoltPort port) {
        SelectionKey key = port.getKey();

        if (key.isValid()) {
            key.interestOps (port.interestOps());
        } else {
            m_ports.remove(port);
            m_numPorts.decrementAndGet();
        }
    }

    private void callPort(final VoltPort port) {
        try {
            port.lockForHandlingWork();
            port.getKey().interestOps(0);
            port.run();
        } catch (CancelledKeyException e) {
            port.m_running = false;
            // no need to do anything here until
            // shutdown makes more sense
        } catch (Exception e) {
            port.die();
            final String trimmed = e.getMessage() == null ? "" : e.getMessage().trim();
            if ((e instanceof IOException && (trimmed.equalsIgnoreCase("Connection reset by peer") || trimmed.equalsIgnoreCase("broken pipe"))) ||
                    e instanceof AsynchronousCloseException ||
                    e instanceof ClosedChannelException ||
                    e instanceof ClosedByInterruptException) {
                m_logger.debug( "Connection closed", e);
            } else {
                e.printStackTrace();
                networkLog.warn( "Connection closed unexpectedly", e);
            }
        } finally {
            installInterests(port);
        }
    }

    /** Set the selected interest set on the port and run it. */
    protected void invokeCallbacks(ThreadLocalRandom r) {
        final Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
        final int keyCount = selectedKeys.size();
        int startInx = r.nextInt(keyCount);
        int itInx = 0;
        Iterator<SelectionKey> it = selectedKeys.iterator();
        while(itInx < startInx) {
            it.next();
            itInx++;
        }
        while(itInx < keyCount) {
            final Object obj = it.next().attachment();
            if (obj == null) {
                continue;
            }
            final VoltPort port = (VoltPort)obj;
            callPort(port);
            itInx++;
        }
        itInx = 0;
        it = selectedKeys.iterator();
        while(itInx < startInx) {
            final Object obj = it.next().attachment();
            if (obj == null) {
                continue;
            }
            final VoltPort port = (VoltPort)obj;
            callPort(port);
            itInx++;
        }
        selectedKeys.clear();
    }

    protected void optimizedInvokeCallbacks(ThreadLocalRandom r) {
        final int numKeys = m_ninjaSelectedKeys.size();
        final int startIndex = r.nextInt(numKeys);
        final SelectionKey keys[] = m_ninjaSelectedKeys.keys();
        for (int ii = startIndex; ii < numKeys; ii++) {
            final Object obj = keys[ii].attachment();
            if (obj == null) {
                continue;
            }
            final VoltPort port = (VoltPort) obj;
            callPort(port);
        }

        for (int ii = 0; ii < startIndex; ii++) {
            final Object obj = keys[ii].attachment();
            if (obj == null) {
                continue;
            }
            final VoltPort port = (VoltPort)obj;
            callPort(port);
        }
        m_ninjaSelectedKeys.clear();
    }

    private Map<Long, Pair<String, long[]>> getIOStatsImpl(boolean interval) {
        final HashMap<Long, Pair<String, long[]>> retval =
                new HashMap<Long, Pair<String, long[]>>();
        long totalRead = 0;
        long totalMessagesRead = 0;
        long totalWritten = 0;
        long totalMessagesWritten = 0;
        for (VoltPort p : m_ports) {
            final long read = p.readStream().getBytesRead(interval);
            final long writeInfo[] = p.writeStream().getBytesAndMessagesWritten(interval);
            final long messagesRead = p.getMessagesRead(interval);
            totalRead += read;
            totalMessagesRead += messagesRead;
            totalWritten += writeInfo[0];
            totalMessagesWritten += writeInfo[1];
            retval.put(
                    p.connectionId(),
                    Pair.of(
                            p.getHostnameOrIP(),
                            new long[] {
                                    read,
                                    messagesRead,
                                    writeInfo[0],
                                    writeInfo[1] }));
        }
        retval.put(
                -1L,
                Pair.of(
                        "GLOBAL",
                        new long[] {
                                totalRead,
                                totalMessagesRead,
                                totalWritten,
                                totalMessagesWritten }));
        return retval;
    }

    @Override
    public Future<Map<Long, Pair<String, long[]>>> getIOStats(final boolean interval) {
        Callable<Map<Long, Pair<String, long[]>>> task = new Callable<Map<Long, Pair<String, long[]>>>() {
            @Override
            public Map<Long, Pair<String, long[]>> call() throws Exception {
                return getIOStatsImpl(interval);
            }
        };

        FutureTask<Map<Long, Pair<String, long[]>>> ft = new FutureTask<Map<Long, Pair<String, long[]>>>(task);

        m_tasks.offer(ft);
        m_selector.wakeup();

        return ft;
    }

    Long getThreadId() {
        return m_thread.getId();
    }

    void queueTask(Runnable r) {
        m_tasks.offer(r);
        m_selector.wakeup();
    }

    int numPorts() {
        return m_numPorts.get();
    }

    public Future<Set<Connection>> getConnections() {
        final SettableFuture<Set<Connection>> connectionsFuture = SettableFuture.create();
        queueTask(new Runnable() {
            @Override
            public void run() {
                // Make a copy of m_ports to avoid concurrent modification
                connectionsFuture.set(new HashSet<Connection>(m_ports));
            }
        });
        return connectionsFuture;
    }
}
