/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

package org.voltcore.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.EstTimeUpdater;
import org.voltcore.utils.Pair;

/** Produces work for registered ports that are selected for read, write */
class VoltNetwork implements Runnable
{
    private final Selector m_selector;
    private static final VoltLogger m_logger = new VoltLogger(VoltNetwork.class.getName());
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");
    private final ConcurrentLinkedQueue<Runnable> m_tasks = new ConcurrentLinkedQueue<Runnable>();
    private volatile boolean m_shouldStop = false;//volatile boolean is sufficient
    private final Thread m_thread;
    private final HashSet<VoltPort> m_ports = new HashSet<VoltPort>();
    final NetworkDBBPool m_pool = new NetworkDBBPool();
    private final String m_coreBindId;

    private final int m_networkId;
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
    VoltNetwork(int networkId, String coreBindId) {
        m_thread = new Thread(this, "Volt Network - " + networkId);
        m_networkId = networkId;
        m_thread.setDaemon(true);
        m_coreBindId = coreBindId;
        try {
            m_selector = Selector.open();
        } catch (IOException ex) {
            m_logger.fatal(null, ex);
            throw new RuntimeException(ex);
        }
    }

    VoltNetwork( Selector s) {
        m_thread = null;
        m_networkId = 0;
        m_selector = s;
        m_coreBindId = null;
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
            final ReverseDNSPolicy dns) throws IOException {
        channel.configureBlocking (false);
        channel.socket().setKeepAlive(true);

        Callable<Connection> registerTask = new Callable<Connection>() {
            @Override
            public Connection call() throws Exception {
                final VoltPort port =
                        new VoltPort(
                                VoltNetwork.this,
                                handler,
                                (InetSocketAddress)channel.socket().getRemoteSocketAddress(),
                                m_pool);
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
        if (m_coreBindId != null) {
            // Remove Affinity for now to make this dependency dissapear from the client.
            // Goal is to remove client dependency on this class in the medium term.
            //PosixJNAAffinity.INSTANCE.setAffinity(m_coreBindId);
        }
        try {
            while (m_shouldStop == false) {
                try {
                    while (m_shouldStop == false) {
                        int readyKeys = 0;
                        if (m_networkId == 0) {
                            readyKeys = m_selector.select(5);
                        } else {
                            readyKeys = m_selector.select();
                        }

                        /*
                         * Run the task queue immediately after selection to catch
                         * any tasks that weren't a result of readiness selection
                         */
                        Runnable task = null;
                        while ((task = m_tasks.poll()) != null) {
                            task.run();
                        }

                        if (readyKeys > 0) {
                            invokeCallbacks();
                        }

                        /*
                         * Poll the task queue again in case new tasks were created
                         * by invoking callbacks.
                         */
                        task = null;
                        while ((task = m_tasks.poll()) != null) {
                            task.run();
                        }

                        if (m_networkId == 0) {
                            Long delta = EstTimeUpdater.update(System.currentTimeMillis());
                            if ( delta != null ) {
                                m_logger.warn("Network was " + delta + " milliseconds late in updating the estimated time");
                            }
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
                } catch (Exception e) {
                    networkLog.error("Exception unregisering port " + port, e);
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
                assert(false); //Shouldn't be running since it is all single threaded now?
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
        }
    }

    private void callPort(final VoltPort port) {
        try {
            port.lockForHandlingWork();
            port.getKey().interestOps(0);
            port.run();
        } catch (CancelledKeyException e) {
            port.m_running = false;
            e.printStackTrace();
            // no need to do anything here until
            // shutdown makes more sense
        } catch (Exception e) {
            port.die();
            if (e instanceof IOException) {
                m_logger.trace( "VoltPort died, probably of natural causes", e);
            } else {
                e.printStackTrace();
                networkLog.error( "VoltPort died due to an unexpected exception", e);
            }
        } finally {
            installInterests(port);
        }
    }

    /** Set the selected interest set on the port and run it. */
    protected void invokeCallbacks() {
        final Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
        for(SelectionKey key : selectedKeys) {
            final Object obj = key.attachment();
            if (obj == null) {
                continue;
            }
            final VoltPort port = (VoltPort) key.attachment();
            callPort(port);
        }
        selectedKeys.clear();
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

    Future<Map<Long, Pair<String, long[]>>> getIOStats(final boolean interval) {
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
}
