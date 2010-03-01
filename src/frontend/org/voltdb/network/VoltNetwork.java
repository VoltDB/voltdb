/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.log4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Set;

import org.voltdb.utils.VoltLoggerFactory;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.EstTimeUpdater;
import org.voltdb.utils.Pair;

/** Produces work for registered ports that are selected for read, write */
 public class VoltNetwork implements Runnable
{
    private final Selector m_selector;
    private static final Logger m_logger =
        Logger.getLogger(VoltNetwork.class.getName(), VoltLoggerFactory.instance());
    private static final Logger networkLog =
        Logger.getLogger("NETWORK", VoltLoggerFactory.instance());
    // keep two lists and swap them in and out to minimize contention
    private final ArrayList<VoltPort> m_selectorUpdates_1 = new ArrayList<VoltPort>();//Used as the lock for swapping lists
    private final ArrayList<VoltPort> m_selectorUpdates_2 = new ArrayList<VoltPort>();
    private ArrayList<VoltPort> m_activeUpdateList = m_selectorUpdates_1;
    private volatile boolean m_shouldStop = false;//volatile boolean is sufficient
    private final ExecutorService m_executor;
    private final Thread m_thread;
    private final HashSet<VoltPort> m_ports = new HashSet<VoltPort>();
    private final boolean m_useBlockingSelect;
    private final boolean m_useExecutorService;
    private final ArrayList<Thread> m_networkThreads = new ArrayList<Thread>();
    private final Runnable m_periodicWork[];
    private final ArrayList<DBBPool> m_poolsToClearOnShutdown = new ArrayList<DBBPool>();

    /**
     * Synchronizes registration and unregistration of channels
     */
    private final ReentrantReadWriteLock m_registrationLock = new ReentrantReadWriteLock();

    /**
     * Start this VoltNetwork's thread;
     */
    public void start() {
        m_thread.start();
    }

    /** Used for test only! */
    public VoltNetwork(Selector selector, ExecutorService executorService) {
        m_thread = null;
        m_selector = selector;
        m_executor = executorService;
        m_periodicWork = new Runnable[0];
        m_useBlockingSelect = true;
        m_useExecutorService = true;
    }

    public Pair<Long, Long> getCounters() {
        long totalRead = 0;
        long totalWritten = 0;
        synchronized (m_ports) {
            for (VoltPort p : m_ports) {
                totalRead += p.readStream().getBytesRead();
                totalWritten += p.writeStream().getBytesWritten();
            }
        }
        return Pair.of(totalRead, totalWritten);
    }

    public VoltNetwork() {
        this( true, true, new Runnable[0], null);
    }

    public VoltNetwork( Runnable periodicWork[]) {
        this( true, true, periodicWork, null);
    }

    /**
     * Initialize a m_selector and become ready to perform real work
     * If the network is not going to provide any threads provideOwnThread should be false
     * and runOnce should be called periodically
     **/
    public VoltNetwork(boolean useExecutorService, boolean blockingSelect, Runnable periodicWork[], Integer threads) {
        m_thread = new Thread(this, "Volt Network");
        m_thread.setDaemon(true);

        m_useExecutorService = useExecutorService;
        m_useBlockingSelect = blockingSelect;
        m_periodicWork = periodicWork;

        try {
            m_selector = Selector.open();
        } catch (IOException ex) {
            m_logger.fatal(null, ex);
            throw new RuntimeException(ex);
        }

        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        if (!useExecutorService) {
            m_executor = null;
            return;
        }

        int threadPoolSize = 1;
        if (threads != null) {
            threadPoolSize = threads.intValue();
        } else if (availableProcessors <= 4) {
            threadPoolSize = 1;
        } else if (availableProcessors <= 8) {
            threadPoolSize = 2;
        } else if (availableProcessors <= 16) {
            threadPoolSize = 2;

        }
        final ThreadFactory tf = new ThreadFactory() {
            private ThreadGroup group = new ThreadGroup(Thread.currentThread().getThreadGroup(), "Network threads");
            private int threadIndex = 0;
            @Override
            public Thread newThread(final Runnable run) {
                final Thread t = new Thread(group, run, "Network Thread - " + threadIndex++) {
                    @Override
                    public void run() {
                        try {
                            run.run();
                        } finally {
                            synchronized (m_poolsToClearOnShutdown) {
                                m_poolsToClearOnShutdown.add(VoltPort.m_pool.get());
                            }
                        }
                    };
                };
                synchronized (m_networkThreads) {
                    m_networkThreads.add(t);
                }
                t.setDaemon(true);
                return t;
            }

        };

        m_executor = Executors.newFixedThreadPool(threadPoolSize, tf);
    }


    /**
     * Lock that causes the selection thread to wait for all threads that
     * are in the process of registering or unregistering channels to finish
     */
    private void waitForRegistrationLock() {
        m_registrationLock.writeLock().lock();
        m_registrationLock.writeLock().unlock();
    }

    /**
     * Acquire a lock that stops the selection thread while a channel is being registered/unregistered
     */
    private void acquireRegistrationLock() {
        m_registrationLock.readLock().lock();
        m_selector.wakeup();
    }

    /**
     * Release a lock that stops the selection thread while a channel is being registered/unregistered
     */
    private void releaseRegistrationLock() {
        m_registrationLock.readLock().unlock();
    }

    /** Instruct the network to stop after the current loop */
    public void shutdown() throws InterruptedException {
        if (m_thread != null) {
            synchronized (this) {
                m_shouldStop = true;
                m_selector.wakeup();
                wait();
            }
            m_thread.join();
        }
    }

    public Connection registerChannel(SocketChannel channel, InputHandler handler) throws IOException {
        return registerChannel(channel, handler, SelectionKey.OP_READ);
    }

    /**
     * Register a channel with the selector and create a Connection that will pass incoming events
     * to the provided handler.
     * @param channel
     * @param handler
     * @throws IOException
     */
    public Connection registerChannel(
            SocketChannel channel,
            InputHandler handler,
            int interestOps) throws IOException {
        channel.configureBlocking (false);
        channel.socket().setKeepAlive(true);

        VoltPort port = new VoltPort( this, handler, handler.getExpectedOutgoingMessageSize());
        synchronized (m_ports) {
            m_ports.add(port);
        }
        port.registering();

        acquireRegistrationLock();
        try {
            SelectionKey key = channel.register (m_selector, interestOps, port);

            port.setKey (key);
            port.registered();

            return port;
        } finally {
            releaseRegistrationLock();
        }
    }

    /**
     * Unregister a channel. The connections streams are not drained before finishing.
     * @param c
     */
    public void unregisterChannel (Connection c) {
        VoltPort port = (VoltPort)c;
        assert(c != null);
        SelectionKey selectionKey = port.getKey();

        acquireRegistrationLock();
        try {
            synchronized (m_ports) {
                if (!m_ports.contains(port)) {
                    return;
                }
            }
            port.unregistering();
            selectionKey.cancel();
            selectionKey.attach(null);
            synchronized (m_ports) {
                m_ports.remove(port);
            }
        } finally {
            releaseRegistrationLock();
        }
        port.unregistered();
    }

    /** Set interest registrations for a port */
    public void addToChangeList(VoltPort port) {
        synchronized (m_selectorUpdates_1) {
            m_activeUpdateList.add(port);
        }
        if (m_useBlockingSelect) {
            m_selector.wakeup();
        }
    }

    @Override
    public void run() {
        while (m_shouldStop == false) {
            try {
                while (m_shouldStop == false) {
                    waitForRegistrationLock();
                    if (m_useBlockingSelect) {
                        m_selector.select(5);
                    } else {
                        m_selector.selectNow();
                    }
                    installInterests();
                    invokeCallbacks(m_useExecutorService);
                    periodicWork();
                }
            } catch (Exception ex) {
                m_logger.error(null, ex);
            }
        }

        p_shutdown();
    }

    private synchronized void p_shutdown() {
        //Synchronized so the interruption won't interrupt the network thread
        //while it is waiting for the executor service to shutdown
        try {
            if (m_executor != null) {
                m_executor.shutdown();
                try {
                    m_executor.awaitTermination( 60, TimeUnit.SECONDS);

                    synchronized (m_networkThreads) {
                        for (final Thread t : m_networkThreads) {
                            if (t != null) {
                                t.join();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    m_logger.error(null, e);
                }
            }

            Set<SelectionKey> keys = m_selector.keys();

            for (SelectionKey key : keys) {
                VoltPort port = (VoltPort) key.attachment();
                if (port != null) {
                    unregisterChannel (port);
                }
            }

            synchronized (m_poolsToClearOnShutdown) {
                for (DBBPool p : m_poolsToClearOnShutdown) {
                    p.clear();
                }
                m_poolsToClearOnShutdown.clear();
            }

            try {
                m_selector.close();
            } catch (IOException e) {
                m_logger.error(null, e);
            }
        } finally {
            this.notifyAll();
        }
    }

    protected void installInterests() {
        // swap the update lists to avoid contention while
        // draining the requested values. also guarantees
        // that the end of the list will be reached if code
        // appends to the update list without bound.
        ArrayList<VoltPort> oldlist;
        synchronized(m_selectorUpdates_1) {
            if (m_activeUpdateList == m_selectorUpdates_1) {
                oldlist = m_selectorUpdates_1;
                m_activeUpdateList = m_selectorUpdates_2;
            }
            else {
                oldlist = m_selectorUpdates_2;
                m_activeUpdateList = m_selectorUpdates_1;
            }
        }

        for (VoltPort port : oldlist) {
            if (port.isRunning()) {
                continue;
            }
            if (port.isDead()) {
                unregisterChannel(port);
                try {
                    port.m_selectionKey.channel().close();
                } catch (IOException e) {}
            } else {
                resumeSelection(port);
            }
        }
        oldlist.clear();
    }

    private void resumeSelection( VoltPort port) {
        SelectionKey key = port.getKey();

        if (key.isValid()) {
            key.interestOps (port.interestOps());
        } else {
            synchronized (m_ports) {
                m_ports.remove(port);
            }
        }
    }

    /** Set the selected interest set on the port and run it. */
    protected void invokeCallbacks(final boolean useExecutor) {
        final Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
        for(SelectionKey key : selectedKeys) {
            final VoltPort port = (VoltPort) key.attachment();
            try {
                port.lockForHandlingWork();
                key.interestOps(0);
                if (useExecutor) {
                    m_executor.execute( new VoltPortFutureTask(port));
                } else {
                    new VoltPortFutureTask(port).run();
                }
            }
            catch (CancelledKeyException e) {
                // no need to do anything here until
                // shutdown makes more sense
            }
        }
        selectedKeys.clear();
    }

    private final void periodicWork() {
        final long fnow = System.currentTimeMillis();
        EstTimeUpdater.update(fnow);
        for (final Runnable r : m_periodicWork) {
            m_executor.execute(r);
        }
    }

    private class VoltPortFutureTask extends FutureTask<VoltPort> {
        private final VoltPort m_port;

        public VoltPortFutureTask (VoltPort p) {
            super (p);
            m_port = p;
        }

        protected void done() {
            try {
                get();
            } catch (ExecutionException e) {
                m_port.die();
                if (e.getCause() instanceof IOException) {
                    m_logger.trace( "VoltPort died, probably of natural causes", e.getCause());
                } else {
                    networkLog.error( "VoltPort died due to an unexpected exception", e.getCause());
                }

            } catch (InterruptedException e) {
                Thread.interrupted();
                m_logger.warn("VoltPort interrupted", e);
            } finally {
                addToChangeList (m_port);
            }
        }
    }
}
