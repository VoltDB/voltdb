/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * Copyright (c) 2004-2006 Ronsoft Technologies (http://ronsoft.com)
 * Contact Ron Hitchens (ron@ronsoft.com) with questions about this code.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */

package org.voltdb.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientResponseImpl;

/*
 * A client designed to create thousands of connections and distribute
 * work across them
 */
public abstract class BulkClient {

    /**
     * A connection wraps a socket channel and input and output streams
     */
    public class Connection implements Callable<Connection> {
        /**
         * Name of the host this channel is connected to
         */
        public String m_hostname = "";
        private SocketChannel m_channel = null;
        private volatile boolean m_dead = false;
        private final Dispatcher m_dispatcher;

        /**
         * Handler that generates invocations and processes the reuslts
         */
        private final VoltProtocolHandler m_handler;
        private volatile int m_interestOps = 0;
        private SelectionKey m_key = null;
        private final Object m_lock = new Object();
        private NIOReadStream m_readStream;
        private volatile boolean m_running = false;
        private volatile boolean m_sendShutdown = false;
        private boolean m_shuttingDown = false;
        private NIOWriteStream m_writeStream;

        /**
         * Number of invocations to attempt to create next time
         * this Connection is handed off to an executor service.
         */
        public AtomicInteger invocationsToCreate = new AtomicInteger(0);



        @Override
        public String toString() {
            return m_hostname + " " + this.hashCode();
        }
        public Connection(Dispatcher dispatcher, VoltProtocolHandler handler) {
            m_dispatcher = dispatcher;
            m_handler = handler;
        }

        @Override
        public Connection call() throws Exception {
            try {
                if (m_sendShutdown) {
                    NullCallback cb = new NullCallback();
                    (m_handler).invokeProcedure(this, cb, "@Shutdown");
                }

                if (!m_writeStream.isEmpty()) {
                   invocationsToCreate.getAndSet(0);
                } else {
                   generateInvocations(invocationsToCreate.getAndSet(0));
                }
                fillInput(m_handler.getMaxDesiredBytes());

                ByteBuffer message;

                // must process all buffered messages because Selector will
                // not fire again for input that's already read and buffered
                while ((message = m_handler.nextMessage(this)) != null) {
                    m_handler.handleInput(message, this);
                }
                drainOutput();
            } finally {
                synchronized (m_lock) {
                    m_running = false;
                }
            }

            return this;
        }

//        private boolean respondedToBackPressure = false;
        private void generateInvocations(int invocationsToCreate) throws IOException {
            if (!m_writeStream.isEmpty()) {
                return;
            }
            for (int ii = 0; ii < invocationsToCreate; ii++) {
                m_handler.generateInvocation(this);
            }
            return;
        }

        public void die() {
            m_dead = true;
        }

        private void disableReadSelection() {
            modifyInterestOps(0, SelectionKey.OP_READ);
        }

        private void disableWriteSelection() {
            modifyInterestOps(0, SelectionKey.OP_WRITE);
        }

        private void drainOutput() throws IOException {
            /*
             * All interactions with write stream must be protected with a lock.
             */
            synchronized (m_writeStream) {
                /*
                 * If there is something to write give it a whirl.
                 */
                if (!m_writeStream.isEmpty()) {
                    m_writeStream.drainTo(m_channel);
                }

                // Write selection is turned on when output data in enqueued,
                // turn it off when the queue becomes empty.
                if (m_writeStream.isEmpty()) {
                    disableWriteSelection();

                    if (m_shuttingDown) {
                        m_channel.close();
                        m_handler.stopped(this);
                    }
                }
            }
        }

        public void enableWriteSelection() {
            modifyInterestOps(SelectionKey.OP_WRITE, 0);
        }

        private void fillInput(int maxBytes) throws IOException {
            if (maxBytes == 0)
                return;
            if (m_shuttingDown)
                return;

            int rc = m_readStream.fillFrom(m_channel, maxBytes);

            if (rc == -1) {
                disableReadSelection();
                if (m_channel instanceof SocketChannel) {
                    SocketChannel sc = m_channel;

                    if (sc.socket().isConnected()) {
                        try {
                            sc.socket().shutdownInput();
                        } catch (SocketException e) {
                            // happens sometimes, ignore
                        }
                    }
                }

                m_shuttingDown = true;
                m_handler.stopping(this);

                // cause drainOutput to run, which will close
                // the socket if/when the output queue is empty
                enableWriteSelection();
            }
        }

        public NIOReadStream inputStream() {
            return m_readStream;
        }

        public NIOWriteStream writeStream() {
            return m_writeStream;
        }

        public int interestOps() {
            return m_interestOps;
        }

        public boolean isDead() {
            return m_dead;
        }

        public boolean isRunning() {
            return m_running;
        }

        public SelectionKey key() {
            return m_key;
        }

        void lockForHandlingWork() {
            synchronized (m_lock) {
                assert m_running == false;
                m_running = true;
            }
        }

        public void modifyInterestOps(int opsToSet, int opsToReset) {
            synchronized (m_lock) {
                int oldInterestOps = m_interestOps;
                m_interestOps = (m_interestOps | opsToSet) & (~opsToReset);

                if (oldInterestOps != m_interestOps && !m_running) {
                    m_dispatcher.addToChangeList(this);
                }
            }
        }

        void registered() {
            m_handler.started(this);
        }

        void registering() {
            m_handler.starting(this);
        }

        void setKey(SelectionKey key) {
            m_key = key;
            m_channel = (SocketChannel)key.channel();
            m_readStream = new NIOReadStream();
            m_writeStream = new NIOWriteStream(this, m_channel);
            m_interestOps = key.interestOps();
        }

        void unregistered() {
            m_handler.stopped(this);
        }

        void unregistering() {
            m_handler.stopping(this);
        }

        public boolean pushback() {
            if (m_handler.m_callbacks.size() > 200
                    || invocationsToCreate.get() > 200) {
                return true;
            }
            return false;
        }
    }

    /**
     * Implements the simple state machine for the remote controller protocol.
     * Hypothetically, you can extend this and override the answerPoll() and
     * answerStart() methods for other clients.
     */
    class ControlPipe implements Runnable {

        public void answerPoll() {
            StringBuilder txncounts = new StringBuilder();
            synchronized (m_counts) {
                for (int i = 0; i < m_counts.length; ++i) {
                    txncounts.append(",");
                    txncounts.append(m_countDisplayNames[i]);
                    txncounts.append(",");
                    txncounts.append(m_counts[i].get());
                }
            }
            System.out.printf("%d,%s%s\n", System.currentTimeMillis(),
                    m_controlState.display, txncounts.toString());
        }

        public void answerStart() {
            m_dispatcher.start();
        }

        public void answerWithError() {
            System.out.printf("%d,%s,%s\n", System.currentTimeMillis(),
                    m_controlState.display, m_reason);
        }

        @Override
        @SuppressWarnings("finally")
        public void run() {
            String command = "";
            InputStreamReader reader = new InputStreamReader(System.in);
            BufferedReader in = new BufferedReader(reader);

            // transition to ready and send ready message
            if (m_controlState == ControlState.PREPARING) {
                System.out.printf("%d,%s\n", System.currentTimeMillis(),
                        ControlState.READY.display);
                m_controlState = ControlState.READY;
            } else {
                System.err.println("Error - not starting prepared!");
                System.err.println(m_controlState.display + " " + m_reason);
            }

            while (true) {
                try {
                    command = in.readLine();
                } catch (IOException e) {
                    // Hm. quit?
                    System.err.println("Error on standard input: "
                            + e.getMessage());
                    System.exit(-1);
                }

                if (command.equalsIgnoreCase("START")) {
                    if (m_controlState != ControlState.READY) {
                        setState(ControlState.ERROR, "START when not READY.");
                        answerWithError();
                        continue;
                    }
                    answerStart();
                    m_controlState = ControlState.RUNNING;
                } else if (command.equalsIgnoreCase("POLL")) {
                    if (m_controlState != ControlState.RUNNING) {
                        setState(ControlState.ERROR, "POLL when not RUNNING.");
                        answerWithError();
                        continue;
                    }
                    answerPoll();
                } else if (command.equalsIgnoreCase("STOP")) {
                    if (m_controlState == ControlState.RUNNING) {
                        // The shutdown will cause all the DB connections to die
                        // and then the client can return from
                        // the run loop at which point ControlWorker can call
                        // System.exit()
                        try {
                            synchronized (m_connections) {
                                m_connections.get(0).m_sendShutdown = true;
                                m_connections.get(0).enableWriteSelection();
                            }
                        } finally {
                            return;
                        }
                    }
                    System.err.println("Error: STOP when not RUNNING");
                    System.exit(-1);
                } else {
                    System.err
                    .println("Error on standard input: unknown command "
                            + command);
                    System.exit(-1);
                }
            }
        }
    }

    /** The states important to the remote controller */
    public static enum ControlState {
        ERROR(
        "ERROR"), PREPARING("PREPARING"), READY("READY"), RUNNING("RUNNING");

        public final String display;

        ControlState(String displayname) {
            display = displayname;
        }
    }

    /**
     * NIO dispatcher that wraps a selector and does the work of registering/unregistering channels
     * with that selector, updating interest ops on selection keys, and handing off selected keys
     * to an executor service. Also distributes the work of generating invocations across
     * all the registered channels.
     *
     */
    private class Dispatcher implements Runnable {
        private class ConnectionFutureTask extends
        FutureTask<BulkClient.Connection> {
            private final Connection m_connection;

            public ConnectionFutureTask(Connection connection) {
                super(connection);
                this.m_connection = connection;
            }

            @Override
            protected void done() {
                addToChangeList(m_connection);

                try {
                    // Get result returned by call(), or cause
                    // deferred exception to be thrown. We know
                    // the result will be the adapter instance
                    // stored above, so we ignore it.
                    get();

                    // Extension point: You may choose to extend the
                    // InputHandler and HandlerAdapter classes to add
                    // methods for handling these exceptions. This
                    // method is still running in the worker thread.
                } catch (ExecutionException e) {
                    m_connection.die();
//                    e.getCause().printStackTrace();
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    e.printStackTrace();
                }
            }
        }

        private final ExecutorService m_executor;
        private final Selector m_selector;
        private final ReadWriteLock m_selectorGuard = new ReentrantReadWriteLock();
        private final ArrayList<Connection> m_selectorUpdates_1 = new ArrayList<Connection>();
        private final ArrayList<Connection> m_selectorUpdates_2 = new ArrayList<Connection>();
        private ArrayList<Connection> m_activeUpdateList = m_selectorUpdates_1;
        private volatile boolean m_shouldStop = false;

        private final Thread m_thread;

        public Dispatcher(ExecutorService executor) {
            m_thread = new Thread(this, "Dispatcher");

            try {
                m_selector = Selector.open();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            m_executor = executor;
        }

        /**
         * Grab a read lock on the selectorGuard object. A handler thread calls
         * this method when it wants to mutate the state of the Selector. It
         * must call releaserSelectorGuard when it is finished, because
         * selection will not resume until all read locks have been released.
         */
        private void acquireSelectorGuard() {
            m_selectorGuard.readLock().lock();
            m_selector.wakeup();
        }

        public void addToChangeList(Connection c) {
            synchronized (m_selectorUpdates_1) {
                m_activeUpdateList.add(c);
            }
            m_selector.wakeup();
        }

        private void installInterests() {
            // swap the update lists to avoid contention while
            // draining the requested values. also guarantees
            // that the end of the list will be reached if code
            // appends to the update list without bound.
            ArrayList<Connection> oldList = null;
            synchronized (m_selectorUpdates_1) {
                if (m_activeUpdateList == m_selectorUpdates_1) {
                    oldList = m_selectorUpdates_1;
                    m_activeUpdateList = m_selectorUpdates_2;
                } else if (m_activeUpdateList == m_selectorUpdates_2) {
                    oldList = m_selectorUpdates_2;
                    m_activeUpdateList = m_selectorUpdates_1;
                } else {
                    System.out.println("WTFBBW!");
                    System.exit(-1);
                }
            }

            for (Connection c : oldList) {
                if (c.isRunning()) {
                    continue;
                }
                if (c.isDead()) {
                    unregisterChannel(c);
                } else {
                    resumeSelection(c);
                }
            }
            oldList.clear();
        }

        // --------------------------------------------------------

        // Reader lock acquire/release, called by non-selector threads

        protected void invokeCallbacks() {
            final Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
            for (SelectionKey key : selectedKeys) {
                final Connection c = (Connection) key.attachment();
                try {
                    c.lockForHandlingWork();
                    c.key().interestOps(0);
                    m_executor.execute(new ConnectionFutureTask(c));
                } catch (CancelledKeyException e) {
                    // no need to do anything here until
                    // shutdown makes more sense
                }
            }
            selectedKeys.clear();
        }

        private synchronized void p_shutdown() {
            // Synchronized so the interruption won't interrupt the network
            // thread
            // while it is waiting for the executor service to shutdown
            try {
                if (m_executor != null) {
                    m_executor.shutdown();
                    try {
                        m_executor.awaitTermination(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                Set<SelectionKey> keys = m_selector.keys();

                for (SelectionKey key : keys) {
                    Connection port = (Connection) key.attachment();

                    unregisterChannel(port);
                }

                try {
                    m_selector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } finally {
                this.notifyAll();
            }
        }

        public Connection registerChannel(SelectableChannel channel,
                VoltProtocolHandler handler) throws IOException {
            channel.configureBlocking(false);

            Connection connection = new Connection(this, handler);

            connection.registering();

            acquireSelectorGuard();

            try {
                SelectionKey key = channel.register(m_selector,
                        SelectionKey.OP_READ, connection);

                connection.setKey(key);
                connection.registered();

                return connection;
            } finally {
                releaseSelectorGuard();
            }
        }

        /**
         * Undo a previous call to acquireSelectorGuard to indicate that the
         * calling thread no longer needs access to the Selector object.
         */
        private void releaseSelectorGuard() {
            m_selectorGuard.readLock().unlock();
        }

        private void resumeSelection(Connection c) {
            SelectionKey key = c.key();

            if (key.isValid()) {
                key.interestOps(c.interestOps());
            }
        }

        private long lastGeneratedWork;
        @Override
        public void run() {
            lastGeneratedWork = System.currentTimeMillis();
            while (m_shouldStop == false) {
                try {
                    while (m_shouldStop == false) {
                        selectorGuardBarrier();
                        m_selector.selectNow();
                        installInterests();
                        invokeCallbacks();
                        generateWork();
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            p_shutdown();
        }

        /**
         * Generate work based on elapsed time the and the desired rate. If a connection has pushback
         * it will not have work distributed to it. If too many connections have pushback then no work
         * work will be generated.
         */
        private final Random r = new Random();
        private void generateWork() {
            final long now = System.currentTimeMillis();
            final long delta = now - lastGeneratedWork;
            if (now < 1) {
                return;
            }

            int txnsToCreate = (int)(delta * m_txnsPerMillisecond);
            if (txnsToCreate < 1) {
                return;
            }

            /*
             * There is never a reason to create obscene amounts of work at once.
             */
            if (txnsToCreate > 5000) {
                txnsToCreate = 5000;
            }

            lastGeneratedWork = now;
            int abortedAssignmentCount = 0;
            synchronized (m_connections) {
                int txnsForConnection[] = new int[m_connections.size()];
                for (int ii = txnsToCreate; ii > 0; ii--) {
                    for (int zz = 0; zz < 10; zz++) {
                        final int connectionIndex = r.nextInt(m_connections.size());
                        if (m_connections.get(connectionIndex).pushback()) {
                            if (zz == 9) {
                                abortedAssignmentCount++;
                            }
                            continue;
                        }
                        txnsForConnection[connectionIndex]++;
                        break;
                    }
                    if (abortedAssignmentCount == 100) {
                        break;
                    }
                }
                for (int ii = 0; ii < txnsForConnection.length; ii++) {
                    if (txnsForConnection[ii] != 0) {
                        final Connection c = m_connections.get(ii);
                        c.invocationsToCreate.addAndGet(txnsForConnection[ii]);
                        c.enableWriteSelection();
                    }
                }
            }
        }
        /**
         * Called to acquire and then immediately release a write lock on the
         * selectorGuard object. This method is only called by the selection
         * thread and it has the effect of making that thread wait until all
         * read locks have been released.
         */
        private void selectorGuardBarrier() {
            m_selectorGuard.writeLock().lock();
            m_selectorGuard.writeLock().unlock();
        }

        /** Instruct the network to stop after the current loop */
        @SuppressWarnings("unused")
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

        /**
         * Start this Dispatcher's thread;
         */
        public void start() {
            if (m_thread != null) {
                m_thread.start();
            }
        }

        public void unregisterChannel(Connection connection) {
            SelectionKey selectionKey = connection.key();

            acquireSelectorGuard();

            try {
                connection.unregistering();
                selectionKey.cancel();
            } finally {
                releaseSelectorGuard();
            }

            connection.unregistered();
        }
    }

    public static class NIOReadStream {
        static final int BUFFER_SIZE = 8192;

        private final ArrayDeque<ByteBuffer> m_readBuffers = new ArrayDeque<ByteBuffer>();

        private ByteBuffer m_writeBuffer = null;

        int totalAvailable = 0;

        /** @returns the number of bytes available to be read. */
        public int dataAvailable() {
            return totalAvailable;
        }
        /**
         * Read at most maxBytes from the network. Will read until the network
         * would block, the stream is closed or the maximum bytes to read is
         * reached.
         *
         * @param maxBytes
         * @return -1 if closed otherwise total buffered bytes. In all cases,
         *         data may be buffered in the stream - even when the channel is
         *         closed.
         */
        public int fillFrom(ReadableByteChannel channel, int maxBytes)
        throws IOException {
            int bytesRead = 0;
            int lastRead = 0;
            while (bytesRead < maxBytes) {
                if (m_writeBuffer == null) {
                    m_writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);
                }

                lastRead = channel.read(m_writeBuffer);

                if (lastRead > 0) {
                    totalAvailable += lastRead;
                    bytesRead += lastRead;
                    m_writeBuffer.flip();
                    m_readBuffers.add(m_writeBuffer);
                    m_writeBuffer = null;
                }
                // EOF
                if (lastRead < 0 && bytesRead == 0)
                    return -1;

                // Couldn't fill buffer w/o blocking
                if (lastRead < BUFFER_SIZE)
                    return totalAvailable;
            }

            return totalAvailable;
        }

        public void getBytes(byte[] output) {
            if (totalAvailable < output.length) {
                throw new IllegalStateException("Requested " + output.length
                        + " bytes; only have " + totalAvailable
                        + " bytes; call tryRead() first");
            }

            int bytesCopied = 0;
            while (bytesCopied < output.length) {
                ByteBuffer first = m_readBuffers.peekFirst();
                if (first == null) {
                    // Steal the write buffer
                    m_writeBuffer.flip();
                    m_readBuffers.add(m_writeBuffer);
                    first = m_writeBuffer;
                    m_writeBuffer = null;
                }
                assert first.remaining() > 0;

                // Copy bytes from first into output
                int bytesRemaining = first.remaining();
                int bytesToCopy = output.length - bytesCopied;
                if (bytesToCopy > bytesRemaining)
                    bytesToCopy = bytesRemaining;
                first.get(output, bytesCopied, bytesToCopy);
                bytesCopied += bytesToCopy;
                totalAvailable -= bytesToCopy;

                if (first.remaining() == 0) {
                    // read an entire block: move it to the empty buffers list
                    m_readBuffers.poll();
                }
            }
        }
        public int getInt() {
            // TODO: Optimize?
            byte[] intbytes = new byte[4];
            getBytes(intbytes);
            int output = 0;
            for (int i = 0; i < intbytes.length; ++i) {
                output <<= 8;
                output |= (intbytes[i]) & 0xff;
            }
            return output;
        }
    }

    public static class NIOWriteStream {

        private final GatheringByteChannel m_channel;
        private final Connection m_connection;

        private final ArrayDeque<ByteBuffer> m_queue;

        private final boolean m_writeOnEnqueue = false;

        public NIOWriteStream(Connection connection,
                GatheringByteChannel channel) {
            m_connection = connection;
            m_queue = new ArrayDeque<ByteBuffer>();
            m_channel = channel;
        }

        public int drainTo(GatheringByteChannel channel) throws IOException {
            int bytesWritten = 0;
            long rc = 0;
            do {
                ByteBuffer buffers[] = null;
                if (m_queue.isEmpty()) {
                    return bytesWritten;
                }
                buffers = new ByteBuffer[m_queue.size() < 10 ? m_queue.size()
                        : 10];
                int ii = 0;
                for (ByteBuffer b : m_queue) {
                    buffers[ii++] = b;
                    if (ii == 10) {
                        break;
                    }
                }
                rc = 0;
                rc = channel.write(buffers);
                bytesWritten += rc;

                for (ByteBuffer b : buffers) {
                    if (!b.hasRemaining()) {
                        m_queue.poll();
                    } else {
                        break;
                    }
                }
            } while (rc > 0);
            return bytesWritten;
        }

        public boolean enqueue(ByteBuffer b) {
            if (b.remaining() == 0) {
                return false;
            }

            if (m_writeOnEnqueue) {
                if (m_queue.isEmpty()) {
                    try {
                        m_channel.write(b);
                        if (!b.hasRemaining()) {
                            return true;
                        }
                    } catch (IOException e) {
                        m_connection.die();
                    }
                }
            }
            m_queue.add(b);
            return true;
        }

        public boolean isEmpty() {
            return m_queue.isEmpty();
        }
    }

    /**
     * Base class for client specific implementations. Handles the work of serializing procedure invocations
     * and invoking callbacks  when messages are received.
     *
     */
    public abstract class VoltProtocolHandler {

        /** serial number of this VoltPort */
        private final int m_connectionId;
        private int m_nextLength;
        /** messages read by this connection */
        private int m_sequenceId;
        private final AtomicLong m_handle = new AtomicLong(Long.MIN_VALUE);

        protected HashMap<Long, ProcedureCallback> m_callbacks = new HashMap<Long, ProcedureCallback>();


        public VoltProtocolHandler() {
            m_sequenceId = 0;
            m_connectionId = m_globalConnectionCounter.incrementAndGet();
        }

        public int connectionId() {
            return m_connectionId;
        }

        /**
         * Derived class should generate a single stored procedure invocation based on its own logic.
         * @param c Connection to be passed to invokeProcedure when generating invocation
         * @throws IOException
         */
        protected abstract void generateInvocation(Connection c) throws IOException;

        /**
         * Retrieve the next message from the specified connection. Retrieves the next length preceded message
         * @param connection Connection to read the next message from
         * @return ByteBuffer containing the next message
         */
        public ByteBuffer nextMessage(Connection connection) {
            final NIOReadStream inputStream = connection.inputStream();
            ByteBuffer result = null;

            if (m_nextLength == 0
                    && inputStream.dataAvailable() > (Integer.SIZE / 8)) {
                m_nextLength = inputStream.getInt();
                assert m_nextLength > 0;
            }
            if (m_nextLength > 0
                    && inputStream.dataAvailable() >= m_nextLength) {
                result = ByteBuffer.allocate(m_nextLength);
                inputStream.getBytes(result.array());
                m_nextLength = 0;
                m_sequenceId++;
            }
            return result;
        }

        public int sequenceId() {
            return m_sequenceId;
        }

        public void started(Connection c) {
            // TODO Auto-generated method stub

        }

        public void starting(Connection c) {
            // TODO Auto-generated method stub

        }

        public void stopped(Connection c) {
            synchronized (m_connections) {
                m_connections.remove(c);
                if (m_connections.isEmpty()) {
                    System.exit(0);
                }
            }
        }

        public void stopping(Connection c) {
            // TODO Auto-generated method stub

        }

        public int getMaxDesiredBytes() {
            return 8192 * 4;
        }

        /**
         * Handle an incoming message
         * @throws IOException
         */
        public void handleInput(ByteBuffer message, Connection connection) throws IOException {
            ClientResponseImpl response = new ClientResponseImpl();
            response.initFromBuffer(message);
            ProcedureCallback cb = null;
            cb = m_callbacks.remove(response.getClientHandle());
            if (cb != null) {
                try {
                    cb.clientCallback(response);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
            else {
                // TODO: what's the right error path here?
                assert(false);
                System.err.println("Invalid response: no callback");
            }
        }

        /**
         * Invoke a stored procedure
         * @param connection
         * @param callback
         * @param procName
         * @param parameters
         * @throws IOException
         */
        protected void invokeProcedure(Connection connection, ProcedureCallback callback, String procName,
                Object... parameters) throws IOException {
            final long handle = m_handle.getAndIncrement();
            ProcedureInvocation invocation = new ProcedureInvocation(
                        handle, procName, -1, parameters);
            m_callbacks.put(handle, callback);
            ByteBuffer buf = ByteBuffer.allocate(invocation.getSerializedSize());
            connection.writeStream().enqueue(invocation.flattenToBuffer(buf));
        }
    }

    private static AtomicInteger m_globalConnectionCounter = new AtomicInteger();

    /**
     * List of connections that have been created. Synchronized on before access or modification.
     */
    private final ArrayList<Connection> m_connections = new ArrayList<Connection>();

    /**
     * Manage input and output to the framework
     */
    private final ControlPipe m_controlPipe = new ControlPipe();

    /**
     * State of this client
     */
    private volatile ControlState m_controlState = ControlState.PREPARING;

    /**
     * Display names for each transaction.
     */
    protected String m_countDisplayNames[];

    /**
     * Count of transactions invoked by this client. This is updated by derived
     * classes directly
     */
    protected AtomicInteger m_counts[];

    private final ExecutorService m_executor = Executors
        .newFixedThreadPool(CoreUtils.availableProcessors());

    private final Dispatcher m_dispatcher = new Dispatcher(m_executor);



    /**
     * Password supplied to the Volt client
     */
    private final String m_password;

    /**
     * Storage for error descriptions
     */
    private String m_reason = "";

    /**
     * Rate at which transactions should be generated. If set to -1 the rate
     * will be controlled by the derived class. Rate is in transactions per
     * second
     */
    @SuppressWarnings("unused")
    private final int m_txnRate;;

    /**
     * Number of transactions to generate for every millisecond of time that
     * passes
     */
    private final double m_txnsPerMillisecond;

    /**
     * Username supplied to the Volt client
     */
    private final String m_username;

    private final int m_numConnections;

    /**
     * Store hostnames that will be connected to in start()
     */
    private final ArrayList<String> m_hosts = new ArrayList<String>();

    /**
     * Constructor that initializes the framework portions of the client.
     * Creates a Volt client and connects it to all the hosts provided on the
     * command line with the specified username and password
     *
     * @param args
     */
    public BulkClient(String args[]) {
        /*
         * Input parameters: HOST=host:port (may occur multiple times)
         * USER=username PASSWORD=password
         */

        // default values
        String username = "";
        String password = "";
        ControlState state = ControlState.PREPARING; // starting state
        String reason = ""; // and error string
        int transactionRate = 1;
        int numConnections = 1;

        // scan the inputs once to read everything but host names
        for (String arg : args) {
            String[] parts = arg.split("=", 2);
            if (parts.length == 1) {
                state = ControlState.ERROR;
                reason = "Invalid parameter: " + arg;
                break;
            } else if (parts[1].startsWith("${")) {
                continue;
            } else if (parts[0].equals("USER")) {
                username = parts[1];
            } else if (parts[0].equals("PASSWORD")) {
                password = parts[1];
            } else if (parts[0].equals("NUMCONNECTIONS")) {
                numConnections = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("TXNRATE")) {
                transactionRate = Integer.parseInt(parts[1]);
            }
        }

        m_numConnections = numConnections;
        m_username = username;
        m_password = password;
        m_txnRate = transactionRate;

        // report any errors that occurred before the client was instantiated
        if (state != ControlState.PREPARING)
            setState(state, reason);

        // scan the inputs again looking for host connections
        for (String arg : args) {
            String[] parts = arg.split("=", 2);
            if (parts.length == 1) {
                continue;
            } else if (parts[0].equals("HOST")) {
                String hostnport[] = parts[1].split("\\:", 2);
                m_hosts.add(hostnport[0]);
            }
        }

        m_txnsPerMillisecond = (transactionRate / 1000.0);
        System.err.println("Transactions per millisecond " + m_txnsPerMillisecond);
        System.err.println(Runtime.getRuntime().maxMemory() + " max memory, " + Runtime.getRuntime().freeMemory() + " free memory, " + Runtime.getRuntime().totalMemory() + " total memory");

        m_countDisplayNames = getTransactionDisplayNames();
        m_counts = new AtomicInteger[m_countDisplayNames.length];
        for (int ii = 0; ii < m_counts.length; ii++) {
            m_counts[ii] = new AtomicInteger(0);
        }
    }

    /**
     * Convert the task of creating connections into tasks for an ExecutorService. Useful to parallelize
     * this trivial blocking operation using a cached thread pool.
     * @param hostname
     * @param connectionCount
     * @param executor
     * @throws UnknownHostException
     * @throws IOException
     */
    private void createConnection(final String hostname, int connectionCount, ExecutorService executor)
    throws UnknownHostException, IOException {
        System.err.println("Creating " + connectionCount + " connections to " + hostname);
        for (int ii = 0; ii < connectionCount; ii++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        InetSocketAddress addr = new InetSocketAddress(
                                hostname, 21212);
                        SocketChannel aChannel = SocketChannel.open(addr);
                        assert (aChannel.isConnected());
                        if (!aChannel.isConnected()) {
                            // TODO Can open() be asynchronous if
                            // configureBlocking(true)?
                            throw new IOException("Failed to open host "
                                    + hostname);
                        }

                        /*
                         * Send login info
                         */
                        aChannel.configureBlocking(true);
                        MessageDigest md = null;
                        try {
                            md = MessageDigest.getInstance("SHA-1");
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                        byte passwordHash[] = md.digest(m_password.getBytes());
                        ByteBuffer b = ByteBuffer.allocate(m_username
                                .getBytes().length
                                + passwordHash.length + 8);
                        b.putInt(m_username.getBytes().length);
                        b.put(m_username.getBytes());
                        b.putInt(passwordHash.length);
                        b.put(passwordHash);
                        b.flip();
                        aChannel.write(b);

                        ByteBuffer loginResponse = ByteBuffer.allocate(1);
                        aChannel.read(loginResponse);
                        loginResponse.flip();
                        if (loginResponse.get() != 0) {
                            aChannel.close();
                            throw new IOException("Authentication rejected");
                        }
                        aChannel.configureBlocking(false);
                        aChannel.socket().setReceiveBufferSize(262144);
                        aChannel.socket().setSendBufferSize(262144);
                        final Connection c = m_dispatcher.registerChannel(
                                aChannel, getNewInputHandler());
                        c.m_hostname = hostname;
                        synchronized (m_connections) {
                            m_connections.add(c);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    /**
     * Derived classes implementing a main that will be invoked at the start of the app should
     * call this main to instantiate themselves
     * @param clientClass Derived class to instantiate
     * @param args
     */
    public static void main(Class<? extends BulkClient> clientClass, String args[]) {
        try {
            Constructor<? extends BulkClient> constructor = clientClass.getConstructor(new Class<?>[] { new String[0].getClass() });
            BulkClient bulkClient = constructor.newInstance(new Object[] {args});
            bulkClient.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    // update the client state and start waiting for a message.
    private void start() {
        int numHosts = 0;
        ExecutorService es = Executors.newCachedThreadPool();
        for (String host : m_hosts) {
            try {
                System.err.println("Creating connection to  "
                        + host);
                createConnection(host, m_numConnections, es);
                System.err.println("Created connection.");
                numHosts++;
            } catch (Exception ex) {
                setState(ControlState.ERROR, "createConnection to " + host
                        + " failed: " + ex.getMessage());
            }
        }

        es.shutdown();
        try {
            es.awaitTermination( 1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        if (numHosts == 0)
            setState(ControlState.ERROR, "No HOSTS specified on command line.");

        m_controlPipe.run();
    }

    /**
     * Implementation to be provided by derived classes implementing specific clients.
     * Create a new VoltProtocolHandler that will be associated with a single connection.
     * It will be responsible for generating all invocations for this connection
     * and processing are responses
     * @return New VoltProtocolHandler
     */
    protected abstract VoltProtocolHandler getNewInputHandler();

    /**
     * Get the display names of the transactions that will be invoked by the
     * dervied class. As a side effect this also retrieves the number of
     * transactions that can be invoked.
     *
     * @return
     */
    abstract protected String[] getTransactionDisplayNames();

    public void setState(ControlState state, String reason) {
        m_controlState = state;
        if (m_reason.equals("") == false)
            m_reason += (" " + reason);
        else
            m_reason = reason;
    }
}
