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

package org.voltdb.client;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLEngine;
import javax.security.auth.Subject;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
import org.ietf.jgss.Oid;
import org.voltcore.network.ReverseDNSCache;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.ssl.MessagingChannel;
import org.voltdb.ClientResponseImpl;
import org.voltdb.common.Constants;
import org.voltdb.utils.SerializationHelper;

import com.google_voltpatches.common.annotations.VisibleForTesting;
import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Optional;
import com.google_voltpatches.common.base.Predicates;
import com.google_voltpatches.common.collect.FluentIterable;

/**
 * A utility class for opening a connection to a Volt server and authenticating as well
 * as sending invocations and receiving responses. It is safe to queue multiple requests
 * @author aweisberg
 *
 */
public class ConnectionUtil {

    private static class TF implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(null, r, "Yet another thread", 65536);
        }
    }

    private static final TF m_tf = new TF();

    public static class ExecutorPair {
        public final ExecutorService m_writeExecutor;
        public final ExecutorService m_readExecutor;
        public ExecutorPair() {
            m_writeExecutor = Executors.newSingleThreadExecutor(m_tf);
            m_readExecutor = Executors.newSingleThreadExecutor(m_tf);
        }

        private void shutdown() throws InterruptedException {
            m_readExecutor.shutdownNow();
            m_writeExecutor.shutdownNow();
            m_readExecutor.awaitTermination(1, TimeUnit.DAYS);
            m_writeExecutor.awaitTermination(1, TimeUnit.DAYS);
        }
    }

    /**
     * Thread instance which delays running a runnable until a delay time is reached or it is canceled.
     * <p>
     * A very similar functionality can be achieved using a {@link java.util.concurrent.ScheduledExecutorService} but in
     * this use case there is no obvious life span for the pool so it is easier to use a fire and forget thread
     */
    @VisibleForTesting
    static final class DelayedExecutionThread extends Thread {
        // nano time which needs to elapse before runnable is executed
        private final long m_runAtNanos;
        // runnable to execute after delay
        private final Runnable m_runnable;
        // current state of the thread
        private volatile State m_state = State.NOT_STARTED;

        public DelayedExecutionThread(long delay, TimeUnit unit, Runnable onTimeout) {
            super(null, null, "Delayed Execution Thread " + unit.toMillis(delay) + "ms", CoreUtils.SMALL_STACK_SIZE);
            m_runAtNanos = unit.toNanos(delay) + System.nanoTime();
            m_runnable = onTimeout;
            setDaemon(true);
        }

        @Override
        public synchronized void run() {
            if (m_state == State.CANCELED) {
                // Already canceled before even being started
                return;
            }

            if (m_state != State.NOT_STARTED) {
                throw new IllegalStateException("Not in state " + State.NOT_STARTED + ": " + m_state);
            }

            setState(State.WAITING);

            long now;
            while (m_state == State.WAITING && (now = System.nanoTime()) <= m_runAtNanos) {
                try {
                    wait(Math.max(1, TimeUnit.NANOSECONDS.toMillis(now - m_runAtNanos)));
                } catch (InterruptedException e) {
                    // Ignore interruptions
                }
            }

            if (!m_state.m_done) {
                setState(State.RUNNING);
                if (m_runnable != null) {
                    m_runnable.run();
                }
                setState(State.COMPLETED);
            }
        }

        /**
         * Cancel the thread if it has not completed yet or is not running. This call will block if the {@code runnable}
         * is being executed.
         *
         * @return {@code true} if the thread was canceled and {@code runnable} has not and will not run
         */
        public synchronized boolean cancel() {
            if (!m_state.m_done) {
                setState(State.CANCELED);
            }
            return m_state == State.CANCELED;
        }

        /**
         * Block until a done state is reached. {@link State#COMPLETED} or {@link State#CANCELED}
         *
         * @return The done {@link State} of the thread
         * @throws InterruptedException
         */
        public synchronized State waitUntilDone() throws InterruptedException {
            while (!m_state.m_done) {
                wait();
            }
            return m_state;
        }

        /**
         * @return The current {@link State} of this thread
         */
        public State state() {
            return m_state;
        }

        private void setState(State state) {
            m_state = state;
            if (state.m_done) {
                notifyAll();
            }
        }

        public enum State {
            NOT_STARTED, WAITING, RUNNING, COMPLETED(true), CANCELED(true);

            public final boolean m_done;

            private State() {
                this(false);
            }

            private State(boolean done) {
                this.m_done = done;
            }
        }
    }

    private static final HashMap<SocketChannel, ExecutorPair> m_executors =
        new HashMap<SocketChannel, ExecutorPair>();
    private static final AtomicLong m_handle = new AtomicLong(Long.MIN_VALUE);

    private static final GSSManager m_gssManager = GSSManager.getInstance();

    /**
     * Get a hashed password using SHA-1 in a consistent way.
     * @param password The password to encode.
     * @return The bytes of the hashed password.
     */
    public static byte[] getHashedPassword(String password) {
        return getHashedPassword(ClientAuthScheme.HASH_SHA256, password);
    }

    /**
     * Get a hashed password using SHA-1 in a consistent way.
     * @param scheme hashing scheme for password.
     * @param password The password to encode.
     * @return The bytes of the hashed password.
     */
    public static byte[] getHashedPassword(ClientAuthScheme scheme, String password) {
        if (password == null) {
            return null;
        }

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance(ClientAuthScheme.getDigestScheme(scheme));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        byte hashedPassword[] = null;
        hashedPassword = md.digest(password.getBytes(Constants.UTF8ENCODING));
        return hashedPassword;
    }

    /**
     * Create a connection to a Volt server and authenticate the connection.
     * @param host
     * @param username
     * @param hashedPassword
     * @param port
     * @param subject
     * @throws IOException
     * @returns An array of objects. The first is an
     * authenticated socket channel, the second. is an array of 4 longs -
     * Integer hostId, Long connectionId, Long timestamp (part of instanceId), Int leaderAddress (part of instanceId).
     * The last object is the build string
     */
    public static Object[] getAuthenticatedConnection(String host, String username,
                                                      byte[] hashedPassword, int port,
                                                      final Subject subject, ClientAuthScheme scheme,
                                                      long timeoutMillis) throws IOException {
        String service = subject == null ? "database" : Constants.KERBEROS;
        return getAuthenticatedConnection(service, host, username, hashedPassword, port, subject, scheme, null, timeoutMillis);
    }

    public static Object[] getAuthenticatedConnection(String host, String username,
                                                      byte[] hashedPassword, int port,
                                                      final Subject subject, ClientAuthScheme scheme, SSLEngine sslEngine,
                                                      long timeoutMillis) throws IOException {
        String service = subject == null ? "database" : Constants.KERBEROS;
        return getAuthenticatedConnection(service, host, username, hashedPassword, port, subject, scheme, sslEngine, timeoutMillis);
    }

    private static Object[] getAuthenticatedConnection(
            String service, String host,
            String username, byte[] hashedPassword, int port, final Subject subject, ClientAuthScheme scheme, SSLEngine sslEngine,
            long timeoutMillis)
    throws IOException {
        InetSocketAddress address = new InetSocketAddress(host, port);
        return getAuthenticatedConnection(service, address, username, hashedPassword, subject, scheme, sslEngine, timeoutMillis);
    }

    private final static Function<Principal, DelegatePrincipal> narrowPrincipal = new Function<Principal, DelegatePrincipal>() {
        @Override
        public DelegatePrincipal apply(Principal input) {
            return DelegatePrincipal.class.cast(input);
        }
    };

    public final static Optional<DelegatePrincipal> getDelegate(Subject s) {
        if (s == null) {
            return Optional.absent();
        }
        return FluentIterable
                .from(s.getPrincipals())
                .filter(Predicates.instanceOf(DelegatePrincipal.class))
                .transform(narrowPrincipal)
                .first();
    }

    private static Object[] getAuthenticatedConnection(
            String service, InetSocketAddress addr, String username,
            byte[] hashedPassword, final Subject subject, ClientAuthScheme scheme, SSLEngine sslEngine,
            long timeoutMillis)
    throws IOException {
        Object returnArray[] = new Object[3];
        boolean success = false;
        if (addr.isUnresolved()) {
            throw new java.net.UnknownHostException(addr.getHostName());
        }
        final SocketChannel aChannel = SocketChannel.open(addr);
        returnArray[0] = aChannel;
        assert(aChannel.isConnected());
        if (!aChannel.isConnected()) {
            // TODO Can open() be asynchronous if configureBlocking(true)?
            throw new IOException("Failed to open host " + ReverseDNSCache.hostnameOrAddress(addr.getAddress()));
        }

        // Setup a timer that times out the authentication if it is stuck (server dies, connection drops, etc.)
        final DelayedExecutionThread timeoutThread;
        if (timeoutMillis > 0) {
            timeoutThread = new DelayedExecutionThread(timeoutMillis, TimeUnit.MILLISECONDS, new Runnable() {
                @Override
                public void run() {
                    try {
                        aChannel.close();
                    } catch (IOException e) {
                        // Ignore
                    }
                }
            });
            timeoutThread.start();
        } else {
            timeoutThread = null;
        }

        MessagingChannel messagingChannel = null;
        try {
            synchronized(aChannel.blockingLock()) {
                aChannel.configureBlocking(false);
                aChannel.socket().setTcpNoDelay(true);
            }

            if (sslEngine != null) {
                TLSHandshaker handshaker = new TLSHandshaker(aChannel, sslEngine);
                boolean shookHands = false;
                try {
                    shookHands = handshaker.handshake();
                } catch (IOException e) {
                    aChannel.close();
                    throw new IOException("SSL handshake failed", e);
                }
                if (! shookHands) {
                    aChannel.close();
                    throw new IOException("SSL handshake failed");
                }
            }

            final long retvals[] = new long[4];
            returnArray[1] = retvals;
            messagingChannel = MessagingChannel.get(aChannel, sslEngine);

            /*
             * Send login info
             */
            synchronized(aChannel.blockingLock()) {
                aChannel.configureBlocking(true);
                aChannel.socket().setTcpNoDelay(true);
            }

            // encode strings
            byte[] serviceBytes = service == null ? null : service.getBytes(Constants.UTF8ENCODING);
            byte[] usernameBytes = username == null ? null : username.getBytes(Constants.UTF8ENCODING);

            // get the length of the data to serialize
            int requestSize = 4;
            requestSize += 2; //version and scheme
            requestSize += serviceBytes == null ? 4 : 4 + serviceBytes.length;
            requestSize += usernameBytes == null ? 4 : 4 + usernameBytes.length;
            requestSize += hashedPassword.length;

            ByteBuffer b = ByteBuffer.allocate(requestSize);

            // serialize it
            b.putInt(requestSize - 4);                            // length prefix
            b.put((byte) 1);                                      // version
            b.put((byte )scheme.getValue());
            SerializationHelper.writeVarbinary(serviceBytes, b);  // data service (export|database)
            SerializationHelper.writeVarbinary(usernameBytes, b);
            b.put(hashedPassword);
            b.flip();

            try {
                messagingChannel.writeMessage(b);
            } catch (IOException e) {
                throw new IOException("Failed to write authentication message to server.", e);
            }
            if (b.hasRemaining()) {
                throw new IOException("Failed to write authentication message to server.");
            }

            ByteBuffer loginResponse;
            try {
                loginResponse = messagingChannel.readMessage();
            } catch (IOException e) {
                throw new IOException("Authentication rejected", e);
            }

            byte version = loginResponse.get();
            byte loginResponseCode = loginResponse.get();

            if (version == Constants.AUTH_HANDSHAKE_VERSION) {
                byte tag = loginResponseCode;
                if (subject == null) {
                    aChannel.close();
                    throw new IOException("Server requires an authenticated JAAS principal");
                }
                if (tag != Constants.AUTH_SERVICE_NAME) {
                    aChannel.close();
                    throw new IOException("Wire protocol format violation error");
                }
                String servicePrincipal = SerializationHelper.getString(loginResponse);
                loginResponse = performAuthenticationHandShake(messagingChannel, subject, servicePrincipal);
                loginResponseCode = loginResponse.get();
            }

            if (loginResponseCode != 0) {
                aChannel.close();
                switch (loginResponseCode) {
                case Constants.MAX_CONNECTIONS_LIMIT_ERROR:
                    throw new IOException("Server has too many connections");
                case Constants.WIRE_PROTOCOL_TIMEOUT_ERROR:
                    throw new IOException("Connection timed out during authentication. " +
                    "The VoltDB server may be overloaded.");
                case Constants.EXPORT_DISABLED_REJECTION:
                    throw new IOException("Export not enabled for server");
                case Constants.WIRE_PROTOCOL_FORMAT_ERROR:
                    throw new IOException("Wire protocol format violation error");
                case Constants.AUTHENTICATION_FAILURE_DUE_TO_REJOIN:
                    throw new IOException("Failed to authenticate to rejoining node");
                default:
                    throw new IOException("Authentication rejected");
                }
            }
            retvals[0] = loginResponse.getInt();
            retvals[1] = loginResponse.getLong();
            retvals[2] = loginResponse.getLong();
            retvals[3] = loginResponse.getInt();
            int buildStringLength = loginResponse.getInt();
            byte buildStringBytes[] = new byte[buildStringLength];
            loginResponse.get(buildStringBytes);
            returnArray[2] = new String(buildStringBytes, Constants.UTF8ENCODING);

            synchronized(aChannel.blockingLock()) {
                aChannel.configureBlocking(false);
                aChannel.socket().setKeepAlive(true);
            }
            success = true;
        } catch (AsynchronousCloseException ignore) {
            // If the authentication times out, the channel will be closed
            // and this exception will be thrown from reads. Ignore it and
            // let the finally block throw the proper timeout exception.
        } finally {
            if (messagingChannel != null) {
                messagingChannel.cleanUp();
            }

            if (timeoutThread != null && !timeoutThread.cancel()) {
                // Failed to cancel, which means the timeout task must have run
                throw new IOException("Authentication timed out");
            }

            if (!success) {
                aChannel.close();
            }
        }
        return returnArray;
    }


    private final static void establishSecurityContext(
            final MessagingChannel channel, GSSContext context, Optional<DelegatePrincipal> delegate)
                    throws IOException, GSSException {

        ByteBuffer writeBuffer = ByteBuffer.allocate(4096);
        ByteBuffer readBuffer = writeBuffer;
        byte [] token;
        int msgSize = 0;

        /*
         * Establishing a kerberos secure context, requires a handshake conversation
         * where client, and server exchange and use tokens generated via calls to initSecContext
         */
        writeBuffer.limit(msgSize);
        while (!context.isEstablished()) {
            token = context.initSecContext(readBuffer.array(), readBuffer.arrayOffset() + readBuffer.position(),
                    readBuffer.remaining());

            if (token != null) {
                msgSize = 4 + 1 + 1 + token.length;
                writeBuffer.clear().limit(msgSize);
                writeBuffer.putInt(msgSize - 4).put(Constants.AUTH_HANDSHAKE_VERSION).put(Constants.AUTH_HANDSHAKE);
                writeBuffer.put(token).flip();

                channel.writeMessage(writeBuffer);
            }

            if (context.isEstablished()) {
                break;
            }

            readBuffer = channel.readMessage();

            byte version = readBuffer.get();
            if (version != Constants.AUTH_HANDSHAKE_VERSION) {
                throw new IOException("Encountered unexpected authentication protocol version " + version);
            }

            byte tag = readBuffer.get();
            if (tag != Constants.AUTH_HANDSHAKE) {
                throw new IOException("Encountered unexpected authentication protocol tag " + tag);
            }
        }


        if (!context.getMutualAuthState()) {
            throw new IOException("Authentication Handshake Failed");
        }

        if (delegate.isPresent() && !context.getConfState()) {
            throw new IOException("Cannot transmit delegate user name securely");
        }

        // encrypt and transmit the delegate principal if it is present
        if (delegate.isPresent()) {
            MessageProp mprop = new MessageProp(0, true);

            writeBuffer.clear().limit(delegate.get().wrappedSize());
            delegate.get().wrap(writeBuffer);
            writeBuffer.flip();

            token = context.wrap(writeBuffer.array(), writeBuffer.arrayOffset() + writeBuffer.position(), writeBuffer.remaining(), mprop);

            msgSize = 4 + 1 + 1 + token.length;
            writeBuffer.clear().limit(msgSize);
            writeBuffer.putInt(msgSize-4).put(Constants.AUTH_HANDSHAKE_VERSION).put(Constants.AUTH_HANDSHAKE);
            writeBuffer.put(token).flip();

            while (writeBuffer.hasRemaining()) {
                channel.writeMessage(writeBuffer);
            }
        }
    }

    private final static ByteBuffer performAuthenticationHandShake(
            final MessagingChannel channel, final Subject subject,
            final String serviceName) throws IOException {

        try {
            String subjectPrincipal = subject.getPrincipals().iterator().next().getName();
            final Optional<DelegatePrincipal> delegate = getDelegate(subject);
            if (delegate.isPresent() && !subjectPrincipal.equals(serviceName)) {
                throw new IOException("Delegate authentication is not allowed for user " + delegate.get().getName());
            }

            Subject.doAs(subject, new PrivilegedAction<GSSContext>() {
                @Override
                public GSSContext run() {
                    GSSContext context = null;
                    try {
                        /*
                         * The standard type designation for kerberos v5 secure service context
                         */
                        final Oid krb5Oid = new Oid("1.2.840.113554.1.2.2");
                        /*
                         * The standard type designation for principal
                         */
                        final Oid krb5PrincipalNameType = new Oid("1.2.840.113554.1.2.2.1");
                        final GSSName serverName = m_gssManager.createName(serviceName, krb5PrincipalNameType);

                        context = m_gssManager.createContext(serverName, krb5Oid, null, GSSContext.INDEFINITE_LIFETIME);
                        context.requestMutualAuth(true);
                        context.requestConf(true);
                        context.requestInteg(true);

                        establishSecurityContext(channel, context, delegate);

                        context.dispose();
                        context = null;
                    } catch (GSSException ex) {
                        throw new RuntimeException(ex);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    } finally {
                        if (context != null) {
                            try { context.dispose(); } catch (Exception ignoreIt) {}
                        }
                    }
                    return null;
                }
            });
        } catch (SecurityException ex) {
            // if we get here the authentication handshake failed.
            // PriviledgedActionException is the first wrapper. The runtime from Throwables would be
            // the second wrapper
            Throwable cause = ex.getCause();
            if (cause != null && (cause instanceof RuntimeException) && cause.getCause() != null) {
                cause = cause.getCause();
            } else if (cause == null) {
                cause = ex;
            }
            if (cause instanceof IOException) {
                throw IOException.class.cast(cause);
            } else {
                throw new IOException("Authentication Handshake Failed", cause);
            }
        }

        ByteBuffer loginResponse = channel.readMessage();

        byte version = loginResponse.get();
        if (version != (byte)0) {
            throw new IOException("Encountered unexpected version for the login response message: " + version);
        }
        return loginResponse;
    }

    public static void closeConnection(SocketChannel connection) throws InterruptedException, IOException {
        synchronized (m_executors) {
            ExecutorPair p = m_executors.remove(connection);
            assert(p != null);
            p.shutdown();
        }
        connection.close();
    }

    private static ExecutorPair getExecutorPair(final SocketChannel channel) {
        synchronized (m_executors) {
            ExecutorPair p = m_executors.get(channel);
            if (p == null) {
                p = new ExecutorPair();
                m_executors.put( channel, p);
            }
            return p;
        }
    }

    public static Future<Long> sendInvocation(final SocketChannel channel, final String procName,final Object ...parameters) {
        final ExecutorPair p = getExecutorPair(channel);
        return sendInvocation(p.m_writeExecutor, channel, procName, parameters);
    }

    public static Future<Long> sendInvocation(final ExecutorService executor, final SocketChannel channel, final String procName,final Object ...parameters) {
        return executor.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                final long handle = m_handle.getAndIncrement();
                final ProcedureInvocation invocation =
                    new ProcedureInvocation(handle, procName, parameters);

                ByteBuffer buf = ByteBuffer.allocate(4 + invocation.getSerializedSize());
                buf.position(4);
                invocation.flattenToBuffer(buf);
                buf.putInt(0, buf.capacity() - 4);
                buf.flip();
                do {
                    channel.write(buf);
                    if (buf.hasRemaining()) {
                        Thread.yield();
                    }
                }
                while(buf.hasRemaining());
                return handle;
            }
        });
    }

    public static Future<ClientResponse> readResponse(final SocketChannel channel) {
        final ExecutorPair p = getExecutorPair(channel);
        return readResponse(p.m_readExecutor, channel);
    }

    public static Future<ClientResponse> readResponse(final ExecutorService executor, final SocketChannel channel) {
        return executor.submit(new Callable<ClientResponse>() {
            @Override
            public ClientResponse call() throws Exception {
                ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
                do {
                    final int read = channel.read(lengthBuffer);
                    if (read == -1) {
                        throw new EOFException();
                    }
                    if (lengthBuffer.hasRemaining()) {
                        Thread.yield();
                    }
                }
                while (lengthBuffer.hasRemaining());

                lengthBuffer.flip();
                ByteBuffer message = ByteBuffer.allocate(lengthBuffer.getInt());
                do {
                    final int read = channel.read(message);
                    if (read == -1) {
                        throw new EOFException();
                    }
                    if (lengthBuffer.hasRemaining()) {
                        Thread.yield();
                    }
                }
                while (message.hasRemaining());
                message.flip();
                ClientResponseImpl response = new ClientResponseImpl();
                response.initFromBuffer(message);
                return response;
            }
        });
    }
}
