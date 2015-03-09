/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.Subject;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.voltcore.network.ReverseDNSCache;
import org.voltdb.ClientResponseImpl;
import org.voltdb.common.Constants;
import org.voltdb.utils.SerializationHelper;

import com.google_voltpatches.common.base.Throwables;

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
        if (password == null)
            return null;

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        byte hashedPassword[] = null;
        try {
            hashedPassword = md.digest(password.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("JVM doesn't support UTF-8. Please use a supported JVM", e);
        }
        return hashedPassword;
    }

    /**
     * Create a connection to a Volt server and authenticate the connection.
     * @param host
     * @param username
     * @param password
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
                                                      final Subject subject) throws IOException {
        String service = subject == null ? "database" : Constants.KERBEROS;
        return getAuthenticatedConnection(service, host, username, hashedPassword, port, subject);
    }

    private static Object[] getAuthenticatedConnection(
            String service, String host,
            String username, byte[] hashedPassword, int port, final Subject subject)
    throws IOException {
        InetSocketAddress address = new InetSocketAddress(host, port);
        return getAuthenticatedConnection(service, address, username, hashedPassword, subject);
    }

    private static Object[] getAuthenticatedConnection(
            String service, InetSocketAddress addr, String username,
            byte[] hashedPassword, final Subject subject)
    throws IOException {
        Object returnArray[] = new Object[3];
        boolean success = false;
        if (addr.isUnresolved()) {
            throw new java.net.UnknownHostException(addr.getHostName());
        }
        SocketChannel aChannel = SocketChannel.open(addr);
        returnArray[0] = aChannel;
        assert(aChannel.isConnected());
        if (!aChannel.isConnected()) {
            // TODO Can open() be asynchronous if configureBlocking(true)?
            throw new IOException("Failed to open host " + ReverseDNSCache.hostnameOrAddress(addr.getAddress()));
        }
        final long retvals[] = new long[4];
        returnArray[1] = retvals;
        try {
            /*
             * Send login info
             */
            aChannel.configureBlocking(true);
            aChannel.socket().setTcpNoDelay(true);

            // encode strings
            byte[] serviceBytes = service == null ? null : service.getBytes(Constants.UTF8ENCODING);
            byte[] usernameBytes = username == null ? null : username.getBytes(Constants.UTF8ENCODING);

            // get the length of the data to serialize
            int requestSize = 4 + 1;
            requestSize += serviceBytes == null ? 4 : 4 + serviceBytes.length;
            requestSize += usernameBytes == null ? 4 : 4 + usernameBytes.length;
            requestSize += hashedPassword.length;

            ByteBuffer b = ByteBuffer.allocate(requestSize);

            // serialize it
            b.putInt(requestSize - 4);                            // length prefix
            b.put((byte) 0);                                      // version
            SerializationHelper.writeVarbinary(serviceBytes, b);  // data service (export|database)
            SerializationHelper.writeVarbinary(usernameBytes, b);
            b.put(hashedPassword);
            b.flip();

            boolean successfulWrite = false;
            IOException writeException = null;
            try {
                for (int ii = 0; ii < 4 && b.hasRemaining(); ii++) {
                    aChannel.write(b);
                }
                if (!b.hasRemaining()) {
                    successfulWrite = true;
                }
            } catch (IOException e) {
                writeException = e;
            }

            int read = 0;
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            while (lengthBuffer.hasRemaining()) {
                read = aChannel.read(lengthBuffer);
                if (read == -1) {
                    if (writeException != null) {
                        throw writeException;
                    }
                    if (!successfulWrite) {
                        throw new IOException("Unable to write authentication info to server");
                    }
                    throw new IOException("Authentication rejected");
                }
            }
            lengthBuffer.flip();

            int len = lengthBuffer.getInt();
            ByteBuffer loginResponse = ByteBuffer.allocate(len);//Read version and length etc.

            while (loginResponse.hasRemaining()) {
                read = aChannel.read(loginResponse);

                if (read == -1) {
                    if (writeException != null) {
                        throw writeException;
                    }
                    if (!successfulWrite) {
                        throw new IOException("Unable to write authentication info to server");
                    }
                    throw new IOException("Authentication rejected");
                }
            }
            loginResponse.flip();
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
                loginResponse = performAuthenticationHandShake(aChannel, subject, servicePrincipal);
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
            returnArray[2] = new String(buildStringBytes, "UTF-8");

            aChannel.configureBlocking(false);
            aChannel.socket().setKeepAlive(true);
            success = true;
        } finally {
            if (!success) {
                aChannel.close();
            }
        }
        return returnArray;
    }

    private final static ByteBuffer performAuthenticationHandShake(
            final SocketChannel channel, final Subject subject,
            final String serviceName) throws IOException {

        try {
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

                        ByteBuffer bb = ByteBuffer.allocate(4096);

                        context = m_gssManager.createContext(serverName, krb5Oid, null, GSSContext.DEFAULT_LIFETIME);
                        context.requestMutualAuth(true);
                        context.requestConf(true);
                        context.requestInteg(true);

                        byte [] token;
                        int msgSize = 0;

                        /*
                         * Establishing a kerberos secure context, requires a handshake conversation
                         * where client, and server exchange and use tokens generated via calls to initSecContext
                         */
                        bb.limit(msgSize);
                        while (!context.isEstablished()) {
                            token = context.initSecContext(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
                            if (token != null) {
                                msgSize = 4 + 1 + 1 + token.length;
                                bb.clear().limit(msgSize);
                                bb.putInt(msgSize-4).put(Constants.AUTH_HANDSHAKE_VERSION).put(Constants.AUTH_HANDSHAKE);
                                bb.put(token).flip();

                                while (bb.hasRemaining()) {
                                    channel.write(bb);
                                }
                            }
                            if (!context.isEstablished()) {
                                bb.clear().limit(4);

                                while (bb.hasRemaining()) {
                                    if (channel.read(bb) == -1) throw new EOFException();
                                }
                                bb.flip();

                                msgSize = bb.getInt();
                                if (msgSize > bb.capacity()) {
                                    throw new IOException("Authentication packet exceeded alloted size");
                                }
                                if (msgSize <= 0) {
                                    throw new IOException("Wire Protocol Format error 0 or negative message length prefix");
                                }
                                bb.clear().limit(msgSize);

                                while (bb.hasRemaining()) {
                                    if (channel.read(bb) == -1) throw new EOFException();
                                }
                                bb.flip();

                                byte version = bb.get();
                                if (version != Constants.AUTH_HANDSHAKE_VERSION) {
                                    throw new IOException("Encountered unexpected authentication protocol version " + version);
                                }

                                byte tag = bb.get();
                                if (tag != Constants.AUTH_HANDSHAKE) {
                                    throw new IOException("Encountered unexpected authentication protocol tag " + tag);
                                }
                            }
                        }

                        if (!context.getMutualAuthState()) {
                            throw new IOException("Authentication Handshake Failed");
                        }
                        context.dispose();
                        context = null;
                    } catch (GSSException ex) {
                        Throwables.propagate(ex);
                    } catch (IOException ex) {
                        Throwables.propagate(ex);
                    } finally {
                        if (context != null) try { context.dispose(); } catch (Exception ignoreIt) {}
                    }
                    return null;
                }
            });
        } catch (SecurityException ex) {
            // if we get here the authentication handshake failed.
            try { channel.close(); } catch (Exception ignoreIt) {}
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

        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        while (lengthBuffer.hasRemaining()) {
            if (channel.read(lengthBuffer) == -1) {
                channel.close();
                throw new EOFException();
            }
        }
        lengthBuffer.flip();
        int responseSize = lengthBuffer.getInt();

        ByteBuffer loginResponse = ByteBuffer.allocate(responseSize);
        while (loginResponse.hasRemaining()) {
            if (channel.read(loginResponse) == -1) {
                channel.close();
                throw new EOFException();
            }
        }
        loginResponse.flip();

        byte version = loginResponse.get();
        if (version != (byte)0) {
            channel.close();
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
