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

package org.voltdb.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.ClientInterface;
import org.voltdb.ClientResponseImpl;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.DBBPool.BBContainer;

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
     * @throws IOException
     * @returns An array of objects. The first is an
     * authenticated socket channel, the second. is an array of 4 longs -
     * Integer hostId, Long connectionId, Long timestamp (part of instanceId), Int leaderAddress (part of instanceId).
     * The last object is the build string
     */
    public static Object[] getAuthenticatedConnection(
            String host, String username, byte[] hashedPassword, int port) throws IOException
    {
        return getAuthenticatedConnection("database", host, username, hashedPassword, port);
    }

    /**
     * Create a connection to a Volt server for export and authenticate the connection.
     * @param host
     * @param username
     * @param password
     * @param port
     * @throws IOException
     * @returns An array of objects. The first is an
     * authenticated socket channel, the second. is an array of 4 longs -
     * Integer hostId, Long connectionId, Long timestamp (part of instanceId), Int leaderAddress (part of instanceId).
     * The last object is the build string
     */
    public static Object[] getAuthenticatedExportConnection(
            String host, String username, byte[] hashedPassword, int port) throws IOException
    {
        return getAuthenticatedConnection("export", host, username, hashedPassword, port);
    }

    public static String getHostnameOrAddress() {
        try {
            final InetAddress addr = InetAddress.getLocalHost();
            return addr.getHostName();
        } catch (UnknownHostException e) {
            try {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                if (interfaces == null) {
                    return "";
                }
                NetworkInterface intf = interfaces.nextElement();
                Enumeration<InetAddress> addresses = intf.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address) {
                        return address.getHostAddress();
                    }
                }
                interfaces = NetworkInterface.getNetworkInterfaces();
                while (addresses.hasMoreElements()) {
                    return addresses.nextElement().getHostAddress();
                }
                return "";
            } catch (SocketException e1) {
                return "";
            }
        }
    }

    public static InetAddress getLocalAddress() {
        try {
            final InetAddress addr = InetAddress.getLocalHost();
            return addr;
        } catch (UnknownHostException e) {
            try {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                if (interfaces == null) {
                    return null;
                }
                NetworkInterface intf = interfaces.nextElement();
                Enumeration<InetAddress> addresses = intf.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address) {
                        return address;
                    }
                }
                interfaces = NetworkInterface.getNetworkInterfaces();
                while (addresses.hasMoreElements()) {
                    return addresses.nextElement();
                }
                return null;
            } catch (SocketException e1) {
                return null;
            }
        }
    }
    private static Object[] getAuthenticatedConnection(
            String service, String host, String username, byte[] hashedPassword, int port)
    throws IOException {
        Object returnArray[] = new Object[3];
        boolean success = false;
        InetSocketAddress addr = new InetSocketAddress(host, port);
        if (addr.isUnresolved()) {
            throw new java.net.UnknownHostException(host);
        }
        SocketChannel aChannel = SocketChannel.open(addr);
        returnArray[0] = aChannel;
        assert(aChannel.isConnected());
        if (!aChannel.isConnected()) {
            // TODO Can open() be asynchronous if configureBlocking(true)?
            throw new IOException("Failed to open host " + host);
        }
        final long retvals[] = new long[4];
        returnArray[1] = retvals;
        try {
            /*
             * Send login info
             */
            aChannel.configureBlocking(true);
            aChannel.socket().setTcpNoDelay(true);
            FastSerializer fs = new FastSerializer();
            fs.writeInt(0);             // placeholder for length
            fs.writeByte(0);            // version
            fs.writeString(service);    // data service (export|database)
            fs.writeString(username);
            fs.write(hashedPassword);
            final ByteBuffer fsBuffer = fs.getBuffer();
            final ByteBuffer b = ByteBuffer.allocate(fsBuffer.remaining());
            b.put(fsBuffer);
            final int size = fsBuffer.limit() - 4;
            b.flip();
            b.putInt(size);
            b.position(0);

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
            loginResponse.position(1);
            byte loginResponseCode = loginResponse.get();

            if (loginResponseCode != 0) {
                aChannel.close();
                switch (loginResponseCode) {
                case ClientInterface.MAX_CONNECTIONS_LIMIT_ERROR:
                    throw new IOException("Server has too many connections");
                case ClientInterface.WIRE_PROTOCOL_TIMEOUT_ERROR:
                    throw new IOException("Connection timed out during authentication. " +
                            "The VoltDB server may be overloaded.");
                case ClientInterface.EXPORT_DISABLED_REJECTION:
                    throw new IOException("Export not enabled for server");
                case ClientInterface.WIRE_PROTOCOL_FORMAT_ERROR:
                    throw new IOException("Wire protocol format violation error");
                case ClientInterface.AUTHENTICATION_FAILURE_DUE_TO_REJOIN:
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
            aChannel.socket().setTcpNoDelay(false);
            aChannel.socket().setKeepAlive(true);
            success = true;
        } finally {
            if (!success) {
                aChannel.close();
            }
        }
        return returnArray;
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

                final FastSerializer fs = new FastSerializer();
                final BBContainer c = fs.writeObjectForMessaging(invocation);
                do {
                    channel.write(c.b);
                    if (c.b.hasRemaining()) {
                        Thread.yield();
                    }
                }
                while(c.b.hasRemaining());
                c.discard();
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
               FastDeserializer fds = new FastDeserializer(message);
               ClientResponseImpl response = fds.readObject(ClientResponseImpl.class);
               return response;
           }
        });
    }
}
