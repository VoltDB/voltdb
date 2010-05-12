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

package org.voltdb.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.voltdb.ClientResponseImpl;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.DBBPool.BBContainer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.io.*;

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
     *
     */
    public static Object[] getAuthenticatedConnection(
            String host, String username, String password, int port)
    throws IOException {
        Object returnArray[] = new Object[3];
        boolean success = false;
        InetSocketAddress addr = new InetSocketAddress(host, port);
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
            MessageDigest md = null;
            try {
                md = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            byte passwordHash[] = md.digest(password.getBytes());
            FastSerializer fs = new FastSerializer();
            fs.writeInt(0);             // placeholder for length
            fs.writeByte(0);            // version
            fs.writeString("database"); // data service (export|database)
            fs.writeString(username);
            fs.write(passwordHash);
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

            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            int read = aChannel.read(lengthBuffer);
            if (read == -1) {
                if (writeException != null) {
                    throw writeException;
                }
                if (!successfulWrite) {
                    throw new IOException("Unable to write authentication info to serer");
                }
                throw new IOException("Authentication rejected");
            } else {
                lengthBuffer.flip();
            }

            ByteBuffer loginResponse = ByteBuffer.allocate(lengthBuffer.getInt());//Read version and length etc.
            read = aChannel.read(loginResponse);
            byte loginResponseCode = 0;
            if (read == -1) {
                if (writeException != null) {
                    throw writeException;
                }
                if (!successfulWrite) {
                    throw new IOException("Unable to write authentication info to serer");
                }
                throw new IOException("Authentication rejected");
            } else {
                loginResponse.flip();
                loginResponse.position(1);
                loginResponseCode = loginResponse.get();
            }

            if (loginResponseCode != 0) {
                aChannel.close();
                switch (loginResponseCode) {
                case 1:
                    throw new IOException("Server has too many connections");
                case 2:
                    throw new IOException("Connection timed out during authentication. " +
                            "Buy a faster computer and stop using VMWare");
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
                    new ProcedureInvocation(handle, procName, -1, parameters);

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
