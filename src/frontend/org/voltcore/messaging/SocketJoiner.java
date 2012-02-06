/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
package org.voltcore.messaging;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.VoltDB;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;

/**
 * SocketJoiner runs all the time listening for new nodes in the cluster. Since it is a dedicated thread
 * it is able to block while a new node joins without disrupting other activities.
 *
 * At startup socket joiner will connect to the rest of the cluster in the start method if it fails
 * to bind to the leader address.
 *
 * If it binds to the leader address and becomes the leader the start method returns immediately and runPrimary
 * is run from a separate thread. runPrimary will wait for the countdown latch for bootstrapping zk to count down
 * before accepting new connections
 */
public class SocketJoiner {

    /**
     * Interface into host messenger to notify it of new connections.
     *
     */
    public interface JoinHandler {
        /*
         * Notify that a specific host has joined with the specified host id.
         */
        public void notifyOfJoin(int hostId, SocketChannel socket, InetSocketAddress listeningAddress);

        /*
         * A node wants to join the socket mesh
         */
        public void requestJoin(SocketChannel socket, InetSocketAddress listeningAddress ) throws Exception;

        /*
         * A connection has been made to all of the specified hosts. Invoked by
         * nodes connected to the cluster
         */
        public void notifyOfHosts(
                int yourLocalHostId,
                int hosts[],
                SocketChannel sockets[],
                InetSocketAddress listeningAddresses[]) throws Exception;
    }

    private static final VoltLogger LOG = new VoltLogger(SocketJoiner.class.getName());
    private static final VoltLogger recoveryLog = new VoltLogger("RECOVERY");

    private final ExecutorService m_es = Executors.newSingleThreadExecutor(
            org.voltcore.utils.MiscUtils.getThreadFactory("Socket Joiner", 1024 * 128));

    InetSocketAddress m_coordIp = null;
    int m_localHostId = 0;
    Map<Integer, SocketChannel> m_sockets = new HashMap<Integer, SocketChannel>();
    ServerSocketChannel m_listenerSocket = null;
    VoltLogger m_hostLog;
    long m_timestamp;//Part of instanceId
    byte m_addr[];
    private final JoinHandler m_joinHandler;

    // from configuration data
    int m_internalPort = 3021;
    String m_internalInterface = "";
    /*
     * The interface we connected to the leader on
     */
    String m_reportedInternalInterface;

    public boolean start(final CountDownLatch externalInitBarrier) {
        boolean retval = false;

        // Try to become leader regardless of configuration.
        try {
            m_listenerSocket = ServerSocketChannel.open();
            m_listenerSocket.socket().bind(m_coordIp);
            m_listenerSocket.socket().setPerformancePreferences(0, 2, 1);
            m_internalPort = m_coordIp.getPort();
            m_internalInterface = m_coordIp.getAddress().getHostAddress();
        }
        catch (IOException e) {
            if (m_listenerSocket != null) {
                try {
                    m_listenerSocket.close();
                    m_listenerSocket = null;
                }
                catch (IOException ex) {
                    new VoltLogger(SocketJoiner.class.getName()).l7dlog(Level.FATAL, null, ex);
                }
            }
        }

        if (m_listenerSocket != null) {
            retval = true;
            if (m_hostLog != null)
                m_hostLog.info("Connecting to VoltDB cluster as the leader...");
            /*
             * Need to wait for external initialization to complete before
             * accepting new connections. This is slang for the leader
             * creating an agreement site that agrees with itself
             */
            m_es.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    externalInitBarrier.await();
                    return null;
                }
            });
        }
        else {
            if (m_hostLog != null) {
                m_hostLog.info("Connecting to the VoltDB cluster leader "
                        + m_coordIp + ":" + m_internalPort);
            }
            /*
             * Not a leader, need to connect to the primary to join the cluster.
             * Once connectToPrimary is finishes this node will be physically connected
             * to all nodes with a working agreement site
             */
            connectToPrimary();
        }

        /*
         * Submit a task to start the main run loop,
         * will wait for agreement to be initialized if this
         * is the leader using the previously queued runnable
         */
        m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    runPrimary();
                } catch (InterruptedException e) {

                } catch (Throwable e) {
                    VoltDB.crashLocalVoltDB("Error in socket joiner run loop", false, e);
                }
            }
        });

        return retval;
    }

    /** Set to true when the thread exits correctly. */
    private final boolean success = false;

    public boolean getSuccess() {
        return success;
    }

    public SocketJoiner(
            InetSocketAddress coordIp,
            String internalInterface,
            int internalPort,
            VoltLogger hostLog,
            JoinHandler jh) {
        if (internalInterface == null || coordIp == null || jh == null) {
            throw new IllegalArgumentException();
        }
        m_coordIp = coordIp;
        m_hostLog = hostLog;
        m_joinHandler = jh;
        m_internalInterface = internalInterface;
        m_internalPort = internalPort;
    }

    /*
     * Bind to the internal interface if one was specified,
     * otherwise bind on all interfaces. The leader won't invoke this.
     */
    private void doBind() throws Exception {
        LOG.debug("Creating listener socket");
        m_listenerSocket = ServerSocketChannel.open();
        InetSocketAddress inetsockaddr;
        if ((m_internalInterface == null) || (m_internalInterface.length() == 0)) {
            inetsockaddr = new InetSocketAddress(m_internalPort);
        }
        else {
            inetsockaddr = new InetSocketAddress(m_internalInterface, m_internalPort);
        }
        m_listenerSocket.socket().bind(inetsockaddr);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Non-Primary Listening on:" + inetsockaddr.toString());
        }
    }

    /*
     * After startup everything is a primary and can accept
     * new nodes into the cluster. This loop accepts the new socket
     * and passes it off the HostMessenger via the JoinHandler interface
     */
    private void runPrimary() throws Exception {
        try {
            // start the server socket on the right interface
            if (m_listenerSocket == null) {
                doBind();
            }

            while (true) {
                try {
                    SocketChannel sc = m_listenerSocket.accept();
                    sc.socket().setTcpNoDelay(true);
                    sc.socket().setPerformancePreferences(0, 2, 1);
                    final String remoteAddress = sc.socket().getRemoteSocketAddress().toString();

                    /*
                     * Read a length prefixed JSON message
                     */
                    ByteBuffer lengthBuffer = ByteBuffer.allocate(4);

                    while (lengthBuffer.remaining() > 0) {
                        int read = sc.read(lengthBuffer);
                        if (read == -1) {
                            throw new EOFException(remoteAddress);
                        }
                    }
                    lengthBuffer.flip();

                    ByteBuffer messageBytes = ByteBuffer.allocate(lengthBuffer.getInt());
                    while (messageBytes.hasRemaining()) {
                        int read = sc.read(messageBytes);
                        if (read == -1) {
                            throw new EOFException(remoteAddress);
                        }
                    }
                    messageBytes.flip();

                    JSONObject jsObj = new JSONObject(new String(messageBytes.array(), "UTF-8"));

                    /*
                     * The type of connection, it can be a new request to join the cluster
                     * or a node that is connecting to the rest of the cluster and publishing its
                     * host id and such
                     */
                    String type = jsObj.getString("type");

                    /*
                     * The new connection may specify the address it is listening on,
                     * or it can be derived from the connection itself
                     */
                    InetSocketAddress listeningAddress;
                    if (jsObj.has("address")) {
                        listeningAddress = new InetSocketAddress(
                                jsObj.getString("address"),
                                jsObj.getInt("port"));
                    } else {
                        listeningAddress =
                            new InetSocketAddress(
                                    ((InetSocketAddress)sc.socket().
                                            getRemoteSocketAddress()).getAddress().getHostAddress(),
                                    jsObj.getInt("port"));
                    }

                    System.out.println("Received request type " + type);
                    if (type.equals("REQUEST_HOSTID")) {
                        m_joinHandler.requestJoin( sc, listeningAddress);
                    } else if (type.equals("PUBLISH_HOSTID")){
                        m_joinHandler.notifyOfJoin(jsObj.getInt("hostId"), sc, listeningAddress);
                    } else {
                        throw new RuntimeException("Unexpected message type " + type + " from " + remoteAddress);
                    }
                } catch (ClosedByInterruptException e) {
                    throw new InterruptedException();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        finally {
            try {
                m_listenerSocket.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /*
     * If this node failed to bind to the leader address
     * it must connect to the leader which will generate a host id and
     * advertise the rest of the cluster so that connectToPrimary can connect to it
     */
    private void connectToPrimary() {
        SocketChannel socket = null;
        try {
            LOG.debug("Non-Primary Starting");
            LOG.debug("Non-Primary Connecting to Primary");

            while (socket == null) {
                try {
                    socket = SocketChannel.open(m_coordIp);
                }
                catch (java.net.ConnectException e) {
                    LOG.warn("Joining primary failed: " + e.getMessage() + " retrying..");
                    try {
                        Thread.sleep(250); //  milliseconds
                    }
                    catch (InterruptedException ex) {
                        // don't really care.
                    }
                }
            }
            socket.socket().setTcpNoDelay(true);
            socket.socket().setPerformancePreferences(0, 2, 1);

            JSONObject jsObj = new JSONObject();
            jsObj.put("type", "REQUEST_HOSTID");

            /*
             * Advertise the port we are going to listen on based on
             * config
             */
            jsObj.put("port", m_internalPort);

            /*
             * If config specified an internal interface use that.
             * Otherwise the leader will echo back what we connected on
             */
            if (!m_internalInterface.isEmpty()) {
                jsObj.put("address", m_internalInterface);
            }
            byte jsBytes[] = jsObj.toString(4).getBytes("UTF-8");
            ByteBuffer requestHostIdBuffer = ByteBuffer.allocate(4 + jsBytes.length);
            requestHostIdBuffer.putInt(jsBytes.length);
            requestHostIdBuffer.put(jsBytes).flip();
            while (requestHostIdBuffer.hasRemaining()) {
                socket.write(requestHostIdBuffer);
            }

            // send the local hostid out
            LOG.debug("Non-Primary requesting its Host ID");
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            while (lengthBuffer.hasRemaining()) {
                socket.read(lengthBuffer);
            }
            lengthBuffer.flip();

            ByteBuffer responseBuffer = ByteBuffer.allocate(lengthBuffer.getInt());
            while (responseBuffer.hasRemaining()) {
                int read = socket.read(responseBuffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            String jsonString = new String(responseBuffer.array(), "UTF-8");
            JSONObject jsonObj = new JSONObject(jsonString);

            /*
             * Get the generated host id, and the interface we connected on
             * that was echoed back
             */
            m_localHostId = jsonObj.getInt("newHostId");
            m_reportedInternalInterface = jsonObj.getString("reportedAddress");

            /*
             * Loop over all the hosts and create a connection (except for the first entry, that is the leader)
             * and publish the host id that was generated. This finishes creating the mesh
             */
            JSONArray otherHosts = jsonObj.getJSONArray("hosts");
            int hostIds[] = new int[otherHosts.length()];
            SocketChannel hostSockets[] = new SocketChannel[hostIds.length];
            InetSocketAddress listeningAddresses[] = new InetSocketAddress[hostIds.length];

            for (int ii = 0; ii < otherHosts.length(); ii++) {
                JSONObject host = otherHosts.getJSONObject(ii);
                String address = host.getString("address");
                int port = host.getInt("port");
                final int hostId = host.getInt("hostId");

                InetSocketAddress hostAddr = new InetSocketAddress(address, port);
                if (ii == 0) {
                    //Leader already has a socket
                    hostIds[ii] = hostId;
                    listeningAddresses[ii] = hostAddr;
                    hostSockets[ii] = socket;
                    continue;
                }

                SocketChannel hostSocket = null;
                while (hostSocket == null) {
                    try {
                        hostSocket = SocketChannel.open(hostAddr);
                    }
                    catch (java.net.ConnectException e) {
                        LOG.warn("Joining primary failed: " + e.getMessage() + " retrying..");
                        try {
                            Thread.sleep(250); //  milliseconds
                        }
                        catch (InterruptedException ex) {
                            // don't really care.
                        }
                    }
                }

                jsObj = new JSONObject();
                jsObj.put("type", "PUBLISH_HOSTID");
                jsObj.put("hostId", m_localHostId);
                jsObj.put("port", m_internalPort);
                jsObj.put(
                        "address",
                        m_internalInterface.isEmpty() ? m_reportedInternalInterface : m_internalInterface);
                jsBytes = jsObj.toString(4).getBytes("UTF-8");
                ByteBuffer pushHostId = ByteBuffer.allocate(4 + jsBytes.length);
                pushHostId.putInt(jsBytes.length);
                pushHostId.put(jsBytes).flip();

                while (pushHostId.hasRemaining()) {
                    hostSocket.write(pushHostId);
                }
                hostIds[ii] = hostId;
                hostSockets[ii] = hostSocket;
                listeningAddresses[ii] = hostAddr;
            }

            /*
             * Notify the leader that we connected to the entire cluster, it will then go
             * and queue a txn for our agreement site to join the lcuster
             */
            ByteBuffer joinCompleteBuffer = ByteBuffer.allocate(1);
            while (joinCompleteBuffer.hasRemaining()) {
                hostSockets[0].write(joinCompleteBuffer);
            }

            /*
             * Let host messenger know about the connections.
             * It will init the agreement site and then we are done.
             */
            m_joinHandler.notifyOfHosts( m_localHostId, hostIds, hostSockets, listeningAddresses);
        } catch (ClosedByInterruptException e) {
            //This is how shutdown is done
        } catch (Exception e) {
            m_hostLog.error("Failed to establish socket mesh.", e);
            throw new RuntimeException(e);
        }
    }

    public void shutdown() throws InterruptedException {
        m_es.shutdownNow();
        m_es.awaitTermination(356, TimeUnit.DAYS);
        if (m_listenerSocket != null) {
            try {
                m_listenerSocket.close();
            } catch (IOException e) {}
        }
    }

    int getLocalHostId() {
        return m_localHostId;
    }
}
