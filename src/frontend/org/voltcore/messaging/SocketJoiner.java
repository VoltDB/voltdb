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
package org.voltcore.messaging;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.ReverseDNSCache;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.utils.MiscUtils;

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

    private static final VoltLogger LOG = new VoltLogger("JOINER");
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private final ExecutorService m_es = CoreUtils.getSingleThreadExecutor("Socket Joiner");

    InetSocketAddress m_coordIp = null;
    int m_localHostId = 0;
    Map<Integer, SocketChannel> m_sockets = new HashMap<Integer, SocketChannel>();
    private final List<ServerSocketChannel> m_listenerSockets = new ArrayList<ServerSocketChannel>();
    private Selector m_selector;
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
            hostLog.info("Attempting to bind to leader ip " + m_coordIp);
            ServerSocketChannel listenerSocket = ServerSocketChannel.open();
            listenerSocket.socket().bind(m_coordIp);
            listenerSocket.socket().setPerformancePreferences(0, 2, 1);
            listenerSocket.configureBlocking(false);
            m_listenerSockets.add(listenerSocket);
        }
        catch (IOException e) {
            if (!m_listenerSockets.isEmpty()) {
                try {
                    m_listenerSockets.get(0).close();
                    m_listenerSockets.clear();
                }
                catch (IOException ex) {
                    new VoltLogger(SocketJoiner.class.getName()).l7dlog(Level.FATAL, null, ex);
                }
            }
        }

        if (!m_listenerSockets.isEmpty()) {
            // if an internal interface was specified, see if it matches any
            // of the forms of the leader address we've bound to.
            if (m_internalInterface != null && !m_internalInterface.equals("")) {
                if (!m_internalInterface.equals(ReverseDNSCache.hostnameOrAddress(m_coordIp.getAddress())) &&
                    !m_internalInterface.equals(m_coordIp.getAddress().getCanonicalHostName()) &&
                    !m_internalInterface.equals(m_coordIp.getAddress().getHostAddress()))
                {
                    String msg = "The provided internal interface (" + m_internalInterface +
                    ") does not match the specified leader address (" +
                     ReverseDNSCache.hostnameOrAddress(m_coordIp.getAddress()) +
                    ", " + m_coordIp.getAddress().getHostAddress() +
                    "). This will result in either a cluster which fails to start" +
                    " or an unintended network topology. The leader will now exit;" +
                    " correct your specified leader and interface and try restarting.";
                    org.voltdb.VoltDB.crashLocalVoltDB(msg, false, null);
                }
            }
            retval = true;
            consoleLog.info("Connecting to VoltDB cluster as the leader...");

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
            consoleLog.info("Connecting to the VoltDB cluster leader " + m_coordIp);

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
                    org.voltdb.VoltDB.crashLocalVoltDB("Error in socket joiner run loop", true, e);
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
            JoinHandler jh) {
        if (internalInterface == null || coordIp == null || jh == null) {
            throw new IllegalArgumentException();
        }
        m_coordIp = coordIp;
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
        try {
            m_selector = Selector.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ServerSocketChannel listenerSocket = ServerSocketChannel.open();
        InetSocketAddress inetsockaddr;
        if ((m_internalInterface == null) || (m_internalInterface.length() == 0)) {
            inetsockaddr = new InetSocketAddress(m_internalPort);
        }
        else {
            inetsockaddr = new InetSocketAddress(m_internalInterface, m_internalPort);
        }
        try {
            hostLog.info("Attempting to bind to internal ip " + inetsockaddr);
            listenerSocket.socket().bind(inetsockaddr);
            listenerSocket.configureBlocking(false);
            m_listenerSockets.add(listenerSocket);
        } catch (Exception e) {
            /*
             * If we bound to the leader address, the internal interface address  might not
             * bind if it is all interfaces
             */
            if (m_listenerSockets.isEmpty()) {
                LOG.fatal("Failed to bind to " + inetsockaddr);
                MiscUtils.printPortsInUse(hostLog);
                throw e;
            }
        }

        for (ServerSocketChannel ssc : m_listenerSockets) {
            ssc.register(m_selector, SelectionKey.OP_ACCEPT);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Non-Primary Listening on:" + inetsockaddr.toString());
        }
    }

    /**
     * Read a length prefixed JSON message
     */
    private JSONObject readJSONObjFromWire(SocketChannel sc, String remoteAddressForErrorMsg) throws IOException, JSONException {
        // length prefix
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        while (lengthBuffer.remaining() > 0) {
            int read = sc.read(lengthBuffer);
            if (read == -1) {
                throw new EOFException(remoteAddressForErrorMsg);
            }
        }
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();

        // don't allow for a crazy unallocatable json payload
        if (length > 16 * 1024) {
            throw new IOException(
                    "Length prefix on wire for expected JSON string is greater than 16K max.");
        }
        if (length < 2) {
            throw new IOException(
                    "Length prefix on wire for expected JSON string is less than minimum document size of 2.");
        }

        // content
        ByteBuffer messageBytes = ByteBuffer.allocate(length);
        while (messageBytes.hasRemaining()) {
            int read = sc.read(messageBytes);
            if (read == -1) {
                throw new EOFException(remoteAddressForErrorMsg);
            }
        }
        messageBytes.flip();

        JSONObject jsObj = new JSONObject(new String(messageBytes.array(), Constants.UTF8ENCODING));
        return jsObj;
    }

    /*
     * Pull all ready to accept sockets
     */
    private void processSSC(ServerSocketChannel ssc) throws Exception {
        SocketChannel sc = null;
        while ((sc = ssc.accept()) != null) {
            sc.socket().setTcpNoDelay(true);
            sc.socket().setPerformancePreferences(0, 2, 1);
            final String remoteAddress = sc.socket().getRemoteSocketAddress().toString();

            /*
             * Send the current time over the new connection for a clock skew check
             */
            ByteBuffer currentTimeBuf = ByteBuffer.allocate(8);
            currentTimeBuf.putLong(System.currentTimeMillis());
            currentTimeBuf.flip();
            while (currentTimeBuf.hasRemaining()) {
                sc.write(currentTimeBuf);
            }

            /*
             * Read a length prefixed JSON message
             */
            JSONObject jsObj = readJSONObjFromWire(sc, remoteAddress);

            LOG.info(jsObj.toString(2));

            // get the connecting node's version string
            String remoteBuildString = jsObj.getString("versionString");

            // send a response with version/build data of this node
            JSONObject returnJs = new JSONObject();
            returnJs.put("versionString", VoltDB.instance().getVersionString());
            returnJs.put("buildString", VoltDB.instance().getBuildString());
            returnJs.put("versionCompatible", VoltDB.instance().isCompatibleVersionString(remoteBuildString));
            byte jsBytes[] = returnJs.toString(4).getBytes(Constants.UTF8ENCODING);

            ByteBuffer returnJsBuffer = ByteBuffer.allocate(4 + jsBytes.length);
            returnJsBuffer.putInt(jsBytes.length);
            returnJsBuffer.put(jsBytes).flip();
            while (returnJsBuffer.hasRemaining()) {
                sc.write(returnJsBuffer);
            }

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
                        InetAddress.getByName(jsObj.getString("address")),
                        jsObj.getInt("port"));
            } else {
                listeningAddress =
                    new InetSocketAddress(
                            ((InetSocketAddress)sc.socket().
                                    getRemoteSocketAddress()).getAddress().getHostAddress(),
                                    jsObj.getInt("port"));
            }

            hostLog.info("Received request type " + type);
            if (type.equals("REQUEST_HOSTID")) {
                m_joinHandler.requestJoin( sc, listeningAddress);
            } else if (type.equals("PUBLISH_HOSTID")){
                m_joinHandler.notifyOfJoin(jsObj.getInt("hostId"), sc, listeningAddress);
            } else {
                throw new RuntimeException("Unexpected message type " + type + " from " + remoteAddress);
            }
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
            doBind();

            while (true) {
                try {
                    final int selectedKeyCount = m_selector.select();
                    if (selectedKeyCount == 0) continue;
                    Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
                    try {
                        for (SelectionKey key : selectedKeys) {
                            processSSC((ServerSocketChannel)key.channel());
                        }
                    } finally {
                        selectedKeys.clear();
                    }
                } catch (ClosedByInterruptException e) {
                    throw new InterruptedException();
                } catch (ClosedSelectorException e) {
                    throw new InterruptedException();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        finally {
            for (ServerSocketChannel ssc : m_listenerSockets) {
                try {
                    ssc.close();
                } catch (Exception e) {}
            }
            m_listenerSockets.clear();
            try {
                m_selector.close();
            } catch (IOException e) {
            }
            m_selector = null;
        }
    }

    /**
     * Read version info from a socket and check compatibility.
     */
    private void processVersionJSONResponse(SocketChannel sc,
                                            String remoteAddress,
                                            String localVersionString,
                                            String localBuildString,
                                            Set<String> activeVersions) throws IOException, JSONException
    {
        // read the json response from socketjoiner with version info
        JSONObject jsonVersionInfo = readJSONObjFromWire(sc, remoteAddress);

        String remoteVersionString = jsonVersionInfo.getString("versionString");
        String remoteBuildString = jsonVersionInfo.getString("buildString");
        boolean remoteAcceptsLocalVersion = jsonVersionInfo.getBoolean("versionCompatible");

        if (remoteVersionString.equals(localVersionString)) {
            if (localBuildString.equals(remoteBuildString) == false) {
                // ignore test/eclipse build string so tests still work
                if (!localBuildString.equals("VoltDB") && !remoteBuildString.equals("VoltDB")) {
                    VoltDB.crashLocalVoltDB("For VoltDB version " + localVersionString +
                            " git tag/hash is not identical across the cluster. Node join failed.\n" +
                            "  joining build string:  " + localBuildString + "\n" +
                            "  existing build string: " + remoteBuildString, false, null);
                }
            }
        }
        else if (!remoteAcceptsLocalVersion) {
            if (!VoltDB.instance().isCompatibleVersionString(remoteVersionString)) {
                VoltDB.crashLocalVoltDB("Cluster contains nodes running VoltDB version " + remoteVersionString +
                        " which is incompatbile with local version " + localVersionString + ".\n", false, null);
            }
        }
        activeVersions.add(remoteVersionString);
    }

    /*
     * If this node failed to bind to the leader address
     * it must connect to the leader which will generate a host id and
     * advertise the rest of the cluster so that connectToPrimary can connect to it
     */
    private void connectToPrimary() {
        // collect clock skews from all nodes
        List<Long> skews = new ArrayList<Long>();

        // collect the set of active voltdb version strings in the cluster
        // this is used to limit simulatanious versions to two
        Set<String> activeVersions = new TreeSet<String>();

        SocketChannel socket = null;
        try {
            LOG.debug("Non-Primary Starting & Connecting to Primary");

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

            final String remoteAddress = socket.socket().getRemoteSocketAddress().toString();

            // Read the timestamp off the wire and calculate skew for this connection
            ByteBuffer currentTimeBuf = ByteBuffer.allocate(8);
            while (currentTimeBuf.hasRemaining()) {
                socket.read(currentTimeBuf);
            }
            currentTimeBuf.flip();
            long skew = System.currentTimeMillis() - currentTimeBuf.getLong();
            skews.add(skew);

            String localVersionString = VoltDB.instance().getVersionString();
            String localBuildString = VoltDB.instance().getBuildString();
            activeVersions.add(localVersionString);

            JSONObject jsObj = new JSONObject();
            jsObj.put("type", "REQUEST_HOSTID");

            // put the version compatibility status in the json
            jsObj.put("versionString", localVersionString);

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

            byte jsBytes[] = jsObj.toString(4).getBytes(Constants.UTF8ENCODING);
            ByteBuffer requestHostIdBuffer = ByteBuffer.allocate(4 + jsBytes.length);
            requestHostIdBuffer.putInt(jsBytes.length);
            requestHostIdBuffer.put(jsBytes).flip();
            while (requestHostIdBuffer.hasRemaining()) {
                socket.write(requestHostIdBuffer);
            }

            // read the json response from socketjoiner with version info and validate it
            processVersionJSONResponse(socket, remoteAddress, localVersionString, localBuildString, activeVersions);

            // read the json response sent by HostMessenger with HostID
            JSONObject jsonObj = readJSONObjFromWire(socket, remoteAddress);

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

                LOG.info("Leader provided address " + address + ":" + port);
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
                        LOG.warn("Joining host failed: " + e.getMessage() + " retrying..");
                        try {
                            Thread.sleep(250); //  milliseconds
                        }
                        catch (InterruptedException ex) {
                            // don't really care.
                        }
                    }
                }

                /*
                 * Get the clock skew value
                 */
                currentTimeBuf.clear();
                while (currentTimeBuf.hasRemaining()) {
                    hostSocket.read(currentTimeBuf);
                }
                currentTimeBuf.flip();
                skew = System.currentTimeMillis() - currentTimeBuf.getLong();
                assert(currentTimeBuf.remaining() == 0);
                skews.add(skew);

                jsObj = new JSONObject();
                jsObj.put("type", "PUBLISH_HOSTID");
                jsObj.put("hostId", m_localHostId);
                jsObj.put("port", m_internalPort);
                jsObj.put(
                        "address",
                        m_internalInterface.isEmpty() ? m_reportedInternalInterface : m_internalInterface);
                jsObj.put("versionString", VoltDB.instance().getVersionString());
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

                // read the json response from socketjoiner with version info and validate it
                processVersionJSONResponse(hostSocket, remoteAddress, localVersionString, localBuildString, activeVersions);
            }

            long maxSkew = Collections.max(skews);
            long minSkew = Collections.min(skews);
            long overallSkew = maxSkew - minSkew;
            if (maxSkew > 0 && minSkew > 0) {
                overallSkew = maxSkew;
            } else if (maxSkew < 0 && minSkew < 0) {
                overallSkew = Math.abs(minSkew);
            }
            if (overallSkew > 100) {
                VoltDB.crashLocalVoltDB("Clock skew is " + overallSkew +
                        " which is > than the 100 millisecond limit. Make sure NTP is running.", false, null);
            } else if (overallSkew > 10) {
                final String msg = "Clock skew is " + overallSkew +
                        " which is high. Ideally it should be sub-millisecond. Make sure NTP is running.";
                hostLog.warn(msg);
                consoleLog.warn(msg);
            } else {
                hostLog.info("Clock skew to across all nodes in the cluster is " + overallSkew);
            }

            /*
             * Limit the number of active versions to 2.
             */
            if (activeVersions.size() > 2) {
                String versions = "";
                // get the list of non-local versions
                for (String version : activeVersions) {
                    if (!version.equals(localVersionString)) {
                        versions += version + ", ";
                    }
                }
                // trim the trailing comma + space
                versions = versions.substring(0, versions.length() - 2);

                VoltDB.crashLocalVoltDB("Cluster already is running mixed voltdb versions (" + versions +").\n" +
                                        "Adding version " + localVersionString + " would add a third version.\n" +
                                        "VoltDB hotfix support supports only two unique versions simulaniously.", false, null);
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
            hostLog.error("Failed to establish socket mesh.", e);
            throw new RuntimeException(e);
        }
    }

    public void shutdown() throws InterruptedException {
        if (m_selector != null) {
            try {
                m_selector.close();
            } catch (Exception e) {}
        }
        m_es.shutdownNow();
        m_es.awaitTermination(356, TimeUnit.DAYS);
        for (ServerSocketChannel ssc : m_listenerSockets) {
            try {
                ssc.close();
            } catch (Exception e) {}
        }
        m_listenerSockets.clear();
        if (m_selector != null) {
            try {
                m_selector.close();
            } catch (Exception e) {}
            m_selector = null;
        }
    }

    int getLocalHostId() {
        return m_localHostId;
    }
}
