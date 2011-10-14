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
package org.voltdb.messaging;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.voltdb.OperationMode;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;

/** SocketJoiner runs at startup to create a fully meshed cluster.
 * The primary (aka: leader, coordinater) node listens on BASE_PORT.
 * All non-primary nodes connect to BASE_PORT and read a 4 byte host
 * id. They then listen on BASE_PORT + host_id.  When the configured
 * number of nodes are joined, the primary instructs each non-primary
 * host to connect to the other non-primary hosts, passing the
 * requisite host ids.
 */
public class SocketJoiner extends Thread {

    private static final VoltLogger LOG = new VoltLogger(SocketJoiner.class.getName());
    private static final VoltLogger recoveryLog = new VoltLogger("RECOVERY");
    //static final int BASE_PORT = 3021;
    static final int COORD_HOSTID = 0;
    static final int COMMAND_NONE = 0;
    static final int COMMAND_CONNECT = 1; // followed by hostId, hostId
    static final int COMMAND_LISTEN = 2;  // followed by hostId
    static final int COMMAND_COMPLETE = 3;
    static final int COMMAND_SENDTIME_AND_CRC = 4;
    static final int COMMAND_JOINFAIL = 5;
    static final int RESPONSE_LISTENING = 0;
    static final int RESPONSE_CONNECTED = 1;
    static final int MAX_ACCEPTABLE_TIME_DIFF_IN_MS = 100;

    static final int NTP_FAILURE = 1;
    static final int CRC_FAILURE = 2;
    static final int HOSTID_FAILURE = 4;
    static final int CATVER_FAILURE = 8;
    static final int DEPCRC_FAILURE = 16;
    static final int CATTXNID_FAILURE = 32;
    static final int FAULT_MISMATCH_FAILURE = 64;

    static final int PING = 333;
    InetAddress m_coordIp = null;
    int m_localHostId = 0;
    Map<Integer, SocketChannel> m_sockets = new HashMap<Integer, SocketChannel>();
    Map<Integer, DataInputStream> m_inputs = new HashMap<Integer, DataInputStream>();
    Map<Integer, DataOutputStream> m_outputs = new HashMap<Integer, DataOutputStream>();
    ServerSocketChannel m_listenerSocket = null;
    int m_expectedHosts;
    VoltLogger m_hostLog;
    long m_timestamp;//Part of instanceId
    Integer m_addr;
    long m_catalogCRC;
    long m_deploymentCRC;
    int m_discoveredCatalogVersion = 0;
    long m_discoveredCatalogTxnId = 0;
    long m_discoveredFaultSequenceNumber;

    // from configuration data
    int m_internalPort;
    String m_internalInterface;

    // helper so all streams in inputs are wrapped uniformly
    private DataInputStream addToInputs(Integer hostId, InputStream s) {
        // if not buffered, in.writeInt() will write 1 byte at time
        // to the network when PSH is set and host order != network order.
        //**update So we used to buffer on both sides, but on the read side
        // this could eagerly pull in bytes to the BIS that were meant to be
        // picked up by VoltNetwork. This only showed up on volt5a for ENG-1066
        // We can afford the read syscall overhead, so we will only buffer on the write side
        // to get around the writeInt issue.
        DataInputStream in = new DataInputStream(s);
        assert (in != null);
        m_inputs.put(hostId, in);
        return in;
    }

    // helper so all streams in outputs are wrapped uniformly
    private DataOutputStream addToOutputs(Integer hostId, OutputStream s) {
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s));
        assert (out != null);
        m_outputs.put(hostId, out);
        return out;
    }

    public SocketJoiner() {
        super("Socket Joiner");
    }
    /** Set to true when the thread exits correctly. */
    private boolean success = false;

    public boolean getSuccess() {
        return success;
    }

    public SocketJoiner(InetAddress coordIp, int expectedHosts,
            long catalogCRC, long deploymentCRC, VoltLogger hostLog) {
        m_coordIp = coordIp;
        m_expectedHosts = expectedHosts;
        m_hostLog = hostLog;
        m_catalogCRC = catalogCRC;
        m_deploymentCRC = deploymentCRC;
    }

    public SocketJoiner(ServerSocketChannel acceptor, int expectedHosts,
            long catalogCRC, long deploymentCRC, VoltLogger hostLog) {
        m_listenerSocket = acceptor;
        m_expectedHosts = expectedHosts;
        m_hostLog = hostLog;
        m_catalogCRC = catalogCRC;
        m_deploymentCRC = deploymentCRC;
        m_addr = ByteBuffer.wrap(acceptor.socket().getInetAddress().getAddress()).getInt();
    }

    @Override
    public void run() {
        // set defaults
        VoltDB.Configuration unsetConfig = new VoltDB.Configuration();
        m_internalPort = unsetConfig.m_internalPort;
        m_internalInterface = unsetConfig.m_internalInterface;

        // if there is config info, use it
        VoltDBInterface vdbinst = VoltDB.instance();
        if (vdbinst != null) {
            VoltDB.Configuration config = vdbinst.getConfig();
            if (config != null) {
                m_internalPort = config.m_internalPort;
                m_internalInterface = config.m_internalInterface;
            }
        }

        // if the cluster has already started and this is a rejoin
        if (m_listenerSocket != null) {
            if (m_hostLog != null)
                m_hostLog.info("Connecting to existing VoltDB cluster as a replacement...");
            runJoinExisting();
        }
        // join a new cluster as primary or existing
        else {

            // Try to become primary regardless of configuration.
            try {
                m_listenerSocket = ServerSocketChannel.open();
                InetSocketAddress inetsockaddr = new InetSocketAddress(m_coordIp, m_internalPort);
                m_listenerSocket.socket().bind(inetsockaddr);
                m_listenerSocket.socket().setPerformancePreferences(0, 2, 1);
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
                if (m_hostLog != null)
                    m_hostLog.info("Connecting to VoltDB cluster as the leader...");
                runPrimary();
            }
            else {
                if (m_hostLog != null)
                    m_hostLog.info("Connecting to the VoltDB cluster leader...");
                runNonPrimary();
            }
        }

        // check that we're well connected
        assert m_outputs.size() == m_inputs.size();
        assert m_inputs.size() == m_sockets.size();
        assert m_sockets.size() == (m_expectedHosts - 1);
        success = true;
    }

    DataInputStream getInputForHost(int hostId) {
        DataInputStream in = m_inputs.get(hostId);
        if (in == null) {
            SocketChannel socket = m_sockets.get(hostId);
            try {
                in = addToInputs(hostId, socket.socket().getInputStream());
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return in;
    }

    DataOutputStream getOutputForHost(int hostId) {
        DataOutputStream out = m_outputs.get(hostId);
        if (out == null) {
            SocketChannel socket = m_sockets.get(hostId);
            try {
                out = addToOutputs(hostId, socket.socket().getOutputStream());
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return out;
    }

    private void runPrimary() {
        m_timestamp = System.currentTimeMillis();
        m_addr = ByteBuffer.wrap(m_coordIp.getAddress()).getInt();
        ByteBuffer instanceIdBuffer = ByteBuffer.allocate(12);
        instanceIdBuffer.putLong(m_timestamp);
        instanceIdBuffer.put(m_coordIp.getAddress());
        instanceIdBuffer.flip();
        SocketChannel socket = null;
        DataInputStream in = null;
        DataOutputStream out = null;
        int nextHostId = 1;

        LOG.debug("Starting Coordinator");

        // make sure the primary node knows it's own metadata
        VoltDB.instance().getClusterMetadataMap().put(0, VoltDB.instance().getLocalMetadata());
        long txnId = org.voltdb.TransactionIdManager.makeIdFromComponents(System.currentTimeMillis(), 0, 0);
        m_discoveredCatalogTxnId = txnId;
        try {
            while (m_sockets.size() < (m_expectedHosts - 1)) {
                socket = m_listenerSocket.accept();
                socket.socket().setTcpNoDelay(true);
                socket.socket().setPerformancePreferences(0, 2, 1);

                m_sockets.put(nextHostId, socket);

                out = getOutputForHost(nextHostId);
                out.writeInt(nextHostId);
                out.write(instanceIdBuffer.array());
                out.writeLong(txnId);
                out.flush();
                in = getInputForHost(nextHostId);
                int response = in.readInt();
                assert response == RESPONSE_LISTENING;
                nextHostId++;
            }

            for (int listenId = 1; listenId < m_expectedHosts; listenId++) {
                for (int connectId = listenId + 1; connectId < m_expectedHosts; connectId++) {

                    out = getOutputForHost(listenId);
                    out.writeInt(COMMAND_LISTEN);
                    out.writeInt(connectId);
                    out.flush();

                    out = getOutputForHost(connectId);
                    out.writeInt(COMMAND_CONNECT);
                    out.writeInt(listenId);
                    socket = m_sockets.get(listenId);
                    // write the socket as bytes
                    InetAddress ip = socket.socket().getInetAddress();
                    byte[] ipBytes = ip.getAddress();
                    out.writeInt(ipBytes.length);
                    out.write(ipBytes);
                    out.flush();

                    in = getInputForHost(listenId);
                    int response = in.readInt();
                    assert response == RESPONSE_CONNECTED;
                }
            }

            long difftimes[] = new long[m_expectedHosts - 1];
            long othercrcs[] = new long[m_expectedHosts - 1];
            long otherdepcrcs[] = new long[m_expectedHosts - 1];

            // ask each connection to send it's time and catalog CRC
            for (int hostId = 1; hostId < m_expectedHosts; hostId++) {
                out = getOutputForHost(hostId);
                out.writeInt(COMMAND_SENDTIME_AND_CRC);
                out.flush();
            }
            for (int hostId = 1; hostId < m_expectedHosts; hostId++) {
                in = getInputForHost(hostId);
                long timestamp = in.readLong();
                difftimes[hostId - 1] = System.currentTimeMillis() - timestamp;
                othercrcs[hostId - 1] = in.readLong();
                otherdepcrcs[hostId - 1] = in.readLong();
            }

            // figure out how bad the skew is and if it's acceptable
            long minimumDiff = 0;
            long maximumDiff = 0;
            for (long diff : difftimes) {
                if (diff > maximumDiff)
                    maximumDiff = diff;
                if (diff < minimumDiff)
                    minimumDiff = diff;
            }

            int errors = 0;

            long maxDiffMS = maximumDiff - minimumDiff;
            if (maxDiffMS > MAX_ACCEPTABLE_TIME_DIFF_IN_MS)
                errors |= NTP_FAILURE;

            // figure out if any catalogs are not identical
            for (long crc : othercrcs) {
                if (crc != m_catalogCRC) {
                    System.err.printf("remote crc: %d, local crc: %d\n", crc, m_catalogCRC);
                    System.err.flush();
                    errors |= CRC_FAILURE;
                }
            }

            // figure out if any deployment files are not identical
            for (long crc : otherdepcrcs) {
                if (crc != m_deploymentCRC) {
                    errors |= DEPCRC_FAILURE;
                }
            }

            for (int hostId = 1; hostId < m_expectedHosts; hostId++) {
                out = getOutputForHost(hostId);
                out.writeLong(maxDiffMS);
                if (errors == 0) {
                    out.writeInt(COMMAND_COMPLETE);
                }
                else {
                    out.writeInt(COMMAND_JOINFAIL);
                    out.writeInt(errors);
                }
                out.flush();
            }

            if (m_hostLog != null)
                m_hostLog.info("Maximum clock/network skew is " + maxDiffMS + " milliseconds (according to leader)");
            if ((errors & NTP_FAILURE) != 0) {
                if (m_hostLog != null)
                    m_hostLog.error("Maximum clock/network is " + (maxDiffMS*100)/MAX_ACCEPTABLE_TIME_DIFF_IN_MS +
                                   "% higher than allowable limit");
            }
            if ((errors & CRC_FAILURE) != 0) {
                if (m_hostLog != null)
                    m_hostLog.error("Catalog checksums do not match across cluster");
            }
            if ((errors & DEPCRC_FAILURE) != 0) {
                if (m_hostLog != null)
                    m_hostLog.error("Deployment file checksums do not match across cluster");
            }
            if (errors != 0) {
                VoltDB.crashVoltDB();
            }

        }
        catch (IOException e) {
            e.printStackTrace();
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

    private void runNonPrimary() {
        SocketChannel socket = null;
        DataInputStream in = null;
        DataOutputStream out = null;
        try {
            LOG.debug("Non-Primary Starting");
            LOG.debug("Non-Primary Connecting to Primary");

            InetSocketAddress inetsockaddr = new InetSocketAddress(m_coordIp, m_internalPort);

            while (socket == null) {
                try {
                    socket = SocketChannel.open(inetsockaddr);
                }
                catch (java.net.ConnectException e) {
                    LOG.warn("Joining primary failed: " + e.getMessage() + " retrying..");
                    try {
                        sleep(250); //  milliseconds
                    }
                    catch (InterruptedException ex) {
                        // don't really care.
                    }
                }
            }
            socket.socket().setTcpNoDelay(true);
            socket.socket().setPerformancePreferences(0, 2, 1);
            in = addToInputs(COORD_HOSTID, socket.socket().getInputStream());

            // send the local hostid out
            LOG.debug("Non-Primary Reading its Host ID");
            m_localHostId = in.readInt();
            byte instanceIdBytes[] = new byte[20];
            in.read(instanceIdBytes);
            ByteBuffer instanceId = ByteBuffer.wrap(instanceIdBytes);
            m_timestamp = instanceId.getLong();
            m_addr = instanceId.getInt();
            m_discoveredCatalogTxnId = instanceId.getLong();
            m_sockets.put(COORD_HOSTID, socket);

            // start the server socket on the right interface
            LOG.debug("Non-Primary Creating its Listener Socket");
            m_listenerSocket = ServerSocketChannel.open();

            if ((m_internalInterface == null) || (m_internalInterface.length() == 0)) {
                inetsockaddr = new InetSocketAddress(m_internalPort + m_localHostId);
            }
            else {
                inetsockaddr = new InetSocketAddress(m_internalInterface, m_internalPort + m_localHostId);
            }
            m_listenerSocket.socket().bind(inetsockaddr);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Non-Primary Listening on port:" + (m_internalPort + m_localHostId));
            }

            out = getOutputForHost(COORD_HOSTID);

            out.writeInt(RESPONSE_LISTENING);
            out.flush();

            LOG.debug("Non-Primary Fetching Instructions");
            int command = COMMAND_NONE;
            while (command != COMMAND_SENDTIME_AND_CRC) {
                command = in.readInt();
                if ((command != COMMAND_CONNECT) && (command != COMMAND_LISTEN)) {
                    continue;
                }

                int hostId = in.readInt();
                SocketChannel newSock = null;

                if (command == COMMAND_CONNECT) {
                    LOG.debug("Non-Primary Connect Request");
                    // read an ip address from bytes
                    int ipSize = in.readInt();
                    byte[] ipBytes = new byte[ipSize];
                    in.readFully(ipBytes);
                    InetAddress ip = InetAddress.getByAddress(ipBytes);
                    LOG.debug("Opening non-primary socket: " + ip.toString());
                    inetsockaddr = new InetSocketAddress(ip, m_internalPort + hostId);
                    newSock = SocketChannel.open(inetsockaddr);
                }
                if (command == COMMAND_LISTEN) {
                    LOG.debug("Non-Primary Listen Request");
                    newSock = m_listenerSocket.accept();
                }
                newSock.socket().setTcpNoDelay(true);
                newSock.socket().setPerformancePreferences(0, 2, 1);

                m_sockets.put(hostId, newSock);
                DataOutputStream out1 = getOutputForHost(hostId);
                out1.writeInt(PING);
                out1.flush();
                DataInputStream in1 = getInputForHost(hostId);
                int pingValue = in1.readInt();
                assert pingValue == PING;

                if (command == COMMAND_LISTEN) {
                    out.writeInt(RESPONSE_CONNECTED);
                    out.flush();
                }

                LOG.debug("Non-Primary Fetching Instructions");
            }

            // write the current time
            out.writeLong(System.currentTimeMillis());
            // write the local catalog crc
            out.writeLong(m_catalogCRC);
            // write the local deployment crc
            out.writeLong(m_deploymentCRC);
            out.flush();
            long maxDiffMS = in.readLong();
            if (m_hostLog != null)
                m_hostLog.info("Maximum clock/network skew is " + maxDiffMS + " milliseconds (according to leader)");
            command = in.readInt();
            if (command == COMMAND_JOINFAIL) {
                int errors = in.readInt();

                if ((errors & NTP_FAILURE) != 0) {
                    if (m_hostLog != null)
                        m_hostLog.error("Maximum clock/network is " + (maxDiffMS*100)/MAX_ACCEPTABLE_TIME_DIFF_IN_MS +
                                       "% higher than allowable limit");
                }
                if ((errors & CRC_FAILURE) != 0) {
                    if (m_hostLog != null)
                        m_hostLog.error("Catalog checksums do not match across cluster");
                }
                VoltDB.crashVoltDB();
            }
            assert(command == COMMAND_COMPLETE);
        }
        catch (IOException e) {
            m_hostLog.error("Failed to establish socket mesh.", e);
            throw new RuntimeException(e);
        }
        finally {
            if (m_listenerSocket != null) {
                try {
                    m_listenerSocket.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            LOG.debug("Non-Primary Done");
        }
    }

    /**
     * @return Catalog version number.
     */
    private void runJoinExisting() {
        SocketChannel socket = null;
        DataInputStream in = null;
        DataOutputStream out = null;

        LOG.debug("Starting Coordinator");

        try {
            HashSet<Integer> hostsFound = null;
            while (m_sockets.size() < (m_expectedHosts - 1)) {
                socket = m_listenerSocket.accept();
                socket.socket().setTcpNoDelay(true);
                socket.socket().setPerformancePreferences(0, 2, 1);

                InputStream s = socket.socket().getInputStream();
                in = new DataInputStream(new BufferedInputStream(s));
                int hostId = in.readInt();
                m_timestamp = in.readLong();
                m_addr = in.readInt();
                int numHosts = in.readInt();
                HashSet<Integer> hosts = new HashSet<Integer>(numHosts);
                for (int ii = 0; ii < numHosts; ii++) {
                    hosts.add(in.readInt());
                }
                if (hostsFound == null) {
                    hostsFound = hosts;
                    m_expectedHosts = hostsFound.size() + 1;
                    recoveryLog.info("Hosts found: " + hostsFound.toString());
                } else if (!hostsFound.equals(hosts)) {
                    recoveryLog.fatal("Inconsistent live host set during rejoin");
                    VoltDB.crashVoltDB();
                }
                // set the admin mode here, not in the local deployment file for rejoining nodes
                VoltDB.instance().setStartMode(in.readBoolean() ? OperationMode.PAUSED
                                                                : OperationMode.RUNNING);
                // read the current catalog
                int catalogLength = in.readInt();
                byte[] catalogBytes = new byte[catalogLength];
                in.readFully(catalogBytes);
                VoltDB.instance().writeNetworkCatalogToTmp(catalogBytes);

                m_sockets.put(hostId, socket);
                recoveryLog.info("Have " + m_sockets.size() + " of " + (m_expectedHosts - 1) + " with hostId " + hostId);
            }

            // read the timestamps from all

            long difftimes[] = new long[m_expectedHosts - 1];
            int readHostIds[] = new int[m_expectedHosts - 1];
            long othercrcs[] = new long[m_expectedHosts - 1];
            long faultSequenceNumbers[] = new long [m_expectedHosts - 1];
            long otherdepcrcs[] = new long[m_expectedHosts - 1];
            int catalogVersions[] = new int[m_expectedHosts - 1];
            long catalogTxnIds[] = new long[m_expectedHosts - 1];
            boolean haveDoneRestore[] = new boolean[m_expectedHosts - 1];
            for (Entry<Integer, SocketChannel> e : m_sockets.entrySet()) {
                out = getOutputForHost(e.getKey());
                out.writeInt(COMMAND_SENDTIME_AND_CRC);
                out.flush();
            }
            int i = 0;
            for (Entry<Integer, SocketChannel> e : m_sockets.entrySet()) {
                in = getInputForHost(e.getKey());

                readHostIds[i] = in.readInt();
                othercrcs[i] = in.readLong();
                otherdepcrcs[i] = in.readLong();
                long timestamp = in.readLong();
                difftimes[i] = System.currentTimeMillis() - timestamp;
                catalogVersions[i] = in.readInt();
                haveDoneRestore[i] = in.readByte() == 0 ? false : true;
                catalogTxnIds[i] = in.readLong();
                faultSequenceNumbers[i] = in.readLong();
                i++;
            }

            int errors = 0;

            // figure out how bad the skew is and if it's acceptable
            long minimumDiff = 0;
            long maximumDiff = 0;
            for (long diff : difftimes) {
                if (diff > maximumDiff)
                    maximumDiff = diff;
                if (diff < minimumDiff)
                    minimumDiff = diff;
            }
            long maxDiffMS = maximumDiff - minimumDiff;
            if (maxDiffMS > MAX_ACCEPTABLE_TIME_DIFF_IN_MS)
                errors |= NTP_FAILURE;

            // ensure all hostids are the same
            m_localHostId = readHostIds[0];
            recoveryLog.info("Selecting host id " + m_localHostId);
            for (i = 1; i < readHostIds.length; i++) {
                if (readHostIds[i] != m_localHostId) {
                    errors |= HOSTID_FAILURE;
                }
            }

            // figure out if any catalogs are not identical
            for (long crc : othercrcs) {
                if (crc != m_catalogCRC) {
                    System.err.printf("remote crc: %d, local crc: %d\n", crc, m_catalogCRC);
                    System.err.flush();
                    errors |= CRC_FAILURE;
                }
            }

            // figure out if any deployment files are not identical
            for (long crc : otherdepcrcs) {
                if (crc != m_deploymentCRC) {
                    errors |= DEPCRC_FAILURE;
                }
            }

            // ensure all catalog versions are the same
            m_discoveredCatalogVersion = catalogVersions[0];
            for (int version : catalogVersions) {
                if (version != m_discoveredCatalogVersion) {
                    errors |= CATVER_FAILURE;
                }
            }

            m_discoveredCatalogTxnId = catalogTxnIds[0];
            for (long txnId : catalogTxnIds) {
                if (txnId != m_discoveredCatalogTxnId) {
                    errors |= CATTXNID_FAILURE;
                }
            }

            m_discoveredFaultSequenceNumber = faultSequenceNumbers[0];
            for (long number : faultSequenceNumbers) {
                if (m_discoveredFaultSequenceNumber != number) {
                    recoveryLog.fatal("Fault sequence number " + number +
                            " does not match previously seen number " + m_discoveredFaultSequenceNumber);
                    errors |= FAULT_MISMATCH_FAILURE;
                }
            }

            // check if a restore has been done on the rest of the cluster
            boolean b_haveDoneRestore = haveDoneRestore[0];
            // and check if all nodes agree about this
            boolean restoreAgreement = true;
            for (boolean b__haveDoneRestore : haveDoneRestore) {
                if (b_haveDoneRestore != b__haveDoneRestore) {
                    restoreAgreement = false;
                    break;
                }
            }
            // if they agree, great
            if (restoreAgreement) {
                org.voltdb.sysprocs.SnapshotRestore.m_haveDoneRestore = b_haveDoneRestore;
            }
            // if not...
            else {
                if (m_hostLog != null)
                    m_hostLog.error("Cluster does not agree on whether a restore has been performed");
                // just assume a restore has been done
                org.voltdb.sysprocs.SnapshotRestore.m_haveDoneRestore = true;
            }


            for (Entry<Integer, SocketChannel> e : m_sockets.entrySet()) {
                out = getOutputForHost(e.getKey());
                out.writeLong(maxDiffMS);
                if (errors == 0) {
                    out.writeInt(COMMAND_COMPLETE);
                }
                else {
                    out.writeInt(COMMAND_JOINFAIL);
                    out.writeInt(errors);
                }
                out.flush();
            }

            if (m_hostLog != null)
                m_hostLog.info("Maximum clock/network skew is " + maxDiffMS + " milliseconds (according to rejoined node)");
            if ((errors & NTP_FAILURE) != 0) {
                if (m_hostLog != null)
                    m_hostLog.error("Maximum clock/network is " + (maxDiffMS*100)/MAX_ACCEPTABLE_TIME_DIFF_IN_MS +
                                   "% higher than allowable limit");
            }
            if ((errors & CRC_FAILURE) != 0) {
                if (m_hostLog != null)
                    m_hostLog.error("Catalog checksums do not match across cluster");
            }
            if ((errors & HOSTID_FAILURE) != 0) {
                if (m_hostLog != null) {
                    m_hostLog.error("Cluster nodes didn't agree on a host id for the rejoining node.");
                    m_hostLog.error("This is likely a bug in VoltDB and you should contact the VoltDB team.");
                }
            }
            if ((errors & CATVER_FAILURE) != 0) {
                if (m_hostLog != null) {
                    m_hostLog.error("Cluster nodes didn't agree on all catalog metadata.");
                    m_hostLog.error("This is likely a bug in VoltDB and you should contact the VoltDB team.");
                }
            }
            if ((errors & DEPCRC_FAILURE) != 0) {
                if (m_hostLog != null)
                    m_hostLog.error("Deployment file checksums do not match across cluster");
            }
            if ((errors & FAULT_MISMATCH_FAILURE) != 0) {
                if (m_hostLog != null)
                    m_hostLog.error("Cluster does not agree on the fault sequence number");
            }

            if (errors != 0) {
                VoltDB.crashVoltDB();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
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

    /**
     * Helper method for the rejoin process. This is called by nodes inside the cluster
     * to initiate a connection to nodes re-joining the cluster.
     *
     * @param hostId The calling node's host id.
     * @param address The address the re-joining node is listening on.
     * @return A connected SocketChannel to the re-joining node, or null on failure.
     * @throws Exception
     */
    static SocketChannel connect(
            int localHostId,
            int rejoiningHostId,
            InetSocketAddress address,
            long catalogCRC,
            long deploymentCRC,
            Set<Integer> liveHosts,
            long faultSequenceNumber,
            int catalogVersionNumber,
            long catalogTxnId,
            byte[] catalogBytes) throws Exception {
        SocketChannel remoteConnection = null;
        try {
            // open a connection to the re-joining node
            remoteConnection = SocketChannel.open(address);
            remoteConnection.socket().setSoTimeout(1000);

            // create helper streams for IO
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(remoteConnection.socket().getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(remoteConnection.socket().getInputStream()));

            // write the id of this host
            out.writeInt(localHostId);
            Object instanceId[] = VoltDB.instance().getInstanceId();
            out.writeLong((Long)instanceId[0]);
            out.writeInt((Integer)instanceId[1]);
            out.writeInt(liveHosts.size());
            for (Integer site : liveHosts) {
                out.writeInt(site);
            }
            // send the admin mode here, don't use local deployment file for rejoining nodes
            out.writeBoolean(VoltDB.instance().getMode() == OperationMode.PAUSED);
            // write the catalog contents
            out.writeInt(catalogBytes.length);
            out.write(catalogBytes);
            out.flush();

            // read in the command to acknowledge connection and to request the time
            int command = in.readInt();
            if (command != COMMAND_SENDTIME_AND_CRC)
                throw new Exception(String.format("Unexpected command (%d) from joining node.", command));

            // write the id of the new host
            out.writeInt(rejoiningHostId);

            // write catalog & deployment crcs
            out.writeLong(catalogCRC);
            out.writeLong(deploymentCRC);

            // write the current time so the re-join node can measure skew
            out.writeLong(System.currentTimeMillis());

            // write catalog version number
            out.writeInt(catalogVersionNumber);

            //Write whether a restore of the cluster has already been done
            out.writeByte(org.voltdb.sysprocs.SnapshotRestore.m_haveDoneRestore ? 1 : 0);

            out.writeLong(catalogTxnId);

            out.writeLong(faultSequenceNumber);

            // flush these 3 writes
            out.flush();

            // read the confirmation command
            long maxDiffMS = in.readLong();
            recoveryLog.info("Re-joining node reports " + maxDiffMS + " ms skew.");
            command = in.readInt();
            if (command == COMMAND_COMPLETE)
                return remoteConnection;
            else {
                int errors = in.readInt();
                System.out.println("Errors " + errors);
                String msg = "";

                if ((errors & NTP_FAILURE) != 0) {
                    msg += String.format("Maximum clock/network is " + (maxDiffMS*100)/MAX_ACCEPTABLE_TIME_DIFF_IN_MS +
                                       "% higher than allowable limit.\n");
                }
                if ((errors & CRC_FAILURE) != 0) {
                    msg += String.format("Catalog checksums do not match across cluster.\n");
                }
                if ((errors & HOSTID_FAILURE) != 0) {
                    msg += String.format("Cluster nodes didn't agree on a host id for the rejoining node.\n");
                    msg += String.format("This is likely a bug in VoltDB and you should contact the VoltDB team.\n");
                }
                if ((errors & CATVER_FAILURE) != 0) {
                    msg += String.format("Cluster nodes didn't agree on all catalog metadata.\n");
                    msg += String.format("This is likely a bug in VoltDB and you should contact the VoltDB team.\n");
                }
                if ((errors & CATTXNID_FAILURE) != 0) {
                    msg += String.format("Cluster nodes didn't agree on catalog txn id.\n");
                    msg += String.format("This is likely a bug in VoltDB and you should contact the VoltDB team.\n");
                }
                throw new Exception(msg);
            }
        }
        catch (Exception e) {
            //e.printStackTrace();
            String emesg = e.getMessage();
            if (emesg == null) {
                emesg = e.getClass().getName();
            }
            String msg = String.format("Unable to re-join node. Socket failure with message: " + emesg);
            throw new Exception(msg);
        }
    }

    int getLocalHostId() {
        return m_localHostId;
    }

    int getDiscoveredCatalogVersionId() {
        return m_discoveredCatalogVersion;
    }

    long getDiscoveredCatalogTxnId() {
        return m_discoveredCatalogTxnId;
    }

    Map<Integer, SocketChannel> getHostsAndSockets() {
        return m_sockets;
    }

    Map<Integer, DataOutputStream> getHostsAndOutputs() {
        return m_outputs;
    }

    Map<Integer, DataInputStream> getHostsAndInputs() {
        return m_inputs;
    }
}
