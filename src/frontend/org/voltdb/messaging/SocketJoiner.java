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
import java.util.Hashtable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.VoltDB;

/** SocketJoiner runs at startup to create a fully meshed cluster.
 * The primary (aka: leader, coordinater) node listens on BASE_PORT.
 * All non-primary nodes connect to BASE_PORT and read a 4 byte host
 * id. They then listen on BASE_PORT + host_id.  When the configured
 * number of nodes are joined, the primary instructs each non-primary
 * host to connect to the other non-primary hosts, passing the
 * requisite host ids.
 */
public class SocketJoiner extends Thread {

    private static final Logger LOG = Logger.getLogger(SocketJoiner.class.getName());
    static final int BASE_PORT = 3021;
    static final int CONTROL_PORT = 23895;
    static final int COORD_HOSTID = 0;
    static final int COMMAND_NONE = 0;
    static final int COMMAND_CONNECT = 1; // followed by hostId, hostId
    static final int COMMAND_LISTEN = 2;  // followed by hostId
    static final int COMMAND_COMPLETE = 3;
    static final int COMMAND_SENDTIME_AND_CRC = 4;
    static final int COMMAND_NTPFAIL = 5;
    static final int COMMAND_CRCFAIL = 6;
    static final int RESPONSE_LISTENING = 0;
    static final int RESPONSE_CONNECTED = 1;
    static final int MAX_ACCEPTABLE_TIME_DIFF_IN_MS = 100;
    static final int PING = 333;
    InetAddress m_coordIp;
    int m_localHostId;
    Hashtable<Integer, SocketChannel> m_sockets = new Hashtable<Integer, SocketChannel>();
    Hashtable<Integer, DataInputStream> m_inputs = new Hashtable<Integer, DataInputStream>();
    Hashtable<Integer, DataOutputStream> m_outputs = new Hashtable<Integer, DataOutputStream>();
    ServerSocketChannel m_listenerSocket = null;
    int m_expectedHosts;
    Logger m_hostLog;
    long m_timestamp;//Part of instanceId
    Integer m_addr;
    long m_catalogCRC;

    // helper so all streams in inputs are wrapped uniformly
    private DataInputStream addToInputs(Integer hostId, InputStream s) {
        // if not buffered, in.writeInt() will write 1 byte at time
        // to the network when PSH is set and host order != network order.
        DataInputStream in = new DataInputStream(new BufferedInputStream(s));
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

    public SocketJoiner(InetAddress coordIp, int expectedHosts, long catalogCRC, Logger hostLog) {
        m_coordIp = coordIp;
        m_expectedHosts = expectedHosts;
        m_hostLog = hostLog;
        m_catalogCRC = catalogCRC;
    }

    @Override
    public void run() {
        // Try to become primary regardless of configuration.
        try {
            m_listenerSocket = ServerSocketChannel.open();
            m_listenerSocket.socket().bind(new InetSocketAddress(m_coordIp, BASE_PORT));
            m_listenerSocket.socket().setPerformancePreferences(0, 2, 1);
        }
        catch (IOException e) {
            if (m_listenerSocket != null) {
                try {
                    m_listenerSocket.close();
                    m_listenerSocket = null;
                }
                catch (IOException ex) {
                    Logger.getLogger(SocketJoiner.class.getName()).l7dlog(Level.FATAL, null, ex);
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

        try {
            while (m_sockets.size() < (m_expectedHosts - 1)) {
                socket = m_listenerSocket.accept();
                socket.socket().setTcpNoDelay(true);
                socket.socket().setPerformancePreferences(0, 2, 1);

                m_sockets.put(nextHostId, socket);

                out = getOutputForHost(nextHostId);
                out.writeInt(nextHostId);
                out.write(instanceIdBuffer.array());
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

            // ask each connection to send it's time and catalog CRC
            for (int hostId = 1; hostId < m_expectedHosts; hostId++) {
                out = getOutputForHost(hostId);
                in = getInputForHost(hostId);

                out.writeInt(COMMAND_SENDTIME_AND_CRC);
                out.flush();
                long timestamp = in.readLong();
                difftimes[hostId - 1] = System.currentTimeMillis() - timestamp;
                othercrcs[hostId - 1] = in.readLong();
            }

            // figure out how bad the skew is and if it's acceptable
            int command = COMMAND_COMPLETE;
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
                command = COMMAND_NTPFAIL;

            // figure out if any catalogs are not identical
            for (long crc : othercrcs) {
                if (crc != m_catalogCRC) {
                    command = COMMAND_CRCFAIL;
                }
            }

            for (int hostId = 1; hostId < m_expectedHosts; hostId++) {
                out = getOutputForHost(hostId);
                out.writeLong(maxDiffMS);
                out.writeInt(command);
                out.flush();
            }

            if (m_hostLog != null)
                m_hostLog.info("Maximum clock/network skew is " + maxDiffMS + " milliseconds (according to leader)");
            if (command == COMMAND_NTPFAIL) {
                if (m_hostLog != null)
                    m_hostLog.info("Maximum clock/network is " + (maxDiffMS*100)/MAX_ACCEPTABLE_TIME_DIFF_IN_MS +
                                   "% higher than allowable limit");
                VoltDB.crashVoltDB();
            }
            if (command == COMMAND_CRCFAIL) {
                if (m_hostLog != null)
                    m_hostLog.info("Catalog checksums do not match across cluster");
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
            while (socket == null) {
                try {
                    socket = SocketChannel.open(new InetSocketAddress(m_coordIp, BASE_PORT));
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
            byte instanceIdBytes[] = new byte[12];
            in.read(instanceIdBytes);
            ByteBuffer instanceId = ByteBuffer.wrap(instanceIdBytes);
            m_timestamp = instanceId.getLong();
            m_addr = instanceId.getInt();
            m_sockets.put(COORD_HOSTID, socket);

            // start the server socket on the main
            LOG.debug("Non-Primary Creating its Listener Socket");
            m_listenerSocket = ServerSocketChannel.open();
            m_listenerSocket.socket().bind(new InetSocketAddress(BASE_PORT + m_localHostId));
            if (LOG.isDebugEnabled()) {
                LOG.debug("Non-Primary Listening on port:" + (BASE_PORT + m_localHostId));
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
                    newSock = SocketChannel.open(new InetSocketAddress(ip, BASE_PORT + hostId));
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
            out.flush();
            long maxDiffMS = in.readLong();
            if (m_hostLog != null)
                m_hostLog.info("Maximum clock/network skew is " + maxDiffMS + " milliseconds (according to leader)");
            command = in.readInt();
            if (command == COMMAND_NTPFAIL) {
                if (m_hostLog != null)
                    m_hostLog.info("Maximum clock/network is " + (maxDiffMS*100)/MAX_ACCEPTABLE_TIME_DIFF_IN_MS +
                                   "% higher than allowable limit");
                VoltDB.crashVoltDB();
            }
            if (command == COMMAND_CRCFAIL) {
                if (m_hostLog != null)
                    m_hostLog.info("Catalog checksums do not match across cluster");
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

    int getLocalHostId() {
        return m_localHostId;
    }

    Hashtable<Integer, SocketChannel> getHostsAndSockets() {
        return m_sockets;
    }

    Hashtable<Integer, DataOutputStream> getHostsAndOutputs() {
        return m_outputs;
    }

    Hashtable<Integer, DataInputStream> getHostsAndInputs() {
        return m_inputs;
    }
}
