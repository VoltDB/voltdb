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

package org.voltdb.elt.processors;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;
import org.voltdb.VoltDB;
import org.voltdb.elt.ELTDataBlock;
import org.voltdb.elt.ELTDataProcessor;
import org.voltdb.network.Connection;
import org.voltdb.network.InputHandler;
import org.voltdb.network.QueueMonitor;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.DBBPool.BBContainer;


/**
 * A processor that streams data blocks over a socket to
 * a remote listener without any translation of data.
 */
public class RawProcessor extends Thread implements ELTDataProcessor {

    /** VoltNetwork connection to the remote destination. */
    private Connection m_connection;
    private Logger m_logger;

    public RawProcessor() {
        m_connection = null;
        m_logger = null;
    }

    /**
     * The data block serializer the network thread pool invokes to
     * produce a serialized block of data on the connection's write
     * stream.
     */
    private static class BlockSerializer implements DeferredSerialization {
        BlockSerializer(final Logger logger, final ELTDataBlock block) {
            m_logger = logger;
            m_block = block;
        }

        @Override
        public BBContainer serialize(final DBBPool p) throws IOException {
            return m_block.m_data;
        }

        @Override
        public void cancel() {
            m_logger.error("Block serializer cancelled by network " +
                           m_block.m_data.b.remaining() + " bytes discarded.");
            m_block.m_data.discard();
        }

        private final Logger m_logger;
        private final ELTDataBlock m_block;
    }

    /**
     * The network read handler for the raw processor network stream.
     */
    private static class NetworkHandler implements InputHandler {
        @Override
        public int getExpectedOutgoingMessageSize() {
            // not necessary as network is not doing fastserialization
            // for the raw processor.
            return 0;
        }

        @Override
        public int getMaxRead() {
            // never any data to read from remote
            return 0;
        }

        @Override
        public void handleMessage(final ByteBuffer message, final Connection c) {
            // ELT protocol doesn't specify input from remote.
            assert(false);
        }

        @Override
        public ByteBuffer retrieveNextMessage(final Connection c) throws IOException {
            // ELT protocol doesn't specify input from remote.
            // Don't expect this to be called but would be called
            // if  reads were not unselected.
            return null;
        }

        @Override
        public void started(final Connection c) {
        }

        @Override
        public void starting(final Connection c) {
        }

        @Override
        public void stopped(final Connection c) {
            // eltxxx: need to handle disconnected remote.
        }

        @Override
        public void stopping(final Connection c) {
        }

        @Override
        public Runnable offBackPressure() {
            return null;
        }

        @Override
        public Runnable onBackPressure() {
            return null;
        }

        @Override
        public QueueMonitor writestreamMonitor() {
            return null;
        }

        @Override
        public long connectionId() {
            return 0;
        }
    }

    /**
     *  Open a socket to the specified destination.
     */
    @Override
    public void addHost(final String url, final String database,
            final String username, final String password)
    {
        SocketAddress sockaddr;
        SocketChannel socket;
        boolean connected = false;

        // raw processor only works with a single destination
        if (m_connection != null) {
            m_logger.error("Multiple destinations not supported. " +
                           "Using first configured host.");
            return;
        }

        // URL in this case is just host:port
        String hostport[] = url.split(":");
        if (hostport.length != 2) {
            m_logger.error("RawProcessor URL must be formatted host:port");
        }
        String host = hostport[0];
        String port = hostport[1];

        do {
            try {
                sockaddr = new InetSocketAddress(InetAddress.getLocalHost(), Integer.parseInt(port));
                socket = SocketChannel.open(sockaddr);
                m_connection = VoltDB.instance().getNetwork().registerChannel(socket, new NetworkHandler());
                m_logger.info("Connected to destination " + url);
                connected = true;

            } catch (final NumberFormatException e) {
                m_logger.error("Invalid port number in host connection configuration.");
            } catch (final UnknownHostException e) {
                m_logger.error("Unknown host " + host + " in host configuration.");
            } catch (final IOException e) {
                m_logger.error("Error connecting to host " + host + " : " + e.getMessage());
            }
        } while (!connected);
    }

    @Override
    public void addTable(final String database, final String tableName, final int tableId) {
        // nothing to do.
    }

    @Override
    public void readyForData() {
        m_logger.info("Processor ready for data.");
    }

    /**
     * Cleverly delegate all block processing to the network pool.
     * This method can be invoked concurrently by multiple execution sites.
     */
    @Override
    synchronized public boolean process(final ELTDataBlock block) {
        if (block.isStopMessage()) {
            m_logger.info("Received stop message. Stopping processor.");
            // Warning: do a blocking flush of the write stream
            while (m_connection.writeStream().isEmpty() == false) {
            }
            // unregister will close and cleanup all connection resources
            VoltDB.instance().getNetwork().unregisterChannel(m_connection);
            return true;
        }
        else {
            m_logger.debug("Sending block (" + block.m_data.b.remaining() +
                           " bytes) to write stream.");

            // The write stream will cancel() anything not queued.
            return m_connection.writeStream().
                enqueue(new BlockSerializer(m_logger, block));
        }
    }

    /**
     * Assumes caller synchronizes this call with process().. which it must do
     * anyway to produce a meaningful or useful answer.
     */
    @Override
    public boolean isIdle() {
        return m_connection.writeStream().isEmpty();
    }

    @Override
    public void addLogger(Logger logger) {
        m_logger = logger;
    }

}
