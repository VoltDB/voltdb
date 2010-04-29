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

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.VoltDB;
import org.voltdb.elt.*;
import org.voltdb.messaging.*;
import org.voltdb.network.*;
import org.voltdb.utils.*;
import org.voltdb.utils.DBBPool.BBContainer;


/**
 * A processor that provides a data block queue over a socket to
 * a remote listener without any translation of data.
 */
public class RawProcessor extends Thread implements ELTDataProcessor {

    // polling protocol states
    public final static int CONNECTED = 1;
    public final static int CLOSED = 2;

    /**
     * This logger facility is set by the ELT manager and is configurable
     * via the standard VoltDB log configuration methods.
     */
    Logger m_logger;

    /**
     * Work messages are queued and polled from this mailbox. All work
     * done by the processor thread is serialized through this mailbox.
     */
    private final LinkedBlockingDeque<ELTInternalMessage> m_mailbox =
        new LinkedBlockingDeque<ELTInternalMessage>();

    /**
     * Data sources, one per table per site, provide the interface to
     * poll() and ack() EL data from the execution engines. Data sources
     * are configured by the ELT manager at initialization time.
     * partitionid : <tableid : datasource>.
     */
    HashMap<Integer, HashMap<Integer,ELTDataSource>> m_sources =
        new HashMap<Integer, HashMap<Integer, ELTDataSource>>();

    /**
     * As long as m_shouldContinue is true, the service will listen for new
     * TCP/IP connections on LISTENER_PORT. At the moment, multiple client
     * connections do not maintain separate poll/ack state. Meaning, if client-1
     * acks data not yet read by client-2, that data will not be visible to
     * client-2. Using multiple clients is heavily advised against.
     */
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final public static int LISTENER_PORT = 5443;
    final AcceptorThread m_acceptor = new AcceptorThread();

    /**
     * Set of accepted client connections. MUST synchronize on m_connections
     * to read or modify this collection. This data member is accessed concurrently
     * by AcceptorThread and the processor thread.
     */
    final ArrayDeque<Connection> m_connections = new ArrayDeque<Connection>();

    /**
     * State for an individual ELT protocol connection. This could probably be
     * integrated with the PollingProtocolHandler but that object is pretty
     * exclusively called by the network threadpool. Separating this makes it
     * easier to think about concurrency.
     */
    class ProtoStateBlock {
        ProtoStateBlock(Connection c) {
            m_c = c;
            m_state = RawProcessor.CLOSED;
        }

        void protocolError(ELTProtoMessage m, String string)
        {
            m_state = RawProcessor.CLOSED;

            final ELTProtoMessage r =
                new ELTProtoMessage(m.getPartitionId(), m.getTableId());
            r.error();

            m_c.writeStream().enqueue(
                new DeferredSerialization() {
                    @Override
                    public BBContainer serialize(DBBPool p) throws IOException {
                        // Must account for length prefix - thus "+4",
                        FastSerializer fs = new FastSerializer(p, r.serializableBytes() + 4);
                        r.writeToFastSerializer(fs);
                        return fs.getBBContainer();
                    }
                    @Override
                    public void cancel() {
                    }
                });
        }

        void event(final ELTProtoMessage m)
        {
            if (m.isError()) {
                protocolError(m, "Error. May indicate that an invalid ack offset was requested.");
                return;
            }

            else if (m.isOpenResponse()) {
                protocolError(m, "Server must not receive open response message.");
                return;
            }

            else if (m.isOpen()) {
                if (m_state != RawProcessor.CLOSED) {
                    protocolError(m, "Must not OPEN an already opened connection.");
                    return;
                }
                if (m.isClose() || m.isPoll() || m.isAck()) {
                    protocolError(m, "Invalid combination of OPEN with other action.");
                    return;
                }
                m_state = RawProcessor.CONNECTED;

                // TODO: need to respond with full topology information
                // For now, return first datasource partition/table pair
                // this is enough to bootstrap the test harness.
                int found_partitionId = -1;
                int found_tableId = -1;
                Set<Integer> keySet = m_sources.keySet();
                for (Integer p_id : keySet) {
                    found_partitionId = p_id;
                    Set<Integer> keySet2 = m_sources.get(p_id).keySet();
                    for (Integer t_id : keySet2) {
                        found_tableId = t_id;
                    }
                }
                assert (found_partitionId != -1);
                assert (found_tableId != -1);

                final ELTProtoMessage r = new ELTProtoMessage(found_partitionId, found_tableId);
                r.openResponse();
                m_c.writeStream().enqueue(
                    new DeferredSerialization() {
                        @Override
                        public BBContainer serialize(DBBPool p) throws IOException {
                            // Must account for length prefix - thus "+4",
                            FastSerializer fs = new FastSerializer(p, r.serializableBytes() + 4);
                            r.writeToFastSerializer(fs);
                            return fs.getBBContainer();
                        }
                        @Override
                        public void cancel() {
                        }
                    });
                return;
            }

            else if (m.isPoll() || m.isAck()) {
                if (m_state != RawProcessor.CONNECTED) {
                    protocolError(m, "Must not poll or ack a closed connection");
                    return;
                }
                ELTDataSource source =
                    RawProcessor.this.getDataSourceFor(m.getPartitionId(), m.getTableId());
                if (source == null) {
                    protocolError(m, "No ELT data source exists for partition(" +
                                  m.getPartitionId() + ") and table(" +
                                  m.getTableId() + ") pair.");
                    return;
                }
                try {
                    source.poll(new ELTInternalMessage(this, m));
                    return;
                } catch (MessagingException e) {
                    protocolError(m, e.getMessage());
                    return;
                }
            }

            else if (m.isClose()) {
                // don't respond to close. maybe should set the execution site
                // state accordingly so that it can re-set its poll point?
                m_state = RawProcessor.CLOSED;
                return;
            }

            else if (m.isPollResponse()) {
                // Forward this response to the IO system.
                m_c.writeStream().enqueue(
                    new DeferredSerialization() {
                        @Override
                        public BBContainer serialize(DBBPool p) throws IOException {
                            // remember +4 longsword of length prefixing.
                            FastSerializer fs = new FastSerializer(p, m.serializableBytes() + 4);
                            m.writeToFastSerializer(fs);
                            return fs.getBBContainer();
                        }
                        @Override
                        public void cancel() {
                        }
                    });
                return;
            }
        }

        final Connection m_c;
        int m_state;
    }

    /**
     * Silly pair to couple a protostate block with a message for the
     * m_mailbox queue. Make this a VoltMessage, though it is sort of
     * meaningless, to satisfy ExecutionSite mailbox requirements.
     */
    public static class ELTInternalMessage extends VoltMessage {
        public final ProtoStateBlock m_sb;
        public final ELTProtoMessage m_m;

        public ELTInternalMessage(ProtoStateBlock sb, ELTProtoMessage m) {
            m_sb = sb;
            m_m = m;
        }

        @Override
        protected void flattenToBuffer(DBBPool pool) {
            throw new NotImplementedException("Invalid serialization request.");
        }

        @Override
        protected void initFromBuffer() {
            throw new NotImplementedException("Invalid serialization request.");
        }
    }

    /**
     * An internal thread that blocks on socket accept. Accepted connections
     * create NetworkHandlers that run the client poll/ack protocol.
     */
    private class AcceptorThread extends Thread
    {
        AcceptorThread() {
            super("ELT client acceptor");
        }

        @Override
        public void run() {
            ServerSocketChannel acceptSock;
            try {
                acceptSock = ServerSocketChannel.open();
                acceptSock.configureBlocking(true);
                acceptSock.socket().bind(new InetSocketAddress(LISTENER_PORT));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            do {
                try {
                    final SocketChannel socket = acceptSock.accept();
                    PollingProtocolHandler ih = new PollingProtocolHandler();

                    Connection conn =
                        VoltDB.instance().getNetwork().registerChannel(socket, ih, 0);

                    ProtoStateBlock sb = new ProtoStateBlock(conn);
                    ih.setStateBlock(sb);

                    synchronized (m_connections) {
                        m_connections.add(conn);
                    }

                    // can now turn on read interest -- fully configured.
                    conn.enableReadSelection();
                }
                catch (IOException e) {
                }
            } while (m_shouldContinue.get() == true);
        }
    }

    /**
     * The network read handler for the raw processor network stream.
     * Must extend VoltPrococolHandler as NIOReadStream has only
     * package private methods. The handler is very simple; it uses
     * the base class to read length prefixed messages from the network
     * and pushes those messages into the processor's mailbox queue.
     */
    private class PollingProtocolHandler extends VoltProtocolHandler
    {
        @Override
        public int getExpectedOutgoingMessageSize() {
            // roughly 2MB plus the message metadata
            return (1024 * 1024 * 2) + 128;
        }

        public void setStateBlock(ProtoStateBlock sb) {
            m_sb = sb;
        }

        @Override
        public int getMaxRead() {
            // only receiving poll requests. 8k should be plenty
            return 1024 * 8;
        }

        @Override
        public void handleMessage(final ByteBuffer message, final Connection c) {
            try {
                FastDeserializer fds = new FastDeserializer(message);
                ELTProtoMessage m = ELTProtoMessage.readExternal(fds);
                m_mailbox.add(new ELTInternalMessage(m_sb, m));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
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

        private ProtoStateBlock m_sb;
    }


    //
    //
    // The actual RawProcessor implementation
    //
    //

    public RawProcessor() {
        m_logger = null;
    }

    ELTDataSource getDataSourceFor(int partitionId, int tableId) {
        HashMap<Integer, ELTDataSource> partmap = m_sources.get(partitionId);
        if (partmap == null) {
            return null;
        }
        ELTDataSource source = partmap.get(tableId);
        return source;
    }


    @Override
    public void addDataSource(ELTDataSource dataSource) {
        int partid = dataSource.getPartitionId();
        int tableid = dataSource.getTableId();

        HashMap<Integer, ELTDataSource> partmap = m_sources.get(partid);
        if (partmap == null) {
            partmap = new HashMap<Integer, ELTDataSource>();
            m_sources.put(partid, partmap);
        }
        assert(partmap.get(tableid) == null);
        partmap.put(tableid, dataSource);
    }

    @Override
    public void readyForData() {
        m_logger.info("Processor ready for data.");

        // allow new connections now
        m_acceptor.start();

        // start the mailbox processing
        this.start();
    }

    @Override
    public boolean isIdle() {
        synchronized (m_connections) {
            for (Connection c : m_connections) {
                if (!c.writeStream().isEmpty()) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public void addLogger(Logger logger) {
        m_logger = logger;
    }

    @Override
    public void queueMessage(ELTInternalMessage m) {
        m_mailbox.add(m);
    }

    // this run loop can almost be eliminated.  the InputHandler can
    // call the sb.event() function directly. event() never blocks.
    // just need a way for execution sites to respond with poll
    // responses. Would be really great if we had an interface to
    // schedule a non-network event against an input handler.
    @Override
    public void run() {
        ELTInternalMessage p;
        while (m_shouldContinue.get() == true) {
            try {
                p = m_mailbox.poll(5000, TimeUnit.MILLISECONDS);
                if (p != null) {
                    p.m_sb.event(p.m_m);
                }
            }
            catch (InterruptedException e) {
                // acceptable. just re-loop.
            }
        }
    }

}
