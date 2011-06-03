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

package org.voltdb.export.processors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.CatalogContext;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportDataSource;
import org.voltdb.export.ExportGeneration;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.network.Connection;
import org.voltdb.network.InputHandler;
import org.voltdb.network.QueueMonitor;
import org.voltdb.network.VoltProtocolHandler;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.NotImplementedException;

/**
 * A processor that provides a data block queue over a socket to
 * a remote listener without any translation of data.
 */
public class RawProcessor implements ExportDataProcessor {

    // polling protocol states
    public final static int CONNECTED = 1;
    public final static int CLOSED = 2;

    /**
     * This logger facility is set by the Export manager and is configurable
     * via the standard VoltDB log configuration methods.
     */
    VoltLogger m_logger;

    /**
     * Work messages are queued and polled from this mailbox. All work
     * done by the processor thread is serialized through this mailbox.
     */
    private final LinkedBlockingDeque<Runnable> m_mailbox =
        new LinkedBlockingDeque<Runnable>();

    private ExportGeneration m_generation = null;

    ArrayList<ExportDataSource> m_sourcesArray =
        new ArrayList<ExportDataSource>();

    /**
     * As long as m_shouldContinue is true, the service will listen for new
     * TCP/IP connections on LISTENER_PORT. At the moment, multiple client
     * connections do not maintain separate poll/ack state. Meaning, if client-1
     * acks data not yet read by client-2, that data will not be visible to
     * client-2. Using multiple clients is heavily advised against.
     */
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);

    private volatile Connection m_currentConnection = null;

    // this run loop can almost be eliminated.  the InputHandler can
    // call the sb.event() function directly. event() never blocks.
    // just need a way for execution sites to respond with poll
    // responses. Would be really great if we had an interface to
    // schedule a non-network event against an input handler.
    private final Runnable m_runLoop = new Runnable() {
        @Override
        public void run() {
            Runnable r;
            while (m_shouldContinue.get() == true) {
                try {
                    r = m_mailbox.poll(5000, TimeUnit.MILLISECONDS);
                    if (r != null) {
                        r.run();
                    }
                }
                catch (InterruptedException e) {
                    // acceptable. just re-loop.
                }
            }
        }
    };

    private Thread m_thread = null;

    public interface ExportStateBlock {
        public void event(ExportProtoMessage message);
    }

    /**
     * State for an individual Export protocol connection. This could probably be
     * integrated with the PollingProtocolHandler but that object is pretty
     * exclusively called by the network threadpool. Separating this makes it
     * easier to think about concurrency.
     */
    public class ProtoStateBlock implements ExportStateBlock {
        ProtoStateBlock(Connection c, boolean isAdmin) {
            m_c = c;
            m_isAdmin = isAdmin;
            m_state = RawProcessor.CLOSED;
        }

        /**
         * This is the only valid method to transition state to closed
         * @throws MessagingException
         */
        void closeConnection() {
            m_state = RawProcessor.CLOSED;
            for (ExportDataSource ds : m_sourcesArray) {
                ExportProtoMessage m =
                    new ExportProtoMessage( ds.getGeneration(), ds.getPartitionId(), ds.getSignature()).close();
                try {
                    ds.exportAction(new ExportInternalMessage(this, m));
                } catch (Exception e) {
                    //
                    throw new RuntimeException(e);
                }
            }
        }

        /**
         * Produce a protocol error.
         * @param m message that caused the error
         * @param string error message
         * @throws MessagingException
         */
        void protocolError(ExportProtoMessage m, String string)
        {
            if (m_logger != null) {
                m_logger.error("Closing Export connection with error: " + string);
            }

            final ExportProtoMessage r =
                new ExportProtoMessage( m.getGeneration(), m.getPartitionId(), m.getSignature());
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
            closeConnection();
        }

        public void event(final ExportProtoMessage m)
        {
            if (m.isError()) {
                protocolError(m, "Internal error message. May indicate that an invalid ack offset was requested.");
                return;
            }

            else if (m.isOpenResponse()) {
                protocolError(m, "Server must not receive open response message.");
                return;
            }

            else if (VoltDB.instance().getMode() == OperationMode.PAUSED && !m_isAdmin)
            {
                protocolError(m, "Server currently unavailable for export connections on this port");
                return;
            }

            else if (VoltDB.instance().getMode() == OperationMode.INITIALIZING)
            {
                protocolError(m, "Server has not finished initialization");
                return;
            }

            else if (m.isOpen()) {
                if (m_state != RawProcessor.CLOSED) {
                    protocolError(m, "Client must not open an already opened connection.");
                    return;
                }
                if (m.isClose() || m.isPoll() || m.isAck()) {
                    protocolError(m, "Invalid combination of open with close, poll or ack.");
                    return;
                }
                m_state = RawProcessor.CONNECTED;

                // Respond by advertising the full data source set and
                //  the set of up nodes that are up
                FastSerializer fs = new FastSerializer();
                try {

                    // serialize an array of DataSources that are locally available
                    fs.writeInt(m_sourcesArray.size());
                    for (ExportDataSource src : m_sourcesArray) {
                        src.writeAdvertisementTo(fs);
                    }

                    // serialize the makup of the cluster
                    //  - the catalog context knows which hosts are up
                    //  - the hostmessenger knows the hostnames of hosts
                    CatalogContext cx = VoltDB.instance().getCatalogContext();
                    if (cx != null) {
                        Set<Integer> liveHosts = cx.siteTracker.getAllLiveHosts();
                        fs.writeInt(liveHosts.size());
                        for (int hostId : liveHosts) {
                            String metadata = VoltDB.instance().getClusterMetadataMap().get(hostId);
                            //System.out.printf("hostid %d, metadata: %s\n", hostId, metadata);
                            assert(metadata.contains(":"));
                            fs.writeString(metadata);
                        }
                    }
                    else {
                        // for test code
                        fs.writeInt(0);
                    }
                }
                catch (IOException e) {
                    protocolError(m, "Error producing open response advertisement.");
                    return;
                }

                final ExportProtoMessage r = new ExportProtoMessage( -1, -1, "");
                r.openResponse(fs.getBuffer());
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
                ExportDataSource source =
                    RawProcessor.this.getDataSourceFor(m.getPartitionId(), m.getSignature());
                if (source == null) {
                    protocolError(m, "No Export data source exists for partition(" +
                                  m.getPartitionId() + ") and table(" +
                                  m.getSignature() + ") pair.");
                    return;
                }
                try {
                    source.exportAction(new ExportInternalMessage(this, m));
                    return;
                } catch (MessagingException e) {
                    protocolError(m, e.getMessage());
                    return;
                }
            }

            else if (m.isClose()) {
                // no  response to CLOSE
                closeConnection();
                return;
            }

            else if (m.isPollResponse()) {
                // Forward this response to the IO system. It originated at an
                // ExecutionSite that processed an exportAction.
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
        final boolean m_isAdmin;
        int m_state;
    }

    /**
     * Silly pair to couple a protostate block with a message for the
     * m_mailbox queue. Make this a VoltMessage, though it is sort of
     * meaningless, to satisfy ExecutionSite mailbox requirements.
     */
    public static class ExportInternalMessage extends VoltMessage {
        public final ExportStateBlock m_sb;
        public final ExportProtoMessage m_m;

        public ExportInternalMessage(ExportStateBlock sb, ExportProtoMessage m) {
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
     * The network read handler for the raw processor network stream.
     * Must extend VoltPrococolHandler as NIOReadStream has only
     * package private methods. The handler is very simple; it uses
     * the base class to read length prefixed messages from the network
     * and pushes those messages into the processor's mailbox queue.
     */
    private class ExportInputHandler extends VoltProtocolHandler
    {
        private boolean m_isAdminPort;

        public ExportInputHandler(boolean isAdminPort)
        {
            m_isAdminPort = isAdminPort;
        }
        /**
         * Called by VoltNetwork after the connection object is constructed
         * and before the channel is registered to the selector.
         */
        @Override
        public void starting(Connection c) {
            m_currentConnection = c;
            m_sb = new ProtoStateBlock(c, m_isAdminPort);
        }

        @Override
        public void started(Connection c) {
            if (!m_shouldContinue.get()) {
                c.unregister();
            }
        }

        /**
         * Called by VoltNetwork when the port is unregistered.
         */
        @Override
        public void stopping(Connection c) {
            m_mailbox.add(new Runnable() {
                @Override
                public void run() {
                    m_currentConnection = null;
                    m_sb.closeConnection();
                }
            });
        }

        @Override
        public int getExpectedOutgoingMessageSize() {
            // roughly 2MB plus the message metadata
            return (1024 * 1024 * 2) + 128;
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
                final ExportProtoMessage m = ExportProtoMessage.readExternal(fds);
                m_mailbox.add(new Runnable() {
                    @Override
                    public void run() {
                        m_sb.event(m);
                    }
                });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Runnable offBackPressure() {
            return new Runnable() {
                @Override
                public void run() {
                    m_sb.m_c.enableReadSelection();
                }
            };
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

    ExportDataSource getDataSourceFor(int partitionId, String signature) {
        if (m_generation == null) {
            return null;
        }
        HashMap<String, ExportDataSource> partmap = m_generation.m_dataSourcesByPartition.get(partitionId);
        if (partmap == null) {
            return null;
        }
        ExportDataSource source = partmap.get(signature);
        return source;
    }

    @Override
    public void readyForData() {
        m_thread = new Thread(m_runLoop, "Raw export processor");
        m_thread.start();
        m_logger.info("Processor ready for data.");
    }

    @Override
    public void addLogger(VoltLogger logger) {
        m_logger = logger;
    }

    @Override
    public void queueWork(Runnable r) {
        m_mailbox.add(r);
    }

    @Override
    public InputHandler createInputHandler(String service, boolean isAdminPort)
    {
        if (service.equalsIgnoreCase("export")) {
            return new ExportInputHandler(isAdminPort);
        }
        return null;
    }

    @Override
    public void shutdown() {
        m_shouldContinue.set(false);
        if (m_thread != null) {
            //Don't interrupt me while I'm talking
            if (m_thread != Thread.currentThread()) {
                m_thread.interrupt();
                try {
                    m_thread.join();
                }
                catch (InterruptedException e) {
                    m_logger.error("Interruption not expected", e);
                }
            }
        }
        if (m_currentConnection != null) {
            m_currentConnection.unregister();
        }
    }

    @Override
    public boolean isConnectorForService(String service) {
        if (service.equalsIgnoreCase("export")) {
            return true;
        }
        return false;
    }

    @Override
    public void setExportGeneration(ExportGeneration generation) {
        m_generation = generation;
        for (HashMap<String, ExportDataSource> sources : generation.m_dataSourcesByPartition.values()) {
            for (ExportDataSource source : sources.values()) {
                m_sourcesArray.add(source);
            }
        }
    }

    @Override
    public void bootClient() {
        final Connection c = m_currentConnection;
        if (c != null) {
            m_logger.info("There was an export connection to boot.");
            c.unregister();
        }
    }
}

