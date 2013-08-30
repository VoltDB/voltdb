/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.export.processors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.network.Connection;
import org.voltcore.network.InputHandler;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportDataSource;
import org.voltdb.export.ExportGeneration;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.NotImplementedException;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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

    private final Map<Integer, String> m_clusterMetadata = new HashMap<Integer, String>();

    /**
     * As long as m_shouldContinue is true, the service will listen for new
     * TCP/IP connections on LISTENER_PORT. At the moment, multiple client
     * connections do not maintain separate poll/ack state. Meaning, if client-1
     * acks data not yet read by client-2, that data will not be visible to
     * client-2. Using multiple clients is heavily advised against.
     */
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);

    private CopyOnWriteArrayList<Connection> m_knownConnections = new CopyOnWriteArrayList<Connection>();

    // this run loop can almost be eliminated.  the InputHandler can
    // call the sb.event() function directly. event() never blocks.
    // just need a way for execution sites to respond with poll
    // responses. Would be really great if we had an interface to
    // schedule a non-network event against an input handler.
    private final Runnable m_runLoop = new Runnable() {
        @Override
        public void run() {
            try {
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
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Error in RawProcessor run loop", true, e);
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
         */
        void closeConnection() {
            m_state = RawProcessor.CLOSED;
            List<ListenableFuture<?>> tasks = new ArrayList<ListenableFuture<?>>();
            for (ExportDataSource ds : m_sourcesArray) {
                ExportProtoMessage m =
                    new ExportProtoMessage( ds.getGeneration(), ds.getPartitionId(), ds.getSignature()).close();
                tasks.add(ds.exportAction(new ExportInternalMessage(this, m)));
            }
            try {
                Futures.allAsList(tasks).get();
            } catch (Exception e) {
                m_logger.error("Error inside ExportDataSource on close", e);
            }
        }

        /**
         * Produce a protocol error.
         * @param m message that caused the error
         * @param string error message
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
                    public ByteBuffer[] serialize() throws IOException {
                        return new ByteBuffer[] { r.toBuffer() };
                    }
                    @Override
                    public void cancel() {
                    }
                });
            closeConnection();
        }

        @Override
        public void event(final ExportProtoMessage m)
        {
            if (m_logger.isTraceEnabled()) {
                m_logger.trace(m);
            }
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

                try {
                    VoltZK.updateClusterMetadata(m_clusterMetadata);
                } catch (Exception e) {
                    protocolError(m, org.voltcore.utils.CoreUtils.throwableToString(e));
                }

                // Respond by advertising the full data source set and
                //  the set of up nodes that are up
                byte jsonBytes[] = null;
                try {
                    JSONStringer stringer = new JSONStringer();
                    stringer.object();
                    stringer.key("sources").array();
                    for (ExportDataSource src : m_sourcesArray) {
                        stringer.object();
                        src.writeAdvertisementTo(stringer);
                        stringer.endObject();
                    }
                    stringer.endArray();

                    // serialize the makup of the cluster
                    //  - the catalog context knows which hosts are up
                    //  - the hostmessenger knows the hostnames of hosts
                    stringer.key("clusterMetadata").array();
                    for (String metadata : m_clusterMetadata.values()) {
                        stringer.value(metadata);
                    }
                    stringer.endArray();
                    stringer.endObject();

                    JSONObject jsObj = new JSONObject(stringer.toString());
                    String msg = jsObj.toString(4);
                    m_logger.trace(msg);
                    jsonBytes = msg.getBytes(Charsets.UTF_8);
//                    else {
//                        // for test code
//                        fs.writeInt(0);
//                    }
                }
                catch (JSONException e) {
                    protocolError(m, "Error producing open response advertisement.");
                    return;
                }

                final ExportProtoMessage r = new ExportProtoMessage( -1, -1, "");
                r.openResponse(ByteBuffer.wrap(jsonBytes));
                m_c.writeStream().enqueue(
                    new DeferredSerialization() {
                        @Override
                        public ByteBuffer[] serialize() throws IOException {
                            return new ByteBuffer[] { r.toBuffer() };
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
                source.exportAction(new ExportInternalMessage(this, m));
                return;
            }

            else if (m.isClose()) {
                // no  response to CLOSE
                closeConnection();
                return;
            }

            else if (m.isPollResponse()) {
                if (m_logger.isTraceEnabled()) {
                    m_logger.trace(m);
                }
                // Forward this response to the IO system. It originated at an
                // ExecutionSite that processed an exportAction.
                m_c.writeStream().enqueue(
                    new DeferredSerialization() {
                        @Override
                        public ByteBuffer[] serialize() throws IOException {
                            return new ByteBuffer[] { m.toBuffer() };
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
        public void flattenToBuffer(ByteBuffer buf) {
            throw new NotImplementedException("Invalid serialization request.");
        }

        @Override
        protected void initFromBuffer(ByteBuffer buf) {
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
        private final boolean m_isAdminPort;

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
            m_knownConnections.add(c);
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
        public void stopping(final Connection c) {
            m_mailbox.add(new Runnable() {
                @Override
                public void run() {
                    m_logger.trace("Nulling out m_currentConnection");
                    m_knownConnections.remove(c);
                    m_sb.closeConnection();
                }
            });
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
    public void setProcessorConfig(Properties config) {
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
        m_logger.trace("Shutting down old processor");
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
            } else {
                m_logger.trace("Current thread calling shutdown is processor thread");
            }
        }
        for (Connection c : m_knownConnections) {
            c.unregister();
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
        for (Connection c : m_knownConnections) {
            m_logger.info("Booting export connection " + c.getHostnameAndIPAndPort());
            c.unregister();
        }
    }
}

