/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

// -*- mode: java; c-basic-offset: 4; -*-

package org.voltdb.exportclient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportManager;
import org.voltdb.export.ExportManagerInterface.ExportMode;
import org.voltdb.exportclient.decode.CSVStringDecoder;

import com.google_voltpatches.common.net.HostAndPort;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class SocketExporter extends ExportClientBase {

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    public static final String SYNC_BLOCK_PROP = "syncblocks";
    public static final String SYNC_BLOCK_MSG = "__SYNC_BLOCK__";
    private static final byte[] SYNC_BLOCK_BYTES;
    static {
        byte[] bytes = null;
        try {
            bytes = (SYNC_BLOCK_MSG+"\n").getBytes("UTF-8");
        } catch(UnsupportedEncodingException e) { /* Will not happen */ }
        SYNC_BLOCK_BYTES = bytes;
    }

    String host;
    boolean m_skipInternals = false;
    TimeZone m_timeZone = VoltDB.REAL_DEFAULT_TIMEZONE;
    ExportDecoderBase.BinaryEncoding m_binaryEncoding = ExportDecoderBase.BinaryEncoding.HEX;
    private String[] serverArray;
    private boolean m_syncBlocks;

    @Override
    public void configure(Properties config) throws Exception {
        host = config.getProperty("socket.dest", "localhost:5001");
        setRunEverywhere(Boolean.parseBoolean(config.getProperty("replicated", "false")));
        String skipVal = config.getProperty("skipinternals", "false").trim();
        m_skipInternals = Boolean.parseBoolean(skipVal);
        String timeZoneID = config.getProperty("timezone", "").trim();
        if (!timeZoneID.isEmpty()) {
            m_timeZone = TimeZone.getTimeZone(timeZoneID);
        }
        m_syncBlocks = Boolean.parseBoolean(config.getProperty(SYNC_BLOCK_PROP));

        //Dont do actual config in check mode.
        boolean configcheck = Boolean.parseBoolean(config.getProperty(ExportManager.CONFIG_CHECK_ONLY, "false"));
        if (configcheck) {
            return;
        }

        serverArray = host.split(",");
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     * @throws IOException
     */
    static Pair<OutputStream, InputStream> connectToOneServer(String server, int port) throws IOException {
        @SuppressWarnings("resource") // We close the outputstream, which closes the socket
        Socket pushSocket = new Socket(server, port);
        return new Pair<>(pushSocket.getOutputStream(), pushSocket.getInputStream());
    }

    /**
     * One instance of {@code SocketExporter} exports rows for 1 stream/partition and opens
     * a socket to each target host.
     *
     */
    class SocketExportDecoder extends ExportDecoderBase {
        private final ListeningExecutorService m_es;

        long transactions = 0;
        long totalDecodeTime = 0;
        long timerStart = 0;
        final CSVStringDecoder m_decoder;
        final Map<HostAndPort, Pair<OutputStream, InputStream>> haplist = new HashMap<HostAndPort, Pair<OutputStream, InputStream>>();

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        SocketExportDecoder(AdvertisedDataSource source) {
            super(source);
            CSVStringDecoder.Builder builder = CSVStringDecoder.builder();
            builder
                .dateFormatter(Constants.ODBC_DATE_FORMAT_STRING)
                .timeZone(m_timeZone)
                .binaryEncoding(m_binaryEncoding)
                .skipInternalFields(m_skipInternals)
            ;
            m_decoder = builder.build();
            if (VoltDB.getExportManager().getExportMode() == ExportMode.BASIC) {
                m_es =
                        CoreUtils.getListeningSingleThreadExecutor(
                                "Socket Export decoder for partition " + source.partitionId, CoreUtils.MEDIUM_STACK_SIZE);
            } else {
                m_es = null;
            }
        }

        /**
         * Connect to a set of servers and track the connections.
         *
         * @param haplist map of hosts to writers filled by this method
         * @throws InterruptedException if anything bad happens with the threads.
         * @throws IOException on any IO error trying to connect to the sockets
         */
        void connect() throws IOException {
            m_logger.info("Connecting to Socket export endpoint...");

            for (final String server : serverArray) {
                int port = 5001;
                HostAndPort hap = HostAndPort.fromString(server);
                if (hap.hasPort()) {
                    port = hap.getPort();
                }
                haplist.put(hap, connectToOneServer(hap.getHost(), port));
            }
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            try {
                for (Pair<OutputStream, InputStream> streams : haplist.values()) {
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("Flushing " + streams.getFirst() + " for source " + source);
                    }
                    try (OutputStream out = streams.getFirst(); InputStream in = streams.getSecond()) {
                        streams.getFirst().flush();
                    } catch(IOException e) {
                        m_logger.error("Failed to close streams for " + source, e);
                    }
                }
            } finally {
                haplist.clear();
            }
            if (m_es != null) {
                m_es.shutdown();
                try {
                    m_es.awaitTermination(365, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public boolean processRow(ExportRow rd) throws ExportDecoderBase.RestartBlockException {
            try {
                if (haplist.isEmpty()) {
                    connect();
                }
                if (haplist.isEmpty()) {
                    m_logger.rateLimitedLog(120, Level.ERROR, null, "Failed to connect to export socket endpoint %s, some servers may be down.", host);
                    throw new RestartBlockException(true);
                }
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug(m_source.tableName + ":P" + m_source.partitionId + " sending seqNum: " + rd.values[2]);
                }
                String decoded = m_decoder.decode(rd.generation, rd.tableName, rd.types, rd.names,null, rd.values).concat("\n");
                byte b[] = decoded.getBytes();
                ByteBuffer buf = ByteBuffer.allocate(b.length);
                buf.put(b);
                buf.flip();
                for (Pair<OutputStream, InputStream> streams : haplist.values()) {
                    streams.getFirst().write(buf.array());
                    streams.getFirst().flush();
                }
            } catch (Exception e) {
                m_logger.warn("Unexpected error processing row: " + e.getLocalizedMessage() + ". Row will be retried.");
                haplist.clear();
                throw new RestartBlockException(true);
            }

            return true;
        }

        @Override
        public void onBlockCompletion(ExportRow row) throws RestartBlockException {
            try {
                for (Pair<OutputStream, InputStream> streams : haplist.values()) {
                    if (m_syncBlocks) {
                        streams.getFirst().write(SYNC_BLOCK_BYTES);
                    }
                    streams.getFirst().flush();
                    if (m_syncBlocks) { // wait for the other side to tell us it got the block
                        if (streams.getSecond().read() == -1) {
                            throw new RestartBlockException("Target may not have received the block", true);
                        }
                    }
                }
            } catch (Exception ex) {
                m_logger.rateLimitedLog(120, Level.WARN, null,
                        "Failed to flush block to socket endpoint %s, some servers may be down. The rows will be retried", host);
                haplist.clear();
                throw new RestartBlockException("Error finishing the block", ex, true);
            }
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new SocketExportDecoder(source);
    }
}
