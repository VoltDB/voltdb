/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportManager;
import org.voltdb.exportclient.ExportDecoderBase.RestartBlockException;
import org.voltdb.exportclient.decode.CSVStringDecoder;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.net.HostAndPort;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class SocketExporterLegacy extends ExportClientBase {

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    String host;
    boolean m_skipInternals = false;
    TimeZone m_timeZone = VoltDB.REAL_DEFAULT_TIMEZONE;
    ExportDecoderBase.BinaryEncoding m_binaryEncoding = ExportDecoderBase.BinaryEncoding.HEX;
    final Map<HostAndPort, OutputStream> haplist = new HashMap<HostAndPort, OutputStream>();
    private String[] serverArray;
    private Set<Callable<Pair<HostAndPort, OutputStream>>> callables;
    ExecutorService m_executorService;

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

        //Dont do actual config in check mode.
        boolean configcheck = Boolean.parseBoolean(config.getProperty(ExportManager.CONFIG_CHECK_ONLY, "false"));
        if (configcheck) {
            return;
        }

        serverArray = host.split(",");
        m_executorService = Executors.newFixedThreadPool(serverArray.length);
        setupConnection();
    }

    private void setupConnection() {
        callables = new HashSet<Callable<Pair<HostAndPort, OutputStream>>>();

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            callables.add(new Callable<Pair<HostAndPort, OutputStream>>() {
                @Override
                public Pair<HostAndPort, OutputStream> call() throws IOException {
                    int port = 5001;
                    HostAndPort hap = HostAndPort.fromString(server);
                    if (hap.hasPort()) {
                        port = hap.getPort();
                    }
                    return new Pair<HostAndPort, OutputStream>(hap, connectToOneServer(hap.getHostText(), port));
                }
            });
        }
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @throws InterruptedException if anything bad happens with the threads.
     * @throws RestartBlockException
     */
    void connect() throws InterruptedException, RestartBlockException {
        m_logger.info("Connecting to Socket export endpoint...");

        // gather the result or retry
        List<Future<Pair<HostAndPort, OutputStream>>> futures = m_executorService.invokeAll(callables);
        try {
            for (Future<Pair<HostAndPort, OutputStream>> future : futures) {
                Pair<HostAndPort, OutputStream> result = future.get();
                HostAndPort hap = result.getFirst();
                OutputStream writer = result.getSecond();
                if (writer != null) {
                    haplist.put(hap, writer);
                } else {
                    throw new RestartBlockException(true);
                }
            }
        } catch (ExecutionException ex) {
            ex.getCause().printStackTrace();
            throw new RestartBlockException(true);
        }
    }

    @Override
    public void shutdown() {
        m_executorService.shutdown();
        try {
            m_executorService.awaitTermination(365, TimeUnit.DAYS);
        } catch( InterruptedException iex) {
            Throwables.propagate(iex);
        }
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     * @throws IOException
     */
    static OutputStream connectToOneServer(String server, int port) throws IOException {
        try {
            Socket pushSocket = new Socket(server, port);
            OutputStream out = pushSocket.getOutputStream();
            m_logger.info("Connected to export endpoint node at: " + server + ":" + port);
            return out;
        } catch (UnknownHostException e) {
            m_logger.rateLimitedLog(120, Level.ERROR, e, "Don't know about host: " + server);
            throw e;
        } catch (IOException e) {
            m_logger.rateLimitedLog(120, Level.ERROR, e, "Couldn't get I/O for the connection to: " + server);
            throw e;
        }
    }

    class SocketExportDecoder extends ExportDecoderBase {
        private final ListeningExecutorService m_es;

        long transactions = 0;
        long totalDecodeTime = 0;
        long timerStart = 0;
        final CSVStringDecoder m_decoder;
        final AdvertisedDataSource m_source;

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        SocketExportDecoder(AdvertisedDataSource source) {
            super(source);
            m_source = source;
            CSVStringDecoder.Builder builder = CSVStringDecoder.builder();
            builder
                .dateFormatter(Constants.ODBC_DATE_FORMAT_STRING)
                .timeZone(m_timeZone)
                .binaryEncoding(m_binaryEncoding)
                .skipInternalFields(m_skipInternals)
            ;
            m_decoder = builder.build();
            m_es =
                    CoreUtils.getListeningSingleThreadExecutor(
                            "Socket Export decoder for partition " + source.partitionId, CoreUtils.MEDIUM_STACK_SIZE);
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            try {
                for (OutputStream writer : haplist.values()) {
                    writer.close();
                }
                haplist.clear();
            } catch (IOException ignore) {}
            m_es.shutdown();
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) throws ExportDecoderBase.RestartBlockException {
            try {
                if (haplist.isEmpty()) {
                    connect();
                }
                if (haplist.isEmpty()) {
                    m_logger.rateLimitedLog(120, Level.ERROR, null, "Failed to connect to export socket endpoint %s, some servers may be down.", host);
                    throw new RestartBlockException(true);
                }
                ExportRowData rd = decodeRow(rowData);
                String decoded = m_decoder.decode(m_source.m_generation, m_source.tableName, m_source.columnTypes, m_source.columnNames, "", rd.values).concat("\n");
                byte b[] = decoded.getBytes();
                ByteBuffer buf = ByteBuffer.allocate(b.length);
                buf.put(b);
                buf.flip();
                for (OutputStream hap : haplist.values()) {
                    hap.write(buf.array());
                    hap.flush();
                }
            } catch (Exception e) {
                m_logger.error(e.getLocalizedMessage());
                haplist.clear();
                throw new RestartBlockException(true);
            }

            return true;
        }

        @Override
        public void onBlockStart() throws RestartBlockException {
            assert isLegacy();
        }

        @Override
        public void onBlockCompletion() {
            assert isLegacy();
            try {
                for (OutputStream hap : haplist.values()) {
                    hap.flush();
                }
            } catch (IOException ex) {
                m_logger.rateLimitedLog(120, Level.ERROR, null, "Failed to flush to export socket endpoint %s, some servers may be down.", host);
                haplist.clear();
            }
        }

        @Override
        public void onBlockStart(ExportRow row) {
            throw new UnsupportedOperationException("onBlockStart(ExportRow row) must not be used on legacy export.");
        }

        @Override
        public void onBlockCompletion(ExportRow row) {
            throw new UnsupportedOperationException("onBlockCompletion(ExportRow row) must not be used on legacy export.");
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new SocketExportDecoder(source);
    }
}
