/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the native VoltDB client
 * library.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */

package exportbenchmark;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Properties;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;

/**
 * Export class for performance measuring.
 * Export statistics are checked for timestamps, and performance metrics are
 * periodically pushed to a UDP socket for collection.
 */
public class SocketExporter extends ExportClientBase {

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    String host;
    int port;
    int statsDuration;
    InetSocketAddress address;
    DatagramChannel channel;

    @Override
    public void configure(Properties config) throws Exception {
        host = config.getProperty("socket.dest", "localhost");
        port = Integer.parseInt(config.getProperty("socket.port", "5001"));
        statsDuration = Integer.parseInt(config.getProperty("stats.duration", "5"));
        setRunEverywhere(Boolean.parseBoolean(config.getProperty("replicated", "false")));

        if ("localhost".equals(host)) {
            address = new InetSocketAddress(CoreUtils.getLocalAddress(), port);
        } else {
            address = new InetSocketAddress(host, port);
        }
        channel = DatagramChannel.open();
    }

    class SocketExportDecoder extends ExportDecoderBase {
        long transactions = 0;
        long totalDecodeTime = 0;
        long timerStart = 0;

        SocketExportDecoder(AdvertisedDataSource source) {
            super(source);
        }

        /**
         * Prints performance statistics to a socket. Stats are:
         *   -Transactions per second
         *   -Average time for decodeRow() to run
         *   -Partition ID
         */
        public void sendStatistics() {
            if (timerStart > 0) {
                ByteBuffer buffer = ByteBuffer.allocate(1024);
                Long endTime = System.currentTimeMillis();

                // Create message
                JSONObject message = new JSONObject();
                try {
                    message.put("transactions", transactions);
                    message.put("decodeTime", totalDecodeTime);
                    message.put("startTime", timerStart);
                    message.put("endTime", endTime);
                    message.put("partitionId", m_source.partitionId);
                } catch (JSONException e) {
                    m_logger.error("Couldn't create JSON object: " + e.getLocalizedMessage());
                }

                String messageString = message.toString();
                buffer.clear();
                buffer.put((byte)messageString.length());
                buffer.put(messageString.getBytes());
                buffer.flip();

                // Send message over socket
                try {
                    int sent = channel.send(buffer, address);
                    if (sent != messageString.getBytes().length+1) {
                        // Should always send the whole packet.
                        m_logger.error("Error sending entire stats message");
                    }
                } catch (IOException e) {
                    m_logger.error("Couldn't send stats to socket");
                }
                transactions = 0;
                totalDecodeTime = 0;
            }
            timerStart = System.currentTimeMillis();
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            try {
                channel.close();
            } catch (IOException ignore) {}
        }

        @Override
        /**
         * Logs the transactions, and determines how long it take to decode
         * the row.
         */
        public boolean processRow(int rowSize, byte[] rowData) throws RestartBlockException {
            // Transaction count
            transactions++;

            // Time decodeRow
            try {
                long startTime = System.nanoTime();
                decodeRow(rowData);
                long endTime = System.nanoTime();

                totalDecodeTime += endTime - startTime;
            } catch (IOException e) {
                m_logger.error(e.getLocalizedMessage());
            }

            return true;
        }

        @Override
        public void onBlockCompletion() {
            if (transactions > 0)
                sendStatistics();
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new SocketExportDecoder(source);
    }
}