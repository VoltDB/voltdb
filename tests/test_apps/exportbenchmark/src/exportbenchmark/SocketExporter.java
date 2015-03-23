/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;

/**
 * Export class for performance measuring.
 * Export statistics are checked for timestamps, and performance metrics are
 * periodically pushed to a UDP socket for collection.
 */
public class SocketExporter extends ExportClientBase {
    String host;
    int port;
    int statsDuration;
    InetSocketAddress address;
    DatagramChannel channel;

    // Statistics
    long transactions = 0;
    long totalDecodeTime = 0;

    @Override
    public void configure(Properties config) throws Exception {
        host = config.getProperty("socket.dest", "localhost");
        port = Integer.parseInt(config.getProperty("socket.port", "5001"));
        statsDuration = Integer.parseInt(config.getProperty("stats.duration", "5"));

        if (host == "localhost") {
            address = new InetSocketAddress(InetAddress.getLocalHost(), port);
        } else {
            address = new InetSocketAddress(host, port);
        }
        channel = DatagramChannel.open();
    }

    class SocketExportDecoder extends ExportDecoderBase {
        Timer timer;
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        SocketExportDecoder(AdvertisedDataSource source) {
            super(source);

            schedulePeriodicStats();
        }

        /**
         * Periodically print statistics to a UDP socket
         */
        public void schedulePeriodicStats() {
            timer = new Timer();
            TimerTask statsPrinting = new TimerTask() {
                @Override
                public void run() { printStatistics(); }
            };
            timer.scheduleAtFixedRate(statsPrinting,
                                      statsDuration * 1000,
                                      statsDuration * 1000);
        }

        /**
         * Prints performance statistics to a socket. Stats are:
         *   -Transactions per second
         *   -Average time for decodeRow() to run
         *   -Partition ID
         */
        public void printStatistics() {
            // Calculate statistics
            double tps, averageDecodeTime;
            try {
                tps = transactions / statsDuration;
                averageDecodeTime = totalDecodeTime / transactions;
            } catch (ArithmeticException e) {
                // Divide by zero error. No transactions, so nothing to report
                return;
            }

            transactions = 0;
            totalDecodeTime = 0;

            // Create message
            String message = "tps:" + tps
                           + ",decodeTime:" + averageDecodeTime
                           + ",partitionId:" + m_source.partitionId
                           + "\n";

            buffer.clear();
            buffer.put((byte)message.length());
            buffer.put(message.getBytes());
            buffer.flip();

            // Send message over socket
            try {
                channel.send(buffer, address);
            } catch (IOException e) {
                System.err.println("Couldn't send stats to socket");
                System.err.println(e.getLocalizedMessage());
            }
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            timer.cancel();
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

                totalDecodeTime += (endTime - startTime);
            } catch (IOException e) {
                System.err.println(e.getLocalizedMessage());
            }

            return true;
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new SocketExportDecoder(source);
    }
}