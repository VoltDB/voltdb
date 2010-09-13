/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.utils;

import org.voltdb.sysprocs.saverestore.TableSaveFile;
import org.voltdb.utils.CSVEscaperUtil.Escaper;
import org.voltdb.utils.CSVEscaperUtil.CSVEscaper;
import org.voltdb.utils.CSVEscaperUtil.TSVEscaper;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.SyncFailedException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.text.SimpleDateFormat;
import java.math.BigDecimal;

public class CSVTableSaveFile {
    private final AtomicInteger m_availableBytes = new AtomicInteger(0);
    private final int m_maxAvailableBytes = 16777216;
    private final LinkedBlockingQueue<byte[]> m_available = new LinkedBlockingQueue<byte[]>();
    private final Thread m_converterThreads[] = new Thread[Runtime.getRuntime().availableProcessors()];
    private final AtomicReference<IOException> m_exception = new AtomicReference<IOException>(null);
    private final AtomicInteger m_activeConverters = new AtomicInteger(Runtime.getRuntime().availableProcessors());
    private final TableSaveFile m_saveFile;
    private final char m_delimeter;
    private final Escaper m_escaper;

    public CSVTableSaveFile(File saveFile, char delimeter, Escaper escaper, int partitions[]) throws IOException {
        m_delimeter = delimeter;
        m_escaper = escaper;
        final FileInputStream fis = new FileInputStream(saveFile);
        m_saveFile = new TableSaveFile(fis.getChannel(), 10, partitions);
        for (int ii = 0; ii < m_converterThreads.length; ii++) {
            m_converterThreads[ii] = new Thread(new ConverterThread());
            m_converterThreads[ii].start();
        }
    }

    /**
     * Returns a more CSV data in UTF-8 format. Returns null when there is no more data. May block.
     * @return null if there is no more data or a byte array contain some number of complete CSV lines
     * @throws IOException
     */
    public byte[] read() throws IOException {
        if (m_exception.get() != null) {
            throw m_exception.get();
        }

        byte bytes[] = null;
        if (m_activeConverters.get() == 0) {
            bytes = m_available.poll();
        } else {
            try {
                bytes = m_available.take();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        if (bytes != null) {
            m_availableBytes.addAndGet( -1 * bytes.length);
        }
        return bytes;
    }

    public void close() throws IOException, InterruptedException {
        m_saveFile.close();
        for (final Thread t : m_converterThreads) {
            t.interrupt();
            t.join();
        }
    }

    private class ConverterThread implements Runnable {

        private void convertChunks() throws IOException, InterruptedException {
            while (!Thread.interrupted() && m_saveFile.hasMoreChunks()) {
                if (m_availableBytes.get() > m_maxAvailableBytes) {
                    Thread.sleep(5);
                    continue;
                }

                BBContainer c = m_saveFile.getNextChunk();
                if (c == null) {
                    return;
                }

                try {
                    final int size = c.b.remaining();
                    final VoltTable vt =
                        PrivateVoltTableFactory.createVoltTableFromBuffer(c.b, true);
                    StringBuilder sb = new StringBuilder(size * 2);
                    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss:SSS:");
                    while (vt.advanceRow()) {
                        for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                            final VoltType type = vt.getColumnType(ii);
                            if (ii != 0) {
                                sb.append(m_delimeter);
                            }
                            if ( type == VoltType.BIGINT ||
                                    type == VoltType.INTEGER ||
                                    type == VoltType.SMALLINT ||
                                    type == VoltType.TINYINT) {
                                long value = vt.getLong(ii);
                                if (vt.wasNull()) {
                                    sb.append("NULL");
                                } else {
                                    sb.append(value);
                                }
                            } else if (type == VoltType.FLOAT) {
                                double value = vt.getDouble(ii);
                                if (vt.wasNull()) {
                                    sb.append("NULL");
                                } else {
                                    sb.append(value);
                                }
                            } else if (type == VoltType.DECIMAL){
                                BigDecimal value = vt.getDecimalAsBigDecimal(ii);
                                if (vt.wasNull()) {
                                    sb.append("NULL");
                                } else {
                                    sb.append(value.toString());
                                }
                            } else if (type == VoltType.STRING){
                                String value = vt.getString(ii);
                                if (vt.wasNull()) {
                                    sb.append("NULL");
                                } else {
                                    sb.append(m_escaper.escape(value));
                                }
                            } else if (type == VoltType.TIMESTAMP) {
                                final TimestampType timestamp = vt.getTimestampAsTimestamp(ii);
                                if (vt.wasNull()) {
                                    sb.append("NULL");
                                } else {
                                    StringBuilder builder = new StringBuilder(64);
                                    builder.append(sdf.format(timestamp.asApproximateJavaDate()));
                                    builder.append(timestamp.getUSec());
                                    sb.append(m_escaper.escape(builder.toString()));
                                }
                            }
                        }
                        sb.append('\n');
                    }
                    byte bytes[] = null;
                    try {
                        bytes = sb.toString().getBytes("UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        return;
                    }
                    m_availableBytes.addAndGet(bytes.length);
                    m_available.offer(bytes);
                } finally {
                    c.discard();
                }
            }
        }

        @Override
        public void run() {
            try {
                try {
                    convertChunks();
                } catch (IOException e) {
                    m_exception.compareAndSet( null, e);
                } catch (InterruptedException e) {
                    return;
                }
            } finally {
                int activeConverters = m_activeConverters.decrementAndGet();
                if (activeConverters == 0) {
                    m_available.offer(new byte[0]);
                }
            }
        }
    }

    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: [--partitions 1,3,4] outfile.[csv | tsv] infile.vpt");
            System.exit(-1);
        }

        Escaper escaper = null;
        char delimeter = '0';
        int partitions[] = null;
        if (args[0].equals("--partitions")) {
            if (args.length < 2) {
                System.err.println("Not enough args");
                System.exit(-1);
            }
            String partitionStrings[] = args[1].split(",");
            partitions = new int[partitionStrings.length];
            int ii = 0;
            for (String partitionString : partitionStrings) {
                partitions[ii++] = Integer.valueOf(partitionString);
            }
        }

        if (args[0].endsWith(".tsv")) {
            escaper = new TSVEscaper();
            delimeter = '\t';
        } else if (args[0].endsWith(".csv")) {
            escaper = new CSVEscaper();
            delimeter = ',';
        } else {
            System.err.println("Output filename must end in .csv or .tsv to indicate format");
            System.exit(-1);
        }

        final File outfile = new File(args[0]);
        if (!outfile.exists() && !outfile.createNewFile()) {
            System.err.println("Can't create output file " + args[0]);
            System.exit(-1);
        }
        if (!outfile.canWrite()) {
            System.err.println("Can't write to output file " + args[0]);
            System.exit(-1);
        }

        final File infile = new File(args[1]);
        if (!infile.exists()) {
            System.err.println("Input file " + args[1] + " does not exist");
            System.exit(-1);
        }
        if (!infile.canRead()) {
            System.err.println("Can't read input file " + args[1]);
            System.exit(-1);
        }

        convertTableSaveFile(escaper, delimeter, partitions, outfile, infile);
    }

    public static void convertTableSaveFile(Escaper escaper, char delimeter,
            int[] partitions, final File outfile, final File infile)
            throws FileNotFoundException, IOException, InterruptedException,
            SyncFailedException {
        final FileOutputStream fos = new FileOutputStream(outfile, true);
        try {
            final CSVTableSaveFile converter = new CSVTableSaveFile(infile, delimeter, escaper, partitions);
            try {
                while (true) {
                    final byte bytes[] = converter.read();
                    if (bytes.length == 0) {
                        break;
                    }
                    fos.write(bytes);
                }
            } finally {
                try {
                    converter.close();
                } finally {
                    fos.getFD().sync();
                }
            }
        } finally {
            fos.close();
        }
    }
}
