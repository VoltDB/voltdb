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

package org.voltdb.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.SyncFailedException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.sysprocs.saverestore.TableSaveFile;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.DelimitedDataWriterUtil.CSVWriter;
import org.voltdb.utils.DelimitedDataWriterUtil.DelimitedDataWriter;
import org.voltdb.utils.DelimitedDataWriterUtil.TSVWriter;

public class CSVTableSaveFile {
    private final AtomicInteger m_availableBytes = new AtomicInteger(0);
    private final int m_maxAvailableBytes = 16777216;
    private final LinkedBlockingQueue<byte[]> m_available = new LinkedBlockingQueue<byte[]>();
    private final Thread m_converterThreads[] = new Thread[Runtime.getRuntime()
            .availableProcessors()];
    private final AtomicReference<IOException> m_exception = new AtomicReference<IOException>(
            null);
    private final AtomicInteger m_activeConverters = new AtomicInteger(Runtime
            .getRuntime().availableProcessors());
    private final TableSaveFile m_saveFile;
    private final DelimitedDataWriter m_escaper;
    private final SimpleDateFormat m_sdf;

    public CSVTableSaveFile(File saveFile, DelimitedDataWriter escaper,
            Integer partitions[]) throws IOException {
        m_escaper = escaper;
        m_sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss:SSS:");
        final FileInputStream fis = new FileInputStream(saveFile);
        m_saveFile = new TableSaveFile(fis.getChannel(), 10, partitions);
        for (int ii = 0; ii < m_converterThreads.length; ii++) {
            m_converterThreads[ii] = new Thread(new ConverterThread());
            m_converterThreads[ii].start();
        }
    }

    /**
     * Returns a more CSV data in UTF-8 format. Returns null when there is no
     * more data. May block.
     *
     * @return null if there is no more data or a byte array contain some number
     *         of complete CSV lines
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
            m_availableBytes.addAndGet(-1 * bytes.length);
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
                    final VoltTable vt = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b, true);
                    StringBuilder sb = new StringBuilder(size * 2);
                    while (vt.advanceRow()) {
                        for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                            final VoltType type = vt.getColumnType(ii);
                            if (vt.wasNull()) {
                                m_escaper.writeRawField(sb, "NULL", ii > 0);
                            } else if (type == VoltType.BIGINT
                                    || type == VoltType.INTEGER
                                    || type == VoltType.SMALLINT
                                    || type == VoltType.TINYINT) {
                                m_escaper.writeRawField(sb, String.valueOf(vt.getLong(ii)), ii > 0);
                            } else if (type == VoltType.FLOAT) {
                                m_escaper.writeRawField(sb, String.valueOf(vt.getDouble(ii)), ii > 0);
                            } else if (type == VoltType.DECIMAL) {
                                m_escaper.writeRawField(sb, vt.getDecimalAsBigDecimal(ii).toString(), ii > 0);
                            } else if (type == VoltType.STRING) {
                                m_escaper.writeEscapedField(sb, vt.getString(ii), ii > 0);
                            } else if (type == VoltType.TIMESTAMP) {
                                final TimestampType timestamp = vt.getTimestampAsTimestamp(ii);
                                m_escaper.writeRawField(sb, m_sdf.format(timestamp.asApproximateJavaDate()), ii > 0);
                                m_escaper.writeRawField(sb, String.valueOf(timestamp.getUSec()), false);
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
                    m_exception.compareAndSet(null, e);
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
            System.err
                    .println("Usage: [--partitions 1,3,4] outfile.[csv | tsv] infile.vpt");
            System.exit(-1);
        }

        DelimitedDataWriter escaper = null;
        Integer partitions[] = null;
        if (args[0].equals("--partitions")) {
            if (args.length < 2) {
                System.err.println("Not enough args");
                System.exit(-1);
            }
            String partitionStrings[] = args[1].split(",");
            partitions = new Integer[partitionStrings.length];
            int ii = 0;
            for (String partitionString : partitionStrings) {
                partitions[ii++] = Integer.valueOf(partitionString);
            }
        }

        if (args[0].endsWith(".tsv")) {
            escaper = new TSVWriter();
        } else if (args[0].endsWith(".csv")) {
            escaper = new CSVWriter();
        } else {
            System.err
                    .println("Output filename must end in .csv or .tsv to indicate format");
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

        convertTableSaveFile(escaper, partitions, outfile, infile);
    }

    public static void convertTableSaveFile(DelimitedDataWriter escaper,
            Integer[] partitions, final File outfile, final File infile)
            throws FileNotFoundException, IOException, InterruptedException,
            SyncFailedException {
        final FileOutputStream fos = new FileOutputStream(outfile, true);
        try {
            final CSVTableSaveFile converter = new CSVTableSaveFile(infile,
                    escaper, partitions);
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
