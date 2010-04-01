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

package org.voltdb.utils;

import org.voltdb.sysprocs.saverestore.TableSaveFile;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.text.SimpleDateFormat;

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
    private static final char[] m_tsvEscapeChars = new char[] { '\t', '\n', '\r', '\\' };
    private static final char[] m_csvEscapeChars = new char[] { ',', '"', '\n', '\r' };

    private interface Escaper {
        public String escape(String s);
    }

    private static boolean contains(final String s, final char characters[]) {
        for (int ii = 0; ii < s.length(); ii++) {
            char c = s.charAt(ii);
            for (int qq = 0; qq < characters.length; qq++) {
                if (characters[qq] == c) {
                    return true;
                }
            }
        }
        return false;
    }

    private static final class CSVEscaper implements Escaper {
        public String escape(String s) {
            if (!contains(s, m_csvEscapeChars)) {
                return s;
            }
            StringBuffer sb = new StringBuffer(s.length() + (int)(s.length() * .10));
            sb.append('"');
            for (int ii = 0; ii < s.length(); ii++) {
                char c = s.charAt(ii);
                if (c == '"') {
                    sb.append("\"\"");
                } else {
                    sb.append(c);
                }
            }
            sb.append('"');
            return sb.toString();
        }
    }

    private static final class TSVEscaper implements Escaper {
        public String escape(String s) {
            if (!contains( s, m_tsvEscapeChars)) {
                return s;
            }
            StringBuffer sb = new StringBuffer(s.length() + (int)(s.length() * .10));
            for (int ii = 0; ii < s.length(); ii++) {
                char c = s.charAt(ii);
                if (c == '\\') {
                    sb.append("\\\\");
                } else if(c == '\t') {
                    sb.append("\\t");
                } else if (c == '\n') {
                    sb.append("\\n");
                } else if (c == '\r') {
                    sb.append("\\r");
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }
    }

    public CSVTableSaveFile(File saveFile, char delimeter, Escaper escaper) throws IOException {
        m_delimeter = delimeter;
        m_escaper = escaper;
        final FileInputStream fis = new FileInputStream(saveFile);
        m_saveFile = new TableSaveFile(fis.getChannel(), 10, null);
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
                    final VoltTable vt = new VoltTable(c.b, true);
                    StringBuilder sb = new StringBuilder(size * 2);
                    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS:");
                    while (vt.advanceRow()) {
                        for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                            final VoltType type = vt.getColumnType(ii);
                            if (ii != 0) {
                                sb.append(',');
                            }
                            if ( type == VoltType.BIGINT ||
                                    type == VoltType.INTEGER ||
                                    type == VoltType.SMALLINT ||
                                    type == VoltType.TINYINT) {
                                sb.append(vt.getLong(ii));
                            } else if (type == VoltType.FLOAT) {
                                sb.append(vt.getDouble(ii));
                            } else if (type == VoltType.DECIMAL){
                                sb.append(vt.getDecimalAsBigDecimal(ii).toString());
                            } else if (type == VoltType.STRING){
                                sb.append(m_escaper.escape(vt.getString(ii)));
                            } else if (type == VoltType.TIMESTAMP) {
                                final TimestampType timestamp = vt.getTimestampAsTimestamp(ii);
                                StringBuilder builder = new StringBuilder(64);
                                builder.append(sdf.format(timestamp.asApproximateJavaDate()));
                                builder.append(timestamp.getUSec());
                                sb.append(m_escaper.escape(builder.toString()));
                            }
                        }
                        sb.append(m_delimeter);
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
            System.err.println("Usage: outfile.[csv | tsv] infile.vpt");
            System.exit(-1);
        }

        Escaper escaper = null;
        char delimeter = '0';

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

        final FileOutputStream fos = new FileOutputStream(outfile, true);
        try {
            final CSVTableSaveFile converter = new CSVTableSaveFile(infile, delimeter, escaper);
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
