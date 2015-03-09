/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.SyncFailedException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.sysprocs.saverestore.TableSaveFile;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;

public class CSVTableSaveFile {
    private final AtomicInteger m_availableBytes = new AtomicInteger(0);
    private final int m_maxAvailableBytes = 16777216;
    private final LinkedBlockingQueue<byte[]> m_available = new LinkedBlockingQueue<byte[]>();
    private final Thread m_converterThreads[] = new Thread[CoreUtils.availableProcessors()];
    private final AtomicReference<IOException> m_exception = new AtomicReference<IOException>(
            null);
    private final AtomicInteger m_activeConverters = new AtomicInteger(CoreUtils.availableProcessors());
    private final TableSaveFile m_saveFile;
    private final char m_delimiter;

    public CSVTableSaveFile(File saveFile, char delimiter, Integer partitions[])
            throws IOException {
        m_delimiter = delimiter;
        final FileInputStream fis = new FileInputStream(saveFile);
        m_saveFile = new TableSaveFile(fis, 10, partitions);
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
     *
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
            int lastNumCharacters = 1024 * 64;
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
                    final VoltTable vt = PrivateVoltTableFactory
                            .createVoltTableFromBuffer(c.b(), true);
                    Pair<Integer, byte[]> p = VoltTableUtil.toCSV( vt, m_delimiter, null, lastNumCharacters);
                    lastNumCharacters = p.getFirst();
                    byte csvBytes[] = p.getSecond();
                    m_availableBytes.addAndGet(csvBytes.length);
                    m_available.offer(csvBytes);
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

        char delimiter = '\0';
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
            delimiter = '\t';
        } else if (args[0].endsWith(".csv")) {
            delimiter = ',';
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

        convertTableSaveFile(delimiter, partitions, outfile, infile);
    }

    public static void convertTableSaveFile(char delimiter,
            Integer[] partitions, final File outfile, final File infile)
            throws FileNotFoundException, IOException, InterruptedException,
            SyncFailedException {
        final FileOutputStream fos = new FileOutputStream(outfile, true);
        try {
            final CSVTableSaveFile converter = new CSVTableSaveFile(infile,
                    delimiter, partitions);
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
