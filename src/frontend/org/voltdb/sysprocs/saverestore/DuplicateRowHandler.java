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
package org.voltdb.sysprocs.saverestore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltDB;
import org.voltdb.utils.VoltTableUtil;

/**
 * A utility class for handling duplicates rows found during snapshot restore.
 * Converts the row data to CSV and writes them to a per table file at the specified location
 *
 */
public class DuplicateRowHandler {

    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    private final File outputPath;
    private final ExecutorService es = CoreUtils.getSingleThreadExecutor("Restore duplicate row handler");
    private final Map<String, FileChannel> m_outputFiles = new HashMap<String, FileChannel>();
    private final String now;

    public DuplicateRowHandler(String path, Date now) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSSZ");
        this.now = sdf.format(now);
        outputPath = new File(path);
        if (!outputPath.exists()) {
            throw new RuntimeException("Output path for duplicates \"" + outputPath + "\" does not exist");
        }
        if (!outputPath.canExecute()) {
            throw new RuntimeException("Output path for duplicates \"" + outputPath + "\" is not executable");
        }
    }

    public void handleDuplicates(final String tableName, byte duplicates[]) throws IOException {
        final byte csvBytes[] = VoltTableUtil.toCSV( PrivateVoltTableFactory.createVoltTableFromBuffer(ByteBuffer.wrap(duplicates), true),
                             ',',
                             null,
                             1024 * 512).getSecond();
        es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    handleDuplicatesInternal(tableName, csvBytes);
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Error handling duplicate rows during snapshot restore", true, e);
                }
            }
        });
    }

    private void handleDuplicatesInternal(final String tableName, byte csvBytes[]) throws Exception {
        FileChannel fc = getTableFile(tableName);
        final ByteBuffer buf = ByteBuffer.wrap(csvBytes);
        while (buf.hasRemaining()) {
            fc.write(buf);
        }
    }

    private FileChannel getTableFile(String tableName) throws Exception {
        FileChannel fc = m_outputFiles.get(tableName);
        if (fc == null) {
            File outfile = new File(outputPath, tableName + "-duplicates-" + now + ".csv");
            String message = "Found duplicate rows for table " + tableName + " they will be output to " + outfile;
            SNAP_LOG.warn(message);
            @SuppressWarnings("resource")
            FileOutputStream fos = new FileOutputStream(outfile);
            fc = fos.getChannel();
            m_outputFiles.put(tableName, fc);
        }
        return fc;
    }

    public void close() throws Exception {
        es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    for (Map.Entry<String, FileChannel> e : m_outputFiles.entrySet()) {
                        FileChannel fc = e.getValue();
                        String message = "Output " + fc.size() + " bytes worth of duplicate row data for table " + e.getKey();
                        SNAP_LOG.warn(message);
                        fc.force(true);
                        fc.close();
                    }
                    m_outputFiles.clear();
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Error syncing and closing duplicate files during snapshot restore", true, e);
                }
            }
        });
        es.shutdown();
        es.awaitTermination(365, TimeUnit.DAYS);
    }
}
