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
package org.voltdb.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Queue;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.voltcore.logging.VoltLogger;


/**
 * Reads trace events from VoltTrace queue and writes them to files.
 */
public class TraceFileWriter implements Runnable {
    private static final VoltLogger s_logger = new VoltLogger("TRACER");

    private final File m_path;
    private final Queue<VoltTrace.TraceEventBatch> m_events;

    public TraceFileWriter(File path, Queue<VoltTrace.TraceEventBatch> events) {
        m_events = events;
        m_path = path;
    }

    @Override
    public void run() {
        final ObjectMapper jsonMapper = new ObjectMapper();
        BufferedWriter fileWriter = null;
        long firstEventTime = 0;
        long count = 0;

        try {
            VoltTrace.TraceEventBatch eventSupplier;
            while ((eventSupplier = m_events.poll()) != null) {
                VoltTrace.TraceEvent event;
                while ((event = eventSupplier.nextEvent()) != null) {
                    if (fileWriter == null) {
                        fileWriter = startTraceFile(m_path);
                        firstEventTime = event.getNanos();
                    } else {
                        fileWriter.write(",");
                    }

                    event.setSyncNanos(firstEventTime);
                    String json = jsonMapper.writeValueAsString(event);
                    fileWriter.newLine();
                    fileWriter.write(json);
                    fileWriter.flush();

                    count++;
                }
            }
        } catch(IOException e) { // also catches JSON exceptions
            s_logger.info("Unexpected IO exception in trace file writer. Stopping trace file writer.", e);
        }

        if (fileWriter != null) {
            close(fileWriter);
        }

        s_logger.info("Wrote " + count + " trace events to " + m_path.getAbsolutePath());
    }

    private static void close(BufferedWriter bw) {
        try {
            bw.newLine();
            bw.write("]");
            bw.newLine();
            bw.flush();
            bw.close();
        } catch(IOException e) {
            if (s_logger.isDebugEnabled()) {
                s_logger.debug("Exception closing trace file buffered writer", e);
            }
        }
    }

    private static BufferedWriter startTraceFile(File path) throws IOException {
        // Uses the default platform encoding for now.
        final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(path))));
        bw.write("[");
        bw.flush();
        return bw;
    }
}
