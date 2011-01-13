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

package org.voltdb.processtools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessData {
    private final Process m_process;
    private final StreamWatcher m_out;
    private final StreamWatcher m_err;

    public enum Stream { STDERR, STDOUT; }

    public static class OutputLine {
        public OutputLine(String processName, Stream stream, String value) {
            this.processName = processName;
            this.stream = stream;
            this.message = value;
        }

        public final String processName;
        public final Stream stream;
        public final String message;
    }

    class StreamWatcher extends Thread {
        final BufferedReader m_reader;
        final String m_processName;
        final Stream m_stream;
        final OutputHandler m_handler;
        final AtomicBoolean m_expectDeath = new AtomicBoolean(false);

        StreamWatcher(BufferedReader reader, String processName, Stream stream,
                      OutputHandler handler) {
            assert(reader != null);
            m_reader = reader;
            m_processName = processName;
            m_stream = stream;
            m_handler = handler;
        }

        void setExpectDeath(boolean expectDeath) {
            m_expectDeath.set(expectDeath);
        }

        @Override
        public void run() {
            while (true) {
                String line = null;
                try {
                    line = m_reader.readLine();
                } catch (IOException e) {
                    if (!m_expectDeath.get()) {
                        e.printStackTrace();
                        System.err.print("Err Stream monitoring thread exiting.");
                        System.err.flush();
                    }
                    return;
                }
                if (line != null) {
                    OutputLine ol = new OutputLine(m_processName, m_stream, line);
                    if (m_handler != null) {
                        m_handler.update(ol);
                    } else {
                        final long now = (System.currentTimeMillis() / 1000) - 1256158053;
                        System.out.println("(" + now + ")" + m_processName + ": " + line);
                    }
                } else {
                    return;
                }
            }
        }
    }

    public ProcessData(String processName, String[] cmd, String cwd,
                       OutputHandler handler) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        if (cwd != null)
            pb.directory(new File(cwd));
        m_process = pb.start();

        BufferedReader out = new BufferedReader(new InputStreamReader(m_process.getInputStream()));
        BufferedReader err = new BufferedReader(new InputStreamReader(m_process.getErrorStream()));
        m_out = new StreamWatcher(out, processName, Stream.STDOUT, handler);
        m_err = new StreamWatcher(err, processName, Stream.STDERR, handler);
        m_out.start();
        m_err.start();
    }

    ProcessData(String processName, OutputHandler handler, Process p) {
        m_process = p;
        BufferedReader out = new BufferedReader(new InputStreamReader(m_process.getInputStream()));
        BufferedReader err = new BufferedReader(new InputStreamReader(m_process.getErrorStream()));
        m_out = new StreamWatcher(out, processName, Stream.STDOUT, handler);
        m_err = new StreamWatcher(err, processName, Stream.STDERR, handler);
        m_out.start();
        m_err.start();
    }

    public int kill() {
        m_out.m_expectDeath.set(true);
        m_err.m_expectDeath.set(true);
        int retval = -255;

        synchronized(m_process) {
            m_process.destroy();
            try {
                m_process.waitFor();
                retval = m_process.exitValue();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return retval;
    }

    public int join() {
        m_out.m_expectDeath.set(true);
        m_err.m_expectDeath.set(true);

        try {
            synchronized(m_process) {
                int waitFor = m_process.waitFor();
                System.err.println("Joined pd.process with exit status: " + waitFor);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return kill();
    }

    public void write(String data) throws IOException {
        OutputStreamWriter out = new OutputStreamWriter(m_process.getOutputStream());
        out.write(data);
        out.flush();
    }

    public boolean isAlive() {
        try {
            synchronized(m_process) {
                m_process.exitValue();
            }
            return false;
        } catch (IllegalThreadStateException e) {
            return true;
        }
    }
}
