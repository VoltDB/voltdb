/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class ProcessData {
    private final Process m_process;
    private final Session m_ssh_session;
    private final Channel m_channel;
    private final StreamWatcher m_out;
//    private final StreamWatcher m_err;

    private static VoltLogger log = new VoltLogger("SSH");

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

    ProcessData(String processName, OutputHandler handler, Session ssh_session, final String command) throws JSchException, IOException {
        m_process = null;
        m_ssh_session = ssh_session;
        m_channel=ssh_session.openChannel("exec");
        ((ChannelExec)m_channel).setCommand(command);

        // Set up the i/o streams, in, out, err
        //channel.setInputStream(System.in);
        m_channel.setInputStream(null);
        ((ChannelExec)m_channel).setErrStream(System.err);

        BufferedReader out = new BufferedReader(new InputStreamReader(m_channel.getInputStream()));
//      BufferedReader err = new BufferedReader(new InputStreamReader(m_channel.getInputStream()));
      m_out = new StreamWatcher(out, processName, Stream.STDOUT, handler);
//      m_err = new StreamWatcher(err, processName, Stream.STDERR, handler);

        /*
         * Execute the command non-blocking.
         */
        final Thread sshThread = new Thread() {
            @Override
            public void run() {
                try {
                    m_channel.connect();
                    m_out.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("Error attempting SSH command: " + command, e);
                }
            }
        };
        sshThread.start();
    }

    public int kill() {
        m_out.m_expectDeath.set(true);
//        m_err.m_expectDeath.set(true);
        int retval = -255;
        m_channel.disconnect();
        m_ssh_session.disconnect();
        return retval;
    }

    public boolean isAlive() {
        try {
            synchronized(m_channel) {
                return m_ssh_session.isConnected();
            }
        } catch (IllegalThreadStateException e) {
            return true;
        }
    }
}
