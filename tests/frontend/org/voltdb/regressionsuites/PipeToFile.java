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

package org.voltdb.regressionsuites;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;

/* class pipes a process's output to a file name.
 * Also watches for "Server completed initialization"
 * in output - the signal of readiness!
 */
    public class PipeToFile extends Thread {
        final static String m_initToken = "Server completed init";
        final static String m_rejoinToken = "Node rejoin completed";
        final static String m_joinToken = "Node join completed";
        final static String m_hostID = "Host id of this node is: ";

        FileWriter m_writer ;
        BufferedReader m_input;
        String m_filename;

        // set m_witnessReady when the m_token byte sequence is seen.
        AtomicBoolean m_witnessedReady;
        AtomicBoolean m_eof = new AtomicBoolean(false);
        final String m_token;
        int m_hostId = Integer.MAX_VALUE;
        long m_initTime;

        // optional watcher interface
        OutputWatcher m_watcher = null;

        // memoize the process here so we can easily check for process death
        Process m_process;

        PipeToFile(String filename, InputStream stream, String token,
                   boolean appendLog, Process proc) {
            m_witnessedReady = new AtomicBoolean(false);
            m_token = token;
            m_filename = filename;
            m_input = new BufferedReader(new InputStreamReader(stream));
            m_process = proc;
            try {
                m_writer = new FileWriter(filename, appendLog);
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        /**
         * Allow callers to get the process object to double check for death
         */
        Process getProcess() {
            return m_process;
        }

        /**
         * Inject a watcher to scan output.
         */
        void setWatcher(OutputWatcher watcher) {
            m_watcher = watcher;
        }

        public int getHostId() {
            synchronized(this) {
                return m_hostId;
            }
        }
        @Override
        public void run() {
            assert(m_writer != null);
            assert(m_input != null);
            boolean initLocationFound = false;
            while (m_eof.get() != true) {
                try {
                    String data = m_input.readLine();
                    if (data == null) {
                        m_eof.set(true);
                        continue;
                    }

                    // let the optional watcher take a peak
                    if (m_watcher != null) {
                        m_watcher.handleLine(data);
                    }

                    // look for the non-exec site id
                    if (data.contains(m_hostID)) {
                        // INITIALIZING INITIATOR ID: 1, SITEID: 0
                        String[] split = data.split(" ");
                        synchronized(this) {
                            try {
                                m_hostId = Long.valueOf(split[split.length - 1].split(":")[0]).intValue();
                            } catch (java.lang.NumberFormatException e) {
                                System.err.println("Had a number format exception processing line: '" + data + "'");
                                throw e;
                            }
                        }
                    }

                    // look for a sequence of letters matching the server ready token.
                    if (!m_witnessedReady.get() && data.contains(m_token)) {
                        synchronized (this) {
                            m_witnessedReady.set(true);
                            this.notifyAll();
                        }
                    }

                    // look for a sequence of letters matching the server ready token.
                    if (!initLocationFound && data.contains(m_initToken)) {
                        initLocationFound = true;
                        m_initTime = System.currentTimeMillis();
                    }

                    m_writer.write(data + "\n");
                    m_writer.flush();
                }
                catch (IOException ex) {
                    m_eof.set(true);
                }
            }
            synchronized (this) {
                notifyAll();
            }
            try {
                m_writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

