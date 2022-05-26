/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB;

/**
 * This class gives an abstract interface for processes run using valgrind.  It has two
 * important member functions, waitForShutdown() and destroy().  These are noops when
 * not running valgrind.  When a valgrind process is run these operations manage the
 * valgrind process and its output.
 */
public class EEProcess {
    private Process m_eeProcess;
    private String m_eePID = null;
    private Thread m_stderrParser = null;
    private Thread m_stdoutParser = null;
    private int m_port;
    private int m_siteCount;

    private final boolean verbose = true;

    //
    // Valgrind will write its output in this file.  The %s will be
    // replaced by the literal string "%p" or else the stringified
    // version of the pid.  The former is used to tell valgrind itself
    // the name.  The latter is used to open up the file.
    //
    private static String VALGRIND_OUTPUT_FILE_PATTERN = "valgrind_%s.xml";

    // The output file where valgrind will leave it's output.  Creators of this
    // class (usually LocalCluster) will be responsible for cleaning this up.
    private File m_valgrindOutputFile = null;

    public int port() {
        return m_port;
    }

    EEProcess(final BackendTarget target, int siteCount, String logfile) {
        if (target != BackendTarget.NATIVE_EE_VALGRIND_IPC) {
            return;
        }

        if (verbose) {
            System.out.println("Running " + target);
        }
        final ArrayList<String> args = new ArrayList<>();
        final String voltdbIPCPath = System.getenv("VOLTDBIPC_PATH");

        args.add("valgrind");
        args.add("--leak-check=full");
        args.add("--show-reachable=yes");
        args.add("--num-callers=32");
        args.add("--error-exitcode=-1");
        args.add("--suppressions=tests/ee/test_utils/vdbsuppressions.supp");
        args.add("--xml=yes");
        // We will write valgrind output to a file.  The %p is replaced by
        // valgrind with the process id of the launched process.
        args.add(String.format("--xml-file=%s", String.format(VALGRIND_OUTPUT_FILE_PATTERN, "%p")));
        /*
         * VOLTDBIPC_PATH is set as part of the regression suites and ant
         * check In that scenario junit will handle logging of Valgrind
         * output. stdout and stderr is forwarded from the backend.
         */
        if (voltdbIPCPath == null) {
            args.add("--quiet");
            args.add("--log-file=" + logfile);
        }
        args.add(voltdbIPCPath == null ? "./voltdbipc" : voltdbIPCPath);
        args.add(String.valueOf(siteCount));

        final ProcessBuilder pb = new ProcessBuilder(args);
        //pb.redirectErrorStream(true);

        try {
            m_eeProcess = pb.start();
            final Process p = m_eeProcess;
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    p.destroy();
                }
            });
        } catch (final IOException e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }

        final BufferedReader stderr = new BufferedReader(new InputStreamReader(
                                                                               m_eeProcess.getErrorStream()));

        /*
         * This block attempts to read the PID and then waits for the
         * listening message indicating that the IPC EE is ready to accept a
         * connection on a socket
         */
        final BufferedReader stdout = new BufferedReader(new InputStreamReader(
                                                                               m_eeProcess.getInputStream()));
        try {
            boolean failure = false;

            // expecting "== pid = NUMBER ==" to be line 1, where NUMBER is the C++ process's PID
            String pidString = stdout.readLine();
            if (pidString == null) {
                failure = true;
            } else {
                if (verbose) {
                    System.out.println("PID string \"" + pidString + "\"");
                }
                pidString = pidString.substring("== pid = ".length());
                pidString = pidString.substring(0, pidString.indexOf(" =="));
                m_eePID = pidString;
            }

            // expecting "== eecount = NUMBER ==" to be line 2, where NUMBER is expected EE threads
            String siteCountString = stdout.readLine();
            if (siteCountString == null) {
                failure = true;
            } else {
                if (verbose) {
                    System.out.println("Site count string \"" + siteCountString + "\"");
                }
                siteCountString = siteCountString.substring("== eecount = ".length());
                siteCountString = siteCountString.substring(0, siteCountString.indexOf(" =="));
                int siteCount2 = Integer.valueOf(siteCountString);
                assert(siteCount2 == siteCount);
                m_siteCount = siteCount;
            }

            // expecting "== port = NUMBER ==" to be line 3, where NUMBER is listening port
            String portString = stdout.readLine();
            if (portString == null) {
                failure = true;
            } else {
                if (verbose) {
                    System.out.println("Port string \"" + portString + "\"");
                }
                portString = portString.substring("== port = ".length());
                portString = portString.substring(0,
                                                  portString.indexOf(" =="));
                m_port = Integer.valueOf(portString);
            }

            while (true) {
                String line = null;
                if (!failure) {
                    line = stdout.readLine();
                }
                if (line != null && !failure) {
                    if (verbose) {
                        System.out.println("[ipc=" + m_eePID + "]:::" + line);
                    }
                    if (line.contains("listening")) {
                        break;
                    } else {
                        continue;
                    }
                } else {
                    while ((line = stderr.readLine()) != null) {
                        if (verbose) {
                            System.err.println(line);
                        }
                    }
                    try {
                        m_eeProcess.waitFor();
                    } catch (final InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    VoltDB.crashLocalVoltDB("[ipc=" + m_eePID
                            + "] Returned end of stream and exit value "
                            + m_eeProcess.exitValue(), false, null);
                }
            }
        } catch (final IOException e) {
            e.printStackTrace();
            return;
        }

        m_valgrindOutputFile = new File(String.format(VALGRIND_OUTPUT_FILE_PATTERN, m_eePID));

        /*
         * Create threads that echo output from stdout and stderr prefixed by
         * a unique ID for the spawned EE process.
         */
        final Process p = m_eeProcess;
        m_stdoutParser = new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        final String line = stdout.readLine();
                        if (line != null) {
                            if (verbose) {
                                System.out.println("[ipc=" + p.hashCode()
                                                   + "]:::" + line);
                            }
                        } else {
                            try {
                                p.waitFor();
                            } catch (final InterruptedException e) {
                                e.printStackTrace();
                            }
                            if (verbose) {
                                System.out
                                .println("[ipc="
                                         + m_eePID
                                         + "] Returned end of stream and exit value "
                                         + p.exitValue());
                            }
                            return;
                        }
                    } catch (final IOException e) {
                        e.printStackTrace();
                        return;
                    }
                }
            }
        };

        m_stderrParser = new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        final String line = stderr.readLine();
                        if (line != null) {
                            if (verbose) {
                                System.err.println("[ipc=" + p.hashCode()
                                                   + "]:::" + line);
                            }
                        } else {
                            try {
                                p.waitFor();
                            } catch (final InterruptedException e) {
                                e.printStackTrace();
                            }
                            if (verbose) {
                                System.out
                                .println("[ipc="
                                         + m_eePID
                                         + "] Returned end of stream and exit value "
                                         + p.exitValue());
                            }
                            return;
                        }
                    } catch (final IOException e) {
                        e.printStackTrace();
                        return;
                    }
                }
            }
        };

        m_stdoutParser.setDaemon(false);
        m_stdoutParser.start();
        m_stderrParser.setDaemon(false);
        m_stderrParser.start();
    }

    public void destroy() {
        if (m_eeProcess != null) {
            m_eeProcess.destroy();
        }
    }

    // In some tests there is no client initiated and the IPC server will hang
    // waiting for the incoming messages.
    // Push EOFs to IPC processes in this case can help end the hanging situation.
    private void signalShutDown() {
        Socket ipcSocket;
        PrintWriter ipcWriter;
        try {
            // Need to send as many EOFs as m_siteCount.
            for (int i=0; i<m_siteCount; i++) {
                // The connection will be closed once an EOF is sent.
                // So we need to re-open the connection if we want to send another EOF.
                ipcSocket = new Socket("localhost", m_port);
                if (! ipcSocket.isConnected()) {
                    ipcSocket.close();
                    return;
                }
                ipcWriter = new PrintWriter(ipcSocket.getOutputStream());
                // 0x04 is the code for EOF.
                ipcWriter.write("\004");
                ipcWriter.flush();
                ipcWriter.close();
                ipcSocket.close();
            }
        } catch (IOException e) {}
    }

    /**
     * Shut down the EE process and return a file containing XML valgrind output.
     * @return a File
     * @throws InterruptedException
     */
    public File waitForShutdown() throws InterruptedException {
        if (m_eeProcess != null) {
            boolean done = false;
            while (!done) {
                try {
                    signalShutDown();
                    m_eeProcess.waitFor();
                    done = true;
                } catch (InterruptedException e) {
                    System.out
                            .println("Interrupted waiting for EE IPC process to die. Wait again.");
                }
            }
        }
        if (m_stdoutParser != null) {
            m_stdoutParser.join();
        }
        if (m_stderrParser != null) {
            m_stderrParser.join();
        }

        return m_valgrindOutputFile;
    }
}
