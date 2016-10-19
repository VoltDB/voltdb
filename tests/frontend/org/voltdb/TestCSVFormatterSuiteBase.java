/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;

import org.junit.Ignore;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.utils.VoltFile;

@Ignore public class TestCSVFormatterSuiteBase extends RegressionSuite {

    public TestCSVFormatterSuiteBase(String name) {
        super(name);
    }

    @Override
    public void setUp() throws Exception {
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    static OutputStream connectToOneServerWithRetry(String server, int port) {
        int sleep = 1000;
        while (true) {
            Socket pushSocket = null;
            try {
                pushSocket = new Socket(server, port);
                OutputStream out = pushSocket.getOutputStream();
                System.out.printf("Connected to VoltDB node at: %s.\n", server);
                return out;
            }
            catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try {
                    Thread.sleep(sleep);
                }
                catch (Exception interrupted) {
                }
                if (sleep < 8000) {
                    sleep += sleep;
                }
            }
        }
    }

    public static CountDownLatch pushDataAsync(int port, String[] data) {
        CountDownLatch latch = new CountDownLatch(1);
        (new SocketDataPusher(port, latch, data)).start();
        return latch;
    }

    public static void pushDataSync(int port, String[] data) {
        CountDownLatch latch = pushDataAsync(port, data);
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static class SocketDataPusher extends Thread {
        private static final String m_server = "localhost";
        private OutputStream m_sout;
        private final CountDownLatch m_latch;
        private final int m_port;
        private final String[] m_data;

        private SocketDataPusher(int port, CountDownLatch latch, String[] data) {
            m_latch = latch;
            m_port = port;
            m_data = data;
        }

        protected void initialize() {
            m_sout = connectToOneServerWithRetry(m_server, m_port);
            System.out.printf("Connected to VoltDB socket importer at: %s.\n", m_server + ":" + m_port);
        }

        @Override
        public void run() {
            initialize();

            try {
                for (int icnt = 0; icnt < m_data.length; icnt++) {
                    m_sout.write(m_data[icnt].getBytes());
                    Thread.sleep(0, 1);
                }
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
            finally {
                close();
                m_latch.countDown();
            }
        }

        protected void close() {
            try {
                m_sout.close();
            }
            catch (IOException ignored) {
            }
        }

    }
}
