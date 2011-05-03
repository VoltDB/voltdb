/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.exportclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.voltdb.client.ClientResponse;

public class TestCtrlC extends TestCase {

    Process m_clientProcess = null;
    int lastByteRead = 0;
    AtomicBoolean allIsGo = new AtomicBoolean(true);
    AtomicBoolean accepted = new AtomicBoolean(false);

    /**
     * Thread connects to the ExportToSocketClient and reads
     * individual bytes off the socket, storing the last
     * successfully read value in lastByteRead.
     *
     * It reads a 1 when the client is dirty, and a 0 when it
     * it clean again.
     */
    class SocketEndpoint extends Thread {

        @Override
        public void run() {
            try {
                ServerSocket accept = null;
                Socket sock = null;
                try {
                    accept = new ServerSocket(9999);
                    sock = accept.accept();
                } finally {
                    if (accept != null) {
                        try { accept.close(); } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

                try {
                    assert(sock.isConnected());
                    accepted.set(true);
                    InputStream in = sock.getInputStream();
                    int realLastByteRead = 0;
                    while ((realLastByteRead = in.read()) != -1) {
                        lastByteRead = realLastByteRead;
                    }
                } finally {
                    sock.close();
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * Send work to VoltDB so the export stream will have data.
     */
    class VoltClientThread extends Thread {
        @Override
        public void run() {
            try {
                org.voltdb.client.Client client = org.voltdb.client.ClientFactory.createClient();
                client.createConnection("localhost");
                for (long i = 0; allIsGo.get(); i++) {
                    ClientResponse r = client.callProcedure("Insert", i);
                    assertEquals(r.getStatus(), ClientResponse.SUCCESS);
                }
            }
            catch (InterruptedIOException e) {
                // stopped by test
                System.out.println("Stopping VoltDB Client Thread");
            }
            catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    /**
     * Collect stdout/stderr from a second process
     * and print it to the console of this process
     *
     */
    static class StreamReader extends Thread {
        @Override
        public void run() {
            while (true) {
                String line = null;
                try {
                    line = br.readLine();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
                if (line == null)
                    break;
                System.out.println("SUBPROCESS: " + line);
            }
        }
        BufferedReader br = null;
        StreamReader(InputStream is) {
            assert(is != null);
            br = new BufferedReader(new InputStreamReader(is));
        }

    }

    /**
     * Send SIGINT (ctrl-c) to a process given a PID.
     * This only work on unix-like systems
     */
    static void sigint(int pid) throws Exception {
        String cmd = "kill -INT " + String.valueOf(pid);
        System.out.println("Running SIGINT with command: " + cmd);

        Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor();
        System.out.println("SIGINT process exited with value: " + String.valueOf(p.exitValue()));
    }

    /**
     * Start up the ExportToScoketClient in another process and return the PID
     */
    int startClientProcess() throws IOException, Exception {
        String classpath = System.getProperty("java.class.path");
        System.out.println(classpath);
        ProcessBuilder pb = new ProcessBuilder("java", "-cp", classpath, "org.voltdb.exportclient.ExportToSocketClient");
        m_clientProcess = pb.redirectErrorStream(true).start();
        assert(m_clientProcess != null);

        // using reflection to get the PID will fail on non-unix
        Field field = m_clientProcess.getClass().getDeclaredField("pid");
        field.setAccessible(true);
        int pid = field.getInt(m_clientProcess);

        // start a thread to redirect output to the console
        new StreamReader(m_clientProcess.getInputStream()).start();

        return pid;
    }

    public void testSimple() throws Exception {

        VoltDBFickleCluster.compile();
        Random r = new Random();

        for (int i = 0; i < 6; i++) {
            // some threads will quit if this is false
            allIsGo.set(true);
            // has the socket connected to the process yet (race)
            accepted.set(false);

            // start voltdb
            VoltDBFickleCluster.start();

            // start the thread that reads output from the client
            SocketEndpoint socketEndpoint = new SocketEndpoint();
            socketEndpoint.start();

            // start voltdb client
            VoltClientThread voltClient = new VoltClientThread();
            voltClient.start();

            // start the export client process
            int pid = startClientProcess();

            // let a random amount of work get done
            Thread.sleep(r.nextInt(1000 + 4000));

            // detect and avoid a race condition where
            // we try to kill the client process before it can
            // connect to this one
            while (accepted.get() == false)
                Thread.sleep(50);

            // send ctrl-c to the export client process
            sigint(pid);

            // wait for everything to stop
            allIsGo.set(false);
            voltClient.interrupt();
            System.out.println("Joining Export Client Process");
            m_clientProcess.waitFor();
            System.out.println("Joining Socket Thread");
            socketEndpoint.join();
            System.out.println("Joining Client Thread");
            voltClient.join();
            System.out.println("Joining Server Thread");
            VoltDBFickleCluster.stop();


            // make sure the last byte sent over the socket was a zero
            assertEquals(0, lastByteRead);
        }
    }
}
