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

package txnIdSelfCheck;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;


public class UpdateClassesThread extends BenchmarkThread {

    Random r = new Random(8278923);
    final Client client;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final AtomicBoolean m_needsBlock = new AtomicBoolean(false);
    final byte[] jarData;
    final long cycletime = 10000;

    public UpdateClassesThread(Client client, long duration_secs) {
        log.info("UpdateClasses initializing...");
        setName("UpdateClasses");
        this.client = client;
        log.info("UpdateClasses cycle " + cycletime+"ms");
        // read the jar file
        File file = new File("txnid.jar");
        log.info("Loaded jar " + file.getAbsolutePath()+ " File size: " + file.length());
        jarData = new byte[(int) file.length()];
        try {
            InputStream input = null;
            try {
                int totalBytesRead = 0;
                input = new BufferedInputStream(new FileInputStream(file));
                while (totalBytesRead < jarData.length) {
                    int bytesRemaining = jarData.length - totalBytesRead;
                    //input.read() returns -1, 0, or more :
                    int bytesRead = input.read(jarData, totalBytesRead, bytesRemaining);
                    if (bytesRead > 0) {
                        totalBytesRead = totalBytesRead + bytesRead;
                    }
                }
                log.info("UpdateClasses Num bytes read: " + totalBytesRead);
            } finally {
                log.info("UpdateClasses Closing input stream.");
                if (input != null) {
                    input.close();
                }
            }
        } catch (FileNotFoundException ex) {
            log.error("UpdateClasses jar file not found.");
        } catch (IOException ex) {
            log.error(ex);
        }
    }

    void shutdown() {
        m_shouldContinue.set(false);
        this.interrupt();
    }


    @Override
    public void run() {

        while (m_shouldContinue.get()) {
            // if not, connected, sleep
            try { Thread.sleep(cycletime); } catch (Exception e) {}
            if (m_needsBlock.get()) {
                do {
                    // bail on wakeup if we're supposed to bail
                    if (!m_shouldContinue.get()) {
                        return;
                    }
                }
                while (client.getConnectedHostList().size() == 0);
                m_needsBlock.set(false);
            }

            // call a transaction
            try {
                // randomly run UC 50% of the time per cycle
                int write = r.nextInt(1);
                switch (write) {

                    case 0:
                        log.info("UpdateClasses running");
                        ClientResponse cr;
                        cr = client.callProcedure("@UpdateClasses", jarData, "");
                        String msg = cr.getStatusString() + " ("+cr.getStatus()+")";
                        if (cr.getStatus() == ClientResponse.GRACEFUL_FAILURE) {
                            hardStop("UpdateClasses failed " + msg);
                        } else if (cr.getStatus() != ClientResponse.SUCCESS ) {
                            // make a note about non-graceful errors , such as CONNECTION_LOST, SERVER_UNAVAILABLE etc
                            log.warn("UpdateClasses non-graceful failure error:" + msg);
                        } else {
                            log.info("UpdateClasses response: " + msg);
                        }
                        break;
                    }

                Thread.sleep(cycletime);
            }
            catch (ProcCallException e) {
                ClientResponse cr = e.getClientResponse();
                log.info("UpdateClasses ProcCallException response: " + cr.getStatusString() + " (" + cr.getStatus() + ")");
                if (cr.getStatus() == ClientResponse.GRACEFUL_FAILURE) {
                    if (!     (cr.getStatusString().contains("Please retry catalog update")
                            || cr.getStatusString().contains("Transaction dropped due to change in mastership")
                            || cr.getStatusString().contains("Server is shutting down")
                            || cr.getStatusString().contains("connection lost")
                            || cr.getStatusString().contains("Procedure call not queued: timed out waiting for host connection")
                            || cr.getStatusString().equals("Invalid catalog update.  Catalog or deployment change was planned against one version of the cluster configuration but that version was no longer live when attempting to apply the change.  This is likely the result of multiple concurrent attempts to change the cluster configuration.  Please make such changes synchronously from a single connection to the cluster.")
                            || cr.getStatusString().equals("An invocation of procedure @VerifyCatalogAndWriteJar on all hosts returned null result or time out.")
                            || cr.getStatusString().equals("An invocation of procedure @VerifyCatalogAndWriteJar on all hosts timed out.")
                    ))
                        hardStop("UpdateClasses Proc failed gracefully", e);
                    else
                        m_needsBlock.set(true);
                }
                if (TxnId2Utils.isServerUnavailableStatus(cr.getStatus())) {
                    log.warn("UpdateClasses got a server unavailable status on proc call. Will sleep.");
                    m_needsBlock.set(true);
                }
            }
            catch (NoConnectionsException e) {
                log.warn("UpdateClasses got NoConnectionsException on proc call. Will sleep.");
                m_needsBlock.set(true);
            }
            catch (Exception e) {
                hardStop("UpdateClasses failed to run client. Will exit.", e);
            }
        }
    }
}
