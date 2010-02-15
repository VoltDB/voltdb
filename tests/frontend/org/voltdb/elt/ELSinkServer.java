/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.elt;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.VoltType;


/**
 *   Listen for ELT output and run a provided tuple verifier
 *   against that stream of data.
 */

public class ELSinkServer extends Thread {

    private final TupleVerifier m_verifier;
    private final int m_listenerPort;
    private ServerSocket m_server;
    private final AtomicBoolean m_shouldContinue;
    private ArrayList<VerificationStream> m_vstreams;

    public final static int PORTNUMBER = 5443;

    public static void main(String[] args) {
        ELSinkServer server = new ELSinkServer(new NullVerifier());
        server.start();
    }

    public ELSinkServer(final TupleVerifier verifer) {
        m_verifier = verifer;
        m_listenerPort = PORTNUMBER;
        m_shouldContinue = new AtomicBoolean(true);
        m_vstreams = new ArrayList<VerificationStream>();
    }

    public void shutDown() {
        m_shouldContinue.set(false);
    }

    /**
     * Do blocking reads on accepted connections and pass received
     * tuples to the verifier.
     */
    private class VerificationStream extends Thread {

        private BufferedInputStream m_stream;

        VerificationStream(final Socket client) {
            try {
                m_stream = new BufferedInputStream(client.getInputStream());
            }
            catch (final IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                while (m_shouldContinue.get()) {

                    // read row length (or start of block header)
                    final byte[] rowsizebuf = new byte[4];
                    int bytes = 0;
                    while ( bytes < 4 ) {
                        System.out.println("Reading row size/header start");
                        int tmp = m_stream.read(rowsizebuf, bytes, 4 - bytes);
                        if (tmp == -1) {
                            System.out.println("No data to read(1)");
                            return;
                        }
                        bytes += tmp;
                    }

                    // Look for the VBINARY header.
                    // Raw processor needs to do better at producing a consumable header
                    if (rowsizebuf[0] == 'V' && rowsizebuf[1] == 'B' &&
                        rowsizebuf[2] == 'I' && rowsizebuf[3] == 'N')
                    {
                        System.out.println("Reading header.");

                        // read the rest of the header
                        // remaining 8 bytes of VBINARY header + 4 bytes of header length
                        final byte[] headerbuf = new byte[12];
                        bytes = 0;
                        while ( bytes < 12 ) {
                            int tmp = m_stream.read(headerbuf, bytes, 12 - bytes);
                            if (tmp == -1) {
                                System.out.println("No data to read(2)");
                                return;
                            }
                            bytes += tmp;
                        }
                        final ByteBuffer lengthBuf = ByteBuffer.wrap(headerbuf);
                        lengthBuf.order(ByteOrder.BIG_ENDIAN);
                        final int hdrsize = lengthBuf.getInt(8);
                        // consume the rest of the (variable sized) header.
                        System.out.println("Reading " + hdrsize + " bytes of variable header.");
                        final byte[] varheaderbuf = new byte[hdrsize];
                        bytes = 0;
                        while (bytes < hdrsize) {
                            int tmp = m_stream.read(varheaderbuf, bytes, hdrsize - bytes);
                            if (tmp == -1) {
                                System.out.println("No data to read(3)");
                                return;
                            }
                            bytes += tmp;
                        }
                    }
                    // Otherwise, process row tuple data.
                    else {
                        System.out.println("Reading row tuples");
                        final ByteBuffer lengthBuf =
                            ByteBuffer.wrap(rowsizebuf);
                        lengthBuf.order(ByteOrder.BIG_ENDIAN);
                        final int rowsize = lengthBuf.getInt();
                        if (rowsize < 0) {
                            System.out.println("No data to read (4)!");
                            return;
                        }

                        bytes = 0;
                        byte[] buf = new byte[rowsize];
                        while (bytes < rowsize) {
                            int read = m_stream.read(buf, bytes, rowsize - bytes);
                            if (read < 0) {
                                System.out.println("No data to read (5)");
                                return;
                            }
                            bytes += read;
                        }
                        m_verifier.verifyRow(rowsize, buf);
                    }
                }
            }
            catch (final IOException e1) {
                // the server will close this channel on quiesce()
                // e1.printStackTrace();
            }
        }
    }


    /** Drops all received tuples. Don't use this for correctness tests! */
    public static class NullVerifier implements TupleVerifier {
        @Override
        public void addRow(String tableName, Object[] data) {
        }

        @Override
        public void addTable(String tableName, ArrayList<VoltType> tableSchema) {
        }

        @Override
        public boolean allRowsVerified() {
            return true;
        }

        @Override
        public void verifyRow(int rowsize, byte[] rowdata) throws IOException {
        }
    }

    /**
     * Start the server. Listen for a connection.
     */
    @Override
    public void run() {
        try{
            // set a retry / timeout so this can die
            // gracefully with shouldContinue in case no data is
            // retrieved.
            m_server = new ServerSocket(m_listenerPort, 10,
                                        InetAddress.getLocalHost());
            m_server.setSoTimeout(500);
            while (m_shouldContinue.get()) {
                try {
                    final Socket accept = m_server.accept();
                    VerificationStream vs = new VerificationStream(accept);
                    m_vstreams.add(vs);
                    System.out.println("Sink server accepted connection. Starting verification stream.");
                    vs.start();
                }
                catch (final SocketTimeoutException ok) {
                    continue;
                }
            }
        }
        catch (final IOException e) {
            System.out.println("ELSinkServer could not listen on port " + m_listenerPort);
            e.printStackTrace();
        }
        catch (final Exception e) {
            System.out.println("Unexpected exception sink server accept loop.");
            e.printStackTrace();
        }
        finally {
            try {
                m_server.close();
            }
            catch (final IOException e) {
            }
        }
    }

    public TupleVerifier tupleVerifier() {
        return m_verifier;
    }

    public void finish() {
        for (VerificationStream vs : m_vstreams) {
            boolean notStopped = true;
            while (notStopped) {
                if (vs.isAlive()) {
                    try {
                        vs.join();
                        notStopped = false;
                    }
                    catch (InterruptedException e) {
                    }
                }
                else {
                    notStopped = false;
                }
            }
        }
    }

}
