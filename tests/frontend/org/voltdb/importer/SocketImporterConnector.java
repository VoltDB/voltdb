/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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


package org.voltdb.importer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/** Allows the test thread to check if the importer is alive by writing data to it. */
public class SocketImporterConnector {
    private final String m_server;
    private final int m_port;
    private final char m_separator;
    private int m_counter = 0;

    public SocketImporterConnector(String server, int port, char separator) {
        m_separator = separator;
        m_server = server;
        m_port = port;
    }

    /** Tries to write data to the importer, but may fail. */
    public void tryPush(int maxAttempts) throws IOException {
        int numConnectAttempts = 0;
        do {
            try {
                Socket pushSocket = new Socket(m_server, m_port);
                OutputStream socketStream = pushSocket.getOutputStream();
                System.out.printf("Connected to VoltDB socket importer at: %s.\n", m_server + ":" + m_port);
                String s = String.valueOf(m_counter) + m_separator + System.currentTimeMillis() + "\n";
                socketStream.write(s.getBytes());
                pushSocket.close();
                m_counter++;
                return;
            } catch (IOException e) {
                numConnectAttempts++;
                if (numConnectAttempts >= maxAttempts) {
                    throw e;
                }
                try {
                    Thread.sleep((int) (Math.random() * 1000) + 500);
                } catch (InterruptedException ignore) {
                }
            }
        } while (true);
    }
}
