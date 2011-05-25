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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.voltdb.VoltDB;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;

/**
 * @see TestCtrlC for use of this test-only export client
 */
public class ExportToSocketClient extends ExportClientBase
{
    Socket m_socket = null;
    OutputStream m_debugOut = null;

    static class SocketDecoder extends ExportDecoderBase {

        public SocketDecoder(AdvertisedDataSource source) {
            super(source);
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) {
            // No need to do much here
            //System.out.printf("Received a row with size: %d\n", rowSize);
            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            assert(false);
        }
    }

    public ExportToSocketClient() throws IOException {
        m_socket = new Socket();
        m_socket.connect(new InetSocketAddress("localhost", 9999));
        assert(m_socket.isConnected());
        m_debugOut = m_socket.getOutputStream();
        System.out.println("Connected to socket");

        super.addServerInfo(new InetSocketAddress("localhost", VoltDB.DEFAULT_PORT));
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new SocketDecoder(source);
    }

    @Override
    protected void startWorkHook() {
        //System.out.println("Starting a block");
        try {
            m_debugOut.write(1);
            m_debugOut.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void endWorkHook() {
        //System.out.println("Finishing a block");
        try {
            m_debugOut.write(0);
            m_debugOut.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void extraShutdownHookWork() {
        System.out.println("Shutdown test hook");
        try {
            if (m_socket != null)
                m_socket.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        ExportToSocketClient client = new ExportToSocketClient();

        // main loop
        try {
            client.run();
        }
        catch (ExportClientException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
