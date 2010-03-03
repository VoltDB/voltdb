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

package org.voltdb.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import junit.framework.TestCase;

public class TestHTTPD extends TestCase {

    public void testSimpleWithQuit() throws IOException {
        NanoHTTPD server = new NanoHTTPD(9999);
        assertTrue(server.myServerSocket != null);
        assertTrue(server.myTcpPort == 9999);
        assertTrue(server.myThread != null);
        assertTrue(server.myThread.isAlive());
        server.shutdown(true);
        assertFalse(server.myThread.isAlive());
        server.shutdown(true);
    }

    public void testSingleFileServer() throws IOException {
        File f = File.createTempFile("testSingleFileServer", ".jar");
        byte[] data = new byte[1000];
        data[777] = 31;
        FileOutputStream fos = new FileOutputStream(f);
        fos.write(data);
        fos.close();

        SingleFileHTTPServer server = SingleFileHTTPServer.serveFile(f);
        String uri = server.m_uri;

        URL url = new URL(uri);
        InputStream is = url.openStream();
        byte[] data2 = new byte[1000];
        is.read(data2);
        assertTrue(data2[777] == data[777]);

        server.shutdown(true);
    }

}
