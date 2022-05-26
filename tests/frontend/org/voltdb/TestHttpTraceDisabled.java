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

package org.voltdb;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;

public class TestHttpTraceDisabled extends JUnit4LocalClusterTest{
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");

    @Test
    public void testCanAccessRootPath() throws IOException {
        try {
            LocalCluster cluster = new LocalCluster("testCanAccessRootPath.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            VoltProjectBuilder builder = new VoltProjectBuilder();
            cluster.compile(builder);
            cluster.startUp();
            int hostId = VoltDB.instance().getHostMessenger().getHostId();
            int port = cluster.httpPort(hostId);
            final String URL = "http://127.0.0.1:" + port + "/";
            URL url = new URL(URL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            int code = conn.getResponseCode();
            assertEquals(HttpServletResponse.SC_OK, code);
            cluster.shutDown();
        } catch (InterruptedException e) {
            networkLog.error(e.getMessage());
        }
    }

    @Test
    public void testCannotAccessPathsByTrace() throws IOException {
        try {
            LocalCluster cluster = new LocalCluster("testCannotAccessRootPathsByTrace.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            VoltProjectBuilder builder = new VoltProjectBuilder();
            cluster.compile(builder);
            cluster.startUp();
            int hostId = VoltDB.instance().getHostMessenger().getHostId();
            int port = cluster.httpPort(hostId);
            final String URL = "http://127.0.0.1:" + port + "/";
            URL[] handlers = new URL[] {new URL(URL), new URL(URL + "css"), new URL(URL + "images"), new URL(URL + "js")};
            for(URL u : handlers) {
                HttpURLConnection conn = (HttpURLConnection) u.openConnection();
                conn.setRequestMethod("TRACE");
                int code = conn.getResponseCode();
                assertEquals(HttpServletResponse.SC_FORBIDDEN, code);
            }
            cluster.shutDown();
        } catch (InterruptedException e) {
            networkLog.error(e.getMessage());
        }
    }
}
