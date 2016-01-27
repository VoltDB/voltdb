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


import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.AuthenticatedConnectionCache;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestAuthenticatedConnectionCache extends TestCase {

    ServerThread server;
    AuthenticatedConnectionCache.ClientWithHashScheme clientAndScheme;

    public void testAuthenticatedConnectionCache() throws Exception {
        try {
            String simpleSchema
                    = "CREATE TABLE foo (\n"
                    + "    bar BIGINT NOT NULL,\n"
                    + "    PRIMARY KEY (bar)\n"
                    + ");";

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(simpleSchema);
            builder.setHTTPDPort(8095);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            VoltProjectBuilder.UserInfo users[] = new VoltProjectBuilder.UserInfo[] {
                new VoltProjectBuilder.UserInfo("admin", "password", new String[] {"AdMINISTRATOR"}),
                new VoltProjectBuilder.UserInfo("user", "password", new String[] {"User"})
            };
            builder.addUsers(users);

            // suite defines its own ADMINISTRATOR user
            builder.setSecurityEnabled(true, false);

            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            AuthenticatedConnectionCache ccache = new AuthenticatedConnectionCache(10, "localhost", server.m_config.m_port, "localhost", server.m_config.m_adminPort);
            clientAndScheme = ccache.getClient(null, null, null, true);

            assertEquals(clientAndScheme.m_client.hashCode(), ccache.getUnauthenticatedAdminClient().hashCode());
            clientAndScheme = ccache.getClient(null, null, null, false);
            assertEquals(clientAndScheme.m_client.hashCode(), ccache.getUnauthenticatedClient().hashCode());

            clientAndScheme = ccache.getClient("admin", "password", null, true);
            assertEquals(1, ccache.getSize());
            AuthenticatedConnectionCache.ClientWithHashScheme clientAndScheme2 = ccache.getClient("admin", "password", null, true);
            assertEquals(clientAndScheme2.m_client.hashCode(), clientAndScheme.m_client.hashCode());

            clientAndScheme = ccache.getClient("admin", "password", null, false);
            assertEquals(2, ccache.getSize());
            clientAndScheme2 = ccache.getClient("admin", "password", null, false);
            assertEquals(clientAndScheme2.m_client.hashCode(), clientAndScheme.m_client.hashCode());

            clientAndScheme = ccache.getClient("user", "password", null, true);
            assertEquals(3, ccache.getSize());
            clientAndScheme2 = ccache.getClient("user", "password", null, true);
            assertEquals(clientAndScheme2.m_client.hashCode(), clientAndScheme.m_client.hashCode());

            clientAndScheme = ccache.getClient("user", "password", null, false);
            assertEquals(4, ccache.getSize());
            clientAndScheme2 = ccache.getClient("user", "password", null, false);
            assertEquals(clientAndScheme2.m_client.hashCode(), clientAndScheme.m_client.hashCode());

        } finally {
            if (server != null) {
                server.shutdown();
                server.join();
            }
            server = null;
            if (clientAndScheme != null) {
                clientAndScheme.m_client.close();
            }
        }
    }

}
