/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import org.voltdb.client.Client;
import org.voltdb.compiler.DeploymentBuilder;

public class TestAuthenticatedConnectionCache extends TestCase {
    public void testAuthenticatedConnectionCache() throws Exception {
        String simpleSchema = "CREATE TABLE foo (bar BIGINT NOT NULL, PRIMARY KEY (bar));";
        DeploymentBuilder.UserInfo users[] = {
                new DeploymentBuilder.UserInfo("admin", "password", "AdMINISTRATOR"),
                new DeploymentBuilder.UserInfo("user", "password", "User")
            };
        DeploymentBuilder db = new DeploymentBuilder()
        .setHTTPDPort(8095)
        .addUsers(users)
        // suite defines its own ADMINISTRATOR user
        .setSecurityEnabled(true, false);
        ;
        Configuration config = Configuration.compile(getClass().getSimpleName(), simpleSchema, db);
        assertNotNull("Configuration failed to compile", config);

        ServerThread server = new ServerThread(config);
        Client client = null;

        try {
            server.start();
            server.waitForInitialization();

            AuthenticatedConnectionCache ccache = new AuthenticatedConnectionCache(10, "localhost", config.m_port, "localhost", config.m_adminPort);
////            client = ccache.getClient(null, null, null, true);
////            assertEquals(client.hashCode(), ccache.getUnauthenticatedAdminClient().hashCode());
            client = ccache.getClient(null, null, null, false);
            assertEquals(client.hashCode(), ccache.getUnauthenticatedClient().hashCode());

            client = ccache.getClient("admin", "password", null, true);
            assertEquals(1, ccache.getSize());
            Client client2 = ccache.getClient("admin", "password", null, true);
            assertEquals(client2.hashCode(), client.hashCode());

            client = ccache.getClient("admin", "password", null, false);
            assertEquals(2, ccache.getSize());
            client2 = ccache.getClient("admin", "password", null, false);
            assertEquals(client2.hashCode(), client.hashCode());

            client = ccache.getClient("user", "password", null, true);
            assertEquals(3, ccache.getSize());
            client2 = ccache.getClient("user", "password", null, true);
            assertEquals(client2.hashCode(), client.hashCode());

            client = ccache.getClient("user", "password", null, false);
            assertEquals(4, ccache.getSize());
            client2 = ccache.getClient("user", "password", null, false);
            assertEquals(client2.hashCode(), client.hashCode());

        } finally {
            if (client != null) {
                client.close();
            }
            server.shutdown();
            server.join();
        }
    }

}
