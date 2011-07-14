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

package everything;

import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;

public class ClientRunner {

    static SubClientRunner m_grower = new SubClientRunner(new Grower());
    static SubClientRunner m_shrinker = new SubClientRunner(new Shrinker());

    public static abstract class SubClient implements Runnable {

        Client m_client = null;
        Random m_rand = new Random();

        class Callback implements org.voltdb.client.ProcedureCallback {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    if (clientResponse.getStatus() == ClientResponse.USER_ABORT)
                        return;

                    System.err.println(clientResponse.getStatusString());
                    System.exit(-1);
                }
            }
        }

        void connect() {
            if (m_client != null)
                return;

            m_client = ClientFactory.createClient();
            try {
                m_client.createConnection("localhost");
            }
            catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        void disconnect() {
            if (m_client == null)
                return;

            try {
                m_client.drain();
                m_client.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                m_client = null;
            }
        }

        long getMBRss() {
            assert(m_client != null);
            try {
                ClientResponse r = m_client.callProcedure("@Statistics", "MEMORY", 0);
                VoltTable stats = r.getResults()[0];
                stats.advanceRow();
                long rss = stats.getLong("RSS") / 1024;
                return rss;
            }
            catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
                return 0;
            }
        }
    }

    public static class SubClientRunner extends Thread {
        final SubClient m_subClient;

        SubClientRunner(SubClient subClient) {
            m_subClient = subClient;
        }

        @Override
        public void run() {
            m_subClient.connect();
            while(true) {
                m_subClient.run();
            }
        }

    }

    public static void main(String[] args) throws Exception {

        /*VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(ClientRunner.class.getResource("ddl.sql"));
        builder.addPartitionInfo("main", "pval");
        builder.addProcedures(Insert你好.class);
        builder.addProcedures(Nibble你好.class);
        builder.setHTTPDPort(8080);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("everything.jar"), 1, 1, 0, "localhost");
        assert(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("everything.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("everything.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("everything.xml");
        config.m_backend = BackendTarget.NATIVE_EE_JNI;
        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();*/

        m_grower.start();
        m_shrinker.start();

        while(true);
    }
}
