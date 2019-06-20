/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.export;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.export.TestExportBaseSocketExport.ServerListener;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.VoltFile;

/**
 * A convenient base class to write export end-to-end test case by using local cluster. Comparing
 * regression suite, in local cluster tests each test case can run individually without run entire
 * test cases. By tweaking setHasLocalServer(true) in cluster setting developers can set breakpoints in
 * server code, which is useful in implementing features or diagnose issues.
 *
 * So far it only supports socket exporter client, a future work can be done to support other types of
 * export client.
 */
public class ExportLocalClusterBase extends JUnit4LocalClusterTest {
    // Verifier needs to be instantiated at subclass
    protected ExportTestExpectedData m_verifier = null;
    // needs to assign different port number per export table
    protected Map<String, Integer> m_portForTable = new HashMap<String, Integer>();
    protected Map<String, ServerListener> m_serverSockets = new HashMap<String, ServerListener>();
    private int m_portCount = 5001;

    public ExportLocalClusterBase() {}

    private Integer getNextPort() {
        return m_portCount++;
    }

    public Properties createSocketExportProperties(String streamName, boolean isReplicatedStream) {
        if (!m_portForTable.containsKey(streamName)) {
            m_portForTable.put(streamName, getNextPort());
        }
        Properties props = new Properties();
        props.put("replicated", String.valueOf(isReplicatedStream));
        props.put("skipinternals", "false");
        props.put("socket.dest", "localhost:" + m_portForTable.get(streamName));
        props.put("timezone", "GMT");
        return props;
    }

    public void startListener() throws IOException {
        for (Entry<String, Integer> target : m_portForTable.entrySet()) {
            ServerListener m_serverSocket = new ServerListener(target.getValue());
            m_serverSockets.put(target.getKey(), m_serverSocket);
            m_serverSocket.start();
        }
    }

    public Client getClient(LocalCluster cluster) throws IOException {
        ClientConfig config = new ClientConfig();
        config.setTopologyChangeAware(true);
        Client client = ClientFactory.createClient(config);
        for (String connectStr : cluster.getListenerAddresses()) {
            client.createConnection(connectStr);
        }
        int sleptTimes = 0;
        while (!((ClientImpl) client).isHashinatorInitialized() && sleptTimes < 60000) {
            try {
                Thread.sleep(1);
                sleptTimes++;
            } catch (InterruptedException ex) {
                ;
            }
        }
        if (sleptTimes >= 60000) {
            throw new IOException("Failed to Initialize Hashinator.");
        }
        return client;
    }

    public static void resetDir() throws IOException {
        File f = new File("/tmp/" + System.getProperty("user.name"));
         VoltFile.recursivelyDelete(f);
         f.mkdirs();
    }

    protected void insertToStream(String streamName, int startPkey, int numberOfRows, Client client, Object[] params) throws Exception {
        for (int i = startPkey; i < startPkey + numberOfRows; i++) {
            params[1] = i; // Pkey column
            m_verifier.addRow(client, streamName, i, params);
            client.callProcedure("@AdHoc", "insert into "+ streamName + " values(" + i + ", 1)");
        }
    }

    protected void insertToStreamWithNewColumn(String streamName, int startPkey, int numberOfRows, Client client, Object[] params) throws Exception {
        for (int i = startPkey; i < startPkey + numberOfRows; i++) {
            params[1] = i; // Pkey column
            params[2] = i; // new column
            m_verifier.addRow(client, streamName, i, params);
            client.callProcedure("@AdHoc", "insert into " + streamName + " values(" + i + "," + i + ",1)");
        }
    }
}
