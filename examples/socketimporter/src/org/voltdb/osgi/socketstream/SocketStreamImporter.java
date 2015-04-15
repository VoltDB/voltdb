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

package org.voltdb.osgi.socketstream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Properties;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.importer.ImportHandlerProxy;

/**
 * Implement a BundleActivator interface and extend ImportHandlerProxy.
 * @author akhanzode
 */
public class SocketStreamImporter extends ImportHandlerProxy implements BundleActivator {

    private Properties m_properties;
    private ServerSocket m_serverSocket;
    private final ArrayList<ClientConnectionHandler> m_clients = new ArrayList<ClientConnectionHandler>();

    // Register ImportHandlerProxy service.
    @Override
    public void start(BundleContext context) throws Exception {
        context.registerService(ImportHandlerProxy.class.getName(), this, null);
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        //Do any bundle related cleanup.
    }

    @Override
    public void stop() {
        try {
            for (ClientConnectionHandler s : m_clients) {
                s.stopClient();
            }
            m_clients.clear();
            m_serverSocket.close();
            m_serverSocket = null;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Return a name for VoltDB to log with friendly name.
     * @return name of the importer.
     */
    @Override
    public String getName() {
        return "SocketImporter";
    }

    /**
     * This is called with the properties that are supplied in the deployment.xml
     * Do any initialization here.
     * @param p
     */
    @Override
    public void configure(Properties p) {
        m_properties = (Properties) p.clone();
        String s = (String )m_properties.get("port");
        try {
            if (m_serverSocket != null) {
                m_serverSocket.close();
            }
            m_serverSocket = new ServerSocket(Integer.parseInt(s));
        } catch (IOException ex) {
           ex.printStackTrace();
        }
    }

    //This is ClientConnection handler to read and dispatch data to stored procedure.
    private class ClientConnectionHandler extends Thread {
        private final Socket m_clientSocket;
        private final String m_procedure;
        private final ImportHandlerProxy m_importHandlerProxy;

        public ClientConnectionHandler(ImportHandlerProxy ic, Socket clientSocket, String procedure) {
            m_importHandlerProxy = ic;
            m_clientSocket = clientSocket;
            m_procedure = procedure;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(m_clientSocket.getInputStream()));
                    while (true) {
                        String line = in.readLine();
                        //You should convert your data to params here.
                        if (line == null) break;
                        if (!callProcedure(m_importHandlerProxy, m_procedure, line)) {
                            System.out.println("Inserted failed: " + line);
                        }
                    }
                    m_clientSocket.close();
                    System.out.println("Client Closed.");
                }
            } catch (IOException ioe) {
                //TODO
            }
        }

        public void stopClient() {
            try {
                m_clientSocket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * This is called when server is ready to accept any transactions.
     */
    @Override
    public void readyForData() {
        try {
            String procedure = m_properties.getProperty("procedure");
            while (true) {
                Socket clientSocket = m_serverSocket.accept();
                ClientConnectionHandler ch = new ClientConnectionHandler(this, clientSocket, procedure);
                m_clients.add(ch);
                ch.start();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
