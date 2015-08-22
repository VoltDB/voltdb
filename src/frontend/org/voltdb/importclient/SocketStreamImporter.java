/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.importclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Properties;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.importer.CSVInvocation;
import org.voltdb.importer.ImportHandlerProxy;

/**
 * Implement a BundleActivator interface and extend ImportHandlerProxy.
 * @author akhanzode
 */
public class SocketStreamImporter extends ImportHandlerProxy implements BundleActivator {

    private Properties m_properties;
    private ServerSocket m_serverSocket;
    private String m_procedure;
    private final ArrayList<ClientConnectionHandler> m_clients = new ArrayList<ClientConnectionHandler>();

    // Register ImportHandlerProxy service.
    @Override
    public void start(BundleContext context) throws Exception {
        context.registerService(SocketStreamImporter.class.getName(), this, null);
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
        m_procedure = (String )m_properties.get("procedure");
        if (m_procedure == null || m_procedure.trim().length() == 0) {
            throw new RuntimeException("Missing procedure.");
        }
        try {
            if (m_serverSocket != null) {
                m_serverSocket.close();
            }
            m_serverSocket = new ServerSocket(Integer.parseInt(s));
        } catch (IOException ex) {
           ex.printStackTrace();
           throw new RuntimeException(ex.getCause());
        }
    }

    @Override
    public void setBackPressure(boolean flag) {
        for (ClientConnectionHandler fetcher : m_clients) {
            fetcher.hasBackPressure(flag);
        }
    }


    //This is ClientConnection handler to read and dispatch data to stored procedure.
    private class ClientConnectionHandler extends Thread {
        private final Socket m_clientSocket;
        private final String m_procedure;
        private volatile boolean m_hasBackPressure = false;

        public ClientConnectionHandler(Socket clientSocket, String procedure) {
            m_clientSocket = clientSocket;
            m_procedure = procedure;
        }

        public void hasBackPressure(boolean flag) {
            m_hasBackPressure = flag;
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
                        CSVInvocation invocation = new CSVInvocation(m_procedure, line);
                        if (!callProcedure(invocation)) {
                            System.out.println("Inserted failed: " + line);
                        }
                        if (m_hasBackPressure) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException ioe) {
                                //
                            }
                        }
                    }
                    m_clientSocket.close();
                    System.out.println("Client Closed.");
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
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
            info("Configured and ready with properties: " + m_properties);
            String procedure = m_properties.getProperty("procedure");
            while (true) {
                Socket clientSocket = m_serverSocket.accept();
                ClientConnectionHandler ch = new ClientConnectionHandler(clientSocket, procedure);
                m_clients.add(ch);
                ch.start();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
