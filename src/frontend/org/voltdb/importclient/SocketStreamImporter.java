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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.importer.CSVInvocation;
import org.voltdb.importer.ImportHandlerProxy;

/**
 * Implement a BundleActivator interface and extend ImportHandlerProxy.
 * @author akhanzode
 */
public class SocketStreamImporter extends ImportHandlerProxy implements BundleActivator {

    private ArrayList<SocketImporterImpl> m_instances = new ArrayList<>();
    private ExecutorService m_es;


    // Register ImportHandlerProxy service.
    @Override
    public void start(BundleContext context) throws Exception {
        context.registerService(SocketStreamImporter.class.getName(), this, null);
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        //Do any bundle related cleanup.
    }

    /**
     * Return a name for VoltDB to log with friendly name.
     * @return name of the importer.
     */
    @Override
    public String getName() {
        return "SocketImporter";
    }

    @Override
    public void stop() {
        for (SocketImporterImpl impl : m_instances) {
            try {
            impl.stop();
            } catch(Exception e) {
                warn(e, "Error trying to stop importer");
            }
        }

        m_instances.clear();

        if (m_es != null) {
            m_es.shutdown();
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException ex) {
                //Should never come here.
                warn(ex, "Exception waiting for SocketImporter executor service to shutdown");
            }
        }
    }

    /**
     * This is called with the properties that are supplied in the deployment.xml
     * Do any initialization here.
     * @param p
     */
    @Override
    public void configure(Properties p) {
        m_instances.add(new SocketImporterImpl(p));
    }

    @Override
    public void readyForData()
    {
        m_es = Executors.newFixedThreadPool(m_instances.size(),
                getThreadFactory("SocketStreamImporter", "SocketImporterImpl", ImportHandlerProxy.MEDIUM_STACK_SIZE));
        for (SocketImporterImpl impl : m_instances) {
            final SocketImporterImpl importer = impl;
            m_es.submit(new Runnable() {
                @Override
                public void run() {
                    importer.readyForData();
                }
            });
        }
    }

    @Override
    public void setBackPressure(boolean hasBackPressure)
    {
        for (SocketImporterImpl impl : m_instances) {
            impl.setBackPressure(hasBackPressure);
        }
    }

    private class SocketImporterImpl {

        private Properties m_properties;
        private ServerSocket m_serverSocket;
        private String m_procedure;
        private final ArrayList<ClientConnectionHandler> m_clients = new ArrayList<ClientConnectionHandler>();

        public SocketImporterImpl(Properties p) {
            m_properties = (Properties) p.clone();
            String s = (String )m_properties.get("port");
            m_procedure = (String )m_properties.get("procedure");
            if (m_procedure == null || m_procedure.trim().length() == 0) {
                throw new IllegalArgumentException("Missing procedure.");
            }
            try {
                if (m_serverSocket != null) {
                    m_serverSocket.close();
                }
                m_serverSocket = new ServerSocket(Integer.parseInt(s));
            } catch (IOException ex) {
                warn(ex, "Exception closing existing server socket and reopening on port %s", s);
                throw new RuntimeException(ex.getCause());
            }
        }

        /**
         * This is called when server is ready to accept any transactions.
         */
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

        public void stop() {
            for (ClientConnectionHandler s : m_clients) {
                try {
                    s.stopClient();
                } catch(Exception e) {
                    warn(e, "Error closing socket client connection");
                }
            }
            m_clients.clear();

            try {
                m_serverSocket.close();
                m_serverSocket = null;
            } catch (IOException ex) {
                warn(ex, "Error closing socket importer server socket");
            }
        }


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
                                error(null, "Socket importer insertion failed");
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
                        info("Client Closed.");
                    }
                } catch (IOException ioe) {
                    error(ioe, "IO exception reading from client socket connection in socket importer");
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

    }
}
