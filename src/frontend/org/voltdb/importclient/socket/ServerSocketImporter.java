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

package org.voltdb.importclient.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.voltcore.logging.Level;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.CSVInvocation;

/**
 * Importer that listens on a server socket for data. Data is expected in CSV format currently,
 * which will be parsed and sent to the procedure specified in the configuration.
 */
public class ServerSocketImporter extends AbstractImporter {

    private final ServerSocketImporterConfig m_config;
    private List<ClientConnectionHandler> m_clients = new ArrayList<>();

    public ServerSocketImporter(ServerSocketImporterConfig config)
    {
        super();
        m_config = config;
    }

    @Override
    public String getName()
    {
        return "SocketServerImporter";
    }

    @Override
    public URI getResourceID()
    {
        return m_config.getResourceID();
    }

    @Override
    protected void accept()
    {
        startListening();
    }

    @Override
    protected void stop()
    {
        try {
            m_config.getServerSocket().close();
        } catch(IOException e) {
            getLogger().warn("Error closing socket importer server socket on port " + m_config.getPort(), e);
        }

        for (ClientConnectionHandler client : m_clients) {
            client.stopClient();
        }
    }

    private void startListening()
    {
        try {
            while (shouldRun()) {
                Socket clientSocket = m_config.getServerSocket().accept();
                ClientConnectionHandler ch = new ClientConnectionHandler(clientSocket, m_config.getProcedure());
                m_clients.add(ch);
                ch.start();
            }
        } catch(IOException e) {
           getLogger().error("Unexpected error accepting client connections for " + getName() + " on port " + m_config.getPort(), e);
        }
    }

    //This is ClientConnection handler to read and dispatch data to stored procedure.
    private class ClientConnectionHandler extends Thread
    {
        private final Socket m_clientSocket;
        private final String m_procedure;

        public ClientConnectionHandler(Socket clientSocket, String procedure)
        {
            m_clientSocket = clientSocket;
            m_procedure = procedure;
        }

        @Override
        public void run()
        {
            try {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(m_clientSocket.getInputStream()));
                while (shouldRun()) {
                    String line = in.readLine();
                    //You should convert your data to params here.
                    if (line == null) continue;
                    CSVInvocation invocation = new CSVInvocation(m_procedure, line);
                    if (!callProcedure(invocation)) {
                        rateLimitedLog(Level.ERROR, null, "Socket importer insertion failed");
                    }
                }
            } catch (IOException ioe) {
                getLogger().error("IO exception reading from client socket connection in socket importer", ioe);
            }

            try {
                m_clientSocket.close();
                getLogger().info("Client Closed.");
            } catch(IOException e) {
                getLogger().warn("Error closing socket importer connection", e);
            }
        }

        public void stopClient()
        {
            // nothing to do for now
        }
    }
}
