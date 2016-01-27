/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.AbstractFormatterFactory;

/**
 * ImporterConfig for server socket importer.
 */
public class ServerSocketImporterConfig implements ImporterConfig
{
    private static final String SOCKET_IMPORTER_URI_SCHEME = "socketimporter";

    private final URI m_resourceID;
    private final AbstractFormatterFactory m_formatterFactory;
    private final String m_procedure;
    private final int m_port;
    private final ServerSocket m_serverSocket;

    public ServerSocketImporterConfig(Properties props, AbstractFormatterFactory formatterFactory)
    {
        Properties propsCopy = (Properties) props.clone();

        m_procedure = (String) propsCopy.get("procedure");
        if (m_procedure == null || m_procedure.trim().length() == 0) {
            throw new IllegalArgumentException("Missing procedure.");
        }

        String portStr = (String) propsCopy.get("port");
        try {
            m_port = Integer.parseInt(portStr);
            if (m_port <= 0) {
                throw new NumberFormatException();
            }
        } catch(NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port specification: " + portStr);
        }

        try {
            m_serverSocket = new ServerSocket(m_port);
        } catch(IOException e) {
            throw new IllegalArgumentException("Error starting socket importer listener on port: " + m_port, e);
        }

        try {
            m_resourceID = new URI(SOCKET_IMPORTER_URI_SCHEME, portStr, null);
        } catch(URISyntaxException e) { // Will not happen
            throw new RuntimeException(e);
        }

        m_formatterFactory = formatterFactory;
    }

    @Override
    public URI getResourceID()
    {
        return m_resourceID;
    }

    @Override
    public AbstractFormatterFactory getFormatterFactory()
    {
        return m_formatterFactory;
    }

    public String getProcedure()
    {
        return m_procedure;
    }

    public int getPort()
    {
        return m_port;
    }

    public ServerSocket getServerSocket()
    {
        return m_serverSocket;
    }
}
