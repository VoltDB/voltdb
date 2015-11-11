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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.voltdb.importer.ImporterConfig;

/**
 */
public class ServerSocketImporterConfig implements ImporterConfig {

    private static final String SOCKET_IMPORTER_URI_SCHEME = "socketimporter";

    private Map<URI, InstanceConfiguration> m_instanceConfigs = new HashMap<>();

    @SuppressWarnings("resource")
    @Override
    public void addConfiguration(Properties props)
    {
        Properties propsCopy = (Properties) props.clone();

        String procedure = (String) propsCopy.get("procedure");
        if (procedure == null || procedure.trim().length() == 0) {
            throw new IllegalArgumentException("Missing procedure.");
        }

        String portStr = (String) propsCopy.get("port");
        if (m_instanceConfigs.containsKey(portStr)) {
            throw new IllegalArgumentException("Multiple configuration sections for socket importer on port: " + portStr);
        }

        int port =-1;
        try {
            port = Integer.parseInt(portStr);
            if (port <= 0) {
                throw new NumberFormatException();
            }
        } catch(NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port specification: " + portStr);
        }

        ServerSocket sSocket = null;
        try {
            sSocket = new ServerSocket(port);
        } catch(IOException e) {
            throw new IllegalArgumentException("Error starting socket importer listener on port: " + port, e);
        }

        try {
            m_instanceConfigs.put(new URI(SOCKET_IMPORTER_URI_SCHEME, portStr, null),
                    new InstanceConfiguration(port, procedure, sSocket));
        } catch(URISyntaxException e) { // Will not happen
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<URI> getAvailableResources()
    {
        return m_instanceConfigs.keySet();
    }

    InstanceConfiguration getInstanceConfiguration(URI resourceID) {
        return m_instanceConfigs.get(resourceID);
    }

    Collection<InstanceConfiguration> getAllInstanceConfigurations() {
        return m_instanceConfigs.values();
    }

    static class InstanceConfiguration
    {
        final int m_port;
        final String m_procedure;
        final ServerSocket m_serverSocket;

        public InstanceConfiguration(int port, String procedure, ServerSocket serverSocket) {
            m_port = port;
            m_procedure = procedure;
            m_serverSocket = serverSocket;
        }
    }
}
