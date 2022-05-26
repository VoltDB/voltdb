/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.importclient.log4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.FormatterBuilder;

/**
 * ImporterConfig implementation to hold configuration for Log4j socket handler importer.
 */
public class Log4jSocketImporterConfig implements ImporterConfig
{
    private static final String URI_SCHEME = "log4jsocketimporter";
    private static final String PORT_CONFIG = "port";
    private static final String EVENT_TABLE_CONFIG = "log-event-table";

    private final int m_port;
    private final String m_tableName;
    private final ServerSocket m_serverSocket;
    private final URI m_resourceID;

    /**
     * This is called with the properties that are supplied in the deployment.xml
     * Do any initialization here.
     * @param p
     */
    public Log4jSocketImporterConfig(Properties p)
    {
        Properties properties = (Properties) p.clone();
        String portStr = properties.getProperty(PORT_CONFIG);
        if (portStr == null || portStr.trim().length() == 0) {
            throw new RuntimeException(PORT_CONFIG + " must be specified as a log4j socket importer property");
        }
        m_port = Integer.parseInt(portStr);

        try {
            m_serverSocket = new ServerSocket(m_port);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }

        m_tableName = properties.getProperty(EVENT_TABLE_CONFIG);
        if (m_tableName==null || m_tableName.trim().length()==0) {
            throw new RuntimeException(EVENT_TABLE_CONFIG + " must be specified as a log4j socket importer property");
        }

        try {
            m_resourceID = new URI(URI_SCHEME, portStr, null);
        } catch(URISyntaxException e) { // Will not happen
            throw new RuntimeException(e);
        }
    }

    @Override
    public URI getResourceID()
    {
        return m_resourceID;
    }

       public int getPort()
    {
        return m_port;
    }

    public String getTableName()
    {
        return m_tableName;
    }

    public ServerSocket getServerSocket()
    {
        return m_serverSocket;
    }

    @Override
    public FormatterBuilder getFormatterBuilder()
    {
        return null;
    }
}
