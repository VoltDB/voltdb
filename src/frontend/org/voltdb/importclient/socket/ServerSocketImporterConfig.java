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

import org.voltdb.importer.ImportDataProcessor;
import org.voltdb.importer.ImporterConfig;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

/**
 * ImporterConfig for server socket importer.
 */
public class ServerSocketImporterConfig implements ImporterConfig
{
    private static final String CSV_FORMATTER_NAME = "csv";
    private static final String TSV_FORMATTER_NAME = "tsv";

    private static final String SOCKET_IMPORTER_URI_SCHEME = "socketimporter";

    private final URI m_resourceID;
    private final String m_procedure;
    private final int m_port;
    private final char m_separator;
    private final ServerSocket m_serverSocket;

    public ServerSocketImporterConfig(Properties props)
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

        String formatter = props.getProperty(ImportDataProcessor.IMPORT_FORMATTER, CSV_FORMATTER_NAME).trim().toLowerCase();
        if (!CSV_FORMATTER_NAME.equals(formatter) && !TSV_FORMATTER_NAME.equals(formatter)) {
            throw new RuntimeException("Invalid formatter: " + formatter);
        }
        m_separator = CSV_FORMATTER_NAME.equals(formatter) ? CSVParser.DEFAULT_SEPARATOR : '\t';
    }

    @Override
    public URI getResourceID()
    {
        return m_resourceID;
    }

    public String getProcedure()
    {
        return m_procedure;
    }

    public int getPort()
    {
        return m_port;
    }

    public char getSeparator()
    {
        return m_separator;
    }

    public ServerSocket getServerSocket()
    {
        return m_serverSocket;
    }
}
