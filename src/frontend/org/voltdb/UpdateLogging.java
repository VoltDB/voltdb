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

package org.voltdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

/**
 * A utility class that can be invoked from the command line to connect
 * to a host running VoltDB and send a specified XML log config file
 * via the UpdateLogging system procedure.
 *
 */
public class UpdateLogging {
    public static void main(String args[]) throws Exception {
        String host = "localhost";
        String configFilePath = null;
        String username = "";
        String password = "";

        // scan the inputs once to read everything but host names
        for (String arg : args) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1) {
                continue;
            } else if (parts[1].startsWith("${")) {
                continue;
            }
            else if (parts[0].equalsIgnoreCase("host")) {
                host = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("config")) {
                configFilePath = parts[1];
            } else if (parts[0].equalsIgnoreCase("user")) {
                username = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("password")) {
                password = parts[1];
            }
        }

        ClientConfig config = new ClientConfig(username, password);
        final Client client = ClientFactory.createClient(config);
        client.createConnection(host);

        File configFile = new File(configFilePath);

        if (!configFile.exists()) {
            System.err.println( configFile.getPath() + " doesn't exist");
        }

        if (!configFile.canRead()) {
            System.err.println( configFile.getPath() + " can't be read");
        }

        final StringBuilder sb = new StringBuilder(10000);
        final BufferedReader reader = new BufferedReader( new FileReader(configFile));
        final char buf[] = new char[1024];
        int numRead = 0;
        while ((numRead = reader.read(buf)) != -1) {
            String readData = String.valueOf(buf, 0, numRead);
            sb.append(readData);
        }
        reader.close();

        client.callProcedure("@UpdateLogging", sb.toString());
        client.close();
    }
}
