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

package org.voltdb.log4j;

import java.util.ArrayList;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;

public class VoltDBLog4JAppender extends AppenderSkeleton implements Appender {
    String server = "localhost";
    int port = 21212;
    String user = null;
    String password = null;
    ClientConfig config = null;
    Client client = null;
    ArrayList<LoggingEvent> cache = new ArrayList<LoggingEvent>();

    public void setCluster(String cluster) { this.server = cluster; }
    public String getCluster() { return this.server; }

    public void setPort(int port) { this.port = port; }
    public int getPort() { return this.port; }

    public void setUser(String user) { this.user = user; }
    public String getUser() { return this.user; }

    public void setPassword(String password) { this.password = password; }
    public String getPassword () { return this.password; }


    public VoltDBLog4JAppender() {
        // Create a connection to VoltDB
        try {
            if (user != null && password != null) {
                config = new ClientConfig(user, password);
            } else {
                config = new ClientConfig("", "");
            }
            config.setReconnectOnConnectionLoss(true);
            client = ClientFactory.createClient(config);
            client.createConnection(server, port);
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        // Close the VoltDB connection
        try {
            client.drain();
            client.close();
        } catch (InterruptedException | NoConnectionsException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    @Override
    protected void append(LoggingEvent arg0) {
        // Extract the message information we need
        long timestamp = arg0.getTimeStamp();
        String level = arg0.getLevel().toString();
        String message = arg0.getMessage().toString();

        // Insert the log message into VoltDB
        try{
            client.callProcedure("VoltdbInsert", timestamp, level, message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}