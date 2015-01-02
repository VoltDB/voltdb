/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.tomcat;

import java.util.ArrayList;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;

public class TomcatVoltDBAppender extends AppenderSkeleton implements Appender {
    ClientConfig config = null;
    Client client = null;
    ArrayList<LoggingEvent> cache = new ArrayList<LoggingEvent>();

    public TomcatVoltDBAppender() {
        // Create a connection to VoltDB
        try {
            config = new ClientConfig("", "");
            config.setReconnectOnConnectionLoss(true);
            client = ClientFactory.createClient(config);
            client.createConnection("localhost");
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