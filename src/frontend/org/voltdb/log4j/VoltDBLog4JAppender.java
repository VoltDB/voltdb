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

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

public class VoltDBLog4JAppender extends AppenderSkeleton implements Appender {
    String server = "localhost";
    int port = 21212;
    String user = null;
    String password = null;
    String table = "log4j";

    ClientConfig config = null;
    Client client = null;
    VoltBulkLoader bulkLoader = null;

    static class VoltDBLog4JAppenderCallback implements BulkLoaderFailureCallBack {

        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList,
                ClientResponse response) {
            System.err.println("Log insertion into VoltDB failed");
        }

    }

    public void setCluster(String cluster) { this.server = cluster; }
    public String getCluster() { return this.server; }

    public void setPort(int port) { this.port = port; }
    public int getPort() { return this.port; }

    public void setUser(String user) { this.user = user; }
    public String getUser() { return this.user; }

    public void setPassword(String password) { this.password = password; }
    public String getPassword () { return this.password; }

    public void setTable(String table) { this.table = table; }
    public String getTable() { return this.table; }


    public VoltDBLog4JAppender() {
        try {
            // Create a connection to VoltDB
            if ((user != null && !user.trim().isEmpty()) && (password != null && !password.trim().isEmpty())) {
                config = new ClientConfig(user, password);
            } else {
                config = new ClientConfig();
            }
            config.setReconnectOnConnectionLoss(true);
            client = ClientFactory.createClient(config);
            client.createConnection(server, port);

            // Make sure we have a table set up.
            setupTable(client);

            // Grab a bulk loader
            bulkLoader = client.getNewBulkLoader("log4j", 1, new VoltDBLog4JAppenderCallback());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setupTable(Client client) throws Exception {
        // See if we have a table
        VoltTable allTables = client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
        for (int i = 0; i < allTables.getRowCount(); i++) {
            allTables.advanceRow();
            String name = allTables.getString("TABLE_NAME");
            if (name.toLowerCase().equals(table)){
                // We have the table, don't need to add it
                System.out.println("Using existing table '" + table + "' in VoltDB");
                return;
            }
        }
       // No table, so we need to add one
       String sqlStmt = "CREATE TABLE " + table + " ( timestamp BIGINT, level VARCHAR(10), message VARCHAR(255))";
       client.callProcedure("@AdHoc", sqlStmt);
    }

    @Override
    public void close() {
        // Close the VoltDB connection
        try {
            bulkLoader.drain();
            bulkLoader.close();
            client.drain();
            client.close();
        } catch (Exception e) {
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
            Object rowHandle = null;
            bulkLoader.insertRow(rowHandle, timestamp, level, message);
        } catch (InterruptedException e) {
            System.err.println("Failed to insert into VoltDB using bulk loader");
            e.printStackTrace();
        }
    }


}