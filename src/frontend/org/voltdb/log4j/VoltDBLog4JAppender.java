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

import java.io.IOException;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

/**
 * A Log4j appender that writes to a VoltDB instance.
 * The appender works automatically with minimal setup; a blank, running VoltDB instance
 * is all that is required.
 */
public class VoltDBLog4JAppender extends AppenderSkeleton implements Appender {
    String m_server = "localhost";
    int m_port = 21212;
    String m_user = null;
    String m_password = null;
    String m_table = "log4j";
    String m_insertMethod = "bulkloader";
    long m_current_index = 0;

    ClientConfig m_config = null;
    Client m_client = null;
    AppenderInsert m_insertDevice = null;

    /**
     * Failure callback for insertions to VoltDB
     */
    static class VoltDBLog4JAppenderCallback implements BulkLoaderFailureCallBack {

        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList,
                ClientResponse response) {
            System.err.println("Log insertion into VoltDB failed:");
            System.err.println(response.getStatusString());
        }

    }

    public static class AppenderException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public AppenderException() {
            super();
            //  Auto-generated constructor stub
        }

        public AppenderException(String message, Throwable cause) {
            super(message, cause);
            //  Auto-generated constructor stub
        }

        public AppenderException(String message) {
            super(message);
            //  Auto-generated constructor stub
        }

        public AppenderException(Throwable cause) {
            super(cause);
            //  Auto-generated constructor stub
        }

    }

    /**
     * Interface that defines one method: insert log information into VoltDB
     */
    interface AppenderInsert {
        public void insert(long id, long timestamp, String level, String message) throws AppenderException;
        public void close();
    }

    /**
     * Insert class that uses a bulkloader
     */
    class BulkLoaderAppenderInsert implements AppenderInsert {
        final VoltBulkLoader bulkLoader;
        public BulkLoaderAppenderInsert(Client client) {
            try{
                bulkLoader = client.getNewBulkLoader(m_table, 1, new VoltDBLog4JAppenderCallback());
            } catch (Exception e) {
                throw new AppenderException("Coundn't get bulkloader for client",e);
            }
        }
        @Override
        public void insert(long id, long timestamp, String level, String message) throws AppenderException{
            try {
                bulkLoader.insertRow(null, id, timestamp, level, message);
            } catch (InterruptedException e) {
                throw new AppenderException(e);
            }
        }
        @Override
        public void close() {
            try {
                bulkLoader.drain();
                bulkLoader.close();
            } catch (Exception e) {
                throw new AppenderException("Couldn't close bulkloader",e);
            }
        }
    }

    /**
     * Insert class that uses a stored procedure
     */
    class ProcedureAppenderInsert implements AppenderInsert {
        final Client client;
        public ProcedureAppenderInsert(Client client) {
            this.client = client;
        }
        @Override
        public void insert(long id, long timestamp, String level, String message) throws AppenderException  {
            try {
                client.callProcedure(new NullCallback(), "LogInsert", id, timestamp, level, message);
            } catch (IOException e) {
                throw new AppenderException("failed to invoke logInsert procedure",e);
            }
        }
        @Override
        public void close() {
            try { client.close(); } catch (Exception ignoreIt) {}
        }
    }

    // Log4j configuration loaders
    public void setCluster(String cluster) { this.m_server = cluster; }
    public String getCluster() { return this.m_server; }

    public void setPort(int port) { this.m_port = port; }
    public int getPort() { return this.m_port; }

    public void setUser(String user) { this.m_user = user; }
    public String getUser() { return this.m_user; }

    public void setPassword(String password) { this.m_password = password; }
    public String getPassword () { return this.m_password; }

    public void setTable(String table) { this.m_table = table; }
    public String getTable() { return this.m_table; }

    public void setInsert(String insertMethod) { this.m_insertMethod = insertMethod.toLowerCase(); }
    public String getInsert() { return this.m_table; }



    @Override
    public void activateOptions() {
        // Create a connection to VoltDB
        if ((m_user != null && !m_user.trim().isEmpty()) && (m_password != null && !m_password.trim().isEmpty())) {
            m_config = new ClientConfig(m_user, m_password);
        } else {
            m_config = new ClientConfig();
        }
        m_config.setReconnectOnConnectionLoss(true);
        m_client = ClientFactory.createClient(m_config);

        // Make sure we have a table set up.
        try {
            m_client.createConnection(m_server, m_port);
            setupTable(m_client);
        } catch (ProcCallException | IOException e1) {
            throw new AppenderException("failed to connect client to "+ m_server,e1);
        }

        // Create the insert device
        if ("bulkloader".equals(m_insertMethod)) {
            m_insertDevice = new BulkLoaderAppenderInsert(m_client);
        }
        else if ("procedure".equals(m_insertMethod)) {
            m_insertDevice = new ProcedureAppenderInsert(m_client);
        }
        else {
            throw new AppenderException("Unrecognized insert method: '" + m_insertMethod + "'");
        }
    }
    /**
     * Initializes a new Log4j appender.
     * Connects to VoltDB and verifies a table is ready for insertion.
     */
    public VoltDBLog4JAppender() {
    }

    /**
     * Checks the running VoltDB instance for a table.
     * If no table exists, create & partition one.
     * @param client       The VoltDB client
     * @throws IOException
     * @throws NoConnectionsException
     * @throws Exception
     */
    private void setupTable(Client client) throws ProcCallException,  IOException {
        // See if we have a table
        VoltTable allTables = client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
        while (allTables.advanceRow()) {
            String name = allTables.getString("TABLE_NAME");
            if (name.toLowerCase().equals(m_table)){
                // We have the table, don't need to add it
                System.out.println("Using existing table '" + m_table + "' in VoltDB");
                m_current_index = findCurrentLogIndex();
                return;
            }
        }
       // No table, so we need to add one
       String sqlStmt = "CREATE TABLE " + m_table + " ( id INT UNIQUE NOT NULL, timestamp BIGINT, level VARCHAR(10), message VARCHAR(255))";
       String sqlStmt2 = "PARTITION TABLE " + m_table + " ON COLUMN id;";
       String sqlStmt3 = "CREATE PROCEDURE LogInsert AS "
                         + "INSERT INTO " + m_table + " (id, timestamp, level, message) "
                         + "VALUES (?,?,?,?);";
       client.callProcedure("@AdHoc", sqlStmt);
       client.callProcedure("@AdHoc", sqlStmt2);
       client.callProcedure("@AdHoc", sqlStmt3);
    }

    /**
     * When starting the appender on an existing VoltDB table, find the current index
     * @return The current index
     * @throws Exception
     */
    private long findCurrentLogIndex() throws ProcCallException,  IOException {
        String sqlStmt = "SELECT MAX(id) from " + m_table + ";";
        VoltTable result = m_client.callProcedure("@AdHoc", sqlStmt).getResults()[0];
        result.advanceRow();
        return result.getLong(0);
    }

    /**
     * Returns the client that the appender is using.
     * @return
     */
    public Client getClient() {
        return m_client;
    }

    @Override
    public void close() {
        // Close the VoltDB connection
        try {
            m_insertDevice.close();
            m_client.drain();
            m_client.close();
        } catch (Exception e) {
            System.err.println("Unable to close connection to VoltDB");
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
            m_insertDevice.insert(m_current_index, timestamp, level, message);
            m_current_index++;
        } catch (Exception e) {
            System.err.println("Failed to insert into VoltDB");
            e.printStackTrace();
        }
    }


}