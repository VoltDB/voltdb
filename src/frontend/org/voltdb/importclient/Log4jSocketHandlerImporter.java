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

package org.voltdb.importclient;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.spi.LoggingEvent;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltcore.network.ReverseDNSCache;
import org.voltdb.importer.ImportHandlerProxy;
import org.voltdb.importer.Invocation;

/**
 * ImportHandlerProxy implementation for importer that picks up log4j socket appender events.
 */
public class Log4jSocketHandlerImporter extends ImportHandlerProxy implements BundleActivator
{

    private static final String PORT_CONFIG = "port";
    private static final String EVENT_TABLE_CONFIG = "log-event-table";

    private int m_port;
    private String m_tableName;
    private ServerSocket m_serverSocket;
    private final ArrayList<SocketReader> m_connections = new ArrayList<SocketReader>();

    @Override
    public void start(BundleContext context)
    {
        context.registerService(Log4jSocketHandlerImporter.class.getName(), this, null);
    }

    @Override
    public void stop(BundleContext context)
    {
        stop();
    }

    @Override
    public void stop()
    {
        closeServerSocket();

        for (SocketReader conn : m_connections) {
            conn.stop();
        }
        m_connections.clear();
    }

    private void closeServerSocket()
    {
        try {
            if (m_serverSocket!=null) {
                m_serverSocket.close();
            }
        } catch(IOException e) { // nothing to do other than log
            info("Unexpected error closing log4j socket appender listener on " + m_port);
        }
    }

    /**
     * Return a name for VoltDB to log with friendly name.
     * @return name of the importer.
     */
    @Override
    public String getName()
    {
        return "Log4jSocketHandlerImporter";
    }

    /**
     * This is called with the properties that are supplied in the deployment.xml
     * Do any initialization here.
     * @param p
     */
    @Override
    public void configure(Properties p)
    {
        Properties properties = (Properties) p.clone();
        String str = properties.getProperty(PORT_CONFIG);
        if (str == null || str.trim().length() == 0) {
            throw new RuntimeException(PORT_CONFIG + " must be specified as a log4j socket importer property");
        }
        m_port = Integer.parseInt(str);

        closeServerSocket(); // just in case something was not cleaned up properly
        try {
            m_serverSocket = new ServerSocket(m_port);
            info("Log4j socket appender listener listening on port: " + m_port);
        } catch(IOException e) {
            error("IOException opening server socket on port " + m_port + " - " + e.getMessage());
            throw new RuntimeException(e);
        }

        m_tableName = properties.getProperty(EVENT_TABLE_CONFIG);
        if (m_tableName==null || m_tableName.trim().length()==0) {
            throw new RuntimeException(EVENT_TABLE_CONFIG + " must be specified as a log4j socket importer property");
        }

        //TODO:
        // - Config for Anish's idea of deleting old events
        // May be use a configuration that says "keep 'n' hrs data" or "keep 'n' rows", whichever is larger.
    }


    /**
     * This is called when server is ready to accept any transactions.
     */
    @Override
    public void readyForData()
    {
        if (!hasTable(m_tableName)) {
            printCreateTableError();
            return;
        }

        try {
            while (true) {
                Socket socket = m_serverSocket.accept();
                SocketReader reader = new SocketReader(socket);
                m_connections.add(reader);
                new Thread(reader).start();
            }
        } catch (IOException e) {
            //TODO: Could check if this was stopped and log info level message to avoid erroneous error log
            error(String.format("Unexpected error [%s] accepting connections on port [%d]", e.getMessage(), m_serverSocket.getLocalPort()));
        } finally {
            closeServerSocket();
        }
    }

    private void printCreateTableError()
    {
            System.err.println("Log event table must exist before Log4j socket importer can be used");
            System.err.println("Please create the table using the following ddl and use appropriate partition:");
            System.err.println("CREATE TABLE " + m_tableName + "\n" +
            "(\n" +
            "  log_event_host    varchar(256) NOT NULL\n" +
            ", logger_name       varchar(256) NOT NULL\n" +
            ", log_level         varchar(25)  NOT NULL\n" +
            ", logging_thread    varchar(25)  NOT NULL\n" +
            ", log_timestamp     timestamp    NOT NULL\n" +
            ", log_message       varchar(1024)\n" +
            ", throwable_str_rep varchar(4096)\n" +
            ");\n" +
            "PARTITION TABLE " + m_tableName + " ON COLUMN log_event_host;");
    }

    /**
     * Read log4j events from socket and persist into volt
     */
    private class SocketReader implements Runnable
    {
        private final Socket m_socket;

        public SocketReader(Socket socket)
        {
            m_socket = socket;
            Log4jSocketHandlerImporter.this.info("Connected to socket appender at " + socket.getRemoteSocketAddress());
        }

        @Override
        public void run()
        {
            try {
                String hostname = ReverseDNSCache.hostnameOrAddress(m_socket.getInetAddress());
                ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(m_socket.getInputStream()));
                while (true) {
                    LoggingEvent event = (LoggingEvent) ois.readObject();
                    if (!Log4jSocketHandlerImporter.this.callProcedure(new SaveLog4jEventInvocation(hostname, event, m_tableName))) {
                        Log4jSocketHandlerImporter.this.error("Failed to insert log4j event");
                    }
                }
            } catch(EOFException e) { // normal exit condition
                Log4jSocketHandlerImporter.this.info("Client disconnected from " + m_socket.getRemoteSocketAddress());
            } catch (ClassNotFoundException | IOException e) { // assume that these are unrecoverable
                                                               // errors and exit from thread
                Log4jSocketHandlerImporter.this.error(String.format("Unexpected error [%s] reading from %s", e.getMessage(), m_socket.getRemoteSocketAddress()));
                e.printStackTrace();
            } finally {
                closeSocket();
            }
        }

        public void stop()
        {
            closeSocket();
        }

        private void closeSocket()
        {
            try {
                m_socket.close();
            } catch(IOException e) {
                Log4jSocketHandlerImporter.this.error("Could not close log4j event reader socket on " + m_socket.getLocalPort());
                e.printStackTrace();
            }
        }
    }

    /**
     * Class with invocation details for the stored procedure to insert a logging event into voltdb.
     */
    private class SaveLog4jEventInvocation implements Invocation {

        private final String m_hostName;
        private final LoggingEvent m_event;
        private final String m_procName;

        public SaveLog4jEventInvocation(String hostName, LoggingEvent loggingEvent, String tableName) {
            m_hostName = hostName;
            m_event = loggingEvent;
            m_procName = tableName + ".insert";
        }
        @Override
        public String getProcedure()
        {
            return m_procName;
        }

        @Override
        public Object[] getParams() throws IOException
        {
            return new Object[] {
                    m_hostName,
                    m_event.getLoggerName(),
                    m_event.getLevel().toString(),
                    m_event.getThreadName(),
                    m_event.getTimeStamp()*1000,
                    m_event.getRenderedMessage(),
                    getThrowableRep(m_event)
           };
        }

        // Gets the throwable representation from LoggingEvent as a single string
        // with newline chars between lines.
        // Returns null if there is no throwable information in the logging event.
        private String getThrowableRep(LoggingEvent event)
        {
            if (event.getThrowableStrRep() == null || event.getThrowableStrRep().length==0) {
                return null;
            }

            StringBuffer sb = new StringBuffer();
            for (String line : event.getThrowableStrRep()) {
                sb.append(line + "\n");
            }

            // remove the last newline and return the string
            return sb.deleteCharAt(sb.length() - 1).toString();
        }
    }
}
