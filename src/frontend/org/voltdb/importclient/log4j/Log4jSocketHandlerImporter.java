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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;

import org.apache.log4j.spi.LoggingEvent;
import org.voltcore.network.ReverseDNSCache;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.Invocation;

/**
 * Log4j socket handler importer that listens on a specified port.
 */
public class Log4jSocketHandlerImporter extends AbstractImporter
{
    private final ArrayList<SocketReader> m_connections = new ArrayList<SocketReader>();
    private final Log4jSocketImporterConfig m_config;

    public Log4jSocketHandlerImporter(Log4jSocketImporterConfig config)
    {
        m_config = config;
    }

    @Override
    public URI getResourceID()
    {
        return m_config.getResourceID();
    }

    @Override
    public String getName()
    {
        return "Log4jSocketHandlerImporter";
    }

    @Override
    public void accept()
    {
        /*
        if (!hasTable(m_config.getTableName())) {
            printCreateTableError();
            return;
        }
        */

        try {
            while (shouldRun()) {
                Socket socket = m_config.getServerSocket().accept();
                SocketReader reader = new SocketReader(socket);
                m_connections.add(reader);
                new Thread(reader).start();
            }
        } catch (IOException e) {
            if (shouldRun()) {
                error(null, String.format("Unexpected error [%s] accepting connections on port [%d]", e.getMessage(), m_config.getPort()));
            }
        } finally {
            closeServerSocket();
        }
    }

    @Override
    public void stop()
    {
        closeServerSocket();

        for (SocketReader reader : m_connections) {
            reader.stop();
        }
    }

    private void closeServerSocket()
    {
        try {
            m_config.getServerSocket().close();
        } catch(IOException e) { // nothing to do other than log
            if (isDebugEnabled()) {
                debug(null, "Unexpected error closing log4j socket appender listener on " + m_config.getPort());
            }
        }
    }

    private void printCreateTableError()
    {
            System.err.println("Log event table must exist before Log4j socket importer can be used");
            System.err.println("Please create the table using the following ddl and use appropriate partition:");
            System.err.println("CREATE TABLE " + m_config.getTableName() + "\n" +
            "(\n" +
            "  log_event_host    varchar(256) NOT NULL\n" +
            ", logger_name       varchar(256) NOT NULL\n" +
            ", log_level         varchar(25)  NOT NULL\n" +
            ", logging_thread    varchar(25)  NOT NULL\n" +
            ", log_timestamp     timestamp    NOT NULL\n" +
            ", log_message       varchar(1024)\n" +
            ", throwable_str_rep varchar(4096)\n" +
            ");\n" +
            "PARTITION TABLE " + m_config.getTableName() + " ON COLUMN log_event_host;");
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
            Log4jSocketHandlerImporter.this.info(null, "Connected to socket appender at " + socket.getRemoteSocketAddress());
        }

        @Override
        public void run()
        {
            try {
                String hostname = ReverseDNSCache.hostnameOrAddress(m_socket.getInetAddress());
                ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(m_socket.getInputStream()));
                while (true) {
                    LoggingEvent event = (LoggingEvent) ois.readObject();
                    if (!Log4jSocketHandlerImporter.this.callProcedure(saveLog4jEventInvocation(hostname, event, m_config.getTableName()))) {
                        Log4jSocketHandlerImporter.this.error(null, "Failed to insert log4j event");
                    }
                }
            } catch(EOFException e) { // normal exit condition
                Log4jSocketHandlerImporter.this.info(null, "Client disconnected from " + m_socket.getRemoteSocketAddress());
            } catch (ClassNotFoundException | IOException e) { // assume that these are unrecoverable
                                                               // errors and exit from thread
                Log4jSocketHandlerImporter.this.error(null, String.format("Unexpected error [%s] reading from %s", e.getMessage(), m_socket.getRemoteSocketAddress()));
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
                Log4jSocketHandlerImporter.this.error(null, "Could not close log4j event reader socket on " + m_socket.getLocalPort());
                e.printStackTrace();
            }
        }
    }

    private Invocation saveLog4jEventInvocation(String hostName, LoggingEvent loggingEvent, String tableName) {
        String throwRep = null;
        if (loggingEvent.getThrowableStrRep() != null && loggingEvent.getThrowableStrRep().length != 0) {
            StringBuffer sb = new StringBuffer();
            for (String line : loggingEvent.getThrowableStrRep()) {
                sb.append(line + "\n");
            }
            throwRep = sb.deleteCharAt(sb.length() - 1).toString();
        }

        return new Invocation(tableName + ".insert",
                              new Object[] {
                                hostName,
                                loggingEvent.getLoggerName(),
                                loggingEvent.getLevel().toString(),
                                loggingEvent.getThreadName(),
                                loggingEvent.getTimeStamp()*1000,
                                loggingEvent.getRenderedMessage(),
                                throwRep}
        );
    }
}
