/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package log4jsocketimporter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

import org.apache.log4j.spi.LoggingEvent;
import org.voltcore.network.ReverseDNSCache;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NullCallback;
import org.voltdb.types.TimestampType;

/**
 * Listens for log4j socket appender messages on the specified port and sends
 * them to VoltDB.
 */
public class Log4jSocketListener
{
    private ServerSocket m_serverSocket;
    private String m_voltHostPort;

    /**
     * Ctor
     *
     * @param log4jListenerPort
     *            Port on which this will start listening
     * @param voltHostPort
     *            Volt server host and port in host:port form. Port may be
     *            omitted.
     * @throws IOException
     *             If an error occurs trying to start the server socket
     */
    public Log4jSocketListener(int log4jListenerPort, String voltHostPort) throws IOException
    {
        m_serverSocket = new ServerSocket(log4jListenerPort);
        System.out.println("Listening on port " + log4jListenerPort + " for log4j socket appender");
        m_voltHostPort = voltHostPort;
    }

    /**
     * Start listening.
     */
    public void start()
    {
        try {
            while (true) {
                Socket socket = m_serverSocket.accept();
                System.out.println("Connected to socket appender at " + socket.getRemoteSocketAddress());
                try {
                    new Thread(new SocketReader(socket, m_voltHostPort)).start();
                } catch (IOException e) {
                    System.err.println("Unexpected error opening input stream to " + socket.getRemoteSocketAddress());
                    e.printStackTrace();
                    System.err.println("Continuing to listen to more connections");
                }
            }
        } catch (IOException e) {
            System.err.println("Unexpected error accepting connections on port " + m_serverSocket.getLocalPort());
            e.printStackTrace();
        }
    }

    /**
     * Read from a socket and persist into volt
     */
    private static class SocketReader implements Runnable
    {
        private Socket m_socket;
        private ObjectInputStream m_ois;
        private Client m_voltClient;

        public SocketReader(Socket socket, String voltHostPort) throws IOException
        {
            m_socket = socket;
            m_ois = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
            m_voltClient = ClientFactory.createClient(); // TODO: no config for
                                                         // now
            connectToOneServerWithRetry(voltHostPort);
        }

        // Copied from voter example
        private void connectToOneServerWithRetry(String server)
        {
            int sleep = 1000;
            while (true) {
                try {
                    m_voltClient.createConnection(server);
                    break;
                } catch (Exception e) {
                    System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                    try {
                        Thread.sleep(sleep);
                    } catch (Exception interruted) {
                    }
                    if (sleep < 8000)
                        sleep += sleep;
                }
            }
            System.out.printf("Connected to VoltDB node at: %s.\n", server);
        }

        @Override
        public void run()
        {
            String hostname = ReverseDNSCache.hostnameOrAddress(m_socket.getInetAddress());
            try {
                while (true) {
                    LoggingEvent event = (LoggingEvent) m_ois.readObject();
                    // Use a NullCallback because this code is temp and will get
                    // pulled into an in-server importer
                    m_voltClient.callProcedure(new NullCallback(), "log_events.insert", hostname,
                            event.getLoggerName(), event.getLevel().toString(), event.getThreadName(),
                            new TimestampType(new Date(event.getTimeStamp())), event.getRenderedMessage(),
                            getThrowableRep(event));
                }
            } catch (ClassNotFoundException | IOException e) { // assume that these are unrecoverable
                                                               // errors and exit from thread
                System.err.println("Unexpected error reading from " + m_socket.getRemoteSocketAddress());
                e.printStackTrace();
            }
        }

        // Gets the throwable representation from LoggingEvent as a single string
        // with newline chars between lines.
        // Returns null if there is no throwable information in the logging event.
        private String getThrowableRep(LoggingEvent event)
        {
            if (event.getThrowableStrRep() == null) {
                return null;
            }

            StringBuffer sb = new StringBuffer();
            for (String line : event.getThrowableStrRep()) {
                sb.append(line + "\n");
            }
            if (sb.length() > 0) { // remove the last newline and return the string
                return sb.deleteCharAt(sb.length() - 1).toString();
            } else {
                return null;
            }
        }
    }

}
