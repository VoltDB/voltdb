/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.log4j.helpers;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.net.DatagramSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TCPSyslogWriter mimics {@link SyslogWriter} behavior,
 * but sends each peace of data (normally, a String) over a TCP socket.
 * The socket is opened and closed on each write.
 *
 * @since 1.2.15-tcp-patch
 * @author Daniil V. Kolpakov (ctxm.com)
 */
public class TCPSyslogWriter extends Writer {
    static final int SYSLOG_TCP_PORT = 514;
    
    private transient final String host;
    private transient final int port;
    
    private final Socket socket;
    private final OutputStream out;
    
    /**
     * Regular expression pattern to match "tcp:host:port" string, with
     * capturing groups for host and port parts. The host can be surrounded by
     * a pair of square brackets, and the tcp: part is optional
     */
    final static Pattern hostPortPattern =
            Pattern.compile("(?:tcp:)?  # optional tcp: prefix\n"
                          + "(?:        # \n"
                          + "\\[        # opening bracket\n"
                          + "([^\\]]+)  # IPv6 host: any chars except. ']'\n"
                          + "]          # closing bracket\n"
                          + "|          # \n"
                          + "([^:]+))   # IPv4 host or domain name\n"
                          + "(?:        # optional part start\n"
                          + ":          # colon\n"
                          + "(\\d+)     # port (digits)\n"
                          + ")?         # optional part end",
                            Pattern.COMMENTS);
    
    /**
     * Creates new writer based on the tcp:host:port specification. The ":port"
     * part is optional; the "tcp:" prefix is optional too; the port is decimal.
     * If you need to specify IPv6 address, enclose it in square brackets.
     * 
     * @param hostPort the host:port specification
     */
    public TCPSyslogWriter(String hostPort) {
        final Matcher m = hostPortPattern.matcher(hostPort);
        Socket tempSocket = null;
        OutputStream tempOut = null;
        if (m.matches()) {
            if (m.group(1) == null) {
                this.host = m.group(2); // IPv4 or domain name
            } else {
                this.host = m.group(1); //IPv6
            }
            if (m.group(3) == null) {
                this.port = SYSLOG_TCP_PORT;
            } else {
                this.port = Integer.parseInt(m.group(3));
            }
            try {
                tempSocket = new Socket(host, port);
                tempSocket.setTcpNoDelay(false);
                tempOut = tempSocket.getOutputStream();
            }
            catch (Exception e) {
                e.printStackTrace();
                LogLog.error("Could not instantiate DatagramSocket to " + host +
                        ". All logging will FAIL.", e);
            }
        } else {
            LogLog.error("Could not parse host:port parameter (" + hostPort
                    + "). All logging will FAIL.");
            this.host = null;
            this.port = -1;
        }
        socket = tempSocket;
        out = tempOut;
    }
    
    /**
     * Close the socket opened by the constructor if it is not null
     */
    public void close() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
            }
        }
    }
    
    /**
     * No-op, since output is not buffered
     */
    public void flush() {
    }
    
    /**
     * Sends the {@code data} specified if the connection exists
     */
    public void write(final String data) throws IOException {
        if (socket != null) {
            out.write(data.getBytes());
        }
    }
    
    /**
     * Opens TCP socket connection, sends the {@code count} chars out of the
     * {@code data} parameter starting with {@code start} and closes the
     * connection.
     */
    public void write(final char[] data,
                      final int start,
                      final int count) throws IOException {
        write(new String(data, start, count));
    }
    
    public void finalize() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
            }
        }
    }
}