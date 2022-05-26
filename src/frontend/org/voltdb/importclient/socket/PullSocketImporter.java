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

package org.voltdb.importclient.socket;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.Level;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.Invocation;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;

import com.google_voltpatches.common.base.Optional;
import java.nio.ByteBuffer;

/**
 * Importer implementation for pull socket importer. At runtime, there will
 * one instance of this per host and socket combination.
 */
public class PullSocketImporter extends AbstractImporter {

    private PullSocketImporterConfig m_config;
    private final AtomicBoolean m_eos = new AtomicBoolean(false);
    private volatile Optional<Thread> m_thread = Optional.absent();
    private volatile Socket m_socket = null;
    private final Object m_socketLock = new Object();

    PullSocketImporter(PullSocketImporterConfig config)
    {
        m_config = config;
    }

    @Override
    public URI getResourceID()
    {
        return m_config.getResourceID();
    }

    @Override
    public void accept() {
        susceptibleRun();
    }

    @Override
    public void stop() {
        close();
    }

    @Override
    public String getName() {
        return "PullSocketImporter";
    }

    private void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException unexpected){
                error(unexpected, "Unexpected exception closing socket");
            }
        }
    }

    /** Set the socket to newSocket, unless we're shutting down.
     * The most reliable way to ensure the importer thread exits is to close its socket.
     * @param newSocket socket to replace any previous socket. May be null.
     */
    private void replaceSocket(Socket newSocket) {
        synchronized (m_socketLock) {
            closeSocket(m_socket);
            if (m_eos.get()) {
                closeSocket(newSocket);
                m_socket = null;
            } else {
                m_socket = newSocket;
            }
        }
    }

    private void susceptibleRun() {
        if (m_eos.get()) return;

        info(null, "Starting socket puller for " + m_config.getResourceID());

        m_thread = Optional.of(Thread.currentThread());
        Optional<BufferedReader> reader = null;
        Formatter formatter = m_config.getFormatterBuilder().create();
        while (!m_eos.get()) {
            try {
                reader = attemptBufferedReader();
                if (!reader.isPresent()) {
                    sleep(2_000);
                    continue;
                }
                info(null, "Socket puller for " + m_config.getResourceID() + " connected.");

                BufferedReader br = reader.get();
                String csv = null;
                while ((csv=br.readLine()) != null) {
                     try{
                        Object params[] = formatter.transform(ByteBuffer.wrap(csv.getBytes()));
                        Invocation invocation = new Invocation(m_config.getProcedure(), params);
                        if (!callProcedure(invocation)) {
                            if (isDebugEnabled()) {
                                 debug(null, "Failed to process Invocation possibly bad data: " + csv);
                            }
                         }
                      } catch (FormatException e){
                          rateLimitedLog(Level.ERROR, e, "Failed to tranform data: %s" ,csv);;
                      }
                }
                if (csv == null) {
                    warn(null, m_config.getResourceID() + " peer terminated stream");
                }
            } catch (EOFException e) {
                rateLimitedLog(Level.WARN, e, m_config.getResourceID() + " peer terminated stream");
            } catch (InterruptedException e) {
                if (m_eos.get()) return;
                rateLimitedLog(Level.ERROR, e, "Socket puller %s was interrupted", m_config.getResourceID());
            } catch (InterruptedIOException e) {
                if (m_eos.get()) return;
                rateLimitedLog(Level.ERROR, e, "Socket puller for %s was interrupted", m_config.getResourceID());
            } catch (IOException e) {
                if (m_eos.get() && e.getMessage().contains("Socket closed")) return;
                rateLimitedLog(Level.ERROR, e, "Read fault for %s", m_config.getResourceID());
            }
        }

        info(null, "Stopping socket puller for " + m_config.getResourceID());
    }

    private Optional<BufferedReader> attemptBufferedReader() {
        Optional<BufferedReader> attempt = Optional.absent();
        if (m_eos.get()) return attempt;

        InetSocketAddress sa = new InetSocketAddress(m_config.getResourceID().getHost(), m_config.getResourceID().getPort());

        @SuppressWarnings("resource")
        Socket skt = new Socket();

        try {
            skt.connect(sa, 4_000);
            InputStreamReader isr = new InputStreamReader(skt.getInputStream(), StandardCharsets.UTF_8);
            attempt = Optional.of(new BufferedReader(isr, 8192));
        } catch (InterruptedIOException e) {
            if (m_eos.get()) return attempt;
            if (isDebugEnabled()) {
                rateLimitedLog(Level.DEBUG, e, "Unable to connect to " + m_config.getResourceID());
            }
        } catch (IOException e) {
            if (isDebugEnabled()) {
                rateLimitedLog(Level.DEBUG, e, "Unable to connect to " + m_config.getResourceID());
            }
        } finally {
            replaceSocket(skt);
        }
        return attempt;
    }

    private boolean sleep(int ms) throws InterruptedException {
        if (m_eos.get()) return true;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            if (m_eos.get()) return true;
            throw e;
        }
        return false;
    }

    public void close() {
        if (m_eos.compareAndSet(false, true) && m_thread.isPresent()) {
            m_thread.get().interrupt();
        }
        replaceSocket(null);
    }
}
