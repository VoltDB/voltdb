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

package org.voltdb.importclient.socket;

import static com.google_voltpatches.common.base.Preconditions.checkNotNull;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.Level;
import org.voltdb.importclient.ImportBaseException;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.CSVInvocation;
import org.voltdb.importer.ImporterConfig;

import com.google_voltpatches.common.base.Optional;

public class PullSocketImporterVersion2 extends AbstractImporter {

    private PullSocketImporterConfig m_config;
    private volatile Map<URI,ReadFromSocket> m_readers = new HashMap<>();

    private PullerException loggedException(Throwable t, String fmt, Object...args) {
        if (!PullerException.isLogged(t)) {
            rateLimitedLog(Level.ERROR, t, fmt, args);
        }
        return new PullerException(fmt, t, args).markLogged();
    }

    @Override
    public boolean isRunEveryWhere() {
        return false;
    }

    @Override
    public ImporterConfig createImporterConfig() {
        m_config = new PullSocketImporterConfig();
        return m_config;
    }

    @Override
    public void accept(URI uri) {
        @SuppressWarnings("resource")
        ReadFromSocket reader = new ReadFromSocket(uri, m_config.getProcedure(uri));
        m_readers.put(uri, reader);
        reader.susceptibleRun();
    }

    @Override
    public void stop(URI uri) {
        ReadFromSocket reader = m_readers.remove(uri);
        if (reader != null) { // TODO: this shouldn't be null. May be get rid of the null check to find bugs
            reader.close();
        }
    }

    @Override
    public void stop() {
        for (ReadFromSocket reader : m_readers.values()) {
            reader.close();
        }

        m_readers.clear();
    }

    @Override
    public String getName() {
        return "PullSocketImporterVer2";
    }

    public static class PullerException extends ImportBaseException {

        private static final long serialVersionUID = 1319947681332896365L;
        private boolean logged = false;

        public PullerException() {
        }

        public PullerException(String format, Object... args) {
            super(format, args);
        }

        public PullerException(String format, Throwable cause, Object... args) {
            super(format, cause, args);
        }

        public PullerException(Throwable cause) {
            super(cause);
        }

        private PullerException markLogged() {
            logged = true;
            return this;
        }

        private final static boolean isLogged(Throwable t) {
            if (t != null && t instanceof PullerException) {
                return ((PullerException)t).logged;
            }
            return false;
        }
    }

    private class ReadFromSocket implements Closeable {

        final private AtomicBoolean m_eos = new AtomicBoolean(false);
        final private URI m_uri;
        private volatile Optional<Thread> m_thread = Optional.absent();
        final private String m_procedure;

        private ReadFromSocket(final URI socketUri, final String procedure) {
            m_uri = checkNotNull(socketUri, "socket URI is null");
            if (procedure == null || procedure.trim().isEmpty()) {
                throw new IllegalArgumentException("procedure name is null or empty or blank");
            }
            m_procedure = procedure;
        }

        private void susceptibleRun() {
            if (m_eos.get()) return;

            getLogger().info("Starting socket puller for " + m_uri);

            m_thread = Optional.of(Thread.currentThread());
            Optional<BufferedReader> reader = null;
            while (!m_eos.get()) {
                try {
                    reader = attemptBufferedReader();
                    if (!reader.isPresent()) {
                        sleep(2_000);
                        continue;
                    }

                    BufferedReader br = reader.get();
                    String csv = null;
                    while ((csv=br.readLine()) != null) {
                        CSVInvocation invocation = new CSVInvocation(m_procedure, csv);
                        if (!callProcedure(invocation)) {
                            if (getLogger().isDebugEnabled()) {
                                getLogger().debug("Failed to process Invocation possibly bad data: " + csv);
                            }
                        }
                    }
                    if (csv == null) {
                        getLogger().warn(m_uri + " peer terminated stream");
                    }
                } catch (EOFException e) {
                    rateLimitedLog(Level.WARN, e, m_uri + " peer terminated stream");
                } catch (InterruptedException e) {
                    if (m_eos.get()) return;
                    rateLimitedLog(Level.ERROR, e, "Socket puller %s was interrupted", m_uri);
                } catch (InterruptedIOException e) {
                    if (m_eos.get()) return;
                    rateLimitedLog(Level.ERROR, e, "Socket puller for %s was interrupted", m_uri);
                } catch (IOException e) {
                    rateLimitedLog(Level.ERROR, e, "Read fault for %s", m_uri);
                }
            }

            getLogger().info("Stopping socket puller for " + m_uri);
        }

        private Optional<BufferedReader> attemptBufferedReader() {
            Optional<BufferedReader> attempt = Optional.absent();
            if (m_eos.get()) return attempt;

            InetSocketAddress sa = new InetSocketAddress(m_uri.getHost(),m_uri.getPort());

            @SuppressWarnings("resource")
            Socket skt = new Socket();

            try {
                skt.connect(sa, 4_000);
                InputStreamReader isr = new InputStreamReader(skt.getInputStream(), StandardCharsets.UTF_8);
                attempt = Optional.of(new BufferedReader(isr, 8192));
            } catch (InterruptedIOException e) {
                if (m_eos.get()) return attempt;
                rateLimitedLog(Level.WARN, e, "Unable to connect to " + m_uri);
            } catch (IOException e) {
                rateLimitedLog(Level.WARN, e, "Unable to connect to " + m_uri);
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

        @Override
        public void close() {
            if (m_eos.compareAndSet(false, true) && m_thread.isPresent()) {
                m_thread.get().interrupt();
            }
        }
    }
}
