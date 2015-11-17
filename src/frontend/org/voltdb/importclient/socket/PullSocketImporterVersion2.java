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
import org.voltdb.importer.CSVInvocation;

import com.google_voltpatches.common.base.Optional;

/**
 * Importer implementation for pull socket importer. At runtime, there will
 * one instance of this per host and socket combination.
 */
public class PullSocketImporterVersion2 extends AbstractImporter {

    private PullSocketImporterConfig m_config;
    private final AtomicBoolean m_eos = new AtomicBoolean(false);
    private volatile Optional<Thread> m_thread = Optional.absent();

    PullSocketImporterVersion2(PullSocketImporterConfig config)
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
        return "PullSocketImporterVer2";
    }

    private void susceptibleRun() {
        if (m_eos.get()) return;

        getLogger().info("Starting socket puller for " + m_config.getResourceID());

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
                    CSVInvocation invocation = new CSVInvocation(m_config.getProcedure(), csv);
                    if (!callProcedure(invocation)) {
                        if (getLogger().isDebugEnabled()) {
                            getLogger().debug("Failed to process Invocation possibly bad data: " + csv);
                        }
                    }
                }
                if (csv == null) {
                    getLogger().warn(m_config.getResourceID() + " peer terminated stream");
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
                rateLimitedLog(Level.ERROR, e, "Read fault for %s", m_config.getResourceID());
            }
        }

        getLogger().info("Stopping socket puller for " + m_config.getResourceID());
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
            rateLimitedLog(Level.WARN, e, "Unable to connect to " + m_config.getResourceID());
        } catch (IOException e) {
            rateLimitedLog(Level.WARN, e, "Unable to connect to " + m_config.getResourceID());
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
    }
}
