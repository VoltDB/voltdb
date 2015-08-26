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

import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static com.google_voltpatches.common.base.Predicates.not;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.importer.CSVInvocation;
import org.voltdb.importer.ImportHandlerProxy;
import org.voltdb.importer.ImporterChannelAssignment;

import com.google_voltpatches.common.base.Optional;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;

public class PullSocketImporter extends ImportHandlerProxy implements BundleActivator {

    final static private Pattern HOST_RE = Pattern.compile(
                "(?<host>[\\w._-]+):(?<port>\\d+)(?:-(?<tail>\\d+)){0,1}"
            );
    final static Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    final private AtomicBoolean m_done = new AtomicBoolean(false);

    private Map<URI,String> m_resources = ImmutableMap.of();
    private volatile Map<URI,ReadFromSocket> m_readers = ImmutableMap.of();
    private ListeningExecutorService m_es;
    private final Semaphore m_completionGuard = new Semaphore(0);

    public PullSocketImporter() {
    }

    private PullerException loggedException(Throwable t, String fmt, Object...args) {
        if (!PullerException.isLogged(t)) {
            error(t, fmt, args);
        }
        return new PullerException(fmt, t, args).markLogged();
    }

    @Override
    public void configure(Properties p) {
        String hosts = p.getProperty("addresses", "").trim();
        if (hosts.isEmpty()) {
            throw loggedException(null, "required hosts property is undefined");
        }
        String procedure = p.getProperty("procedure", "").trim();
        if (procedure.isEmpty()) {
            throw loggedException(null, "required procedure property is undefined");
        }

        ImmutableMap.Builder<URI,String> sbldr = ImmutableMap.builder();
        sbldr.putAll(m_resources);
        for (String host: COMMA_SPLITTER.split(hosts)) {
            sbldr.putAll(checkHost(host,procedure));
        }
        try {
            m_resources = sbldr.build();
        } catch (IllegalArgumentException e) {
            throw loggedException(
                    e, "one or more host addresses are being assigned to "
                     + "more than one store procedure"
                  );
        }

        ThreadPoolExecutor tpe = new ThreadPoolExecutor(
                2 + m_resources.size(),
                2 + m_resources.size(),
                5_000,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                getThreadFactory(getName(), getName()+"Reader", ImportHandlerProxy.MEDIUM_STACK_SIZE)
                );
        tpe.allowCoreThreadTimeOut(true);

        m_es = MoreExecutors.listeningDecorator(tpe);
    }

    @Override
    public void readyForData() {
        info("importer " + getName() + " is ready for assignments for sockets " + m_resources);
        try {
            m_completionGuard.acquire();
            m_completionGuard.release();
        } catch (InterruptedException e) {
            throw loggedException(e, "Interrupted while waiting for %s to complete", getName());
        }
    }

    @Override
    public void stop() {
        if (!m_done.compareAndSet(false, true)) return;
        for (ReadFromSocket reader: m_readers.values()) {
            reader.close();
        }
        m_es.shutdown();
        try {
            m_es.awaitTermination(365, TimeUnit.DAYS);
        } catch (InterruptedException ex) {
            throw loggedException(ex, "failed to terminate executor for %s", getName());
        } finally {
            m_completionGuard.release();
        }
    }

    @Override
    public String getName() {
        return "PullSocketImporter";
    }

    @Override
    public Set<URI> getAllResponsibleResources() {
        return m_resources.keySet();
    }

    @Override
    public void onChange(ImporterChannelAssignment ment) {
        if (m_done.get()) return;

        ImmutableMap.Builder<URI,ReadFromSocket> mbldr = ImmutableMap.builder();
        mbldr.putAll(Maps.filterKeys(m_readers, not(uriIn(ment.getRemoved()))));

        for (URI removed: ment.getRemoved()) {
            ReadFromSocket reader = m_readers.get(removed);
            if (reader != null) reader.close();
        }

        for (URI added: ment.getAdded()) {
            ReadFromSocket reader = new ReadFromSocket(added, m_resources.get(added));
            m_es.submit(reader);
            mbldr.put(added, reader);
        }
        m_readers = mbldr.build();
    }

    private final static Predicate<URI> uriIn(final Set<URI> uris) {
        return new Predicate<URI>() {
            @Override
            final public boolean apply(URI uri) {
                return uris.contains(uri);
            }
        };
    }

    @Override
    public void setBackPressure(boolean hasBackPressure) {
        for (ReadFromSocket reader: m_readers.values()) {
            reader.setBackPressure(hasBackPressure);
        }
    }

    @Override
    public void start(BundleContext ctx) throws Exception {
        ctx.registerService(this.getClass().getName(), this, null);
    }

    @Override
    public void stop(BundleContext ctx) throws Exception {
    }

    @Override
    public boolean isRunEveryWhere() {
        return false;
    }

    private final Map<URI,String> checkHost(String hspec, String procedure) {
        Matcher mtc = HOST_RE.matcher(hspec);
        if (!mtc.matches()) {
            throw loggedException(null, "host spec %s is malformed", hspec);
        }
        int port = Integer.parseInt(mtc.group("port"));
        int tail = port;
        if (mtc.group("tail") != null) {
            tail = Integer.parseInt(mtc.group("tail"));
        }
        if (port>tail) {
            throw loggedException(null, "invalid port range in host spec %s", hspec);
        }
        ImmutableMap.Builder<URI,String> mbldr = ImmutableMap.builder();
        for (int p = port; p <= tail; ++p) {
            InetAddress a;
            try {
                a = InetAddress.getByName(mtc.group("host"));
            } catch (UnknownHostException e) {
                throw loggedException(e, "failed to resolve host %s", mtc.group("host"));
            }
            InetSocketAddress sa = new InetSocketAddress(a, p);
            mbldr.put(URI.create("tcp://" + sa.getHostString() + ":" + sa.getPort() + "/"),procedure);
        }
        return mbldr.build();
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

    abstract class PullerRunnable implements Runnable {
        @Override
        public void run() {
            if (!m_done.get()) try {
                susceptibleRun();
            } catch (Exception e) {
                throw loggedException(e, "Fault occured while executing runnable");
            }
        }

        public abstract void susceptibleRun() throws Exception;
    }

    class ReadFromSocket extends PullerRunnable implements Closeable {

        final private AtomicBoolean m_eos = new AtomicBoolean(false);
        final private URI m_uri;
        private volatile Optional<Thread> m_thread = Optional.absent();
        final private AtomicBoolean m_onBackPressure = new AtomicBoolean(false);
        final private String m_procedure;

        private ReadFromSocket(final URI socketUri, final String procedure) {
            m_uri = checkNotNull(socketUri, "socket URI is null");
            if (procedure == null || procedure.trim().isEmpty()) {
                throw new IllegalArgumentException("procedure name is null or empty or blank");
            }
            m_procedure = procedure;
        }

        private void setBackPressure(final boolean onBackPressure) {
            m_onBackPressure.compareAndSet(!onBackPressure, onBackPressure);
        }

        @Override
        public void susceptibleRun() throws Exception {
            if (m_eos.get()) return;

            info("starting socket puller for " + m_uri);

            m_thread = Optional.of(Thread.currentThread());
            Optional<BufferedReader> reader = null;
            while (!m_eos.get()) {
                reader = attempBufferedReader();
                if (!reader.isPresent()) {
                    sleep(2_000);
                    continue;
                }
                try (BufferedReader br = reader.get()) {
                    String csv = null;
                    READER: while ((csv=br.readLine()) != null) {
                        while (m_onBackPressure.get()) {
                            if (sleep(10)) break READER;
                        }
                        CSVInvocation invocation = new CSVInvocation(m_procedure, csv);
                        if (!callProcedure(invocation)) {
                            if (isDebugEnabled()) {
                                debug("Failed to process Invocation possibly bad data: " + csv);
                            }
                        }
                    }
                    if (csv == null) {
                        warn(m_uri + " peer terminated stream");
                    }
                } catch (EOFException e) {
                    warn(e, m_uri + " peer terminated stream");
                } catch (InterruptedIOException e) {
                    if (m_eos.get()) return;
                    error(e, "socket puller for %s was interrupted", m_uri);
                } catch (IOException e) {
                    error(e, "read fault for %s", m_uri);
                }
            }

            info("stopping socket puller for " + m_uri);
        }

        private Optional<BufferedReader> attempBufferedReader() {
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
                warn(e, "unable to connect to " + m_uri);
            } catch (IOException e) {
                warn(e, "unable to connect to " + m_uri);
            }
            return attempt;
        }

        private boolean sleep(int ms) {
            if (m_eos.get()) return true;
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                if (m_eos.get()) return true;
                throw loggedException(e, "sleep interrupted on %s puller", m_uri);
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
