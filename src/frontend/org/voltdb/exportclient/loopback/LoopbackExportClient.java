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

package org.voltdb.exportclient.loopback;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.ConfigFactory;
import org.voltcore.utils.CoreUtils;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.InternalConnectionHandler;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportManagerInterface;
import org.voltdb.export.ExportManagerInterface.ExportMode;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportClientLogger;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.ExportRow;
import org.voltdb.exportclient.decode.CSVWriterDecoder;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

public class LoopbackExportClient extends ExportClientBase {

    private static final ExportClientLogger LOG = new ExportClientLogger();

    private String m_procedure;
    private String m_failureLog;
    private File m_rejectedDH;
    private boolean m_skipInternals = true;

    public LoopbackExportClient() {
    }

    public interface Config extends Accessible {
        final static String PROCEDURE = "procedure";
        final static String FAILURE_LOG_FILE = "failurelogfile";
        final static String SKIP_INTERNALS = "skipinternals";

        @Key(PROCEDURE)
        public String getProcedureName();

        @Key(SKIP_INTERNALS)
        public String getSkipInternals();

        @Key(FAILURE_LOG_FILE)
        public String getFailureLogFile();

        public static Config create(Map<?, ?>... imports) {
            return ConfigFactory.create(Config.class, imports);
        }
    }

    VoltDBInterface getVoltDB() {
        return VoltDB.instance();
    }

    @Override
    public void configure(Properties props) throws Exception {
        Config config = Config.create(props);
        checkArgument(isNotBlank(config.getProcedureName()), "procedure name is not defined");
        m_procedure = config.getProcedureName();

        String skipVal = config.getSkipInternals();
        if (skipVal != null && !skipVal.isEmpty()) {
            m_skipInternals = Boolean.parseBoolean(skipVal);
        }

        m_failureLog = config.getFailureLogFile();
        if (m_failureLog != null && m_failureLog.trim().length() > 0) {
            File rejectedDH = new File(m_failureLog);
            if (!rejectedDH.isAbsolute()) {
                File voltdbDH = getVoltDB()
                        .getCatalogContext()
                        .getNodeSettings()
                        .getVoltDBRoot();
                rejectedDH = new File(voltdbDH, m_failureLog);
            }
            if (!rejectedDH.exists() && !rejectedDH.mkdirs()) {
                LOG.error("failed to create directory " + rejectedDH);
                throw new LoopbackExportException("failed to create directory %s", rejectedDH);
            }
            if (!rejectedDH.isDirectory()
                || !rejectedDH.canRead()
                || !rejectedDH.canWrite()
                || !rejectedDH.canExecute()
             ) {
                throw new LoopbackExportException("failed to gained write access to %s", rejectedDH);
             }
            m_rejectedDH = rejectedDH;
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(
            AdvertisedDataSource source) {
        return new LoopbackExportDecoder(source);
    }

    class LoopbackExportDecoder extends ExportDecoderBase {

        private final CSVWriterDecoder m_csvWriterDecoder;
        private final ListeningExecutorService m_es;
        private final AuthUser m_user;
        private final InternalConnectionHandler m_invoker;
        private final Predicate<Integer> m_shouldContinue;

        private BitSet m_failed = new BitSet(0);
        private BitSet m_resubmit = new BitSet(0);

        private BlockContext m_ctx;
        private boolean m_restarted = false;
        private boolean m_wrote = false;
        private volatile boolean m_isShutDown;
        private volatile boolean m_isPaused;

        private final Supplier<CSVWriter> m_rejs;

        public LoopbackExportDecoder(AdvertisedDataSource source) {
            super(source);
            if (m_rejectedDH != null) {
                m_rejs = Suppliers.memoize(new Supplier<CSVWriter>() {
                    @Override
                    public CSVWriter get() {
                        String fileFN = String.format(
                                "rejected-%s-%d.tsv", source.tableName, source.partitionId
                                );
                        File rejectedFH = new File(m_rejectedDH, fileFN);
                        LOG.warn("writing failed invocations parameters to " + rejectedFH);
                        try {
                            OutputStreamWriter osw = new OutputStreamWriter(
                                    new FileOutputStream(rejectedFH, false), "UTF-8");
                            CSVWriter wrtr =  CSVWriter.getStrictTSVWriter(new BufferedWriter(osw, 4096 * 4));
                            m_wrote = true;
                            return wrtr;
                        } catch (UnsupportedEncodingException | FileNotFoundException e) {
                            LOG.error("failed to create TSV writer for " + rejectedFH, e);
                            throw new LoopbackExportException("failed to create TSV writer for %s", e, rejectedFH);
                        }
                    }
                });
            } else {
                m_rejs = null;
            }
            final String tmpl = "yyyy-MM-dd'T'HH:mm:ss.SSS";
            CSVWriterDecoder.Builder builder = new CSVWriterDecoder.Builder();
            builder
                .dateFormatter(tmpl)
                .skipInternalFields(m_skipInternals)
            ;
            m_csvWriterDecoder = builder.build();
            if (VoltDB.getExportManager().getExportMode() == ExportMode.BASIC) {
                m_es = CoreUtils.getListeningSingleThreadExecutor(
                        "Loopback Export decoder for partition " + source.partitionId, CoreUtils.MEDIUM_STACK_SIZE);
            } else {
                m_es = null;
            }
            m_user = getVoltDB().getCatalogContext().authSystem.getImporterUser();
            m_invoker = getVoltDB().getClientInterface().getInternalConnectionHandler();
            m_shouldContinue = (x) -> !isShutDown() && !isPaused();
        }

        public boolean isShutDown() {
            return m_isShutDown;
        }

        @Override
        public synchronized void pause() {
            m_isPaused = true;
            notifyAll();
        }

        @Override
        public void resume() {
            m_isPaused = false;
        }

        private boolean isPaused() {
            return m_isPaused;
        }

        @Override
        public void onBlockCompletion(ExportRow row) throws RestartBlockException {
            synchronized (this) {
                if (m_ctx.m_outstandingTransactions.get() > 0 && !m_isShutDown && !m_isPaused) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new LoopbackExportException("failed to wait for block callback", e);
                    }
                }
            }
            if (m_isShutDown || m_isPaused) { // if shut down or paused, always re-process the block
                return;
            }
            m_restarted = !m_ctx.m_rq.isEmpty();

            if (m_restarted) {
                m_failed = new BitSet(m_ctx.recs);
                m_resubmit = new BitSet(m_ctx.recs);

                Reject rj = m_ctx.m_rq.poll();
                while (rj != null) {
                    m_failed.set(rj.bix);
                    if (rj.status == ClientResponse.RESPONSE_UNKNOWN) {
                        m_resubmit.set(rj.bix);
                    }
                    rj = m_ctx.m_rq.poll();
                }
                throw new RestartBlockException(true);
            } else {
                m_failed = new BitSet(m_ctx.recs);
                m_resubmit = new BitSet(m_ctx.recs);
            }
        }

        @Override
        public void onBlockStart(ExportRow row) throws RestartBlockException {
            m_ctx = new BlockContext();
        }

        @Override
        public boolean processRow(ExportRow rd)
                throws RestartBlockException {
            final int bix = m_ctx.recs++;
            if (m_restarted && !m_failed.get(bix)) {
                return true;
            }
            if (m_restarted && !m_resubmit.get(bix) && m_rejs != null) {
                try {
                    m_csvWriterDecoder.decode(rd.generation, rd.tableName, rd.types, rd.names, m_rejs.get(), rd.values);
                } catch (IOException e) {
                    LOG.error("failed to write failed invocation to rejected file", e);
                    return false;
                }
                return true;
            }
            int firstFieldOffset = m_skipInternals ? INTERNAL_FIELD_COUNT : 0;
            LoopbackCallback cb = m_ctx.createCallback(bix);
            if (m_invoker.callProcedure(m_user, false,
                    BatchTimeoutOverrideType.NO_TIMEOUT,
                    cb, false, m_shouldContinue, m_procedure,
                    Arrays.copyOfRange(rd.values, firstFieldOffset, rd.values.length))) {
                m_ctx.m_outstandingTransactions.getAndIncrement();
            } else {
                LOG.error("failed to Invoke procedure: " + m_procedure);
            }

            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            if (m_wrote && m_rejs != null) {
                try {
                    m_rejs.get().close();
                } catch (IOException ignoreIt) {}
            }
            synchronized(this) {
                m_isShutDown = true;
                notifyAll();
            }
            if (m_es != null) {
                m_es.shutdown();
                try {
                    m_es.awaitTermination(365, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while awaiting executor shutdown", e);
                }
            }
        }

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        class LoopbackCallback implements ProcedureCallback {

            private final AtomicInteger m_outstandingTransactions;
            private final ConcurrentLinkedDeque<Reject> m_rq;
            private final int m_bix;

            LoopbackCallback(AtomicInteger oustandingTransactions,
                    ConcurrentLinkedDeque<Reject> rq,
                    int bix) {
                this.m_outstandingTransactions = oustandingTransactions;
                this.m_rq = rq;
                this.m_bix = bix;
            }

            @Override
            public void clientCallback(ClientResponse cr) throws Exception {
                try {
                    if (cr.getStatus() == ClientResponse.RESPONSE_UNKNOWN) {
                        m_rq.offer(new Reject(cr.getStatus(), m_bix));
                        return;
                    }
                    if (cr.getStatus() != ClientResponse.SUCCESS) {
                        if (m_rejs != null) {
                            m_rq.offer(new Reject(cr.getStatus(), m_bix));
                        }
                        LOG.error("Loopback Invocation failed: %s", cr.getStatusString());
                    }
                } finally {
                    if (m_outstandingTransactions.decrementAndGet() == 0) {
                        synchronized(LoopbackExportDecoder.this) {
                            LoopbackExportDecoder.this.notifyAll();
                        }
                    }
                }
            }
        }

        class BlockContext {
            final ConcurrentLinkedDeque<Reject> m_rq = new ConcurrentLinkedDeque<>();
            int recs = 0;
            final AtomicInteger m_outstandingTransactions = new AtomicInteger();

            LoopbackCallback createCallback(int bix) {
                return new LoopbackCallback(m_outstandingTransactions, m_rq, bix);
            }
        }

    }

    final static class Reject {
        final byte status;
        final int bix;
        Reject(byte status, int bix) {
            this.status = status;
            this.bix = bix;
        }
    }
}
