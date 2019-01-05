/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportClientLogger;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.decode.CSVWriterDecoder;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;
import org.voltdb.exportclient.ExportRow;

public class LoopbackExportClient extends ExportClientBase {

    private static final ExportClientLogger LOG = new ExportClientLogger();

    private String m_procedure;
    private String m_failureLog;
    private File m_rejectedDH;

    public LoopbackExportClient() {
    }

    public interface Config extends Accessible {
        final static String PROCEDURE = "procedure";
        final static String FAILURE_LOG_FILE = "failurelogfile";

        @Key(PROCEDURE)
        public String getProcedureName();

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
        private final Function<Integer, Boolean> m_shouldContinue;

        private BitSet m_failed = new BitSet(0);
        private BitSet m_resubmit = new BitSet(0);

        private BlockContext m_ctx;
        private boolean m_restarted = false;
        private boolean m_wrote = false;

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
                .skipInternalFields(true)
            ;
            m_csvWriterDecoder = builder.build();
            m_es = CoreUtils.getListeningSingleThreadExecutor(
                    "Loopback Export decoder for partition " + source.partitionId, CoreUtils.MEDIUM_STACK_SIZE);
            m_user = getVoltDB().getCatalogContext().authSystem.getImporterUser();
            m_invoker = getVoltDB().getClientInterface().getInternalConnectionHandler();
            m_shouldContinue = (x) -> !m_es.isShutdown();
        }

        @Override
        public void onBlockCompletion(ExportRow row) throws RestartBlockException {
            if (m_ctx.invokes > 0) {
                try {
                    m_ctx.m_done.acquire(m_ctx.invokes);
                } catch (InterruptedException e) {
                    throw new LoopbackExportException("failed to wait for block callback", e);
                }
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
            LoopbackCallback cb = m_ctx.createCallback(bix);
            if (m_invoker.callProcedure(m_user, false,
                    BatchTimeoutOverrideType.NO_TIMEOUT,
                    cb, false, m_shouldContinue, m_procedure,
                    Arrays.copyOfRange(rd.values, 6, rd.values.length))) {
                ++m_ctx.invokes;
            } else {
                LOG.error("failed to Invoke procedure: " + m_procedure);
                m_ctx.m_done.release();
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
            m_es.shutdown();
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                LOG.error("Interrupted while awaiting executor shutdown", e);
            }
        }

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        class LoopbackCallback implements ProcedureCallback {

            private final Semaphore m_done;
            private final ConcurrentLinkedDeque<Reject> m_rq;
            private final int m_bix;

            LoopbackCallback(Semaphore done,
                    ConcurrentLinkedDeque<Reject> rq,
                    int bix) {
                this.m_done = done;
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
                    m_done.release();
                }
            }
        }

        class BlockContext {
            final Semaphore m_done = new Semaphore(0);
            final ConcurrentLinkedDeque<Reject> m_rq = new ConcurrentLinkedDeque<>();
            int recs = 0;
            int invokes = 0;

            LoopbackCallback createCallback(int bix) {
                return new LoopbackCallback(m_done, m_rq, bix);
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
