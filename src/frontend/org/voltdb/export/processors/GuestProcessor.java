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

package org.voltdb.export.processors;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.export.AckingContainer;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportDataSource;
import org.voltdb.export.ExportDataSource.ReentrantPollException;
import org.voltdb.export.ExportGeneration;
import org.voltdb.export.StreamBlockQueue;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.ExportDecoderBase.RestartBlockException;
import org.voltdb.exportclient.ExportRow;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

public class GuestProcessor implements ExportDataProcessor {

    public static final String EXPORT_TO_TYPE = "__EXPORT_TO_TYPE__";

    // FIXME - replace with fixed list of ExportDataSource. That is all we need from m_generation.
    private ExportGeneration m_generation;
    private volatile boolean m_shutdown = false;
    private VoltLogger m_logger;

    private Map<String, ExportClientBase> m_clientsByTarget = new HashMap<>();
    private Map<String, String> m_targetsByTableName = new HashMap<>();

    private final List<Pair<ExportDecoderBase, AdvertisedDataSource>> m_decoders = new ArrayList<Pair<ExportDecoderBase, AdvertisedDataSource>>();

    private final long m_startTS = System.currentTimeMillis();
    private volatile boolean m_startPolling = false;
    private long m_genId;

    // Instantiated at ExportManager
    public GuestProcessor() {
    }

    @Override
    public void addLogger(VoltLogger logger) {
        m_logger = logger;
    }

    @Override
    public void setProcessorConfig(Map<String, Pair<Properties, Set<String>>> config) {
        Map<String, Properties> configProcessed = new HashMap<>();
        for (Entry<String, Pair<Properties, Set<String>>> e : config.entrySet()) {

            String targetName = e.getKey();
            Properties properties = e.getValue().getFirst();
            Set<String> tableNames = e.getValue().getSecond();

            for (String tableName : tableNames) {
                tableName = tableName.toLowerCase();
                assert(!m_targetsByTableName.containsKey(tableName));
                m_targetsByTableName.put(tableName, targetName);
            }

            String exportClientClass = properties.getProperty(EXPORT_TO_TYPE);
            Preconditions.checkNotNull(exportClientClass, "export to type is undefined or custom export plugin class missing.");

            try {
                final Class<?> clientClass = Class.forName(exportClientClass);
                ExportClientBase client = (ExportClientBase) clientClass.newInstance();
                client.configure(properties);
                m_clientsByTarget.put(targetName, client);
                client.setTargetName(targetName);
            } catch(Throwable t) {
                throw new RuntimeException(t);
            }
            configProcessed.put(targetName, new Properties(properties));
        }
    }

    @Override
    public ExportClientBase getExportClient(final String tableName) {
        ExportClientBase client = null;
        synchronized (GuestProcessor.this) {
            String groupName = m_targetsByTableName.get(tableName.toLowerCase());
            // skip export tables that don't have an enabled connector and are still in catalog
            if (groupName == null) {
                m_logger.warn("Table " + tableName + " has no enabled export connector.");
                return null;
            }
            //If we have a new client for the target use it or see if we have an older client which is set before
            //If no client is found dont create the runner and log
            client = m_clientsByTarget.get(groupName);
            if (client == null) {
                m_logger.warn("Table " + tableName + " has no configured connector.");
                return null;
            }
        }
        return client;
    }

    @Override
    public void readyForData() {
        for (Map<String, ExportDataSource> sources : m_generation.getDataSourceByPartition().values()) {

            for (final ExportDataSource source : sources.values()) {
                synchronized(GuestProcessor.this) {
                    if (m_shutdown) {
                        if (m_logger.isDebugEnabled()) {
                            m_logger.info("Skipping mastership notification for export because processor has been shut down.");
                        }
                        return;
                    }
                    String tableName = source.getTableName().toLowerCase();
                    String groupName = m_targetsByTableName.get(tableName);
                    if (source.getClient() == null) {
                        m_logger.warn("Table " + tableName + " has no configured connector.");
                        continue;
                    }
                    //If we configured a new client we already mapped it if not old client will be placed for cleanup at shutdown.
                    m_clientsByTarget.putIfAbsent(groupName, source.getClient());
                    ExportRunner runner = new ExportRunner(m_targetsByTableName.get(tableName), source.getClient(), source);
                    // DataSource should start polling only after command log replay on a recover
                    source.setReadyForPolling(m_startPolling);
                    source.setOnMastership(runner);
                }
            }
        }
        //This will log any targets that are not there but draining datasources will keep them in list.
        m_logger.info("Active Targets are: " + m_clientsByTarget.keySet().toString());
    }

    /**
     * Pass processor specific processor configuration properties for checking
     */
    @Override
    public void checkProcessorConfig(Properties properties) {
        String exportClientClass = properties.getProperty(EXPORT_TO_TYPE);
        Preconditions.checkNotNull(exportClientClass, "export to type is undefined or custom export plugin class missing.");

        try {
            final Class<?> clientClass = Class.forName(exportClientClass);
            ExportClientBase client = (ExportClientBase) clientClass.newInstance();
            client.configure(properties);
        } catch(Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public void setExportGeneration(ExportGeneration generation) {
        assert generation != null;
        m_generation = generation;
    }

    private class ExportRunner implements Runnable {
        final ExportClientBase m_client;
        final ExportDataSource m_source;
        final ArrayList<VoltType> m_types = new ArrayList<VoltType>();

        ExportRunner(String groupName, ExportClientBase client, ExportDataSource source) {
            m_client = Preconditions.checkNotNull(m_clientsByTarget.get(groupName), "null client");
            m_source = source;

            for (int type : m_source.m_columnTypes) {
                m_types.add(VoltType.get((byte)type));
            }
        }

        @Override
        public void run() {
            runDataSource();
        }

        private void detectDecoder(ExportClientBase client, ExportDecoderBase edb) {
            try {
                Method m = edb.getClass().getDeclaredMethod("processRow", int.class, byte[].class);
                if (m != null) {
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug("Found Legacy ExportClient: " + client.getClass().getCanonicalName());
                    }
                    edb.setLegacy(true);
                }
            } catch (Exception ex) {
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Found Modern export client: " + client.getClass().getCanonicalName());
                }
            }
        }

        //Utility method to build and add listener.
        private void buildListener(AdvertisedDataSource ads) {
            //Dont construct if we are shutdown
            if (m_shutdown) return;
            final ExportDecoderBase edb = m_client.constructExportDecoder(ads);
            detectDecoder(m_client, edb);
            Pair<ExportDecoderBase, AdvertisedDataSource> pair = Pair.of(edb, ads);
            m_decoders.add(pair);
            final ListenableFuture<AckingContainer> fut = m_source.poll(true);
            addBlockListener(m_source, fut, edb);
            m_source.forwardAckToOtherReplicas();
        }

        private void runDataSource() {
            synchronized (GuestProcessor.this) {

                final AdvertisedDataSource ads =
                        new AdvertisedDataSource(
                                m_source.getPartitionId(),
                                m_source.getTableName(),
                                m_source.getPartitionColumnName(),
                                System.currentTimeMillis(),
                                m_source.getGeneration(),
                                m_source.m_columnNames,
                                m_types,
                                m_source.m_columnLengths,
                                m_source.getExportFormat());

                // in this case we cannot poll until the initial truncation is complete
                final Runnable waitForBarrierRelease = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (m_startPolling) { // Wait for command log replay to be done.
                                if (m_logger.isDebugEnabled()) {
                                    m_logger.debug("Beginning export processing for export source " + m_source.getTableName()
                                    + " partition " + m_source.getPartitionId());
                                }
                                m_source.setReadyForPolling(true); // Tell source it is OK to start polling now.
                                synchronized (GuestProcessor.this) {
                                    if (m_shutdown) return;
                                    buildListener(ads);
                                }
                            } else {
                                Thread.sleep(5);
                                resubmitSelf();
                            }
                        } catch(InterruptedException e) {
                            resubmitSelf();
                        } catch (Exception e) {
                            VoltDB.crashLocalVoltDB("Failed to initiate export binary deque poll", true, e);
                        }
                    }

                    private void resubmitSelf() {
                        synchronized (GuestProcessor.this) {
                            if (m_shutdown) return;
                            if (!m_source.getExecutorService().isShutdown()) try {
                                m_source.getExecutorService().submit(this);
                            } catch (RejectedExecutionException whenExportDataSourceIsClosed) {
                                // it is truncated so we no longer need to wait

                                // TODO: When truncation is finished, generation roll-over does not happen.
                                // Log a message to and revisit the error handling for this case
                                m_logger.warn("Got rejected execution exception while waiting for truncation to finish");
                            }
                        }
                    }
                };
                if (m_shutdown) return;
                if (!m_source.getExecutorService().isShutdown()) try {
                    m_source.getExecutorService().submit(waitForBarrierRelease);
                } catch (RejectedExecutionException whenExportDataSourceIsClosed) {
                    // it is truncated so we no longer need to wait

                    // TODO: When truncation is finished, generation roll-over does not happen.
                    // Log a message to and revisit the error handling for this case
                    m_logger.warn("Got rejected execution exception while waiting for truncation to finish");
                }
            }
        }
    }

    @Override
    public void startPolling() {
        Preconditions.checkState(!m_clientsByTarget.isEmpty(), "processor was not configured with setProcessorConfig()");
        m_startPolling = true;
    }


    private void addBlockListener(
            final ExportDataSource source,
            final ListenableFuture<AckingContainer> fut,
            final ExportDecoderBase edb) {
        /*
         * The listener runs in the thread specified by the EDB.
         *
         * For JDBC we want a dedicated thread to block on calls to the remote database
         * so the data source thread can overflow data to disk.
         */

        if (fut == null) {
            return;
        }
        fut.addListener(new Runnable() {
            @Override
            public void run() {
                AckingContainer cont = null;
                try {
                    cont = fut.get();
                    if (cont == null) {
                        return;
                    }
                    // If export master accepts promotion in case of mastership migration or leader re-election,
                    // we need an extra poll to get the schema of current buffer to setup the decoder
                    try {
                        //Position to restart at on error
                        final int startPosition = cont.b().position();

                        //Track the amount of backoff to use next time, will be updated on repeated failure
                        int backoffQuantity = 10 + (int)(10 * ThreadLocalRandom.current().nextDouble());

                        // Extract the sp handle of the last committed row in the block, if present
                        long committedSpHandle = 0L;

                        /*
                         * If there is an error processing the block the decoder thinks is recoverable
                         * start the block from the beginning and repeat until it is processed.
                         * Also allow the decoder to request exponential backoff
                         */
                        while (!m_shutdown) {
                            try {
                                ByteBuffer sbuf = null;
                                int schemaSize = 0;
                                final ByteBuffer buf = cont.b();
                                buf.position(startPosition);
                                buf.order(ByteOrder.LITTLE_ENDIAN);
                                ByteBuffer schemaBuf = cont.schema();
                                if (schemaBuf != null) {
                                    schemaBuf.position(0);
                                    schemaBuf.order(ByteOrder.LITTLE_ENDIAN);
                                    byte version = schemaBuf.get();
                                    assert(version == StreamBlockQueue.EXPORT_BUFFER_VERSION);
                                    // update the global generation id of guest processor
                                    m_genId = schemaBuf.getLong();
                                    schemaSize = schemaBuf.getInt();
                                    ExportRow previousRow = edb.getExportRowSchema();
                                    // update the decoder if current generation is different than previous row
                                    if (previousRow == null || previousRow.generation != m_genId) {
                                        byte[] schemadata = new byte[schemaSize];
                                        schemaBuf.get(schemadata, 0, schemaSize);
                                        sbuf = ByteBuffer.wrap(schemadata);
                                        sbuf.order(ByteOrder.LITTLE_ENDIAN);
                                        edb.setExportRowSchema(
                                                ExportRow.decodeBufferSchema(
                                                        sbuf, schemaSize,
                                                        source.getPartitionId(), m_genId));
                                    }
                                }
                                ExportRow row = null;
                                boolean firstRowOfBlock = true;
                                while (buf.hasRemaining() && !m_shutdown) {
                                    int length = buf.getInt();
                                    byte[] rowdata = new byte[length];
                                    buf.get(rowdata, 0, length);
                                    if (edb.isLegacy()) {
                                        cont.updateStartTime(System.currentTimeMillis());
                                        if (firstRowOfBlock) {
                                            edb.onBlockStart(row);
                                            firstRowOfBlock = false;
                                        }
                                        edb.processRow(length, rowdata);
                                    } else {
                                        //New style connector.
                                        try {
                                            cont.updateStartTime(System.currentTimeMillis());
                                            if (edb.getExportRowSchema() == null && sbuf != null) {
                                                edb.setExportRowSchema(
                                                        ExportRow.decodeBufferSchema(
                                                                sbuf, schemaSize,
                                                                source.getPartitionId(), m_genId));
                                            }
                                            row = ExportRow.decodeRow(edb.getExportRowSchema(), source.getPartitionId(), m_startTS, rowdata);
                                            edb.setExportRowSchema(row);
                                        } catch (IOException ioe) {
                                            m_logger.warn("Failed decoding row for partition " + source.getPartitionId() + ". " + ioe.getMessage());
                                            cont.discard();
                                            cont = null;
                                            break;
                                        }
                                        if (firstRowOfBlock) {
                                            edb.onBlockStart(row);
                                            firstRowOfBlock = false;
                                        }
                                        edb.processRow(row);
                                        if (committedSpHandle == 0) {
                                            committedSpHandle = extractCommittedSpHandle(row,
                                                    cont.getCommittedSeqNo());
                                        }
                                    }
                                }
                                if (edb.isLegacy()) {
                                    edb.onBlockCompletion();
                                }
                                if (row != null) {
                                    edb.onBlockCompletion(row);
                                }
                                // Make sure to discard after onBlockCompletion so that if completion
                                // wants to retry we don't lose block.
                                // Please note that if export manager is shutting down it's possible
                                // that container isn't fully consumed. Discard the buffer prematurely
                                // would cause missing rows in export stream.
                                if (!m_shutdown && cont != null) {
                                    if (committedSpHandle != 0) {
                                        // We came across the last committed row in the buffer,
                                        // record its sp handle
                                        cont.setCommittedSpHandle(committedSpHandle);
                                    }
                                    cont.discard();
                                    cont = null;
                                }
                                break;
                            } catch (RestartBlockException e) {
                                if (m_shutdown) {
                                    if (m_logger.isDebugEnabled()) {
                                        // log message for debugging.
                                        m_logger.debug("Shutdown detected, ignore restart exception. " + e);
                                    }
                                    break;
                                }
                                if (e.requestBackoff) {
                                    Thread.sleep(backoffQuantity);
                                    //Cap backoff to 8 seconds, then double modulo some randomness
                                    if (backoffQuantity < 8000) {
                                        backoffQuantity += (backoffQuantity * .5);
                                        backoffQuantity +=
                                                (backoffQuantity * .5 * ThreadLocalRandom.current().nextDouble());
                                    }
                                }
                            }
                        }
                        //Don't discard the block also set the start position to the beginning.
                        if (m_shutdown && cont != null) {
                            if (m_logger.isDebugEnabled()) {
                                // log message for debugging.
                                m_logger.debug("Shutdown detected, queue block to pending");
                            }
                            cont.b().position(startPosition);
                            source.setPendingContainer(cont);
                            cont = null;
                        }
                    } finally {
                        if (cont != null) {
                            cont.discard();
                            cont = null;
                        }
                    }
                } catch (Exception e) {
                    if (e.getCause() instanceof ReentrantPollException) {
                        m_logger.info("Stopping processing export blocks: " + e.getMessage());
                        return;

                    } else {
                        m_logger.error("Error processing export block, continuing processing: ", e);
                    }
                } finally {
                    if (cont != null) {
                        cont.discard();
                        cont = null;
                    }
                }
                if (!m_shutdown) {
                    addBlockListener(source, source.poll(false), edb);
                }
            }
        }, edb.getExecutor());
    }

    /**
     * If the row is the last committed row, return the SpHandle, otherwise return 0
     *
     * @param row the export row
     * @param committedSeqNo the sequence number of the last committed row
     * @return
     */
    private long extractCommittedSpHandle(ExportRow row, long committedSeqNo) {
        long ret = 0;
        if (committedSeqNo == ExportDataSource.NULL_COMMITTED_SEQNO) {
            return ret;
        }

        // Get the rows's sequence number (3rd column)
        long seqNo = (long) row.values[2];
        if (seqNo != committedSeqNo) {
            return ret;
        }

        // Get the row's sp handle (1rst column)
        ret = (long) row.values[0];
        return ret;
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            m_shutdown = true;
            for (final Pair<ExportDecoderBase, AdvertisedDataSource> p : m_decoders) {
                try {
                    if (p == null) {
                        m_logger.warn("ExportDecoderBase pair was unexpectedly null");
                        continue;
                    }
                    ExportDecoderBase edb = p.getFirst();
                    if (edb == null) {
                        m_logger.warn("ExportDecoderBase was unexpectedly null");
                        continue;
                    }
                    if (p.getSecond() == null) {
                        m_logger.warn("AdvertisedDataSource was unexpectedly null");
                        continue;
                    }
                    synchronized(p.getSecond()) {
                        edb.sourceNoLongerAdvertised(p.getSecond());
                    }
                } catch (RejectedExecutionException e) {
                    //It's okay, means it was already shut down
                }
            }
        }
        m_decoders.clear();
        for (ExportClientBase client : m_clientsByTarget.values()) {
            client.shutdown();
        }
        m_clientsByTarget.clear();
        m_targetsByTableName.clear();
        m_generation = null;
    }

}
