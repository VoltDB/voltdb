/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportDataSource;
import org.voltdb.export.ExportGeneration;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.ExportDecoderBase.RestartBlockException;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

public class GuestProcessor implements ExportDataProcessor {

    public static final String EXPORT_TO_TYPE = "__EXPORT_TO_TYPE__";

    private ExportGeneration m_generation;
    private boolean m_shutdown = false;
    private VoltLogger m_logger;

    private Map<String, ExportClientBase> m_clientsByTarget = new HashMap<>();
    private Map<String, String> m_targetsByTableName = new HashMap<>();

    private final List<Pair<ExportDecoderBase, AdvertisedDataSource>> m_decoders = new ArrayList<Pair<ExportDecoderBase, AdvertisedDataSource>>();

    private final Semaphore m_pollBarrier = new Semaphore(0);


    // Instantiated at ExportManager
    public GuestProcessor() {
    }

    @Override
    public void addLogger(VoltLogger logger) {
        m_logger = logger;
    }

    @Override
    public void setProcessorConfig(Map<String, Pair<Properties, Set<String>>> config) {
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
            } catch(Throwable t) {
                Throwables.propagate(t);
            }
        }
    }

    /**
     * Pass processor specific processor configuration properties for checking
     * @param config an instance of {@linkplain Properties}
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
            Throwables.propagate(t);
        }
    }

    @Override
    public void setExportGeneration(ExportGeneration generation) {
        // This is called when a persisted generation is ready to be drained. So, activate the data sources
        for (Map<String, ExportDataSource> sourcesMap : generation.getDataSourceByPartition().values()) {
            for (ExportDataSource source : sourcesMap.values()) {
                source.activate();
            }
        }
        m_generation = generation;
    }

    @Override
    public ExportGeneration getExportGeneration() {
        return m_generation;
    }

    private class ExportRunner implements Runnable {
        final ExportClientBase m_client;
        final ExportDataSource m_source;
        final ArrayList<VoltType> m_types = new ArrayList<VoltType>();
        final boolean m_startup;

        ExportRunner(boolean startup, String groupName, ExportDataSource source) {
            m_startup = startup;
            m_client = Preconditions.checkNotNull(m_clientsByTarget.get(groupName), "null client");
            m_source = source;

            for (int type : m_source.m_columnTypes) {
                m_types.add(VoltType.get((byte)type));
            }
            m_source.setRunEveryWhere(m_client.isRunEverywhere());
        }

        @Override
        public void run() {
            runDataSource();
        }

        public boolean isRunEvewhere() {
            return m_client.isRunEverywhere();
        }

        private void runDataSource() {
            synchronized (GuestProcessor.this) {
                m_logger.info(
                        "Beginning export processing for export source " + m_source.getTableName()
                        + " partition " + m_source.getPartitionId() + " generation " + m_source.getGeneration());
                final AdvertisedDataSource ads =
                        new AdvertisedDataSource(
                                m_source.getPartitionId(),
                                m_source.getSignature(),
                                m_source.getTableName(),
                                m_source.getPartitionColumnName(),
                                System.currentTimeMillis(),
                                m_source.getGeneration(),
                                m_source.m_columnNames,
                                m_types,
                                new ArrayList<Integer>(m_source.m_columnLengths),
                                m_source.getExportFormat());

                if (m_startup) {
                    // in this case we cannot poll until the initial truncation is
                    // complete
                    final Runnable waitForBarrierRelease = new Runnable() {
                        @Override
                        public void run() {
                            try {

                                if (m_pollBarrier.tryAcquire(2, TimeUnit.MILLISECONDS)) {
                                    synchronized (GuestProcessor.this) {
                                        //Dont construct if we are shutdown
                                        if (m_shutdown) return;
                                        final ExportDecoderBase edb = m_client.constructExportDecoder(ads);

                                        m_decoders.add(Pair.of(edb, ads));
                                        final ListenableFuture<BBContainer> fut = m_source.poll();
                                        constructListener(m_source, fut, edb, ads, isRunEvewhere());
                                    }
                                } else {
                                    resubmitSelf();
                                }
                            } catch (InterruptedException ignoreIt) {
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
                                }
                            }
                        }
                    };
                    if (m_shutdown) return;
                    if (!m_source.getExecutorService().isShutdown()) try {
                        m_source.getExecutorService().submit(waitForBarrierRelease);
                    } catch (RejectedExecutionException whenExportDataSourceIsClosed) {
                        // it is truncated so we no longer need to wait
                    }
                } else {
                    synchronized (GuestProcessor.this) {
                        //Dont construct if we are shutdown
                        if (m_shutdown) return;
                        final ExportDecoderBase edb = m_client.constructExportDecoder(ads);

                        m_decoders.add(Pair.of(edb, ads));
                        final ListenableFuture<BBContainer> fut = m_source.poll();
                        constructListener(m_source, fut, edb, ads, isRunEvewhere());
                    }
                }
            }
        }

    }

    @Override
    public void startPolling() {
        Preconditions.checkState(!m_clientsByTarget.isEmpty(), "processor was not configured with setProcessorConfig()");
        final int permits = m_pollBarrier.availablePermits();

        if (permits != 0) return;

        int sourcesCount = 0;
        for (Map<String, ExportDataSource> sources : m_generation.getDataSourceByPartition().values()) {
            for (final ExportDataSource source : sources.values()) {
                String tableName = source.getTableName().toLowerCase();
                String groupName = m_targetsByTableName.get(tableName);

                // skip export tables that don't have an enabled connector
                if (groupName == null) {
                    continue;
                }
                sourcesCount++;
            }
        }
        if (sourcesCount == 0) return;
        m_logger.info("Export Processor for " + m_generation + " releasing " + sourcesCount + " permits.");
        m_pollBarrier.release(sourcesCount);
    }


    @Override
    public void readyForData(final boolean startup) {
        Preconditions.checkState(!m_clientsByTarget.isEmpty(), "processor was not configured with setProcessorConfig()");
        for (Map<String, ExportDataSource> sources : m_generation.getDataSourceByPartition().values()) {

            for (final ExportDataSource source : sources.values()) {
                synchronized(GuestProcessor.this) {
                    if (m_shutdown) {
                        m_logger.info("Skipping mastership notification for export.");
                        return;
                    }
                    String tableName = source.getTableName().toLowerCase();
                    final String groupName = m_targetsByTableName.get(tableName);

                    // skip export tables that don't have an enabled connector
                    if (groupName == null) {
                        m_logger.warn("Table " + tableName + " has no enabled export connector.");
                        continue;
                    }
                    ExportRunner runner = new ExportRunner(startup, groupName, source);
                    source.setOnMastership(runner);
                }
            }
        }
    }

    private void constructListener(
            final ExportDataSource source,
            final ListenableFuture<BBContainer> fut,
            final ExportDecoderBase edb,
            final AdvertisedDataSource ads, final boolean runEveryWhere) {
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
                try {
                    BBContainer cont = fut.get();
                    if (cont == null) {
                        return;
                    }
                    try {
                        //Position to restart at on error
                        final int startPosition = cont.b().position();

                        //Track the amount of backoff to use next time, will be updated on repeated failure
                        int backoffQuantity = 10 + (int)(10 * ThreadLocalRandom.current().nextDouble());

                        /*
                         * If there is an error processing the block the decoder thinks is recoverable
                         * start the block from the beginning and repeat until it is processed.
                         * Also allow the decoder to request exponential backoff
                         */
                        while (true) {
                            try {
                                final ByteBuffer buf = cont.b();
                                buf.position(startPosition);
                                edb.onBlockStart();
                                buf.order(ByteOrder.LITTLE_ENDIAN);
                                while (buf.hasRemaining()) {
                                    int length = buf.getInt();
                                    byte[] rowdata = new byte[length];
                                    buf.get(rowdata, 0, length);
                                    edb.processRow(length, rowdata);
                                }
                                edb.onBlockCompletion();
                                break;
                            } catch (RestartBlockException e) {
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
                    } finally {
                        cont.discard();
                    }
                } catch (Exception e) {
                    m_logger.error("Error processing export block", e);
                }
                constructListener(source, source.poll(), edb, ads, runEveryWhere);
            }
        }, edb.getExecutor());
    }

    @Override
    public void queueWork(Runnable r) {
        new Thread(r, "GuestProcessor gen " + m_generation + " shutdown task").start();
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
        for (ExportClientBase client : m_clientsByTarget.values()) {
            client.shutdown();
        }
        m_generation = null;
    }
}
