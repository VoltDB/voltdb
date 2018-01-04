/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.*;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.ExportDecoderBase.RestartBlockException;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import org.voltdb.VoltType;
import org.voltdb.export.StandaloneExportDataProcessor;
import org.voltdb.export.StandaloneExportGeneration;
import org.voltdb.exportclient.ExportRow;

public class StandaloneGuestProcessor implements StandaloneExportDataProcessor {

    public static final String EXPORT_TO_TYPE = "__EXPORT_TO_TYPE__";

    private StandaloneExportGeneration m_generation;
    private ExportClientBase m_client;
    private boolean m_shutdown = false;
    private VoltLogger m_logger;
    private final long m_startTS = System.currentTimeMillis();

    private final List<Pair<ExportDecoderBase, AdvertisedDataSource>> m_decoders =
            new ArrayList<Pair<ExportDecoderBase, AdvertisedDataSource>>();


    // Instantiated at ExportManager
    public StandaloneGuestProcessor() {
    }

    @Override
    public void addLogger(VoltLogger logger) {
        m_logger = logger;
    }

    @Override
    public void setProcessorConfig( Properties config) {
        String exportClientClass = config.getProperty(EXPORT_TO_TYPE);
        Preconditions.checkNotNull(exportClientClass, "export to type is undefined or custom export plugin class missing.");

        try {
            final Class<?> clientClass = Class.forName(exportClientClass);
            m_client = (ExportClientBase)clientClass.newInstance();
            m_client.configure(config);
        } catch( Throwable t) {
            Throwables.propagate(t);
        }
    }

    @Override
    public void setExportGeneration(StandaloneExportGeneration generation) {
        m_generation = generation;
    }

    @Override
    public void readyForData() {
        Preconditions.checkState(m_client != null, "processor was not configured with setProcessorConfig()");
        for (Map<String, ExportDataSource> sources : m_generation.getDataSourceByPartition().values()) {

            for (final ExportDataSource source : sources.values()) {
                source.setOnMastership(new Runnable() {

                    @Override
                    public void run() {
                        synchronized (StandaloneGuestProcessor.this) {
                            if (m_shutdown) {
                                m_logger.info("Skipping mastership notification for export.");
                                return;
                            }
                            m_logger.info(
                                    "Beginning export processing for export source " + source.getTableName()
                                    + " partition " + source.getPartitionId());
                            ArrayList<VoltType> types = new ArrayList<VoltType>();
                            for (int type : source.m_columnTypes) {
                                types.add(VoltType.get((byte)type));
                            }
                            final AdvertisedDataSource ads =
                                    new AdvertisedDataSource(
                                            source.getPartitionId(),
                                            source.getSignature(),
                                            source.getTableName(),
                                            source.getPartitionColumnName(),
                                            System.currentTimeMillis(),
                                            source.getGeneration(),
                                            source.m_columnNames,
                                            types,
                                            source.m_columnLengths,
                                            source.getExportFormat());
                            ExportDecoderBase edb = m_client.constructExportDecoder(ads);
                            m_decoders.add(Pair.of(edb, ads));
                            final ListenableFuture<BBContainer> fut = source.poll();
                            constructListener(source, fut, edb);
                        }
                    }
                }, false);
            }
        }
    }

    private void constructListener(
            final ExportDataSource source,
            final ListenableFuture<BBContainer> fut,
            final ExportDecoderBase edb) {
        /*
         * The listener runs in the thread specified by the EDB.
         * It can be same thread executor for things like export to file where the destination
         * is always available and there is no reason to do additional buffering.
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
                                buf.order(ByteOrder.LITTLE_ENDIAN);
                                long generation = -1L;
                                ExportRow row = null;
                                ExportRow refRow = null;
                                while (buf.hasRemaining()) {
                                    int length = buf.getInt();
                                    byte[] rowdata = new byte[length];
                                    buf.get(rowdata, 0, length);
                                    try {
                                        row = ExportRow.decodeRow(refRow, source.getPartitionId(), m_startTS, rowdata);
                                        refRow = row;
                                    } catch (IOException ioe) {
                                        //TODO: LOG
                                        cont.discard();
                                        continue;
                                    }
                                    if (generation == -1L) {
                                        edb.onBlockStart(row);
                                    }
                                    edb.processRow(row);
                                    if (generation != -1L && row.generation != generation) {
                                        //Do block completion if generation dont match.
                                        edb.onBlockCompletion(row);
                                        edb.onBlockStart(row);
                                    }
                                    generation = row.generation;
                                }
                                if (row != null) {
                                    edb.onBlockCompletion(row);
                                }
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
                constructListener(source, source.poll(), edb);
            }
        }, edb.getExecutor());
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            m_shutdown = true;
            for (final Pair<ExportDecoderBase, AdvertisedDataSource> p : m_decoders) {
                try {
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
        m_client.shutdown();
    }
}
