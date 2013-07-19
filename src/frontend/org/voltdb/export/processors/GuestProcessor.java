/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;

import jsr166y.ThreadLocalRandom;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.InputHandler;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportDataSource;
import org.voltdb.export.ExportGeneration;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase2;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.ExportDecoderBase.RestartBlockException;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

public class GuestProcessor implements ExportDataProcessor {

    public static final String EXPORT_TO_TYPE = "__EXPORT_TO_TYPE__";

    private ExportGeneration m_generation;
    private ExportClientBase2 m_client;
    private VoltLogger m_logger;

    private final List<Pair<ExportDecoderBase, AdvertisedDataSource>> m_decoders =
            new ArrayList<Pair<ExportDecoderBase, AdvertisedDataSource>>();


    // Instantiated at ExportManager
    public GuestProcessor() {
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
            m_client = (ExportClientBase2)clientClass.newInstance();
            m_client.configure(config);
        } catch( Throwable t) {
            Throwables.propagate(t);
        }
    }

    @Override
    public void setExportGeneration(ExportGeneration generation) {
        m_generation = generation;
    }

    @Override
    public void readyForData() {
        Preconditions.checkState(m_client != null, "processor was not configured with setProcessorConfig()");

        for (HashMap<String, ExportDataSource> sources : m_generation.m_dataSourcesByPartition.values()) {

            for (final ExportDataSource source : sources.values()) {
                source.setOnMastership(new Runnable() {

                    @Override
                    public void run() {
                        m_logger.info(
                                "Beginning export processing for export source " + source.getTableName()
                                + " partition " + source.getPartitionId() + " generation " + source.getGeneration());
                        ArrayList<VoltType> types = new ArrayList<VoltType>();
                        for (int type : source.m_columnTypes) {
                            types.add(VoltType.get((byte)type));
                        }
                        AdvertisedDataSource ads =
                                new AdvertisedDataSource(
                                        source.getPartitionId(),
                                        source.getSignature(),
                                        source.getTableName(),
                                        System.currentTimeMillis(),
                                        source.getGeneration(),
                                        source.m_columnNames,
                                        types,
                                        new ArrayList<Integer>(source.m_columnLengths));
                        ExportDecoderBase edb = m_client.constructExportDecoder(ads);
                        m_decoders.add(Pair.of(edb, ads));
                        final ListenableFuture<BBContainer> fut = source.poll();
                        constructListener( source, fut, edb, ads);

                    }
                });
            }
        }
    }

    private void constructListener(
            final ExportDataSource source,
            final ListenableFuture<BBContainer> fut,
            final ExportDecoderBase edb,
            final AdvertisedDataSource ads) {
        /*
         * The listener runs in the thread specified by the EDB.
         * It can be same thread executor for things like export to file where the destination
         * is always available and there is no reason to do additional buffering.
         *
         * For JDBC we want a dedicated thread to block on calls to the remote database
         * so the data source thread can overflow data to disk.
         */
        fut.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    BBContainer cont = fut.get();
                    if (cont == null) {
                        synchronized (ads) {
                            edb.sourceNoLongerAdvertised(ads);
                        }
                        return;
                    }
                    try {
                        //Position to restart at on error
                        final int startPosition = cont.b.position();

                        //Track the amount of backoff to use next time, will be updated on repeated failure
                        int backoffQuantity = 10 + (int)(10 * ThreadLocalRandom.current().nextDouble());

                        /*
                         * If there is an error processing the block the decoder thinks is recoverable
                         * start the block from the beginning and repeat until it is processed.
                         * Also allow the decoder to request exponential backoff
                         */
                        while (true) {
                            cont.b.position(startPosition);
                            try {
                                edb.onBlockStart();
                                cont.b.order(ByteOrder.LITTLE_ENDIAN);
                                while (cont.b.hasRemaining()) {
                                    int length = cont.b.getInt();
                                    byte[] rowdata = new byte[length];
                                    cont.b.get(rowdata, 0, length);
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
                constructListener(source, source.poll(), edb, ads);
            }
        }, edb.getExecutor());
    }

    @Override
    public void queueWork(Runnable r) {
        new Thread(r, "GuestProcessor gen " + m_generation + " shutdown task").start();
    }

    @Override
    public InputHandler createInputHandler(String service, boolean isAdminPort) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void shutdown() {
        for (final Pair<ExportDecoderBase, AdvertisedDataSource> p : m_decoders) {
            try {
                p.getFirst().getExecutor().submit(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (p.getSecond()) {
                            p.getFirst().sourceNoLongerAdvertised(p.getSecond());
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                //It's okay, means it was already shut down
            }
        }
        m_client.shutdown();
    }

    @Override
    public boolean isConnectorForService(String service) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void bootClient() {
        // TODO Auto-generated method stub

    }

}
