/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.export.processors;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

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
        Preconditions.checkNotNull(exportClientClass, "export to type is undefined");

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
                                        types);
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
                        edb.onBlockStart();
                        try {
                            cont.b.order(ByteOrder.LITTLE_ENDIAN);
                            while (cont.b.hasRemaining()) {
                                int length = cont.b.getInt();
                                byte[] rowdata = new byte[length];
                                cont.b.get(rowdata, 0, length);
                                edb.processRow(length, rowdata);
                            }
                        } finally {
                            edb.onBlockCompletion();
                        }
                    } finally {
                        cont.discard();
                    }
                } catch (Exception e) {
                    m_logger.error("Error processing export block", e);
                }
                constructListener(source, source.poll(), edb, ads);
            }
        }, MoreExecutors.sameThreadExecutor());
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
        for (Pair<ExportDecoderBase, AdvertisedDataSource> p : m_decoders) {
            synchronized (p.getSecond()) {
                p.getFirst().sourceNoLongerAdvertised(p.getSecond());
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
