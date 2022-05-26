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

package org.voltdb.importer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ImporterServerAdapterImpl;
import org.voltdb.VoltDB;
import org.voltdb.importer.formatter.FormatterBuilder;
import org.voltdb.utils.CatalogUtil.ImportConfiguration;

import com.google_voltpatches.common.base.Throwables;

public class ImportProcessor implements ImportDataProcessor {

    private static final VoltLogger m_logger = new VoltLogger("IMPORT");
    private final Map<String, ImporterWrapper> m_importers = new HashMap<String, ImporterWrapper>();
    private final ChannelDistributer m_distributer;
    private final ExecutorService m_es = CoreUtils.getSingleThreadExecutor("ImportProcessor");
    private final ImporterServerAdapter m_importServerAdapter;
    private final String m_clusterTag;


    public ImportProcessor(int myHostId,
            ChannelDistributer distributer,
            ImporterStatsCollector statsCollector,
            String clusterTag)
    {
        m_distributer = distributer;
        m_importServerAdapter = new ImporterServerAdapterImpl(statsCollector);
        m_clusterTag = clusterTag;
    }

    //This abstracts OSGi based and class based importers.
    public class ImporterWrapper {
        private AbstractImporterFactory m_importerFactory;
        private ImporterLifeCycleManager m_importerTypeMgr;

        public ImporterWrapper(AbstractImporterFactory importerFactory, int priority) {
            m_importerFactory = importerFactory;
            m_importerFactory.setImportServerAdapter(m_importServerAdapter);
            m_importerTypeMgr = new ImporterLifeCycleManager(
                    priority, m_importerFactory, m_distributer, m_clusterTag);
        }

        public String getImporterType() {
            return m_importerFactory.getTypeName();
        }

        public void configure(Properties props, FormatterBuilder formatterBuilder) {
            m_importerTypeMgr.configure(props, formatterBuilder);
        }

        public int getConfigsCount() {
            return m_importerTypeMgr.getConfigsCount();
        }

        public void stop() {
            try {
                //Handler can be null for initial period if shutdown come quickly.
                if (m_importerTypeMgr != null) {
                    m_importerTypeMgr.stop();
                }
            } catch (Exception ex) {
                m_logger.error("Failed to stop the import bundles.", ex);
            }
        }
    }

    @Override
    public int getPartitionsCount() {
        int count = 0;
        for (ImporterWrapper wapper : m_importers.values()) {
            if (wapper != null) {
                count += wapper.getConfigsCount();
            }
        }
        return count;
    }

    @Override
    public synchronized void readyForData() {
        m_es.submit(new Runnable() {
            @Override
            public void run() {
                for (ImporterWrapper bw : m_importers.values()) {
                    try {
                        bw.m_importerTypeMgr.readyForData();
                    } catch (Exception ex) {
                        //Should never fail. crash.
                        VoltDB.crashLocalVoltDB("Import failed to set Handler", true, ex);
                        m_logger.error("Failed to start the import handler: " + bw.m_importerFactory.getTypeName(), ex);
                    }
                }
            }
        });
    }

    @Override
    public synchronized void shutdown() {
        //Task that shutdowns all the bundles we wait for it to finish.
        Future<?> task = m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    //Stop all the bundle wrappers.
                    for (ImporterWrapper bw : m_importers.values()) {
                        try {
                            bw.stop();
                        } catch (Exception ex) {
                            m_logger.error("Failed to stop the import handler: " + bw.m_importerFactory.getTypeName(), ex);
                        }
                    }
                    m_importers.clear();
                } catch (Exception ex) {
                    m_logger.error("Failed to stop the import bundles.", ex);
                    Throwables.propagate(ex);
                }
            }
        });

        //And wait for it.
        try {
            task.get();
        } catch (InterruptedException | ExecutionException ex) {
            m_logger.error("Failed to stop import processor.", ex);
        }
        try {
            m_es.shutdown();
            m_es.awaitTermination(365, TimeUnit.DAYS);
        } catch (InterruptedException ex) {
            m_logger.error("Failed to stop import processor executor.", ex);
        }
    }

    private void addProcessorConfig(ImportConfiguration config, Map<String, AbstractImporterFactory> importerModules) {
        Properties properties = config.getmoduleProperties();

        String module = properties.getProperty(ImportDataProcessor.IMPORT_MODULE);
        assert(module != null);
        String attrs[] = module.split("\\|");
        String bundleJar = attrs[1];

        FormatterBuilder formatterBuilder = config.getFormatterBuilder();
        try {

            ImporterWrapper wrapper = m_importers.get(bundleJar);
            if (wrapper == null) {
                AbstractImporterFactory importFactory = importerModules.get(bundleJar);
                wrapper = new ImporterWrapper(importFactory, config.getPriority());
                String name = wrapper.getImporterType();
                if (name == null || name.trim().isEmpty()) {
                    throw new RuntimeException("Importer must implement and return a valid unique name.");
                }
                m_importers.put(bundleJar, wrapper);
            }
            wrapper.configure(properties, formatterBuilder);
        } catch(Throwable t) {
            m_logger.error("Failed to configure import handler for " + bundleJar, t);
            Throwables.propagate(t);
        }
    }

    @Override
    public void setProcessorConfig(Map<String, ImportConfiguration> config,
            final Map<String, AbstractImporterFactory> importerModules) {
        for (String configName : config.keySet()) {
            ImportConfiguration importConfig = config.get(configName);
            addProcessorConfig(importConfig, importerModules);
        }
    }

}
