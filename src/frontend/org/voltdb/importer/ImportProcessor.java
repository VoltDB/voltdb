/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.launch.Framework;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CatalogContext;
import org.voltdb.ImporterServerAdapterImpl;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.CatalogUtil.ImportConfiguration;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;

public class ImportProcessor implements ImportDataProcessor {

    private static final VoltLogger m_logger = new VoltLogger("IMPORT");
    private final Map<String, BundleWrapper> m_bundles = new HashMap<String, BundleWrapper>();
    private final Map<String, BundleWrapper> m_bundlesByName = new HashMap<String, BundleWrapper>();
    private final Framework m_framework;
    private final ChannelDistributer m_distributer;
    private final ExecutorService m_es = CoreUtils.getSingleThreadExecutor("ImportProcessor");
    private final ImporterServerAdapter m_importServerAdapter;

    public ImportProcessor(int myHostId, ChannelDistributer distributer, Framework framework, ImporterStatsCollector statsCollector)
            throws BundleException {
        m_framework = framework;
        m_distributer = distributer;
        m_importServerAdapter = new ImporterServerAdapterImpl(statsCollector);
    }

    //This abstracts OSGi based and class based importers.
    public class BundleWrapper {
        private final Bundle m_bundle;
        private AbstractImporterFactory m_importerFactory;
        private ImporterLifeCycleManager m_importerTypeMgr;

        public BundleWrapper(Object o, Bundle bundle) {
            m_bundle = bundle;
            m_importerFactory = (AbstractImporterFactory) o;
            m_importerFactory.setImportServerAdapter(m_importServerAdapter);
            m_importerTypeMgr = new ImporterLifeCycleManager(m_importerFactory);
        }

        public String getImporterType() {
            return m_importerFactory.getTypeName();
        }

        public void configure(ImportConfiguration config) {
            m_importerTypeMgr.configure(config);
        }

        public void stop() {
            try {
                //Handler can be null for initial period if shutdown come quickly.
                if (m_importerFactory != null) {
                    m_importerTypeMgr.stop(m_distributer);
                }
                if (m_bundle != null) {
                    m_bundle.stop();
                }
            } catch (Exception ex) {
                m_logger.error("Failed to stop the import bundles.", ex);
            }
        }
    }

    public void addProcessorConfig(ImportConfiguration config) {
        String module = config.getmoduleProperties().getProperty(ImportDataProcessor.IMPORT_MODULE);
        String attrs[] = module.split("\\|");
        String bundleJar = attrs[1];
        String moduleType = attrs[0];

        try {
            BundleWrapper wrapper = m_bundles.get(bundleJar);
            if (wrapper == null) {
                if (moduleType.equalsIgnoreCase("osgi")) {

                    Bundle bundle = m_framework.getBundleContext().installBundle(bundleJar);
                    bundle.start();
                    ServiceReference refs[] = bundle.getRegisteredServices();
                    //Must have one service only.
                    ServiceReference reference = refs[0];
                    if (reference == null) {
                        m_logger.error("Failed to initialize importer from: " + bundleJar);
                        bundle.stop();
                        return;
                    }
                    Object o = bundle.getBundleContext().getService(reference);
                    wrapper = new BundleWrapper(o, bundle);
                } else {
                    //Class based importer.
                    Class reference = this.getClass().getClassLoader().loadClass(bundleJar);
                    if (reference == null) {
                        m_logger.error("Failed to initialize importer from: " + bundleJar);
                        return;
                    }

                     wrapper = new BundleWrapper(reference.newInstance(), null);
                }
                String name = wrapper.getImporterType();
                if (name == null || name.trim().length() == 0) {
                    throw new RuntimeException("Importer must implement and return a valid unique name.");
                }
                Preconditions.checkState(!m_bundlesByName.containsKey(name), "Importer must implement and return a valid unique name: " + name);
                wrapper.configure(config);
                m_bundlesByName.put(name, wrapper);
                m_bundles.put(bundleJar, wrapper);
            } else {
                wrapper.configure(config);
            }
        } catch(Throwable t) {
            m_logger.error("Failed to configure import handler for " + bundleJar, t);
            Throwables.propagate(t);
        }
    }

    @Override
    public synchronized void readyForData(final CatalogContext catContext, final HostMessenger messenger) {

        m_es.submit(new Runnable() {
            @Override
            public void run() {
                for (BundleWrapper bw : m_bundles.values()) {
                    try {
                        bw.m_importerTypeMgr.readyForData(m_distributer);
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
                    for (BundleWrapper bw : m_bundles.values()) {
                        try {
                            bw.stop();
                        } catch (Exception ex) {
                            m_logger.error("Failed to stop the import handler: " + bw.m_importerFactory.getTypeName(), ex);
                        }
                    }
                    m_bundles.clear();
                } catch (Exception ex) {
                    m_logger.error("Failed to stop the import bundles.", ex);
                    Throwables.propagate(ex);
                }
            }
        });
        //And wait for it.
        try {
            task.get();
        } catch (Exception ex) {
            m_logger.error("Failed to stop import processor.", ex);
            ex.printStackTrace();
        }
        try {
            m_es.shutdown();
            m_es.awaitTermination(365, TimeUnit.DAYS);
        } catch (Exception ex) {
            m_logger.error("Failed to stop import processor executor.", ex);
            ex.printStackTrace();
        }
    }

    @Override
    public void setProcessorConfig(CatalogContext catalogContext, Map<String, ImportConfiguration> config) {
        List<String> configuredImporters = new ArrayList<String>();
        for (String cname : config.keySet()) {
            ImportConfiguration iConfig = config.get(cname);
            Properties properties = iConfig.getmoduleProperties();

            String importBundleJar = properties.getProperty(IMPORT_MODULE);
            Preconditions.checkNotNull(importBundleJar, "Import source is undefined or custom export plugin class missing.");
            String procedure = properties.getProperty(IMPORT_PROCEDURE);
            //TODO: If processors is a list dont start till all procedures exists.
            Procedure catProc = catalogContext.procedures.get(procedure);
            if (catProc == null) {
                catProc = catalogContext.m_defaultProcs.checkForDefaultProcedure(procedure);
            }

            if (catProc == null) {
                m_logger.info("Importer " + cname + " Procedure " + procedure + " is missing will disable this importer until the procedure becomes available.");
                continue;
            }
            configuredImporters.add(cname);
            addProcessorConfig(iConfig);
        }
        m_logger.info("Import Processor is configured. Configured Importers: " + configuredImporters);
    }

}
