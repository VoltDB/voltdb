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

package org.voltdb.importer;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.launch.Framework;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.ImportHandler;
import org.voltdb.VoltDB;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;

public class ImportProcessor implements ImportDataProcessor {

    private static final VoltLogger m_logger = new VoltLogger("IMPORT");
    private final Map<String, BundleWrapper> m_bundles = new HashMap<String, BundleWrapper>();
    private final Map<String, BundleWrapper> m_bundlesByName = new HashMap<String, BundleWrapper>();
    private final Framework m_framework;
    private final ChannelDistributer m_distributer;

    public ImportProcessor(int myHostId, ChannelDistributer distributer, Framework framework) throws BundleException {
        m_framework = framework;
        m_distributer = distributer;
    }

    //This abstracts OSGi based and class based importers.
    public class BundleWrapper {
        public final Bundle m_bundle;
        public final Properties m_properties;
        public final ImportHandlerProxy m_handlerProxy;
        private ImportHandler m_handler;
        private ChannelDistributer m_channelDistributer;

        public BundleWrapper(ImportHandlerProxy handler, Properties properties, Bundle bundle) {
            m_bundle = bundle;
            m_handlerProxy = handler;
            m_properties = properties;
        }

        public void setChannelDistributer(ChannelDistributer distributer) {
            m_channelDistributer = distributer;
        }

        public void setHandler(ImportHandler handler) throws Exception {
            Preconditions.checkState((m_handler == null), "ImportHandler can only be set once.");
            m_handler = handler;
            m_handlerProxy.setHandler(handler);
        }

        public ImportHandler getHandler() {
            return m_handler;
        }

        public void stop() {
            try {
                m_handler.stop();
                if (m_bundle != null) {
                    m_bundle.stop();
                }
                if (m_channelDistributer != null) {
                    m_channelDistributer.registerChannels(m_handlerProxy.getName(), new HashSet<URI>());
                }
            } catch (Exception ex) {
                m_logger.error("Failed to stop the import bundles.", ex);
            }
        }
    }

    public void addProcessorConfig(Properties properties) {
        String module = properties.getProperty(ImportDataProcessor.IMPORT_MODULE);
        String moduleAttrs[] = module.split("\\|");
        String bundleJar = moduleAttrs[1];
        String moduleType = moduleAttrs[0];

        Preconditions.checkState(!m_bundles.containsKey(bundleJar), "Import to source is already defined.");
        try {
            BundleWrapper wrapper = null;
            ImportHandlerProxy importHandlerProxy = null;
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
                importHandlerProxy = (ImportHandlerProxy )o;
                wrapper = new BundleWrapper(importHandlerProxy, properties, bundle);
            } else {
                //Class based importer.
                Class reference = this.getClass().getClassLoader().loadClass(bundleJar);
                if (reference == null) {
                    m_logger.error("Failed to initialize importer from: " + bundleJar);
                    return;
                }

                importHandlerProxy = (ImportHandlerProxy )reference.newInstance();
                 wrapper = new BundleWrapper(importHandlerProxy, properties, null);
            }
            importHandlerProxy.configure(properties);
            String name = importHandlerProxy.getName();
            if (name == null || name.trim().length() == 0) {
                throw new RuntimeException("Importer must implement and return a valid unique name.");
            }
            Preconditions.checkState(!m_bundlesByName.containsKey(name), "Importer must implement and return a valid unique name: " + name);
            m_bundlesByName.put(name, wrapper);
            m_bundles.put(bundleJar, wrapper);
        } catch(Throwable t) {
            m_logger.error("Failed to configure import handler for " + bundleJar, t);
            Throwables.propagate(t);
        }
    }

    @Override
    public synchronized void readyForData(CatalogContext catContext, HostMessenger messenger) {

        for (BundleWrapper bw : m_bundles.values()) {
            try {
                ImportHandler importHandler = new ImportHandler(bw.m_handlerProxy, catContext);
                //Set the internal handler
                bw.setHandler(importHandler);
                if (!bw.m_handlerProxy.isRunEveryWhere()) {
                    //This is a distributed and fault tolerant importer so get the resources.
                    Set<URI> allResources = bw.m_handlerProxy.getAllResponsibleResources();
                    m_logger.info("All Available Resources for " + bw.m_handlerProxy.getName() + " Are: " + allResources);

                    bw.setChannelDistributer(m_distributer);
                    //Register callback
                    m_distributer.registerCallback(bw.m_handlerProxy.getName(), bw.m_handlerProxy);
                    m_distributer.registerChannels(bw.m_handlerProxy.getName(), allResources);
                }
                importHandler.readyForData();
                m_logger.info("Importer started: " + bw.m_handlerProxy.getName());
            } catch (Exception ex) {
                //Should never fail. crash.
                VoltDB.crashLocalVoltDB("Import failed to set Handler", true, ex);
                m_logger.error("Failed to start the import handler: " + bw.m_handlerProxy.getName(), ex);
            }
        }
        //Start polling for channel assignments.
    }

    @Override
    public synchronized void shutdown() {
        try {
            //Stop all the bundle wrappers.
            for (BundleWrapper bw : m_bundles.values()) {
                try {
                    bw.stop();
                } catch (Exception ex) {
                    m_logger.error("Failed to stop the import handler: " + bw.m_handlerProxy.getName(), ex);
                }
            }
            m_bundles.clear();
        } catch (Exception ex) {
            m_logger.error("Failed to stop the import bundles.", ex);
        }
    }

    @Override
    public void setProcessorConfig(Map<String, Properties> config) {
        for (String cname : config.keySet()) {
            Properties properties = config.get(cname);

            String importBundleJar = properties.getProperty(IMPORT_MODULE);
            Preconditions.checkNotNull(importBundleJar, "Import source is undefined or custom export plugin class missing.");
            addProcessorConfig(properties);
        }
    }

}
