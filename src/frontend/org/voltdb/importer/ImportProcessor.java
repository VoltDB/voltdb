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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.ImportHandler;
import org.voltdb.VoltDB;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;

public class ImportProcessor implements ImportDataProcessor {

    private static final VoltLogger m_logger = new VoltLogger("IMPORT");
    private final FrameworkFactory m_frameworkFactory;
    private final Map<String, String> m_frameworkProps;
    private final Map<String, BundleWrapper> m_bundles = new HashMap<String, BundleWrapper>();
    private final Map<String, BundleWrapper> m_bundlesByName = new HashMap<String, BundleWrapper>();

    public ImportProcessor() {
        //create properties for osgi
        m_frameworkProps = new HashMap<String, String>();
        //Need this so that ImportContext is available.
        m_frameworkProps.put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, "org.voltcore.network;version=1.0.0"
                + ",org.voltdb.importer;version=1.0.0,org.apache.log4j;version=1.0.0,org.voltdb.client;version=1.0.0,org.slf4j;version=1.0.0");
        // more properties available at: http://felix.apache.org/documentation/subprojects/apache-felix-service-component-runtime.html
        //m_frameworkProps.put("felix.cache.rootdir", "/tmp"); ?? Should this be under voltdbroot?
        m_frameworkFactory = ServiceLoader.load(FrameworkFactory.class).iterator().next();
    }

    //This abstracts OSGi based and class based importers.
    public class BundleWrapper {
        public final Bundle m_bundle;
        public final Framework m_framework;
        public final Properties m_properties;
        public final ImportHandlerProxy m_handlerProxy;
        private ImportHandler m_handler;

        public BundleWrapper(Bundle bundle, Framework framework, ImportHandlerProxy handler, Properties properties) {
            m_bundle = bundle;
            m_framework = framework;
            m_handlerProxy = handler;
            m_properties = properties;
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
                if (m_framework != null) {
                    m_framework.stop();
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
            ImportHandlerProxy importHandlerProxy = null;
            BundleWrapper wrapper = null;
            if (moduleType.equalsIgnoreCase("osgi")) {
                Framework framework = m_frameworkFactory.newFramework(m_frameworkProps);
                framework.start();

                Bundle bundle = framework.getBundleContext().installBundle(bundleJar);
                bundle.start();

                ServiceReference reference = framework.getBundleContext().getServiceReference(ImportDataProcessor.IMPORTER_SERVICE_CLASS);
                if (reference == null) {
                    m_logger.error("Failed to initialize importer from: " + bundleJar);
                    bundle.stop();
                    framework.stop();
                    return;
                }
                Object o = framework.getBundleContext().getService(reference);
                importHandlerProxy = (ImportHandlerProxy )o;
                //Save bundle and properties
                wrapper = new BundleWrapper(bundle, framework, importHandlerProxy, properties);
            } else {
                //Class based importer.
                Class reference = this.getClass().getClassLoader().loadClass(bundleJar);
                if (reference == null) {
                    m_logger.error("Failed to initialize importer from: " + bundleJar);
                    return;
                }

                Object o = reference.newInstance();
                importHandlerProxy = (ImportHandlerProxy )o;
                //Save bundle and properties - no bundle and framework.
                 wrapper = new BundleWrapper(null, null, importHandlerProxy, properties);
            }
            importHandlerProxy.configure(properties);
            String name = importHandlerProxy.getName();
            if (name == null || name.trim().length() == 0) {
                throw new RuntimeException("Importer must implement and return a valid unique name.");
            }
            Preconditions.checkState(!m_bundlesByName.containsKey(name), "Importer must implement and return a valid unique name.");
            m_bundlesByName.put(name, wrapper);
            m_bundles.put(bundleJar, wrapper);
        } catch(Throwable t) {
            m_logger.error("Failed to configure import handler for " + bundleJar, t);
            Throwables.propagate(t);
        }
    }

    private void registerImporterMetaData(CatalogContext catContext, HostMessenger messenger) {
        ZooKeeper zk = messenger.getZK();
        //TODO: Do resource allocation.
    }

    @Override
    public void readyForData(CatalogContext catContext, HostMessenger messenger) {
        //Register and launch watchers. - See if UAC path needs this. TODO.
        registerImporterMetaData(catContext, messenger);

        //Clean any pending and invoked stuff.
        synchronized (this) {
            for (BundleWrapper bw : m_bundles.values()) {
                try {
                    ImportHandler importHandler = new ImportHandler(bw.m_handlerProxy, catContext);
                    //Set the internal handler
                    bw.setHandler(importHandler);
                    importHandler.readyForData();
                    m_logger.info("Importer started: " + bw.m_handlerProxy.getName());
                } catch (Exception ex) {
                    //Should never fail. crash.
                    VoltDB.crashLocalVoltDB("Import failed to set Handler", true, ex);
                    m_logger.error("Failed to start the import handler: " + bw.m_handlerProxy.getName(), ex);
                }
            }
        }
    }

    @Override
    public void shutdown() {
        synchronized (this) {
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
