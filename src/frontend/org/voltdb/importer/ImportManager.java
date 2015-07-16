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

import static org.voltcore.common.Constants.VOLT_TMP_DIR;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.utils.CatalogUtil;

/**
 *
 * @author akhanzode
 */
public class ImportManager {

    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger importLog = new VoltLogger("IMPORT");

    AtomicReference<ImportDataProcessor> m_processor = new AtomicReference<ImportDataProcessor>();
    private volatile Map<String, Properties> m_processorConfig = new HashMap<>();

    /** Obtain the global ImportManager via its instance() method */
    private static ImportManager m_self;
    private final HostMessenger m_messenger;

    private final FrameworkFactory m_frameworkFactory;
    private final Map<String, String> m_frameworkProps;
    private final Framework m_framework;
    private final int m_myHostId;
    private final ChannelDistributer m_distributer;
    /**
     * Get the global instance of the ImportManager.
     * @return The global single instance of the ImportManager.
     */
    public static ImportManager instance() {
        return m_self;
    }

    protected ImportManager(int myHostId, HostMessenger messenger) throws BundleException {
        m_myHostId = myHostId;
        m_messenger = messenger;
        m_distributer = new ChannelDistributer(m_messenger.getZK(), String.valueOf(m_myHostId), null);

        //create properties for osgi
        m_frameworkProps = new HashMap<String, String>();
        //Need this so that ImportContext is available.
        m_frameworkProps.put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, "org.voltcore.network;version=1.0.0"
                + ",org.voltdb.importer;version=1.0.0,org.apache.log4j;version=1.0.0,org.voltdb.client;version=1.0.0,org.slf4j;version=1.0.0,org.voltcore.utils;version=1.0.0");
        // more properties available at: http://felix.apache.org/documentation/subprojects/apache-felix-framework/apache-felix-framework-configuration-properties.html
        m_frameworkProps.put("org.osgi.framework.storage.clean", "onFirstInit");
        String tmpFilePath = System.getProperty(VOLT_TMP_DIR, System.getProperty("java.io.tmpdir"));
        m_frameworkProps.put("felix.cache.rootdir", tmpFilePath);
        m_frameworkFactory = ServiceLoader.load(FrameworkFactory.class).iterator().next();
        importLog.info("Framework properties are: " + m_frameworkProps);
        m_framework = m_frameworkFactory.newFramework(m_frameworkProps);
        m_framework.start();
    }

    /**
     * Create the singleton ImportManager and initialize.
     */
    public static synchronized void initialize(int myHostId, CatalogContext catalogContext, List<Integer> partitions, HostMessenger messenger) throws BundleException {
        ImportManager em = new ImportManager(myHostId, messenger);

        m_self = em;
        em.create(myHostId, m_self.m_distributer, catalogContext, messenger.getZK());
    }

    /**
     * This creates a import connector from configuration provided.
     * @param catalogContext
     * @param partitions
     */
    private synchronized void create(int myHostId, ChannelDistributer distributer, CatalogContext catalogContext, ZooKeeper zk) {
        try {
            if (catalogContext.getDeployment().getImport() == null) {
                return;
            }
            ImportDataProcessor newProcessor = new ImportProcessor(myHostId, distributer, m_framework);
            m_processorConfig = CatalogUtil.getImportProcessorConfig(catalogContext.getDeployment().getImport());
            newProcessor.setProcessorConfig(m_processorConfig);
            m_processor.set(newProcessor);
            importLog.info("Import Processor is configured.");
        } catch (final Exception e) {
            VoltDB.crashLocalVoltDB("Error creating import processor", true, e);
        }
    }

    public synchronized void shutdown() {
        close();
        m_distributer.shutdown();
    }

    public synchronized void close() {
        //If no processor set we dont have any import configuration
        if (m_processor.get() == null) {
            return;
        }
        m_processor.get().shutdown();
        //Unset until it gets recreated.
        m_processor.set(null);
    }

    //Call this method to restart the whole importer system. It takes current catalogcontext and hostmessenger
    public synchronized void restart(CatalogContext catalogContext, HostMessenger messenger) {
        //Shutdown and recreate.
        m_self.close();
        assert(m_processor.get() == null);
        m_self.create(m_myHostId, m_distributer, catalogContext, messenger.getZK());
        m_self.readyForData(catalogContext, messenger);
    }

    public void updateCatalog(CatalogContext catalogContext, HostMessenger messenger) {
        restart(catalogContext, messenger);
    }

    public synchronized void readyForData(CatalogContext catalogContext, HostMessenger messenger) {
        //If we dont have any processors we dont have any import configured.
        if (m_processor.get() == null) {
            return;
        }
        //Tell import processors and in turn ImportHandlers that we are ready to take in data.
        m_processor.get().readyForData(catalogContext, messenger);
    }

}
