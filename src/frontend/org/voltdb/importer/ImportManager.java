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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.compiler.deploymentfile.ImportType;
import org.voltdb.utils.CatalogUtil;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 *
 * @author akhanzode
 */
public class ImportManager implements ChannelChangeCallback {

    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger importLog = new VoltLogger("IMPORT");

    AtomicReference<ImportDataProcessor> m_processor = new AtomicReference<ImportDataProcessor>();
    private volatile Map<String, Properties> m_processorConfig = new HashMap<>();

    /** Obtain the global ImportManager via its instance() method */
    private static ImportManager m_self;
    private final HostMessenger m_messenger;

    private final Map<String, String> m_frameworkProps;
    private final int m_myHostId;
    private Framework m_framework;
    private ChannelDistributer m_distributer;
    /**
     * Get the global instance of the ImportManager.
     * @return The global single instance of the ImportManager.
     */
    public static ImportManager instance() {
        return m_self;
    }

    protected ImportManager(int myHostId, HostMessenger messenger) throws IOException {
        m_myHostId = myHostId;
        m_messenger = messenger;

        String tmpFilePath = System.getProperty(VOLT_TMP_DIR, System.getProperty("java.io.tmpdir"));
        //Create a directory in temp + username
        File f = new File(tmpFilePath, System.getProperty("user.name"));
        if (!f.exists() && !f.mkdirs()) {
            throw new IOException("Failed to create required OSGI cache directory: " + f.getAbsolutePath());
        }

        if (!f.isDirectory() || !f.canRead() || !f.canWrite() || !f.canExecute()) {
            throw new IOException("Cannot access OSGI cache directory: " + f.getAbsolutePath());
        }

        m_frameworkProps = ImmutableMap.<String,String>builder()
                .put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, "org.voltcore.network;version=1.0.0"
                    + ",org.voltdb.importer;version=1.0.0,org.apache.log4j;version=1.0.0"
                    + ",org.voltdb.client;version=1.0.0,org.slf4j;version=1.0.0,org.voltcore.utils;version=1.0.0")
                .put("org.osgi.framework.storage.clean", "onFirstInit")
                .put("felix.cache.rootdir", f.getAbsolutePath())
                .put("felix.cache.locking", Boolean.FALSE.toString())
                .build();

    }

    private void startOSGiFramework() throws BundleException {
        if (m_distributer != null) return;

        m_distributer = new ChannelDistributer(m_messenger.getZK(), String.valueOf(m_myHostId));
        m_distributer.registerCallback("__IMPORT_MANAGER__", this);

        FrameworkFactory frameworkFactory = ServiceLoader.load(FrameworkFactory.class).iterator().next();
        importLog.info("Framework properties are: " + m_frameworkProps);
        m_framework = frameworkFactory.newFramework(m_frameworkProps);
        m_framework.start();
    }

    /**
     * Create the singleton ImportManager and initialize.
     * @param myHostId my host id in cluster
     * @param catalogContext current catalog context
     * @param messenger messenger to get to ZK
     * @throws org.osgi.framework.BundleException
     * @throws java.io.IOException
     */
    public static synchronized void initialize(int myHostId, CatalogContext catalogContext, HostMessenger messenger) throws BundleException, IOException {
        ImportManager em = new ImportManager(myHostId, messenger);

        m_self = em;
        em.create(myHostId, catalogContext);
    }

    /**
     * This creates a import connector from configuration provided.
     * @param catalogContext
     * @param partitions
     */
    private synchronized void create(int myHostId, CatalogContext catalogContext) {
        try {
            ImportType importElement = catalogContext.getDeployment().getImport();
            if (importElement == null || importElement.getConfiguration().isEmpty()) {
                return;
            }
            startOSGiFramework();

            ImportDataProcessor newProcessor = new ImportProcessor(myHostId, m_distributer, m_framework);
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
        if (m_distributer != null) {
            m_distributer.shutdown();
        }
    }

    public synchronized void close() {
        //If no processor set we dont have any import configuration
        if (m_processor.get() == null) {
            return;
        }
        m_processor.get().shutdown();
        //Unset until it gets started.
        m_processor.set(null);
    }

    public synchronized void start(CatalogContext catalogContext, HostMessenger messenger) {
        m_self.create(m_myHostId, catalogContext);
        m_self.readyForData(catalogContext, messenger);
    }

    //Call this method to restart the whole importer system. It takes current catalogcontext and hostmessenger
    public synchronized void restart(CatalogContext catalogContext, HostMessenger messenger) {
        //Shutdown and recreate.
        m_self.close();
        assert(m_processor.get() == null);
        m_self.start(catalogContext, messenger);
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

    @Override
    public void onChange(ImporterChannelAssignment assignment) {
        //We do nothing each importer will get notified with their assignments.
    }

    @Override
    public void onClusterStateChange(VersionedOperationMode mode) {
        switch (mode.getMode()) {
            case PAUSED:
                importLog.info("Cluster is paused shutting down all importers.");
                close();
                importLog.info("Cluster is paused all importers shutdown.");
                break;
            case RUNNING:
                importLog.info("Cluster is resumed STARTING all importers.");
                start(VoltDB.instance().getCatalogContext(), VoltDB.instance().getHostMessenger());
                importLog.info("Cluster is resumed STARTED all importers.");
                break;
            default:
                break;

        }
    }

}
