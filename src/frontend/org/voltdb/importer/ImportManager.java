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

package org.voltdb.importer;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.osgi.framework.BundleException;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.StatsSelector;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Procedure;
import org.voltdb.compiler.deploymentfile.ImportType;
import org.voltdb.importer.formatter.AbstractFormatterFactory;
import org.voltdb.modular.ModuleManager;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.ImportConfiguration;

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
    private volatile Map<String, ImportConfiguration> m_processorConfig = new HashMap<>();
    private final Map<String, AbstractFormatterFactory> m_formatterFactories = new HashMap<String, AbstractFormatterFactory>();

    /** Obtain the global ImportManager via its instance() method */
    private static ImportManager m_self;
    private final HostMessenger m_messenger;

    private final int m_myHostId;
    private ChannelDistributer m_distributer;
    private volatile boolean m_serverStarted;
    private final ImporterStatsCollector m_statsCollector;
    private final ModuleManager m_moduleManager;

    // Maintains the record of the importer bundles that have been loaded into the memory based on importer config.
    // These loaded loaded bundles remains in memory. ready for use and don't get loaded/unloaded
    private Map<String, AbstractImporterFactory> m_loadedBundles = new HashMap<String, AbstractImporterFactory>();
    private Map<String, AbstractImporterFactory> m_importersByType = new HashMap<String, AbstractImporterFactory>();
    // maintains mapping of active importer config to the bundle bundle path used for communicating import processor
    // about the jars to use
    private Map<String, ImportConfiguration> m_configsForProcessor= new HashMap<String, ImportConfiguration>();

    /**
     * Get the global instance of the ImportManager.
     * @return The global single instance of the ImportManager.
     */
    public static ImportManager instance() {
        return m_self;
    }

    private ModuleManager getModuleManager() {
        return ModuleManager.instance();
    }

    protected ImportManager(int myHostId, HostMessenger messenger, ImporterStatsCollector statsCollector) throws IOException {
        m_myHostId = myHostId;
        m_messenger = messenger;
        m_statsCollector = statsCollector;
        m_moduleManager = getModuleManager();
    }

    private void initializeChannelDistributer() throws BundleException {
        if (m_distributer != null) return;

        m_distributer = new ChannelDistributer(m_messenger.getZK(), String.valueOf(m_myHostId));
        m_distributer.registerCallback("__IMPORT_MANAGER__", this);
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
        ImporterStatsCollector statsCollector = new ImporterStatsCollector(myHostId);
        ImportManager em = new ImportManager(myHostId, messenger, statsCollector);
        VoltDB.instance().getStatsAgent().registerStatsSource(
                StatsSelector.IMPORTER,
                myHostId,
                statsCollector);

        m_self = em;
        em.create(myHostId, catalogContext);
    }

    /**
     * This creates a import connector from configuration provided.
     * @param myHostId
     * @param catalogContext
     */
    private synchronized void create(int myHostId, CatalogContext catalogContext) {
        try {
            ImportType importElement = catalogContext.getDeployment().getImport();
            if (importElement == null || importElement.getConfiguration().isEmpty()) {
                return;
            }
            initializeChannelDistributer();

            final String clusterTag = m_distributer.getClusterTag();

            ImportDataProcessor newProcessor = new ImportProcessor(
                    myHostId, m_distributer, m_statsCollector, clusterTag);
            m_processorConfig = CatalogUtil.getImportProcessorConfig(catalogContext.getDeployment().getImport());
            m_formatterFactories.clear();

            for (ImportConfiguration config : m_processorConfig.values()) {
                Properties prop = config.getformatterProperties();
                String module = prop.getProperty(ImportDataProcessor.IMPORT_FORMATTER);
                try {
                    AbstractFormatterFactory formatterFactory = m_formatterFactories.get(module);
                    if (formatterFactory == null) {
                        URI moduleURI = URI.create(module);
                        formatterFactory = m_moduleManager.getService(moduleURI, AbstractFormatterFactory.class);
                        if (formatterFactory == null) {
                            VoltDB.crashLocalVoltDB("Failed to initialize formatter from: " + module);
                        }
                        m_formatterFactories.put(module, formatterFactory);
                    }
                    config.setFormatterFactory(formatterFactory);
                } catch(Throwable t) {
                    VoltDB.crashLocalVoltDB("Failed to configure import handler for " + module);
                }
            }

            discoverConfigsAndLoadBundles(catalogContext);
            if (!m_configsForProcessor.isEmpty()) {
                newProcessor.setProcessorConfig(m_configsForProcessor, m_loadedBundles);
                m_processor.set(newProcessor);
            } else {
                m_processor.set(null);
            }
        } catch (final Exception e) {
            VoltDB.crashLocalVoltDB("Error creating import processor", true, e);
        }
    }

    /**
     * Parses importer configs and loads the bundle needed for importer into memory. Updates
     * mapping of active importer bundle to the bundle jar needed by the impoter
     * @param catalogContext
     */
    private void discoverConfigsAndLoadBundles(CatalogContext catalogContext) {
        m_configsForProcessor.clear();
        for (String configName : m_processorConfig.keySet()) {
            ImportConfiguration importConfig = m_processorConfig.get(configName);
            Properties properties = importConfig.getmoduleProperties();

            String importBundleJar = properties.getProperty(ImportDataProcessor.IMPORT_MODULE);
            Preconditions.checkNotNull(importBundleJar,
                    "Import source is undefined or custom import plugin class missing.");
            String procedure = properties.getProperty(ImportDataProcessor.IMPORT_PROCEDURE);
            //TODO: If processors is a list dont start till all procedures exists.
            Procedure catProc = catalogContext.procedures.get(procedure);
            if (catProc == null) {
                catProc = catalogContext.m_defaultProcs.checkForDefaultProcedure(procedure);
            }

            if (catProc == null) {
                importLog.info("Importer " + configName + " Procedure " + procedure +
                        " is missing will disable this importer until the procedure becomes available.");
                continue;
            }

            if (loadImporterBundle(properties)) {
                m_configsForProcessor.put(configName, importConfig);
            }
        }
        if (!m_configsForProcessor.isEmpty()) {
            importLog.info("Loaded importer modules: " + m_loadedBundles.keySet() +", types: " + m_importersByType.keySet());
        }
    }



    /**
     * Checks if the module for importer has been loaded in the memory. If bundle doesn't exists, it loades one and
     * updates the mapping records of the bundles.
     *
     * @param moduleProperties
     * @return true, if the bundle corresponding to the importer is in memory (uploaded or was already present)
     */
    private boolean loadImporterBundle(Properties moduleProperties){
        String importModuleName = moduleProperties.getProperty(ImportDataProcessor.IMPORT_MODULE);
        String attrs[] = importModuleName.split("\\|");
        String bundleJar = attrs[1];
        String moduleType = attrs[0];

        try {
            AbstractImporterFactory importerFactory = m_loadedBundles.get(bundleJar);
            if (importerFactory == null) {
                if (moduleType.equalsIgnoreCase("osgi")) {
                    URI bundleURI = URI.create(bundleJar);
                    importerFactory = m_moduleManager.getService(bundleURI, AbstractImporterFactory.class);
                    if (importerFactory == null) {
                        importLog.error("Failed to initialize importer from: " + bundleJar);
                        return false;
                    }
                } else {
                    // class based importer.
                    Class<?> reference = this.getClass().getClassLoader().loadClass(bundleJar);
                    if (reference == null) {
                        importLog.error("Failed to initialize importer from: " + bundleJar);
                        return false;
                    }
                    importerFactory = (AbstractImporterFactory)reference.newInstance();
                }
                String importerType = importerFactory.getTypeName();
                if (importerType == null || importerType.trim().isEmpty()) {
                    throw new RuntimeException("Importer must implement and return a valid unique name.");
                }
                Preconditions.checkState(!m_importersByType.containsKey(importerType),
                        "Importer must implement and return a valid unique name: " + importerType);
                m_importersByType.put(importerType, importerFactory);
                m_loadedBundles.put(bundleJar, importerFactory);
            }
        } catch(Throwable t) {
            importLog.error("Failed to configure import handler for " + bundleJar, t);
            Throwables.propagate(t);
        }
        return true;
}

    public static int getPartitionsCount() {
        if (m_self.m_processor.get() != null) {
            return m_self.m_processor.get().getPartitionsCount();
        }
        return 0;
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
        if (m_serverStarted) {
            m_processor.get().shutdown();
        }
        //Unset until it gets started.
        m_processor.set(null);
    }

    public synchronized void start(CatalogContext catalogContext, HostMessenger messenger) {
        m_self.create(m_myHostId, catalogContext);
        m_self.readyForDataInternal(catalogContext, messenger);
    }

    //Call this method to restart the whole importer system. It takes current catalogcontext and hostmessenger
    private synchronized void restart(CatalogContext catalogContext, HostMessenger messenger) {
        //Shutdown and recreate.
        m_self.close();
        assert(m_processor.get() == null);
        m_self.start(catalogContext, messenger);
    }

    public void updateCatalog(CatalogContext catalogContext, HostMessenger messenger) {
        restart(catalogContext, messenger);
    }

    public synchronized void readyForData(CatalogContext catalogContext, HostMessenger messenger) {
        m_serverStarted = true; // Note that server is ready, so that we know whether to process catalog updates
        readyForDataInternal(catalogContext, messenger);
    }

    public synchronized void readyForDataInternal(CatalogContext catalogContext, HostMessenger messenger) {
        if (!m_serverStarted) {
            if (importLog.isDebugEnabled()) {
                importLog.debug("Server not started. Not sending readyForData to ImportProcessor");
            }
            return;
        }

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
    public synchronized void onClusterStateChange(VersionedOperationMode mode) {
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
