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

import static org.voltcore.common.Constants.VOLT_TMP_DIR;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.StatsSelector;
import org.voltdb.VoltDB;
import org.voltdb.compiler.deploymentfile.ImportType;
import org.voltdb.importer.formatter.AbstractFormatterFactory;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.ImportConfiguration;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableList;
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

    private final static Joiner COMMA_JOINER = Joiner.on(",").skipNulls();

    AtomicReference<ImportDataProcessor> m_processor = new AtomicReference<ImportDataProcessor>();
    private volatile Map<String, ImportConfiguration> m_processorConfig = new HashMap<>();
    private final Map<String, AbstractFormatterFactory> m_formatterFactories = new HashMap<String, AbstractFormatterFactory>();

    /** Obtain the global ImportManager via its instance() method */
    private static ImportManager m_self;
    private final HostMessenger m_messenger;

    private final Map<String, String> m_frameworkProps;
    private final int m_myHostId;
    private Framework m_framework;
    private ChannelDistributer m_distributer;
    private boolean m_serverStarted;
    private final ImporterStatsCollector m_statsCollector;

    /**
     * Get the global instance of the ImportManager.
     * @return The global single instance of the ImportManager.
     */
    public static ImportManager instance() {
        return m_self;
    }

    private final static Function<String,String> appendVersion = new Function<String, String>() {
        @Override
        public String apply(String input) {
            return input + ";version=1.0.0";
        }
    };

    protected ImportManager(int myHostId, HostMessenger messenger, ImporterStatsCollector statsCollector) throws IOException {
        m_myHostId = myHostId;
        m_messenger = messenger;
        m_statsCollector = statsCollector;

        String tmpFilePath = System.getProperty(VOLT_TMP_DIR, System.getProperty("java.io.tmpdir"));
        //Create a directory in temp + username
        File f = new File(tmpFilePath, System.getProperty("user.name"));
        if (!f.exists() && !f.mkdirs()) {
            throw new IOException("Failed to create required OSGI cache directory: " + f.getAbsolutePath());
        }

        if (!f.isDirectory() || !f.canRead() || !f.canWrite() || !f.canExecute()) {
            throw new IOException("Cannot access OSGI cache directory: " + f.getAbsolutePath());
        }

        /*
         * Note for developers: please keep list in alpha-numerical order
         */
        List<String> packages = ImmutableList.<String>builder()
                .add("com.google_voltpatches.common.base")
                .add("com.google_voltpatches.common.collect")
                .add("com.google_voltpatches.common.io")
                .add("com.google_voltpatches.common.net")
                .add("com.google_voltpatches.common.util.concurrent")
                .add("jsr166y")
                .add("org.apache.log4j")
                .add("org.slf4j")
                .add("org.voltcore.network")
                .add("org.voltcore.logging")
                .add("org.voltcore.utils")
                .add("org.voltdb.client")
                .add("org.voltdb.importer")
                .add("org.voltdb.importer.formatter")
                .build();

        String systemPackagesSpec = FluentIterable.from(packages).transform(appendVersion).join(COMMA_JOINER);

        m_frameworkProps = ImmutableMap.<String,String>builder()
                .put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, systemPackagesSpec)
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

            ImportDataProcessor newProcessor = new ImportProcessor(myHostId, m_distributer, m_framework, m_statsCollector);
            m_processorConfig = CatalogUtil.getImportProcessorConfig(catalogContext.getDeployment().getImport());
            m_formatterFactories.clear();

            for (ImportConfiguration config : m_processorConfig.values()) {
                Properties prop = config.getformatterProperties();
                String module = prop.getProperty(ImportDataProcessor.IMPORT_FORMATTER);
                try {
                    AbstractFormatterFactory formatterFactory = m_formatterFactories.get(module);
                    if (formatterFactory == null) {
                        Bundle bundle = m_framework.getBundleContext().installBundle(module);
                        bundle.start();
                        ServiceReference<?> refs[] = bundle.getRegisteredServices();
                        //Must have one service only.
                        ServiceReference<?> reference = refs[0];
                        if (reference == null) {
                            VoltDB.crashLocalVoltDB("Failed to initialize formatter from: " + module);
                        }
                        formatterFactory = (AbstractFormatterFactory)bundle.getBundleContext().getService(reference);
                        m_formatterFactories.put(module, formatterFactory);
                    }
                    formatterFactory.configureFormatterFactory(config.getFormatName(), prop);
                    config.setFormatterFactory(formatterFactory);
                } catch(Throwable t) {
                    VoltDB.crashLocalVoltDB("Failed to configure import handler for " + module);
                }
            }

            newProcessor.setProcessorConfig(catalogContext, m_processorConfig);
            m_processor.set(newProcessor);
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
