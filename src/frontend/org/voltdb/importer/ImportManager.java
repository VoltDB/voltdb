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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.voltcore.logging.VoltLogger;
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

    //Default to OSGI based processor there is also a native processor that will be easy to use for testing.
    private final String m_loaderClass = System.getProperty("ImportProcessor", "org.voltdb.importer.processors.ImportProcessor");

    AtomicReference<ImportDataProcessor> m_processor = new AtomicReference<ImportDataProcessor>();
    private volatile Map<String, Properties> m_processorConfig = new HashMap<>();

    /** Obtain the global ImportManager via its instance() method */
    private static ImportManager m_self;

    /**
     * Get the global instance of the ImportManager.
     * @return The global single instance of the ImportManager.
     */
    public static ImportManager instance() {
        return m_self;
    }

    public static void setInstanceForTest(ImportManager self) {
        m_self = self;
    }

    protected ImportManager() {
    }

    /**
     * Create the singleton ImportManager and initialize.
     * @param catalogContext
     * @param partitions
     */
    public static synchronized void initialize(CatalogContext catalogContext, List<Integer> partitions) {
        ImportManager em = new ImportManager();

        m_self = em;
        em.create(catalogContext, partitions);
    }

    /**
     * This creates a import connector from configuration provided.
     * @param catalogContext
     * @param partitions
     */
    private synchronized void create(CatalogContext catalogContext, List<Integer> partitions) {
        try {
            if (catalogContext.getDeployment().getImport() == null) {
                return;
            }
            if (!org.voltdb.utils.MiscUtils.isPro()) {
                importLog.info("Importer specified in deployment. Import is not supported in Community Edition.");
                return;
            }
            importLog.info("Creating import connector " + m_loaderClass);
            ImportDataProcessor newProcessor = null;
            final Class<?> loaderClass = Class.forName(m_loaderClass);
            newProcessor = (ImportDataProcessor)loaderClass.newInstance();
            m_processorConfig = CatalogUtil.getImportProcessorConfig(catalogContext.getDeployment().getImport());
            newProcessor.setProcessorConfig(m_processorConfig);
            m_processor.set(newProcessor);
        } catch (final ClassNotFoundException e) {
            VoltDB.crashLocalVoltDB("Import is not supported in Community Edition.", true, e);
        } catch (final Exception e) {
            VoltDB.crashLocalVoltDB("Error creating next import processor", true, e);
        }
    }

    public synchronized void shutdown() {
        //If no processor set we dont have any import configuration
        if (m_processor.get() == null) {
            return;
        }
        m_processor.get().shutdown();
        //Unset until it gets recreated.
        m_processor.set(null);
    }

    public synchronized void updateCatalog(CatalogContext catalogContext, List<Integer> partitions) {
        //Shutdown and recreate.
        m_self.shutdown();
        assert(m_processor.get() == null);
        m_self.create(catalogContext, partitions);
        m_self.readyForData(catalogContext, partitions);
    }

    public synchronized void readyForData(CatalogContext catalogContext, List<Integer> partitions) {
        //If we dont have any processors we dont have any import configured.
        if (m_processor.get() == null) {
            return;
        }
        //Tell import processors and in turn ImportHandlers that we are ready to take in data.
        m_processor.get().readyForData(catalogContext, partitions);
    }


}
