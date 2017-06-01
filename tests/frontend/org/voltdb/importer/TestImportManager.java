/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.importer;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.*;
import org.voltdb.importclient.junit.JUnitImporter;
import org.voltdb.importclient.junit.JUnitImporterConfig;
import org.voltdb.importclient.junit.JUnitImporterEventExaminer;
import org.voltdb.importclient.junit.JUnitImporterMessenger;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.DbSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.utils.CatalogUtil;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by bshaw on 5/26/17.
 */
public class TestImportManager {

    private static final int HOSTID = 0;
    private static final int SITEID = 0;

    private ImportManager m_manager = null;
    private MockChannelDistributer m_mockChannelDistributer = null;
    private ImporterStatsCollector m_statsCollector = null;

    private CatalogContext m_initialCatalogContext = null;

    private static final int RESTART_CHECK_MAX_MS              = (int) TimeUnit.SECONDS.toMillis(5);
    private static final int RESTART_CHECK_POLLING_INTERVAL_MS = 500;

    private static final boolean DEFAULT_CATALOG_ENABLES_COMMAND_LOG = false; // this is used to generate non-importer-related config changes
    private static String m_previousBundlePath;

    private static final int NUM_IMPORTERS = 2;
    private static final String SCHEMA = "CREATE TABLE test (foo BIGINT NOT NULL, bar VARCHAR(8) NOT NULL);\n"
                                       + "CREATE PROCEDURE procedure1 AS INSERT INTO test VALUES (?, ?);\n"
                                       + "CREATE PROCEDURE procedure2 AS INSERT INTO test VALUES (? * 2, lower(?));";


    public class DetectImporterRestart implements JUnitImporterEventExaminer {

        private Set<URI> m_importersWithRestarts = new HashSet<>();

        public Set<URI> getImportersWithRestarts() {
            return m_importersWithRestarts;
        }

        @Override
        public void examine(Map<URI, List<JUnitImporter.Event>> eventTracker) {
            // TODO: detect the pattern which indicates an importer restarted and add applicable importers to the set.
        }
    }

    /** Builds a CatalogContext for the import manager to use.
     * @param numImporters Number of Kafka topics to use. Supply 0 to disable all importers.
     * @param enableCommandLogs Whether or not command logs should be on. This is to allow testing how an unrelated change affects importer behavior.
     * @return Catalog context
     * @throws Exception upon error or test failure
     */
    private static CatalogContext buildMockCatalogContext(int numImporters, boolean enableCommandLogs) throws Exception {

        // create a dummy catalog to load deployment info into
        //File schemaFile = VoltProjectBuilder.createFileForSchema(SCHEMA);
        //Catalog catalog = new VoltCompiler(false, false).compileCatalogFromDDL();
        Catalog catalog = new Catalog();
        // Need these in the dummy catalog
        Cluster cluster = catalog.getClusters().add("cluster");
        cluster.getDatabases().add("database");
        HostMessenger dummyHostMessenger = new HostMessenger(new HostMessenger.Config(), null);
        DbSettings dbSettings = new DbSettings(ClusterSettings.create().asSupplier(), NodeSettings.create());

        VoltProjectBuilder projectBuilder = new VoltProjectBuilder();
        for (int i = 0; i < numImporters; i++) {
            Properties importerProperties = new Properties();
            importerProperties.setProperty(ImportDataProcessor.IMPORT_PROCEDURE, "procedure" + i);
            // NOTE: importFormat=null defaults to "csv", which isn't something we need but doesn't cause any additional pain.
            // We need to load an OSGI bundle either way since JUnitImporter is packaged as a normal importer.
            projectBuilder.addImport(true, "custom", null, JUnitImporterConfig.URI_SCHEME + ".jar", importerProperties);
        }
        projectBuilder.configureLogging(false, enableCommandLogs, 100, 1000, 10000);
        projectBuilder.setUseDDLSchema(true); // BSDBG this makes manually debugging the test easier

        File deploymentFilePath = new File(projectBuilder.compileDeploymentOnly("voltdbroot", 1, 1, 0, 0));

        System.out.println("Deployment file written to " + deploymentFilePath.getCanonicalPath());

        byte[] deploymentBytes = new byte[(int) deploymentFilePath.length()];
        new FileInputStream(deploymentFilePath).read(deploymentBytes);
        DeploymentType inMemoryDeployment = CatalogUtil.getDeployment(new ByteArrayInputStream(deploymentBytes));
        assertNotNull("Error parsing deployment schema - see log file for details", inMemoryDeployment);

        // NOTE: this is borrowed from RealVoltDB.readDeploymentAndCreateStarterCatalogContext() at the very end
        String result = CatalogUtil.compileDeployment(catalog, inMemoryDeployment, true);
        assertTrue(result, result == null); // Any other non-enterprise deployment errors will be caught and handled here

        CatalogContext context = new CatalogContext(
                TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId(), //txnid
                0, //timestamp
                catalog,
                dbSettings,
                new byte[] {},
                null,
                deploymentBytes, // literal bytes for deployment
                0,
                dummyHostMessenger);

        return context;
    }

    /** Before test setup code.
     * NOTE: ImportManager is a singleton.
     * @throws Exception upon error or test failure.
     */
    @Before
    public void setUp() throws Exception {
        m_previousBundlePath = System.getProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME);

        String locationOfCatalogUtil = CatalogUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        if (locationOfCatalogUtil.contains("voltdb" + File.separator + "obj")) {
            // for junits run from ant, VoltDB's default bundles directory is incorrect.
            int index = locationOfCatalogUtil.indexOf("obj");
            String pathToBundles = locationOfCatalogUtil.substring(0, index) + File.separator + "bundles";
            System.setProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME, pathToBundles);
        }

        CatalogContext catalogContextWith3Kafka = buildMockCatalogContext(NUM_IMPORTERS, false);
        m_mockChannelDistributer = new MockChannelDistributer(Integer.toString(HOSTID));
        m_statsCollector = new ImporterStatsCollector(SITEID);
        ImportManager.initializeWithMocks(HOSTID, catalogContextWith3Kafka, m_mockChannelDistributer, m_statsCollector);
        m_initialCatalogContext = catalogContextWith3Kafka;
        m_manager = ImportManager.instance();
        JUnitImporterMessenger.initialize(); // if initial config doesn't have importers which are enabled, this is required for checkForImporterRestart().
    }

    @After
    public void tearDown() throws Exception {
        try {
            m_manager = null;
            m_initialCatalogContext = null;
        } finally {
            if (m_previousBundlePath == null) {
                System.clearProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME);
            } else {
                System.setProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME, m_previousBundlePath);
            }
        }
    }

    private boolean checkForImporterRestart() throws Exception {
        System.out.println("Waiting up to " + TimeUnit.MILLISECONDS.toSeconds(RESTART_CHECK_MAX_MS) + " seconds for importers to restart.");

        // FIXME: this is the OPPOSITE of what I need! I need one with all importers, not none of them!
        Set<URI> expectedImporterSet = new HashSet<>();

        for (int i = 0; i < RESTART_CHECK_MAX_MS / RESTART_CHECK_POLLING_INTERVAL_MS; i++) {
            Thread.sleep(RESTART_CHECK_POLLING_INTERVAL_MS);
            DetectImporterRestart detector = new DetectImporterRestart();
            JUnitImporterMessenger.instance().checkEventList(detector);
            if (detector.getImportersWithRestarts().equals(expectedImporterSet)) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void testImporterRestartDeploymentChanges() throws Exception {
        CatalogContext catalogContextWithoutKafka = buildMockCatalogContext(0, DEFAULT_CATALOG_ENABLES_COMMAND_LOG);

        m_manager.updateCatalog(catalogContextWithoutKafka);
        m_manager.updateCatalog(m_initialCatalogContext);
        assertEquals("Importers did not restart after importers were removed and re-added.", true, checkForImporterRestart());
    }
}
