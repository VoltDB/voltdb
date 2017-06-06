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

import com.google_voltpatches.common.io.Files;
import org.junit.*;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.*;
import org.voltdb.importclient.junit.*;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.modular.ModuleManager;
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
 * Tests importer restart behavior with respect to UpdateApplicationCatalog.
 */
public class TestImportManager {

    private static final int HOSTID = 0;
    private static final int SITEID = 0;

    private CatalogContext m_initialCatalogContext = null;
    private File m_defaultSchemaFile = null;
    private Set<URI> m_defaultImporters = null;
    private File m_voltDbRoot = null;
    private ImportManager m_manager = null;
    private MockChannelDistributer m_mockChannelDistributer = null;
    private ImporterStatsCollector m_statsCollector = null;

    private static final int RESTART_CHECK_MAX_MS              = (int) TimeUnit.SECONDS.toMillis(5);
    private static final int RESTART_CHECK_POLLING_INTERVAL_MS = 500;

    private static final boolean ENABLE_COMMAND_LOG = false; // this is used to generate non-importer-related config changes
    private static String m_previousBundlePath;

    private static final int NUM_IMPORTERS_FOR_TEST = 2;
    private static final String SCHEMA = "CREATE TABLE test (foo BIGINT NOT NULL, bar VARCHAR(8) NOT NULL);\n"
                                       + "CREATE PROCEDURE procedure0 AS INSERT INTO test VALUES (?, ?);\n"
                                       + "CREATE PROCEDURE procedure1 AS INSERT INTO test VALUES (CAST(? AS BIGINT) * 2, lower(CAST(? AS VARCHAR(8))));";

    /* ---- Helper methods ---- */

    private static String generateImporterID(int index) {
        return "MockImporter" + Integer.toString(index);
    }

    /** Builds a CatalogContext for the import manager to use.
     * @param numImporters Number of Kafka topics to use. Supply 0 to disable all importers.
     * @param enableCommandLogs Whether or not command logs should be on. This is to allow testing how an unrelated change affects importer behavior.
     * @return Catalog context
     * @throws Exception upon error or test failure
     */
    private static CatalogContext buildMockCatalogContext(File voltdbroot, File schemaFile, int numImporters, boolean enableCommandLogs) throws Exception {

        Catalog catalog = new VoltCompiler(false, false).compileCatalogFromDDL(schemaFile.getAbsolutePath());
        assertNotNull("Catalog could not be compiled", catalog);
        HostMessenger dummyHostMessenger = new HostMessenger(new HostMessenger.Config(), null);
        DbSettings dbSettings = new DbSettings(ClusterSettings.create().asSupplier(), NodeSettings.create());

        VoltProjectBuilder projectBuilder = new VoltProjectBuilder();
        for (int i = 0; i < numImporters; i++) {
            Properties importerProperties = new Properties();
            importerProperties.setProperty(ImportDataProcessor.IMPORT_PROCEDURE, "procedure" + i);
            importerProperties.setProperty(JUnitImporterConfig.IMPORTER_ID_PROPERTY, generateImporterID(i));
            projectBuilder.addImport(true, "custom", null, JUnitImporterFactory.class.getCanonicalName(), importerProperties);
        }
        projectBuilder.configureLogging(false, enableCommandLogs, 100, 1000, 10000);
        projectBuilder.setUseDDLSchema(true); // this makes manually debugging the test deployment easier

        File deploymentFilePath = new File(projectBuilder.compileDeploymentOnly(voltdbroot.getAbsolutePath(), 1, 1, 0, 0));
        System.out.println("Deployment file written to " + deploymentFilePath.getCanonicalPath());

        byte[] deploymentBytes = new byte[(int) deploymentFilePath.length()];
        new FileInputStream(deploymentFilePath).read(deploymentBytes);
        DeploymentType inMemoryDeployment = CatalogUtil.getDeployment(new ByteArrayInputStream(deploymentBytes));
        assertNotNull("Error parsing deployment schema - see log file for details", inMemoryDeployment);
        String result = CatalogUtil.compileDeployment(catalog, inMemoryDeployment, true);
        assertTrue(result, result == null);

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

    private Set<URI> generateExpectedImporters(int numImportersExpected) {
        Set<URI> expectedImporterSet = new HashSet<>();
        for (int i = 0; i < numImportersExpected; i++) {
            expectedImporterSet.add(JUnitImporterConfig.generateURIForImporter(generateImporterID(i)));
        }
        return expectedImporterSet;
    }

    private void checkImportersAreRunning(Set<URI> expectedImporters) throws Exception {
        System.out.println("Waiting up to " + TimeUnit.MILLISECONDS.toSeconds(RESTART_CHECK_MAX_MS) + " seconds for " + expectedImporters.size() + " importers to be running");

        Set<URI> runningImporters = new HashSet<>();
        for (int i = 0; i < RESTART_CHECK_MAX_MS / RESTART_CHECK_POLLING_INTERVAL_MS; i++) {
            Thread.sleep(RESTART_CHECK_POLLING_INTERVAL_MS);
            JUnitImporterMessenger.instance().getRunningImporters(runningImporters, false);
            if (runningImporters.equals(expectedImporters)) {
                return;
            }
        }
        throw new AssertionError("Expected running importers " + expectedImporters.toString() + " but found running importers " + runningImporters.toString());
    }

    private void checkImportersAreOff() throws Exception {
        System.out.println("Waiting up to " + TimeUnit.MILLISECONDS.toSeconds(RESTART_CHECK_MAX_MS) + " seconds for importers to stop");

        Set<URI> runningImporters = new HashSet<>();
        for (int i = 0; i < RESTART_CHECK_MAX_MS / RESTART_CHECK_POLLING_INTERVAL_MS; i++) {
            Thread.sleep(RESTART_CHECK_POLLING_INTERVAL_MS);
            JUnitImporterMessenger.instance().getRunningImporters(runningImporters, true);
            if (runningImporters.size() == 0) {
                return;
            }
        }
        throw new AssertionError("Expected importers to stop but found running importers " + runningImporters.toString());
    }

    private void checkImporterRestartCountEquals(int expectedRestarts) throws Exception {
        Map<URI, Integer> actualRestarts = JUnitImporterMessenger.instance().countRestarts();
        for (Map.Entry<URI, Integer> importer : actualRestarts.entrySet()) {
            assertEquals(importer.getKey() + " has restarted an unexpected number of times", expectedRestarts, importer.getValue().intValue());
        }
    }

    private void checkImporterRestartCountEquals(int expectedRestarts, URI excludedImporter) throws Exception {
        Map<URI, Integer> actualRestarts = JUnitImporterMessenger.instance().countRestarts();
        for (Map.Entry<URI, Integer> importer : actualRestarts.entrySet()) {
            if (!importer.getKey().equals(excludedImporter)) {
                assertEquals(importer.getKey() + " has restarted an unexpected number of times", expectedRestarts, importer.getValue().intValue());
            }
        }
    }

    /* ---- Test setup and tear down ---- */

    @BeforeClass
    public static void setUp() throws Exception {
        // Importers default to using a CSV formatter. Ensure it can be found.
        m_previousBundlePath = System.getProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME);
        String locationOfCatalogUtil = CatalogUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        int indexOfObj = locationOfCatalogUtil.indexOf("obj");
        if (indexOfObj != -1) {
            // for junits run from ant, VoltDB's default bundles directory is incorrect.
            String pathToBundles = locationOfCatalogUtil.substring(0, indexOfObj) + File.separator + "bundles";
            System.setProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME, pathToBundles);
        }
    }

    @Before
    public void beginTest() throws Exception {
        m_voltDbRoot = Files.createTempDir();
        ModuleManager.initializeCacheRoot(m_voltDbRoot);

        m_defaultSchemaFile = VoltProjectBuilder.createFileForSchema(SCHEMA);
        m_initialCatalogContext = buildMockCatalogContext(m_voltDbRoot, m_defaultSchemaFile, NUM_IMPORTERS_FOR_TEST, ENABLE_COMMAND_LOG);
        m_defaultImporters = generateExpectedImporters(NUM_IMPORTERS_FOR_TEST);

        m_statsCollector = new ImporterStatsCollector(SITEID);
        m_mockChannelDistributer = new MockChannelDistributer(Integer.toString(HOSTID));
        ImportManager.initializeWithMocks(m_voltDbRoot, HOSTID, m_initialCatalogContext, m_mockChannelDistributer, m_statsCollector);
        m_manager = ImportManager.instance();
        JUnitImporterMessenger.initialize();

        checkImportersAreRunning(m_defaultImporters);
        checkImporterRestartCountEquals(0);
    }

    @After
    public void endTest() throws Throwable {
        try {
            m_manager.close();
            m_defaultSchemaFile.delete();
            m_voltDbRoot.delete();
            Throwable detectedException = JUnitImporterMessenger.instance().checkForAndClearExceptions();
            if (detectedException != null) {
                throw detectedException;
            }
        } finally {
            JUnitImporterMessenger.deinitialize();
            ModuleManager.resetCacheRoot();
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (m_previousBundlePath == null) {
            System.clearProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME);
        } else {
            System.setProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME, m_previousBundlePath);
        }
    }

    /* ---- Tests ---- */

    @Test
    public void testImporterDeploymentChanges() throws Exception {
        System.out.println("Removing and re-adding importers should cause them to restart");
        CatalogContext catalogContextWithoutKafka = buildMockCatalogContext(m_voltDbRoot, m_defaultSchemaFile, 0, ENABLE_COMMAND_LOG);
        m_manager.updateCatalog(catalogContextWithoutKafka);
        checkImportersAreOff();
        m_manager.updateCatalog(m_initialCatalogContext);
        checkImportersAreRunning(m_defaultImporters);
        checkImporterRestartCountEquals(1);
    }

    @Test
    public void testUnrelatedDeploymentChanges() throws Exception {
        System.out.println("Unrelated change to deployment should not cause importers to restart");
        CatalogContext catalogContextWithUnrelatedChange = buildMockCatalogContext(m_voltDbRoot, m_defaultSchemaFile, NUM_IMPORTERS_FOR_TEST, !ENABLE_COMMAND_LOG);
        m_manager.updateCatalog(catalogContextWithUnrelatedChange);
        checkImportersAreRunning(m_defaultImporters);
        checkImporterRestartCountEquals(0);
    }

    @Test
    public void testUnrelatedProcedureChanges() throws Exception {
        System.out.println("Adding unrelated procedures and tables should not result in a restart");
        String schemaWithExtraTable = SCHEMA;
        schemaWithExtraTable += "CREATE TABLE dummy (key VARCHAR(30) PRIMARY KEY, value VARBINARY(64) NOT NULL, stamp TIMESTAMP NOT NULL);\n";
        schemaWithExtraTable += "CREATE PROCEDURE AppendTo AS UPSERT INTO dummy VALUES(?, ?, CURRENT_TIMESTAMP());\n";
        File schemaFile = VoltProjectBuilder.createFileForSchema(schemaWithExtraTable);
        CatalogContext catalogWithExtraTableAndProcs = buildMockCatalogContext(m_voltDbRoot, schemaFile, NUM_IMPORTERS_FOR_TEST, ENABLE_COMMAND_LOG);
        m_manager.updateCatalog(catalogWithExtraTableAndProcs);
        checkImportersAreRunning(m_defaultImporters);
        checkImporterRestartCountEquals(0);

        System.out.println("Removing unrelated procedures and tables should not result in a restart");
        m_manager.updateCatalog(m_initialCatalogContext);
        checkImportersAreRunning(m_defaultImporters);
        checkImporterRestartCountEquals(0);
    }

    @Test
    public void testAddRemoveImporterProcedure() throws Exception {
        System.out.println("Removing and adding procedures that the importers rely on should result in a restart");

        // default schema includes all procedures importers expect, so remove one
        String[] schemaLines = SCHEMA.split("\n");
        StringBuilder builder = new StringBuilder();
        for (String line : schemaLines) {
            if (!line.contains("procedure1")) {
                builder.append(line);
            }
        }
        File schemaFile = VoltProjectBuilder.createFileForSchema(builder.toString());
        CatalogContext catalogMissingProc1 = buildMockCatalogContext(m_voltDbRoot, schemaFile, NUM_IMPORTERS_FOR_TEST, ENABLE_COMMAND_LOG);
        m_manager.updateCatalog(catalogMissingProc1);

        Set<URI> importersWithoutProc1 = new HashSet<>(m_defaultImporters);
        URI excludedImporter = JUnitImporterConfig.generateURIForImporter(generateImporterID(1));
        final boolean removed = importersWithoutProc1.remove(excludedImporter);
        assertTrue("Bug in test - unable to remove importer with procedure1 from set", removed);
        checkImportersAreRunning(importersWithoutProc1);
        // The importer with procedure 1 will restart one fewer times than everyone else, so don't check it.
        checkImporterRestartCountEquals(1, excludedImporter);

        System.out.println("Restoring missing procedure should result in a restart");
        m_manager.updateCatalog(m_initialCatalogContext);
        checkImportersAreRunning(m_defaultImporters);
        checkImporterRestartCountEquals(2, excludedImporter);
    }

    /* FIXME - this test fails because partitioning procedure doesn't restart importers any more
    @Test
    public void testPartitionImporterProcedure() throws Exception {
        System.out.println("Partitioning a procedure that importers rely on should result in a restart");
        String[] schemaLines = SCHEMA.split("\n");
        StringBuilder builder = new StringBuilder();
        for (String line : schemaLines) {
            if (line.contains("CREATE TABLE test")) {
                builder.append(line);
                builder.append("PARTITION TABLE test ON COLUMN foo;\n");
            } else if (line.contains("procedure1")) {
                String[] proc1parts = line.split("AS", 2);
                assertEquals(2, proc1parts.length);
                builder.append(proc1parts[0]);
                builder.append("PARTITION ON TABLE test COLUMN foo AS");
                builder.append(proc1parts[1]);
            } else {
                builder.append(line);
            }
        }
        File schemaFile = VoltProjectBuilder.createFileForSchema(builder.toString());
        CatalogContext partitionedCatalog = buildMockCatalogContext(m_voltDbRoot, schemaFile, NUM_IMPORTERS_FOR_TEST, ENABLE_COMMAND_LOG);
        m_manager.updateCatalog(partitionedCatalog);
        checkImportersAreRunning(m_defaultImporters);
        checkImporterRestartCountEquals(1);

        System.out.println("Un-partitioning a procedure that importers rely on should result in a restart");
        m_manager.updateCatalog(m_initialCatalogContext);
        checkImportersAreRunning(m_defaultImporters);
        checkImporterRestartCountEquals(2);
    }
    */
}
