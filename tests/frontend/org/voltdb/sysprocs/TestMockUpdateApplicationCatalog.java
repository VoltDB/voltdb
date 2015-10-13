package org.voltdb.sysprocs;

import java.io.File;
import java.io.IOException;

import org.mockito.Matchers;
import org.mockito.Mockito;
import org.voltdb.OperationMode;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.sysprocs.UpdateApplicationCatalog.JavaClassForTest;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.InMemoryJarfile.JarLoader;

import junit.framework.TestCase;

public class TestMockUpdateApplicationCatalog extends TestCase {
    private ServerThread m_localServer;
    private VoltDB.Configuration m_config;
    private Client m_client;

    @Override
    public void setUp() throws Exception
    {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar"), 1, 1, 0);
        assert(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml"));

        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
        m_config.m_pathToDeployment = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
        m_localServer = new ServerThread(m_config);
        m_localServer.start();
        m_localServer.waitForInitialization();

        JavaClassForTest testClass = Mockito.mock(JavaClassForTest.class);
        Mockito.when(testClass.forName(Matchers.anyString(), Matchers.anyBoolean(), Mockito.any(JarLoader.class))).
                     thenThrow(new UnsupportedClassVersionError("Unsupported major.minor version 52.0"));
        UpdateApplicationCatalog.setJavaClassForTest(testClass);

        assertEquals(OperationMode.RUNNING, VoltDB.instance().getMode());
        m_client = ClientFactory.createClient();
        m_client.createConnection("localhost:" + m_config.m_adminPort);
    }

    @Override
    public void tearDown() throws Exception {
        if (m_client != null) {
            m_client.close();
        }
        if (m_localServer != null) {
            m_localServer.shutdown();
        }
    }

    public void testVersionCheck() throws IOException, ClassNotFoundException {


        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
        try {
            m_client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL));
            fail("Update catalog should fail with version error.");
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Cannot load classes compiled with a higher version"));
        }
    }
}
