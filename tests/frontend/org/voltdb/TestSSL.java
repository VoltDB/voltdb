/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestSSL extends JUnit4LocalClusterTest {

    private LocalCluster m_cluster;
    private ServerThread m_server;
    private Client m_client;
    private Client m_admin;

    private static final String KEYSTORE_RESOURCE = "keystore";
    private static final String KEYSTORE_PASSWD = "password";
    private static final String KEYSTORE_PASSWD_OBFUSCATED = "OBF:1v2j1uum1xtv1zej1zer1xtn1uvk1v1v";
    private static final String KEYSTORE_SYSPROP = "javax.net.ssl.keyStore";
    private static final String KEYSTORE_PASSWD_SYSPROP = "javax.net.ssl.keyStorePassword";
    private static final String TRUSTSTORE_SYSPROP = "javax.net.ssl.trustStore";
    private static final String TRUSTSTORE_PASSWD_SYSPROP = "javax.net.ssl.trustStorePassword";
    private static final String SSL_PROPS_FILE = "ssl-config";
    private static final String SSL_PROPS_FILE_INVALID = "ssl-config-invalid";

    static VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("CREATE TABLE T(A1 INTEGER NOT NULL, A2 INTEGER, PRIMARY KEY(A1));");
        builder.addPartitionInfo("T", "A1");
        builder.addStmtProcedure("InsertA", "INSERT INTO T VALUES(?,?);",
                new ProcedurePartitionData("T", "A1"));
        builder.addStmtProcedure("CountA", "SELECT COUNT(*) FROM T");
        builder.addStmtProcedure("SelectA", "SELECT * FROM T");
        builder.setSslEnabled(true);
        builder.setSslExternal(true);
        builder.setSslInternal(true);

        return builder;
    }

    @After
    public void tearDown() throws Exception {
        if (m_admin != null) {
            m_admin.close();
        }
        if (m_client != null) {
            m_client.close();
        }
        if (m_server != null) {
            m_server.shutdown();
            m_server.join();
            m_server = null;
        }
        if (m_cluster != null) {
            m_cluster.shutDown();
            m_cluster = null;
        }
    }

    private void startLocalCluster(String keyStorePath, String keyStorePasswd, String certStorePath,
            String certStorePasswd) throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();

        if (keyStorePath != null) {
            builder.setKeyStoreInfo(keyStorePath, keyStorePasswd);
        }

        if (certStorePath != null) {
            builder.setCertStoreInfo(certStorePath, certStorePasswd);
        }
        System.setProperty("io.netty.leakDetection.level", "PARANOID");
        Map<String,String> env = new TreeMap<>();
        env.put("io.netty.leakDetection.level","PARANOID");
        m_cluster = new LocalCluster("ssl.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING, false, env);
        boolean success = m_cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("ssl.xml"));
        m_cluster.setHasLocalServer(false);

        m_cluster.startUp();
    }

    private void startServerThread(String keyStorePath, String keyStorePasswd, String certStorePath,
            String certStorePasswd) throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();

        if (keyStorePath != null) {
            String keystore = getResourcePath(keyStorePath);
            builder.setKeyStoreInfo(keystore, keyStorePasswd);
        }

        if (certStorePath != null) {
            String certstore = getResourcePath(certStorePath);
            builder.setCertStoreInfo(certstore, certStorePasswd);
        }
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("ssl.jar"));
        assertTrue(success);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = config.setPathToCatalogForTest("ssl.jar");
        config.m_pathToDeployment = builder.getPathToDeployment();
        m_server = new ServerThread(config);
        m_server.start();
        m_server.waitForInitialization();
    }

    private void checkAdminAndClient(int adminPort, int clientPort, String sslPropsFile) throws Exception {
        ClientConfig clientConfig = new ClientConfig("", "", null);
        clientConfig.setTrustStoreConfigFromPropertyFile(getResourcePath(sslPropsFile));
        clientConfig.enableSSL();
        //Add long loopy test
        for (int i = 0; i < 100;  i++) {
            m_admin = ClientFactory.createClient(clientConfig);
            m_admin.createConnection("localhost", adminPort);
            m_admin.close();
            m_client = ClientFactory.createClient(clientConfig);
            m_client.createConnection("localhost", clientPort);
            m_client.close();
        }

        m_admin = ClientFactory.createClient(clientConfig);
        m_admin.createConnection("localhost", adminPort);
        m_admin.callProcedure("@Pause");
        VoltTable[] results = m_admin.callProcedure("@SystemInformation").getResults();
        checkSystemInformationClusterState(results[0], "Paused");

        m_client = ClientFactory.createClient(clientConfig);
        m_client.createConnection("localhost", clientPort);

        results = m_client.callProcedure("CountA").getResults();
        assertEquals(0, results[0].asScalarLong());
    }

    private String getResourcePath(String resource) {
        URL res = this.getClass().getResource(resource);
        return res == null ? resource : res.getPath();
    }

    @Test
    public void ntestServerThreadDefaultPortDeployment() throws Exception {
        startServerThread(KEYSTORE_RESOURCE, KEYSTORE_PASSWD, KEYSTORE_RESOURCE, KEYSTORE_PASSWD);
        checkAdminAndClient(m_server.m_config.m_adminPort, m_server.m_config.m_port, SSL_PROPS_FILE);
    }

    @Test
    public void ntestServerThreadObfuscatedPassword() throws Exception {
        startServerThread(KEYSTORE_RESOURCE, KEYSTORE_PASSWD_OBFUSCATED, KEYSTORE_RESOURCE, KEYSTORE_PASSWD_OBFUSCATED);
        checkAdminAndClient(m_server.m_config.m_adminPort, m_server.m_config.m_port, SSL_PROPS_FILE);
    }

    @Test
    public void ntestServerThreadKeystoreCertStoreInDeployment() throws Exception {
        System.setProperty(KEYSTORE_SYSPROP, getResourcePath(KEYSTORE_RESOURCE));
        System.setProperty(KEYSTORE_PASSWD_SYSPROP, KEYSTORE_PASSWD);
        System.setProperty(TRUSTSTORE_SYSPROP, getResourcePath(KEYSTORE_RESOURCE));
        System.setProperty(TRUSTSTORE_PASSWD_SYSPROP, KEYSTORE_PASSWD);
        startServerThread("invalid", "invalid", null, null);
        checkAdminAndClient(m_server.m_config.m_adminPort, m_server.m_config.m_port, SSL_PROPS_FILE_INVALID);
    }

    @Test
    public void testLocalClusterDefaultPortDeployment() throws Exception {
        startLocalCluster(KEYSTORE_RESOURCE, KEYSTORE_PASSWD, KEYSTORE_RESOURCE, KEYSTORE_PASSWD);
        checkAdminAndClient(m_cluster.adminPort(0), m_cluster.port(0), SSL_PROPS_FILE);
    }

    @Test
    public void ntestLocalCLusterObfuscatedPassword() throws Exception {
        startLocalCluster(KEYSTORE_RESOURCE, KEYSTORE_PASSWD_OBFUSCATED, KEYSTORE_RESOURCE, KEYSTORE_PASSWD_OBFUSCATED);
        checkAdminAndClient(m_cluster.adminPort(0), m_cluster.port(0), SSL_PROPS_FILE);
    }

    @Test
    public void ntestLocalClusterRejoin() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setKeyStoreInfo(getResourcePath(KEYSTORE_RESOURCE), KEYSTORE_PASSWD);
        builder.setCertStoreInfo(getResourcePath(KEYSTORE_RESOURCE), KEYSTORE_PASSWD);

        int sitesPerHost = 2;
        int hostCount = 3;
        int kFactor = 2;
        m_cluster = new LocalCluster("sslRejoin.jar", sitesPerHost, hostCount, kFactor, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setMaxHeap(1400);
        m_cluster.overrideAnyRequestForValgrind();
        m_cluster.setHasLocalServer(false);
        boolean success = m_cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("sslRejoin.xml"));


        m_cluster.startUp();

        ClientConfig clientConfig = new ClientConfig("", "", null);
        clientConfig.setTrustStoreConfigFromPropertyFile(getResourcePath(SSL_PROPS_FILE));
        clientConfig.enableSSL();

        ClientResponse response;
        Client client;

        client = ClientFactory.createClient(clientConfig);
        client.createConnection("localhost", m_cluster.port(0));
        response = client.callProcedure("InsertA",1,1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("CountA");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(1,response.getResults()[0].asScalarLong());
        client.drain();
        client.close();

        client = ClientFactory.createClient(clientConfig);
        client.createConnection("localhost", m_cluster.port(1));
        response = client.callProcedure("InsertA",2,2);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("CountA");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(2,response.getResults()[0].asScalarLong());
        client.drain();
        client.close();

        m_cluster.killSingleHost(0);
        Thread.sleep(100);

        VoltDB.Configuration config = new VoltDB.Configuration(LocalCluster.portGenerator);
        config.m_startAction = StartAction.PROBE;
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("sslRejoin.jar");
        config.m_voltdbRoot = new File(m_cluster.getServerSpecificRoot("0"));
        config.m_forceVoltdbCreate = false;
        config.m_hostCount = hostCount;
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("sslRejoin.xml");
        config.m_leader = ":" + m_cluster.internalPort(1);
        config.m_coordinators = m_cluster.coordinators(1);

        config.m_isRejoinTest = true;
        m_cluster.setPortsFromConfig(0, config);
        m_server = new ServerThread(config);

        m_server.start();
        m_server.waitForRejoin();
        Thread.sleep(5000);

        System.out.println("ServerThread joined");

        client = ClientFactory.createClient(clientConfig);
        System.out.println("Try connect to port:" + m_cluster.port(0));
        client.createConnection("localhost", m_cluster.port(0));
        System.out.println("Start Proc");
        response = client.callProcedure("InsertA",3,3);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("CountA");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(3,response.getResults()[0].asScalarLong());
        client.drain();
        client.close();

        client = ClientFactory.createClient(clientConfig);
        System.out.println("Try connect to port:" + m_cluster.port(2));
        client.createConnection("localhost", m_cluster.port(2));
        System.out.println("Start Proc");
        response = client.callProcedure("InsertA",4,4);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("CountA");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(4,response.getResults()[0].asScalarLong());
        client.drain();
        client.close();

        m_server.shutdown();
        m_cluster.shutDown();

    }

    void checkSystemInformationClusterState(VoltTable sysinfo, String state) {
        for (int i = 0; i < sysinfo.getRowCount(); i++) {
            sysinfo.advanceRow();
            if (sysinfo.get("KEY", VoltType.STRING).equals("CLUSTERSTATE")) {
                assertTrue(state.equalsIgnoreCase((String) sysinfo.get("VALUE", VoltType.STRING)));
                return;
            }
        }
        fail("Failed to find CLUSTERSTATE key in SystemInformation results");
    }
}
