package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.AsyncCompilerAgent;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;

public class TestNotEnoughMemory extends NotEnoughMemoryTester {

    Client m_client;
    private final static boolean m_debug = false;
    public static final boolean retry_on_mismatch = true;

/*    @AfterClass
    public static void tearDownClass()
    {
        try {
            VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        }
        catch (IOException e) {};
    } */

    @Test
    public void testProcedureAdhoc() throws Exception {
        VoltDB.Configuration config = setUpSPDB();
        ServerThread localServer = new ServerThread(config);
        
        try {
            localServer.start();
            localServer.waitForInitialization();
            
            m_client = ClientFactory.createClient();
            m_client.createConnection("localhost", config.m_port);
            
            // change this. 
            m_client.callProcedure("@AdHoc", "insert into PARTED1 values ( 23, 3 )");
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        
    }

}
