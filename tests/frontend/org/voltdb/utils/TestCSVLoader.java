package org.voltdb.utils;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.TestCase;

public class TestCSVLoader extends TestCase {
	public void testSimple() throws Exception {
        String simpleSchema =
            "create table BLAH (" +
            "IVAL bigint default 0 not null, " +
            //"TVAL timestamp default null," +
            "DVAL decimal default null," +
            "VVAL varchar(10) default null, " +
            "PRIMARY KEY(IVAL));";

        String pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("BLAH", "IVAL");
        builder.addStmtProcedure("Insert", "insert into blah values (?, ?, ?);", null);
        builder.addStmtProcedure("InsertWithDate", "INSERT INTO BLAH VALUES (974599638818488300, 5, 'nullchar');");
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        ServerThread localServer = new ServerThread(config);

        Client client = null;
        try {
            localServer.start();
            localServer.waitForInitialization();

            String []paras = {"/Users/xinjia/testdb.csv","Insert"};
            CSVLoader.main(paras);
            
            
            // do the test
            client = ClientFactory.createClient();
            client.createConnection("localhost");
            
          //VoltTable modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (1, 1, 1, 1);").getResults()[0];
            //assertTrue(modCount.getRowCount() == 1);
            //assertTrue(modCount.asScalarLong() == 1);
            
        }
        finally {
            if (client != null) client.close();
            client = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
        
        
	}
	
	
	
	
	
}
