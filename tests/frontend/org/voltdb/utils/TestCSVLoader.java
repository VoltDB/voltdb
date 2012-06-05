package org.voltdb.utils;

import java.io.FileReader;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;

import au.com.bytecode.opencsv_voltpatches.CSVReader;

import junit.framework.TestCase;

public class TestCSVLoader extends TestCase {

	public void testSimple() throws Exception {
        String simpleSchema =
            "create table BLAH (" +
            "clm_integer integer default 0 not null, " + // column that is partitioned on

            "clm_tinyint tinyint default 0, " +
            "clm_smallint smallint default 0, " +
            "clm_bigint bigint default 0, " +
            
            "clm_string varchar(10) default null, " +
            "clm_decimal decimal default null, " +
            //"clm_float float default 1.0, " + // for later
            //"clm_timestamp timestamp default null, " + // for later
            //"clm_varinary varbinary default null" + // for later
            "); ";

        String pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("BLAH", "clm_integer");
        //builder.addStmtProcedure("Insert", "insert into blah values (?, ?, ?);", null);
        //builder.addStmtProcedure("InsertWithDate", "INSERT INTO BLAH VALUES (974599638818488300, 5, 'nullchar');");
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
            String userHome = System.getProperty("user.home");
            String []params = {"--inputfile=" + userHome + "/testdb.csv", 
            		"--procedurename=BLAH.insert",
            		"--reportDir=" + userHome + "/",
            		"--tablename=BLAH",
            		"--abortfailurecount=50",
            		"--skipEmptyRecords=true",
            		"--trimWhiteSpace=true"

            		};
            long lineCount = CSVLoader.main(params);
            // do the test
            client = ClientFactory.createClient();
            client.createConnection("localhost");
            
            VoltTable modCount;
            modCount = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            System.out.println("data inserted to table BLAH:\n" + modCount);
            
            modCount = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM BLAH;").getResults()[0];
            int rowct = 0;
            while(modCount.advanceRow()) {
            	rowct = (Integer) modCount.get(0, VoltType.INTEGER);
            }
            System.out.println(String.format("The rows infected: (%d,%s)", lineCount, rowct));
            
            assertEquals(lineCount, rowct);
            
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
