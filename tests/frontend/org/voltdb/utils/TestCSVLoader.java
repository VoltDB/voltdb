package org.voltdb.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import junit.framework.TestCase;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestCSVLoader extends TestCase {
	
	private String simpleSchema;
	private String pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
    private String pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");
    private ServerThread localServer;
    private VoltDB.Configuration config;
    private VoltProjectBuilder builder;
    private Client client;
    
    private String userHome = System.getProperty("user.home"); 
    private String reportdir = userHome + "/";
    String path_csv = userHome + "/" + "test.csv";
    
    private String []options = {
    		"--inputfile=" + userHome + "/test.csv", 
    		//"--procedurename=BLAH.insert",
    		"--reportdir=" + reportdir,
    		"--tablename=BLAH",
    		"--abortfailurecount=50",
    		};

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        
        simpleSchema =
                "create table BLAH (" +
                "clm_integer integer default 0 not null, " + // column that is partitioned on

                "clm_tinyint tinyint default 0, " +
                //"clm_smallint smallint default 0, " +
                //"clm_bigint bigint default 0, " +
                
               	//"clm_string varchar(10) default null, " +
                //"clm_decimal decimal default null, " +
                //"clm_float float default 1.0, "+ // for later
                //"clm_timestamp timestamp default null, " + // for later
                //"clm_varinary varbinary default null" + // for later
                "); ";
   
        pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
        pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");
        builder = new VoltProjectBuilder();
        //builder.addStmtProcedure("Insert", "insert into blah values (?, ?, ?);", null);
        //builder.addStmtProcedure("InsertWithDate", "INSERT INTO BLAH VALUES (974599638818488300, 5, 'nullchar');");
        

        config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        localServer = new ServerThread(config);
        client = null;
        
        try{
        	localServer.start();
        	localServer.waitForInitialization();
        
        	client = ClientFactory.createClient();
        	client.createConnection("localhost");
        }
        catch(Exception e )
        {
        	System.err.println(e.getMessage());
        };
    }
    
    public void testCommon() throws Exception 
	{
     String mySchema =
                "create table BLAH (" +
                "clm_integer integer default 0 not null, " + // column that is partitioned on

                //"clm_tinyint tinyint default 0, " +
                //"clm_smallint smallint default 0, " +
                "clm_bigint bigint default 0, " +
                
               // "clm_string varchar(10) default null, " +
                //"clm_decimal decimal default null, " +
                //"clm_float float default 1.0, "+ // for later
                //"clm_timestamp timestamp default null, " + // for later
                //"clm_varinary varbinary default null" + // for later
                "); ";
     String []myOptions = {
     		"--inputfile=" + userHome + "/test.csv", 
     		//"--procedurename=BLAH.insert",
     		"--reportdir=" + reportdir,
     		"--tablename=BLAH",
     		"--abortfailurecount=50",
     		};
	    String []myData = { "1,111111",
	    		  			"10,1 000",
	    					"2,null",
	    					"3,NULL",
	    					"4,Null",
	    					"5, null",
	    	 			    "6,null ",
	    					"7, null ",
	    					
	    					"8, ",
	    					"",
	    					"11,1,000",
	    					"12,n ull"
	    					};
	    int invalidLineCnt = 4;
		test_Interface( mySchema, myOptions, myData, invalidLineCnt );
	}
	
//	public void testDelimeters () throws Exception
//	{
//		simpleSchema =
//                "create table BLAH (" +
//                "clm_integer integer default 0 not null, " + // column that is partitioned on
//
//                "clm_tinyint tinyint default 0, " +
//                //"clm_smallint smallint default 0, " +
//                //"clm_bigint bigint default 0, " +
//                
//               // "clm_string varchar(10) default null, " +
//                //"clm_decimal decimal default null, " +
//                //"clm_float float default 1.0, "+ // for later
//                //"clm_timestamp timestamp default null, " + // for later
//                //"clm_varinary varbinary default null" + // for later
//                "); ";
//		char str = '.';
//		 String []params = {
//		    		//userHome + "/testdb.csv",
//		    		"--inputfile=" + userHome + "/test.csv", 
//		    		//"--procedurename=BLAH.insert",
//		    		"--reportdir=" + reportdir,
//		    		"--tablename=BLAH",
//		    		"--abortfailurecount=50",
//		    		"--separator=",""
//		    		};
//		 String []myData = {"1, 1","","2, 2"};
//		 test_Interface( params, myData );
//	}
/*
	public void testSimple() throws Exception {
		 String []params_simple = {
	    		//"--inputfile=" + userHome + "/testdb.csv", 
	    		//"--procedurename=BLAH.insert",
	    		//"--reportdir=" + reportdir,
	    		//"--tablename=BLAH",
	    		//"--abortfailurecount=50",
				 "-help"
	    		};
        try {
            CSVLoader.main(params_simple);
            // do the test
            VoltTable modCount;
            modCount = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            System.out.println("data inserted to table BLAH:\n" + modCount);
            int rowct = modCount.getRowCount();
                        
            BufferedReader csvreport = new BufferedReader(new FileReader(new String(reportdir + CSVLoader.reportfile)));
            int lineCount = 0;
            String line = "";
            String promptMsg = "Number of acknowledged tuples:";
            
            while ((line = csvreport.readLine()) != null) {
            	if (line.startsWith(promptMsg)) {
            		String num = line.substring(promptMsg.length());
            		lineCount = Integer.parseInt(num.replaceAll("\\s",""));
            		break;
            	}
            }
            System.out.println(String.format("The rows infected: (%d,%s)", lineCount, rowct));
            assertEquals(lineCount, rowct);
            CSVLoader.flush();
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
	*/
	
	public void test_Interface( String my_schema, String[] my_options, String[] my_data, int invalidLineCnt ) throws Exception {
		try{
			BufferedWriter out_csv = new BufferedWriter( new FileWriter( path_csv ) );
			for( int i = 0; i < my_data.length; i++ )
				out_csv.write( my_data[ i ]+"\n" );
			out_csv.flush();
			out_csv.close();
		}
		catch( Exception e) {
			System.err.print( e.getMessage() );
		}
		
		try {
			builder.addLiteralSchema(my_schema);
	        builder.addPartitionInfo("BLAH", "clm_integer");
	        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
	        assertTrue(success);
	        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);
	        
            CSVLoader.main(my_options);
            // do the test
            
            VoltTable modCount;
            modCount = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            System.out.println("data inserted to table BLAH:\n" + modCount);
            int rowct = modCount.getRowCount();
                        
            BufferedReader csvreport = new BufferedReader(new FileReader(new String(reportdir + CSVLoader.reportfile)));
            int lineCount = 0;
            String line = "";
            String promptMsg = "Number of acknowledged tuples:";
            
            String promptFailMsg = "Number of failed tuples:";
            int invalidlinecnt = 0;
            
            while ((line = csvreport.readLine()) != null) {
            	if (line.startsWith(promptMsg)) {
            		String num = line.substring(promptMsg.length());
            		lineCount = Integer.parseInt(num.replaceAll("\\s",""));
                    }
            	if( line.startsWith(promptFailMsg)){
            		String num = line.substring(promptFailMsg.length());
            		invalidlinecnt = Integer.parseInt(num.replaceAll("\\s",""));
            	}	
            }
            System.out.println(String.format("The rows infected: (%d,%s)", lineCount, rowct));
            assertEquals(lineCount, rowct);
            assertEquals(invalidlinecnt, invalidLineCnt);
            CSVLoader.flush();
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
