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
	private String pathToCatalog;
    private String pathToDeployment;
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

        
    }
    
    public void testCommon() throws Exception 
	{
     String mySchema =
                "create table BLAH (" +
                "clm_integer integer default 0 not null, " + // column that is partitioned on

                "clm_tinyint tinyint default 0, " +
                "clm_smallint smallint default 0, " +
                "clm_bigint bigint default 0, " +
                
                "clm_string varchar(10) default null, " +
                "clm_decimal decimal default null, " +
                "clm_float float default null "+ // for later
                "clm_timestamp timestamp default null, " + // for later
                //"clm_varinary varbinary default null" + // for later
                "); ";
     String []myOptions = {
     		"--inputfile=" + userHome + "/test.csv", 
     		//"--procedurename=BLAH.insert",
     		"--reportdir=" + reportdir,
     		"--tablename=BLAH",
     		"--abortfailurecount=50",
     		//"--separator=','"
     		};
     
	    String []myData = { "1,1,1,11111111,first,1.10,1.11",
	    		  			"10,10,10,10 101 010,second,2.20,2.22",
	    					"2,2,2,222222,second,3.30,null",
	    					"3,3,3,333333, third ,NULL, 3.33",
	    					"4,4,4,444444, null ,4.40 ,4.44",
	    					"5,5,5,5555555, fifth, 5.50, 5.55",
	    	 			    "6,6,null,666666, sixth, 6.60, 6.66",
	    					"7,null,7,7777777, seventh, 7.70, 7.77 ",
	    					"11, 1,1,\"1,000\",first,1.10,1.11",
	    					//invalid lines below
	    					"8, 8",
	    					"",
	    					"12,n ull,12,12121212,twelveth,12.12,12.12"
	    					};
	    int invalidLineCnt = 3;
		//test_Interface( mySchema, myOptions, myData, invalidLineCnt );
	    test_Interface_lineByLine( mySchema, myOptions, myData, invalidLineCnt );
	}
    /*
    public void testNew() throws Exception 
   	{
        String mySchema =
                   "create table BLAH (" +
                   "clm_integer integer default 0 not null, " + // column that is partitioned on

//                   "clm_tinyint tinyint default 0, " +
//                   "clm_smallint smallint default 0, " +
//                   "clm_bigint bigint default 0, " +
//                   
//                   "clm_string varchar(10) default null, " +
//                   "clm_decimal decimal default null, " +
//                   "clm_float float default null "+ // for later
                   "clm_timestamp timestamp default null, " + // for later
                   //"clm_varinary varbinary default null" + // for later
                   "); ";
        String []myOptions = {
        		"--inputfile=" + userHome + "/test.csv", 
        		//"--procedurename=BLAH.insert",
        		"--reportdir=" + reportdir,
        		"--tablename=BLAH",
        		"--abortfailurecount=50",
        		};
        
   	    String []myData = { "1,1111111111",
   	    					"2,12131231231"
   	    					};
   	    int invalidLineCnt = 0;
   		test_Interface( mySchema, myOptions, myData, invalidLineCnt );
   	}
    */
    
    //csvloader --inputfile=/tmp/ --tablename=VOTES --abortfailurecount=50 
//    public void testOptions() throws Exception 
//	{
//     String mySchema =
//                "create table BLAH (" +
//                "clm_integer integer default 0 not null, " + // column that is partitioned on
//
//                "clm_tinyint tinyint default 0, " +
//                "); ";
//     String []myOptions = {
//     		"--inputfile=" + userHome + "/test.csv", 
//     		//"--procedurename=BLAH.insert",
//     		"--reportdir=" + reportdir,
//     		"--tablename=BLAH",
//     		"--abortfailurecount=50",
//     		//"--separator=','"
//     		};
//	    String []myData = { "1,1,1,11111111,first,1.10,1.11",
//	    		  			"10,10,10,10 101 010,second,2.20,2.22",
//	    					"2,2,2,222222,second,3.30,null",
//	    					"3,3,3,333333, third ,NULL, 3.33",
//	    					"4,4,4,444444, null ,4.40 ,4.44",
//	    					"5,5,5,5555555, fifth, 5.50, 5.55",
//	    	 			    "6,6,null,666666, sixth, 6.60, 6.66",
//	    					"7,null,7,7777777, seventh, 7.70, 7.77 ",
//	    					"11, 1,1,\"1,000\",first,1.10,1.11",
//	    					
//	    					"8, 8",
//	    					"",
//	    					"12,n ull,12,12121212,twelveth,12.12,12.12"
//	    					};
//	    int invalidLineCnt = 3;
//		test_Interface( mySchema, myOptions, myData, invalidLineCnt );
//	}
	
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
		
		try{
			pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
			pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");
			builder = new VoltProjectBuilder();
			//builder.addStmtProcedure("Insert", "insert into blah values (?, ?, ?);", null);
			//builder.addStmtProcedure("InsertWithDate", "INSERT INTO BLAH VALUES (974599638818488300, 5, 'nullchar');");
        
			builder.addLiteralSchema(my_schema);
			builder.addPartitionInfo("BLAH", "clm_integer");
			boolean success = builder.compile(pathToCatalog, 2, 1, 0);
			assertTrue(success);
			MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);
			config = new VoltDB.Configuration();
			config.m_pathToCatalog = pathToCatalog;
			config.m_pathToDeployment = pathToDeployment;
			localServer = new ServerThread(config);
			client = null;
        
          	localServer.start();
        	localServer.waitForInitialization();
        
        	client = ClientFactory.createClient();
        	client.createConnection("localhost");
        	
        	CSVLoader loader = new CSVLoader( my_options );
        	loader.main(my_options);
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
            CSVLoader.flush();
            assertEquals(lineCount, rowct);
            assertEquals(invalidlinecnt, invalidLineCnt);
            
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
	
	public void test_Interface_lineByLine( String my_schema, String[] my_options, String[] my_data, int invalidLineCnt ) throws Exception {
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
		
		try{
			pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
			pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");
			builder = new VoltProjectBuilder();
			//builder.addStmtProcedure("Insert", "insert into blah values (?, ?, ?);", null);
			//builder.addStmtProcedure("InsertWithDate", "INSERT INTO BLAH VALUES (974599638818488300, 5, 'nullchar');");
        
			builder.addLiteralSchema(my_schema);
			builder.addPartitionInfo("BLAH", "clm_integer");
			boolean success = builder.compile(pathToCatalog, 2, 1, 0);
			assertTrue(success);
			MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);
			config = new VoltDB.Configuration();
			config.m_pathToCatalog = pathToCatalog;
			config.m_pathToDeployment = pathToDeployment;
			localServer = new ServerThread(config);
			client = null;
        
          	localServer.start();
        	localServer.waitForInitialization();
        
        	client = ClientFactory.createClient();
        	client.createConnection("localhost");
        	
        	CSVLoader loader = new CSVLoader( my_options );
        	long start = System.currentTimeMillis();
        	while( loader.hasNext() )
        	{
        		String [] addStr= {"896798797"};
        		loader.insertLine( addStr );
        	}
        	loader.setLatency(System.currentTimeMillis()-start);
        	System.out.println("CSVLoader elaspsed: " + loader.getLatency()/1000F + " seconds");
        	loader.produceInvalidRowsFile();
        	CSVLoader.flush();
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
            //assertEquals(lineCount, rowct);
            assertEquals(invalidLineCnt,invalidlinecnt);
            
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
