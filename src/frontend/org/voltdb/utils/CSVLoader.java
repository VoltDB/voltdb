/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import au.com.bytecode.opencsv_voltpatches.CSVReader;

/**
 * CSVLoader is a simple utility to load data from a CSV formatted file to a table
 * (or pass it to any stored proc, but ignoring any result other than the success code.).
 *
 * TODO:
 *   - Nulls are not handled (or at least I didn't test them).
 *   - Assumes localhost
 *   - Assumes no username/password
 *   - Usage help is ugly and cryptic: options are listed but not described.
 *   - No associated test suite.
 *   - Forces JVM into UTC. All input date TZs assumed to be GMT+0
 *   - Requires canonical JDBC SQL timestamp format
 */
class CSVLoader {

    private static final AtomicLong inCount = new AtomicLong(0);
    private static final AtomicLong outCount = new AtomicLong(0);
    
    private static int reportEveryNRows = 10000;
    private static int waitSeconds = 10;
    
    final CSVConfig config;

    private static String insertProcedure = "";
    private static Map <Long,String> errorMsg = new TreeMap<Long,String>();
    private static final class MyCallback implements ProcedureCallback {
        private final long m_lineNum;
        private final CSVConfig mycfg;
        MyCallback(long lineNumber, CSVConfig cfg)
        {
            m_lineNum = lineNumber;
            mycfg = cfg;
        }
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                System.err.println(response.getStatusString());
                System.err.println("<xin>Stop at line " + m_lineNum);
                synchronized (errorMsg) {
                	if (!errorMsg.containsKey(m_lineNum)) {
                		errorMsg.put(m_lineNum, response.getStatusString());
                	}
                	if (errorMsg.size() >= mycfg.abortfailurecount) {
                		System.err.println("The number of Failure row data exceeds " + mycfg.abortfailurecount);
                		System.exit(1);
                	}
                }
                return;
            }
            
            long currentCount = inCount.incrementAndGet();

            System.out.println("Put line " + inCount.get() + " to databse");
            if (currentCount % reportEveryNRows == 0) {
                System.out.println("Inserted " + currentCount + " rows");
            }
        }
    }
    
    private static class CSVConfig extends CLIConfig {
    	@Option(desc = "directory path to produce report files.")
    	String inputfile = "";
    	
    	@Option(desc = "procedure name to insert the data into the database.")
    	String procedurename = "";
    	
    	@Option(desc = "insert the data into database by TABLENAME.INSERT procedure by default.")
    	String tablename = "";
    	
    	@Option(desc = "a placeholder for several options that will deal with delimiters, charsets, etc..")
    	String[] csvoptions = null;
    	
    	@Option(desc = ".")
    	boolean setSkipEmptyRecords = false;
    	
    	@Option(desc = ".")
    	boolean setTrimWhitespace = false;
    	
    	@Option(desc = "Maximum rows to be read of the csv file.")
    	int limitRows = Integer.MAX_VALUE;
    	
    	@Option(desc = "directory path to produce report files.")
    	String reportDir = "/Users/xinjia/progs/invalid.csv";
    	
    	@Option(desc = "")
    	int abortfailurecount =  100;
    	
    	
    	@Override
    	public void validate() {
    		if (abortfailurecount <= 0) exitWithMessageAndUsage("");
    		// add more checking
    		if (procedurename.equals("") && tablename.equals("") )
    			exitWithMessageAndUsage("procedure name or a table name required");
    		if (!procedurename.equals("") && !tablename.equals("") )
    			exitWithMessageAndUsage("Only a procedure name or a table name required, pass only one please");
    	}
    }
    
    public CSVLoader (CSVConfig cfg) {
    	this.config = cfg;
    	
    	if(!config.tablename.equals("")) {
    		insertProcedure = config.tablename + ".insert";
    	} else {
    		insertProcedure = config.procedurename;
    	}
    }
    
    public long run() {
    	int waits = 0;
        int shortWaits = 0;
        
    	try {
            final CSVReader reader = new CSVReader(new FileReader(config.inputfile));
            ProcedureCallback cb = null;

            final Client client = ClientFactory.createClient();
            client.createConnection("localhost");
            
            boolean lastOK = true;
            String line[] = null;

            while ((config.limitRows-- > 0) && (line = reader.readNext()) != null) {
            	long counter = outCount.incrementAndGet();
                boolean queued = false;
                while (queued == false) {
                    String[] correctedLine = line;
                    cb = new MyCallback(outCount.get(), config);
                    
                    // This message will be removed later
                    // print out the parameters right now
                    String msg = "<xin>params: ";
                    for (int i=0; i < correctedLine.length; i++) {
                    	msg += correctedLine[i] + ",";
                    }
                    System.out.println(msg);

                    String lineCheckResult;
                    if( (lineCheckResult = checkLineFormat(correctedLine, client))!= null){
                    	System.err.println("Stop at line " + (outCount.get()));
                    	synchronized (errorMsg) {
                    		if (!errorMsg.containsKey(outCount.get())) {
                    			errorMsg.put(outCount.get(),lineCheckResult);
                    		}
                    		if (errorMsg.size() >= config.abortfailurecount) {
                    			System.err.println("The number of Failure row data exceeds " + config.abortfailurecount);
                        		System.exit(1);
                    		}
                    	}
                    	break;
                    }
                    queued = client.callProcedure(cb, insertProcedure, (Object[])correctedLine);
                    if (queued == false) {
                        ++waits;
                        if (lastOK == false) {
                            ++shortWaits;
                        }
                        Thread.sleep(waitSeconds);
                    }
                    lastOK = queued;
                }
            }

            reader.close();
            client.drain();
            client.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Inserted " + outCount.get() + " and acknowledged " + inCount.get() + " rows (final)");
        if (waits > 0) {
            System.out.println("Waited " + waits + " times");
            if (shortWaits > 0) {
                System.out.println("Waited too briefly? " + shortWaits + " times");
            }
        }
        return inCount.get();
    	
    }
    
    
    
    /**
     * TODO(xin): add line number data into the callback and add the invalid line number 
     * into a list that will help us to produce a separate file to record the invalid line
     * data in the csv file.
     * 
     * Asynchronously invoking procedures to response the actual wrong line number and 
     * start from last wrong line in the csv file is not easy. You can not ensure the 
     * FIFO order of the callback.
     * @param args
     * @return long number of the actual rows acknowledged by the database server
     */
    

    public static long main(String[] args) {
    	//CSVLoader.config.abortfailurecount = 100;
        if (args.length < 1) {
            System.err.println("csv filename required");
            System.exit(1);
        }
        
    	CSVConfig cfg = new CSVConfig();
    	cfg.inputfile = args[0];
    	cfg.parse(CSVLoader.class.getName(), args);
    	
    	CSVLoader loader = new CSVLoader(cfg);
    	long result = loader.run();
    	loader.produceInvalidRowsFile();
        return result;
    }
    /**
     * Check for each line
     * TODO(zheng):
     * Use the client handler to get the schema of the table, and then check the number of
     * parameters it expects with the input line fragements.
     * Check the following:
     * 1.blank line
     * 2.# of attributes in the insertion procedure
     * And does other pre-checks...(figure out it later) 
     * @param linefragement
     */
    private static String checkLineFormat(Object[] linefragement, Client client ) {
    	String msg = "";
    	VoltTable[] results = null;
        int columnCnt = 0;
        VoltTable colInfo = null;
        
        int posOfDot = insertProcedure.indexOf(".");
        String tableName = insertProcedure.substring( 0, posOfDot );
        try {
        	colInfo = client.callProcedure("@SystemCatalog",
        			"COLUMNS").getResults()[0];
        	
        	while( colInfo.advanceRow() )
        	{
        		if( tableName.matches( (String) colInfo.get("TABLE_NAME", VoltType.STRING) ) )
        		{
        			columnCnt++;
           		}
        	}
        	
        }
        catch (Exception e) {
        	e.printStackTrace();
        }  
    	
    	if( linefragement.length != columnCnt )//# attributes not match including blank line
    		return msg;
    	else 
    		return null;
    }
    
    /**
	 * TODO(xin): produce the invalid row file from
	 * Bulk the flush later...
	 * @param inputFile
	 */
	private void produceInvalidRowsFile() {
		System.out.println("All the invalid row numbers are:" + errorMsg.keySet());
		String line = "";
		// TODO: add the inputFileName to the outputFileName
		
		String invaliderowsfile = config.reportDir + "invalidrows.csv";
		String logfile =  config.reportDir +"csvLoaderLogMessage.log";
    	String reportfile = config.reportDir  + "csvLoaderReport.log";
    	
		int bulkflush = 100; // by default right now
		try {
			BufferedReader csvfile = new BufferedReader(new FileReader(config.inputfile));
			
			BufferedWriter out_invaliderowfile = new BufferedWriter(new FileWriter(invaliderowsfile));

			BufferedWriter out_logfile = new BufferedWriter(new FileWriter(logfile));
			BufferedWriter out_reportfile = new BufferedWriter(new FileWriter(reportfile));
			long linect = 0;
			for (Long irow : errorMsg.keySet()) {
				while ((line = csvfile.readLine()) != null) {
					if (++linect == irow) {
						String message = "invalid line " + irow + ":" + line + "\n";
						System.err.print(message);
						out_invaliderowfile.write(message);
						out_logfile.write(message + errorMsg.get(irow).toString() + "\n"); 
						break;
					}
				}
				if (linect % bulkflush == 0) {
					out_invaliderowfile.flush();
					out_logfile.flush();
				}
			}
			out_reportfile.write("Number of failed tuples:" + errorMsg.size() + "\n");
			out_reportfile.write("Number of loaded tuples:" + (outCount.get() - errorMsg.size()) + "\n");
			
			out_invaliderowfile.flush();
			out_logfile.flush();
			out_reportfile.flush();
			out_invaliderowfile.close();
			out_logfile.close();
			out_reportfile.close();

		} catch (FileNotFoundException e) {
			System.err.println("CSV file '" + config.inputfile
					+ "' could not be found.");
		} catch (Exception x) {
			System.err.println(x.getMessage());
		}
    }
    
}
