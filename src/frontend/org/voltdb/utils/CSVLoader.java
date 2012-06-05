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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.String;

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

//    public synchronized static void setDefaultTimezone() {
//        TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));
//    }

    private static final AtomicLong inCount = new AtomicLong(0);
    private static final AtomicLong outCount = new AtomicLong(0);
    
    private static int reportEveryNRows = 10000;
    private static int waitSeconds = 10;
    
    final CSVConfig config;
    private static String insertProcedure = "";
    private static List<Long> invalidLines = new ArrayList<Long>();

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
                synchronized (invalidLines) {
                	if (!invalidLines.contains(m_lineNum))
                		invalidLines.add(m_lineNum);
                	if (invalidLines.size() >= mycfg.abortfailurecount) {
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
    
    /**
	 * TODO(xin): produce the invalid row file from
	 * 
	 * @param inputFile
	 */
	private void produceInvalidRowsFile() {
		Collections.sort(invalidLines);
		System.out.println("All the invalid row numbers are:" + invalidLines);
		String line = "";
		try {
			FileWriter fstream = new FileWriter(config.reportDir);
			BufferedWriter out = new BufferedWriter(fstream);
			BufferedReader csvfile = new BufferedReader(new FileReader(config.inputfile));

			long linect = 0;
			for (Long irow : invalidLines) {
				while ((line = csvfile.readLine()) != null) {
					if (++linect == irow) {
						out.write(line);
						out.write("\n");
						System.err.println("invalid row:" + line);
						break;
					}
				}

			}
			out.flush();
			out.close();

		} catch (FileNotFoundException e) {
			System.err.println("CSV file '" + config.inputfile
					+ "' could not be found.");
		} catch (Exception x) {
			System.err.println(x.getMessage());
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
                    boolean setTrimWhiteSpace = true;
                    boolean setSkipEmptyRecords = true;
                    if(!checkLineFormat(correctedLine, client, insertProcedure, setSkipEmptyRecords, setTrimWhiteSpace )){
                    	System.err.println("Stop at line " + (outCount.get()));
                    	synchronized (invalidLines) {
                    		if (!invalidLines.contains(outCount.get())) {
                    			invalidLines.add(outCount.get());
                    		}
                    		if (invalidLines.size() >= config.abortfailurecount) {
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
    private static boolean checkLineFormat(Object[] linefragement, 
    									   Client client,
    									   final String insertProcedure,
    									   boolean setSkipEmptyRecords,
    									   boolean setTrimWhitespace ) {
      	VoltTable colInfo = null;
        int columnCnt = 0;
        
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
    	
        if( linefragement.length == 0 && !setSkipEmptyRecords )
  		{
        	for( int i = 0; i < columnCnt; i++)
        		linefragement[ i ] = "";
        	return true;
  		}
        
    	if( linefragement.length != columnCnt )//# attributes not match
    		return false;
    	
    	else if( setTrimWhitespace )
    	{//trim white space for non in this line.
    		for(int i=0; i<linefragement.length;i++) {
    			linefragement[i] = ((String)linefragement[i]).replaceAll( "\\s+", "" );
    		}
    	} 
    	return true;
    }
    
}
