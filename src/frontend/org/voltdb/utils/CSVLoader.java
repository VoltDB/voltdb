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
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import au.com.bytecode.opencsv_voltpatches.CSVParser;
import au.com.bytecode.opencsv_voltpatches.CSVReader;

/**
 * CSVLoader is a simple utility to load data from a CSV formatted file to a table
 * (or pass it to any stored proc, but ignoring any result other than the success code.).
 *
 * TODO:
 *   - No associated test suite.
 *   - Forces JVM into UTC. All input date TZs assumed to be GMT+0
 *   - Requires canonical JDBC SQL timestamp format
 */
class CSVLoader {

	public synchronized static void setDefaultTimezone() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));
    }

    private static final AtomicLong inCount = new AtomicLong(0);
    private static final AtomicLong outCount = new AtomicLong(0);
    
    private static int reportEveryNRows = 10000;
    private static int waitSeconds = 10;
    
    private static CSVConfig config = null;
    private static long latency = -1;
	private static boolean standin = false;
    
    public static final String invaliderowsfile = "csvloaderinvalidrows.csv";
    public static final String reportfile = "csvloaderReport.log";
    public static final String logfile = "csvloaderLog.log";
    private static String insertProcedure = "";
    private static Map <Long,String[]> errorInfo = new TreeMap<Long, String[]>();

    private static final class MyCallback implements ProcedureCallback {
    	private final long m_lineNum;
    	private final CSVConfig m_config;
    	private final String m_rowdata;
    	MyCallback(long lineNumber, CSVConfig cfg, String rowdata)
    	{
    		m_lineNum = lineNumber;
    		m_config = cfg;
    		m_rowdata = rowdata;
    	}
    	@Override
    	public void clientCallback(ClientResponse response) throws Exception {
    		if (response.getStatus() != ClientResponse.SUCCESS) {
    			System.err.println(response.getStatusString());
    			System.err.println("<xin>Stop at line " + m_lineNum);
    			synchronized (errorInfo) {
    				if (!errorInfo.containsKey(m_lineNum)) {
    					String[] info = {m_rowdata, response.getStatusString()};
    					errorInfo.put(m_lineNum, info);
    				}
    				if (errorInfo.size() >= m_config.abortfailurecount) {
    					System.err.println("The number of Failure row data exceeds " + m_config.abortfailurecount);
    					flush();
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
    	
    	@Option(desc = "maximum rows to be read of the csv file.")
    	int limitrows = Integer.MAX_VALUE;
    	
    	@Option(desc = "directory path to produce report files.")
    	String reportdir ="./";
    	
    	@Option(desc = "stop the process after NUMBER confirmed failures. The actual number of failures may be much higher.")
    	int abortfailurecount =  100;
    	
    	@Option(desc = "the delimiter to use for separating entries.")
    	char separator = CSVParser.DEFAULT_SEPARATOR;
    	
    	@Option(desc = "the character to use for quoted elements.")
    	char quotechar = CSVParser.DEFAULT_QUOTE_CHARACTER;
    	
    	@Option(desc = "the character to use for escaping a separator or quote.")
    	char escape = CSVParser.DEFAULT_ESCAPE_CHARACTER;
    	
    	@Option(desc = "sets if characters outside the quotes are ignored.")
    	boolean strictQuotes = CSVParser.DEFAULT_STRICT_QUOTES;
    	
    	@Option(desc = "the line number to skip for start reading.")
    	int skipline = CSVReader.DEFAULT_SKIP_LINES;
    	
    	@Option(desc = "the default leading whitespace behavior to use if none is supplied.")
    	boolean ignoreLeadingWhiteSpace = CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE;
    	
    	@Option(desc = "the servers to be connected")
    	String servers = "localhost";
    	
    	@Option(desc = "user name that is used to connect to the servers,by defalut null")
    	String user = "";
    	
    	@Option(desc = "password for this user to use to connect the servers,by defalut null")
    	String password = "";
    	
    	@Option(desc = "port to be used for the servers right now")
    	int port = Client.VOLTDB_SERVER_PORT;
    	
    	@Override
    	public void validate() {
    		if (abortfailurecount < 0) exitWithMessageAndUsage("abortfailurecount must be >=0");
    		if (procedurename.equals("") && tablename.equals("") )
    			exitWithMessageAndUsage("procedure name or a table name required,but only one of them is fine");
    		if (!procedurename.equals("") && !tablename.equals("") )
    			exitWithMessageAndUsage("Only a procedure name or a table name required, pass only one please");
    		if (inputfile.equals("")) 
    			standin = true;
    		if (skipline < 0) exitWithMessageAndUsage("skipline must be >= 0");
    		if (limitrows > Integer.MAX_VALUE) exitWithMessageAndUsage("limitrows to read must be < " + Integer.MAX_VALUE);
    		if (port < 0) exitWithMessageAndUsage("port number must be >= 0");
    	}
    }
    
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        
        config = new CSVConfig();
    	
        config.parse(CSVLoader.class.getName(), args);
        if(!config.tablename.equals("")) {
    		insertProcedure = config.tablename + ".insert";
    	} else {
    		insertProcedure = config.procedurename;
    	}
    	int waits = 0;
        int shortWaits = 0;
        
    	try {
    		final CSVReader reader;
    		if (CSVLoader.standin)
    			reader = new CSVReader(new BufferedReader(new InputStreamReader(System.in)),
    					config.separator, config.quotechar, config.escape, config.skipline,
    					config.strictQuotes, config.ignoreLeadingWhiteSpace);
    		else 
    			reader = new CSVReader(new FileReader(config.inputfile),
    					config.separator, config.quotechar, config.escape, config.skipline,
    					config.strictQuotes, config.ignoreLeadingWhiteSpace);
            ProcedureCallback cb = null;

            // Split server list
            String[] serverlist = config.servers.split(",");
            
            // Create connection
            ClientConfig c_config = new ClientConfig(config.user,config.password);
            c_config.setProcedureCallTimeout(0);  // Set procedure all to infinite timeout, see ENG-2670
            final Client client = CSVLoader.getClient(c_config, serverlist, config.port);
            
            boolean lastOK = true;
            String line[] = null;

            while ((config.limitrows-- > 0) && (line = reader.readNext()) != null) {
            	outCount.incrementAndGet();
                boolean queued = false;
                while (queued == false) {
                	StringBuilder linedata = new StringBuilder();
                    for(int i = 0; i < line.length; i++) {
                    	linedata.append(line[i]);
                    	if (i != line.length -1)
                    		linedata.append(",");
                    }
                	String[] correctedLine = line;
                    cb = new MyCallback(outCount.get(), config, linedata.toString());
                    String lineCheckResult;
                    if( (lineCheckResult = checkLineFormat(correctedLine, client))!= null){
                    	System.err.println("<zheng>Stop at line " + (outCount.get()));
                    	synchronized (errorInfo) {
                    		if (!errorInfo.containsKey(outCount.get())) {
                    			String[] info = {linedata.toString(), lineCheckResult};
                    			errorInfo.put(outCount.get(), info);
                    		}
                    		if (errorInfo.size() >= config.abortfailurecount) {
                    			System.err.println("The number of Failure row data exceeds " + config.abortfailurecount);
                    			flush();
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
    	
        latency = System.currentTimeMillis()-start;
        System.out.println("CSVLoader elaspsed: " + latency/1000F + " seconds");
    	CSVLoader.produceFiles();
    	flush();
    }
    
    public static Client getClient(ClientConfig config, String[] servers, int port) throws Exception
    {
        final Client client = ClientFactory.createClient(config);

        for (String server : servers)
            client.createConnection(server.trim(), port);
        return client;
    }
    
    private static String checkLineFormat(String[] linefragement, Client client ) {
    	String msg = "";
    	int columnCnt = 0;
    	VoltTable procInfo = null;
    	Vector<Integer> strColIndex = new Vector<Integer>();

    	try {
    		procInfo = client.callProcedure("@SystemCatalog",
    				"PROCEDURECOLUMNS").getResults()[0];

    		while( procInfo.advanceRow() ) {
    			if( insertProcedure.matches( (String) procInfo.get("PROCEDURE_NAME", VoltType.STRING) ) ) {
    				if( procInfo.get( "TYPE_NAME", VoltType.STRING ).toString().matches("VARCHAR") )
    					strColIndex.add( Integer.parseInt( procInfo.get( "ORDINAL_POSITION", VoltType.INTEGER ).toString()) - 1 );
    				columnCnt++;

    			}
    		}
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}

    	if( linefragement.length == 1 && linefragement[0].equals( "" ) ) {
    		msg = "checkLineFormat Error: blank line";
    		return msg;
    	}
    	if( linefragement.length != columnCnt ){
    		msg = "checkLineFormat Error: # of attributes do not match, # of attributes needed: "+columnCnt;
    		return msg;
    	} else {
    		 for(int i=0; i<linefragement.length;i++) {
     			//trim white space in this line always.
    			 linefragement[i] = linefragement[i].trim();

     			// treat "\N", \N and NULL as actual null value for each data type later
     			// CSVReader will trim \ by default
     			if ((linefragement[i]).equals("NULL"))
     				linefragement[i] = null;
     		}
    	} 
    	return null;
    }
    
	private static void produceFiles() {
		System.out.println("All the invalid row numbers are:" + errorInfo.keySet());

		String path_invaliderowsfile = config.reportdir + CSVLoader.invaliderowsfile;
		String path_logfile =  config.reportdir + CSVLoader.logfile;
    	String path_reportfile = config.reportdir  + CSVLoader.reportfile;
    	
		int bulkflush = 300; // by default right now
		try {
			BufferedWriter out_invaliderowfile = new BufferedWriter(new FileWriter(path_invaliderowsfile));

			BufferedWriter out_logfile = new BufferedWriter(new FileWriter(path_logfile));
			BufferedWriter out_reportfile = new BufferedWriter(new FileWriter(path_reportfile));
			long linect = 0;

			for (Long irow : errorInfo.keySet()) {
				String info[] = errorInfo.get(irow);
				if (info.length != 2)
					System.out.println("internal error, infomation is not enough");
				linect++;
				out_invaliderowfile.write(info[0] + "\n");
				String message = "invalid line " + irow + ":  " + info[0] + "\n";
				System.err.print(message);
				out_logfile.write(message + info[1] + "\n"); 
				if (linect % bulkflush == 0) {
					out_invaliderowfile.flush();
					out_logfile.flush();
				}
			}
			// Get elapsed time in seconds
			float elapsedTimeSec = latency / 1000F;
			out_reportfile.write("CSVLoader elaspsed: " + elapsedTimeSec + " seconds\n");
			out_reportfile.write("Number of tuples tring to insert:" + outCount.get() + "\n");
			out_reportfile.write("Number of failed tuples:" + errorInfo.size() + "\n");
			out_reportfile.write("Number of acknowledged tuples:     " + inCount.get() + "\n");
			out_reportfile.write("CSVLoader rate: " + outCount.get() / elapsedTimeSec + " row/s\n");

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
	
	public static void flush() {
		inCount.set( 0 );
		outCount.set( 0 );
		errorInfo.clear();
	}
}