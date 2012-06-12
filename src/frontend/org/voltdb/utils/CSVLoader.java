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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.ArrayUtils;
import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
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
public class CSVLoader {

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

    private static CSVReader csvReader;
    private static Client csvClient;
    private static String [] csvLine;
    
    private static BufferedWriter out_invaliderowfile;
    private static BufferedWriter out_logfile;
	private static BufferedWriter out_reportfile;
	
    public CSVLoader( String[] options ) {
    	CSVConfig cfg = new CSVConfig();
    	cfg.parse(CSVLoader.class.getName(), options);
    	
    	config = cfg;
    	configuration();
    	try {
    		if (CSVLoader.standin)
    			csvReader = new CSVReader(new BufferedReader(new InputStreamReader(System.in)),
    					config.separator, config.quotechar, config.escape, config.skipline,
    					config.strictQuotes, config.ignoreLeadingWhiteSpace);
    		else 
    			csvReader = new CSVReader(new FileReader(config.inputfile),
    					config.separator, config.quotechar, config.escape, config.skipline,
    					config.strictQuotes, config.ignoreLeadingWhiteSpace);

            csvClient = ClientFactory.createClient();
            csvClient.createConnection("localhost");
    	} catch (Exception e) {
            e.printStackTrace();
        }
    	
    	String path_invaliderowsfile = config.reportdir + CSVLoader.invaliderowsfile;
		String path_logfile =  config.reportdir + CSVLoader.logfile;
    	String path_reportfile = config.reportdir  + CSVLoader.reportfile;
		try {
			 out_invaliderowfile = new BufferedWriter(new FileWriter(path_invaliderowsfile));
			 out_logfile = new BufferedWriter(new FileWriter(path_logfile));
			 out_reportfile = new BufferedWriter(new FileWriter(path_reportfile)); 
		}catch (FileNotFoundException e) {
			System.err.println("CSV file '" + config.inputfile
					+ "' could not be found.");
		} catch (Exception x) {
			System.err.println(x.getMessage());
		}
    }
    
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
    		if (skipline < 0) exitWithMessageAndUsage("skipline must be >= 0");
    		if (limitrows > Integer.MAX_VALUE) exitWithMessageAndUsage("limitrows to read must be < " + Integer.MAX_VALUE);
    		if (port < 0) exitWithMessageAndUsage("port number must be >= 0");
    	}
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
    	int waits = 0;
        int shortWaits = 0;
        new CSVLoader( args );
        
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
            
            int columnCnt = 0;
            VoltTable procInfo = null;
            //Vector<Integer> strColIndex = new Vector<Integer>();
            try {
            	procInfo = client.callProcedure("@SystemCatalog",
                "PROCEDURECOLUMNS").getResults()[0];
                while( procInfo.advanceRow() ) {
                	if( insertProcedure.matches( (String) procInfo.get("PROCEDURE_NAME", VoltType.STRING) ) ) {
                			//if( procInfo.get( "TYPE_NAME", VoltType.STRING ).toString().matches("VARCHAR") )
                				//strColIndex.add( Integer.parseInt( procInfo.get( "ORDINAL_POSITION", VoltType.INTEGER ).toString()) - 1 );
                			
                		columnCnt++;
                	}
                }
             }
             catch (Exception e) {
                e.printStackTrace();
             }

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
                     
                    if( (lineCheckResult = checkLineFormat(correctedLine, columnCnt))!= null){
                    	System.err.println("<zheng>Stop at line " + (outCount.get()) + lineCheckResult );
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
            client.drain();
            
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
    	produceFiles();
    	flush();
    }
    
    //works with insertLine
    //return true if next line of csv file is not null, load the line into this.csvLine.
    public static boolean readNext() throws IOException {
    	if ( (config.limitrows-- > 0) && (csvLine = csvReader.readNext()) != null )
    		return true;
    	else{
    		return false;
    	}
    }
    
    //insert the current line read from csv file ( read by readNext ). 
    //So this is a break down of main(), can be used to attach additional columns to each line.
    //invoke drain() before produceFile() while you use insertLine() with readNext()
    public static String[] insertLine( String[] additionalStr, int columnCnt ) throws NoConnectionsException, IOException, InterruptedException {
    	int waits = 0;
        int shortWaits = 0;
        String[] correctedLine = null;

            boolean lastOK = true;

              	outCount.incrementAndGet();
                boolean queued = false;
                while (queued == false) {
                	StringBuilder linedata = new StringBuilder();
                	
                	csvLine = ArrayUtils.addAll(csvLine, additionalStr);
                	
                    for(String s : csvLine) 
                    	linedata.append(s);
                	
                	correctedLine = csvLine;
                    
                    MyCallback cb = new MyCallback(outCount.get(), config, linedata.toString());             
                    String lineCheckResult;

                    if( (lineCheckResult = checkLineFormat(correctedLine, columnCnt))!= null){
                    System.err.println("<zheng>Stop at line " + (outCount.get()) + lineCheckResult );
                    synchronized (errorInfo) {
                    	if (!errorInfo.containsKey(outCount.get())) {
                    		String[] info = {linedata.toString(), lineCheckResult};
                    		errorInfo.put(outCount.get(), info);
                    	}
                    	if (errorInfo.size() >= config.abortfailurecount) {
                    		System.err.println("The number of Failure row data exceeds " + config.abortfailurecount);
                        	System.exit(1);
                    	}
                    }
                    break;
                    }
                    queued = csvClient.callProcedure(cb, insertProcedure, (Object[])correctedLine);
                    if (queued == false) {
                        ++waits;
                        if (lastOK == false) {
                            ++shortWaits;
                        }
                        Thread.sleep(waitSeconds);
                    }
                    lastOK = queued;
            }

        System.out.println("Inserted " + outCount.get() + " and acknowledged " + inCount.get() + " rows (final)");
        if (waits > 0) {
            System.out.println("Waited " + waits + " times");
            if (shortWaits > 0) {
                System.out.println("Waited too briefly? " + shortWaits + " times");
            }
        }
        return correctedLine;
    }
    
    private static String checkLineFormat(String[] linefragement, int columnCnt ) {
    	String msg = "";
        
         if( linefragement.length == 1 && linefragement[0].equals( "" ) ) {
        		msg = "checkLineFormat Error: blank line";
        		return msg;
         }
        if( linefragement.length != columnCnt ){
        	msg = "checkLineFormat Error: # of attributes do not match, # of attributes needed: "+columnCnt;
        	return msg;
        }
        
        else {
        	for(int i=0; i<linefragement.length;i++) {
        		//trim white space in this line.
        		linefragement[i] = linefragement[i].trim();
        		if ((linefragement[i]).equals("NULL"))
        			linefragement[i] = null;
        	}
        } 
        return null;
    }
    
    private static void configuration () {
    	if (config.inputfile.equals("")) 
			standin = true;
        if(!config.tablename.equals("")) {
    		insertProcedure = config.tablename + ".insert";
    	} else {
    		insertProcedure = config.procedurename;
    	}
        if (!config.reportdir.endsWith("/")) 
        	config.reportdir += "/";
        try {
        	File dir = new File(config.reportdir);
        	if (!dir.exists()) {
        		dir.mkdirs();
        	}
        } catch (Exception x) {
        	System.err.println(x.getMessage());
        }
    }
    
    public static Client getClient(ClientConfig config, String[] servers, int port) throws Exception
    {
        final Client client = ClientFactory.createClient(config);

        for (String server : servers)
            client.createConnection(server.trim(), port);
        return client;
    }
 
	public static void produceFiles() { 
		String myinsert = insertProcedure;
		myinsert = myinsert.replaceAll("\\.", "_");
		String path_invalidrowfile = config.reportdir + myinsert + "_"+ CSVLoader.invaliderowsfile;
		String path_logfile =  config.reportdir + myinsert + "_"+ CSVLoader.logfile;
    	String path_reportfile = config.reportdir  + myinsert + "_"+ CSVLoader.reportfile;
    	
		int bulkflush = 300; // by default right now
		try {
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
			
			System.out.println("invalid row file is generated to:" + path_invalidrowfile + "\n"
					+ "log file is generated to:" + path_logfile + "\n"
					+ "report file is generated to:" + path_reportfile);
			
			out_invaliderowfile.flush();
			out_logfile.flush();
			out_reportfile.flush();
		} catch (FileNotFoundException e) {
			System.err.println("CSV report directory '" + config.reportdir
					+ "' does not exist.");
		} catch (Exception x) {
			System.err.println(x.getMessage());
		}
		
    }
	
	public static void drain() throws NoConnectionsException, InterruptedException {
		csvClient.drain();
	}
	
	public static void flush() throws IOException, InterruptedException {
		inCount.set( 0 );
		outCount.set( 0 );
		errorInfo.clear();
		
		csvReader.close();
        csvClient.close();
		out_invaliderowfile.close();
		out_logfile.close();
		out_reportfile.close();
	}
	
}
