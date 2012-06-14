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
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
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
 */
public class CSVLoader {

	private static final AtomicLong inCount = new AtomicLong(0);
	private static final AtomicLong outCount = new AtomicLong(0);
	private static final int reportEveryNRows = 10000;
	private static final int waitSeconds = 10;

	private static CSVConfig config = null;
	private static long latency = -1;
	private static boolean standin = false;

	public static String pathInvalidrowfile = "";
	public static String pathReportfile = "csvloaderReport.log";
	public static String pathLogfile = "csvloaderLog.log";

	private static BufferedWriter out_invaliderowfile;
	private static BufferedWriter out_logfile;
	private static BufferedWriter out_reportfile;

	private static String insertProcedure = "";
	private static Map <Long,String[]> errorInfo = new TreeMap<Long, String[]>();

	private static CSVReader csvReader;
	private static Client csvClient;
	private static ArrayList<String> firstIds = new ArrayList<String>();

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
					if (errorInfo.size() >= m_config.maxerrors) {
						System.err.println("The number of Failure row data exceeds " + m_config.maxerrors);
						close_cleanup();
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
		@Option(shortOpt = "f",desc = "directory path to produce report files.")
		String file = "";

		@Option(shortOpt = "p",desc = "procedure name to insert the data into the database.")
		String procedure = "";

		@Option(desc = "maximum rows to be read of the csv file.")
		int limitrows = Integer.MAX_VALUE;

		@Option(shortOpt = "r",desc = "directory path to produce report files.")
		String reportdir ="./";

		@Option(desc = "stop the process after NUMBER confirmed failures. The actual number of failures may be much higher.")
		int maxerrors =  100;

		@Option(desc = "the delimiter to use for separating entries.")
		char separator = CSVParser.DEFAULT_SEPARATOR;

		@Option(desc = "the character to use for quoted elements.")
		char quotechar = CSVParser.DEFAULT_QUOTE_CHARACTER;

		@Option(desc = "the character to use for escaping a separator or quote.")
		char escape = CSVParser.DEFAULT_ESCAPE_CHARACTER;

		@Option(desc = "sets if characters outside the quotes are ignored.", hasArg = false)
		boolean strictquotes = CSVParser.DEFAULT_STRICT_QUOTES;

		@Option(desc = "the line number to skip for start reading.")
		int skip = CSVReader.DEFAULT_SKIP_LINES;

		@Option(desc = "the default leading whitespace behavior to use if none is supplied.", hasArg = false)
		boolean nowhitespace = !CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE;

		@Option(desc = "the servers to be connected")
		String servers = "localhost";

		@Option(shortOpt = "u",desc = "user name that is used to connect to the servers,by defalut null")
		String user = "";

		@Option(shortOpt = "pw",desc = "password for this user to use to connect the servers,by defalut null")
		String password = "";

		@Option(desc = "port to be used for the servers right now")
		int port = Client.VOLTDB_SERVER_PORT;


		@Option(desc = "skip format checking when loading the csv file on the client")
		boolean skipPreChecks = false;

		@AdditionalArgs(desc = "insert the data into database by TABLENAME.INSERT procedure by default.")
		String table = "";

		@Override
		public void validate() {
			if (maxerrors < 0) exitWithMessageAndUsage("abortfailurecount must be >=0");
			if (procedure.equals("") && table.equals("") )
				exitWithMessageAndUsage("procedure name or a table name required");
			if (!procedure.equals("") && !table.equals("") )
				exitWithMessageAndUsage("Only a procedure name or a table name required, pass only one please");
			if (skip < 0) exitWithMessageAndUsage("skipline must be >= 0");
			if (limitrows > Integer.MAX_VALUE) exitWithMessageAndUsage("limitrows to read must be < " + Integer.MAX_VALUE);
			if (port < 0) exitWithMessageAndUsage("port number must be >= 0");
		}

		@Override
		public void printUsage() {
			System.out.println("Semantics: csvloader [args] tablename or csvloader [args] -p procedurename");
			super.printUsage();
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		long start = System.currentTimeMillis();
		int waits = 0;
		int shortWaits = 0;

		CSVConfig cfg = new CSVConfig();
		cfg.parse(CSVLoader.class.getName(), args);

		config = cfg;
		configuration();
		try {
			if (CSVLoader.standin)
				csvReader = new CSVReader(new BufferedReader(new InputStreamReader(System.in)),
						config.separator, config.quotechar, config.escape, config.skip,
						config.strictquotes, config.nowhitespace);
			else 
				csvReader = new CSVReader(new FileReader(config.file),
						config.separator, config.quotechar, config.escape, config.skip,
						config.strictquotes, config.nowhitespace);

		} catch (FileNotFoundException e) {
			System.err.println("CSV file '" + config.file
					+ "' could not be found.");
			System.exit(1);
		}
		// Split server list
		String[] serverlist = config.servers.split(",");

		// Create connection
		ClientConfig c_config = new ClientConfig(config.user,config.password);
		c_config.setProcedureCallTimeout(0);  // Set procedure all to infinite timeout, see ENG-2670
		try {
			csvClient = CSVLoader.getClient(c_config, serverlist, config.port);
		} catch (Exception e) {
			System.err.println("Error to connect to the servers:" + config.servers);
			System.exit(1);
		}

		try {
			ProcedureCallback cb = null;

			boolean lastOK = true;
			String line[] = null;

			int columnCnt = 0;
			VoltTable procInfo = null;
			try {
				procInfo = csvClient.callProcedure("@SystemCatalog",
						"PROCEDURECOLUMNS").getResults()[0];
				while( procInfo.advanceRow() ) {
					if( insertProcedure.matches( (String) procInfo.get("PROCEDURE_NAME", VoltType.STRING) ) )
						columnCnt++;
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}

			while ((config.limitrows-- > 0) && (line = csvReader.readNext()) != null) {
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

					if( !config.skipPreChecks && (lineCheckResult = checkparams_trimspace(correctedLine, columnCnt))!= null){
						synchronized (errorInfo) {
							if (!errorInfo.containsKey(outCount.get())) {
								String[] info = {linedata.toString(), lineCheckResult};
								errorInfo.put(outCount.get(), info);
							}
							if (errorInfo.size() >= config.maxerrors) {
								System.err.println("The number of Failure row data exceeds " + config.maxerrors);
								close_cleanup();
								System.exit(1);
							}
						}
						break;
					}

					queued = csvClient.callProcedure(cb, insertProcedure, (Object[])correctedLine);
					firstIds.add( correctedLine[0] );

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
			csvClient.drain();

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
		close_cleanup();
	}

	public static ArrayList<String> getFirstIds() {
		return firstIds;
	}

	private static String checkparams_trimspace(String[] linefragement, int columnCnt ) {
		if( linefragement.length == 1 && linefragement[0].equals( "" ) ) {
			System.err.println("<zheng>Stop at line " + (outCount.get()) );
			return "Error: blank line";
		}
		if( linefragement.length != columnCnt ){
			System.err.println("<zheng>Stop at line " + (outCount.get()) );
			return "Error: # of attributes do not match, # of attributes needed: " + columnCnt + "# of attributes inputed: " + linefragement.length;
		}

		for(int i=0; i<linefragement.length;i++) {
			//trim white space in this line.
			linefragement[i] = linefragement[i].trim();
			if ((linefragement[i]).equals("NULL"))
				linefragement[i] = null;
		}

		return null;
	}

	private static void configuration () {
		if (config.file.equals("")) 
			standin = true;
		if(!config.table.equals("")) {
			insertProcedure = config.table + ".insert";
		} else {
			insertProcedure = config.procedure;
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
			x.printStackTrace();
			System.exit(1);
		}

		String myinsert = insertProcedure;
		myinsert = myinsert.replaceAll("\\.", "_");
		pathInvalidrowfile = config.reportdir + myinsert + "_" + "csvloaderinvalidrows.csv";
		pathLogfile =  config.reportdir + myinsert + "_"+ "csvloaderReport.log";
		pathReportfile = config.reportdir  + myinsert + "_"+ "csvloaderLog.log";

		try {
			out_invaliderowfile = new BufferedWriter(new FileWriter(pathInvalidrowfile));
			out_logfile = new BufferedWriter(new FileWriter(pathLogfile));
			out_reportfile = new BufferedWriter(new FileWriter(pathReportfile)); 
		} catch (IOException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}

	private static Client getClient(ClientConfig config, String[] servers, int port) throws Exception {
		final Client client = ClientFactory.createClient(config);

		for (String server : servers)
			client.createConnection(server.trim(), port);
		return client;
	}

	private static void produceFiles() { 

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

			System.out.println("invalid row file is generated to:" + pathInvalidrowfile + "\n"
					+ "log file is generated to:" + pathLogfile + "\n"
					+ "report file is generated to:" + pathReportfile);

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

	private static void close_cleanup() throws IOException, InterruptedException {
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
