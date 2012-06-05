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

    public synchronized static void setDefaultTimezone() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));
    }

    private static final AtomicLong inCount = new AtomicLong(0);
    private static final AtomicLong outCount = new AtomicLong(0);

    private static int reportEveryNRows = 10000;
    private static int limitRows = Integer.MAX_VALUE;
    private static int skipRows = 0;
    private static int auditRows = 0;
    private static int waitSeconds = 10;
    private static boolean stripQuotes = false;
    private static int[] colProjection = null;
    
    private static List<Long> invalidLines = new ArrayList<Long>();

    private static final class MyCallback implements ProcedureCallback {
        private final long m_lineNum;
        MyCallback(long lineNumber)
        {
            m_lineNum = lineNumber;
        }
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
//                if (m_lineNum == 0) {
//                    System.err.print("Line ~" + inCount.get() + "-" + outCount.get() + ":");
//                } else {
//                    System.err.print("Line " + m_lineNum + ":");
//                }
            	
                
                System.err.println(response.getStatusString());
                System.err.println("Stop at line " + (inCount.get()));
                synchronized (invalidLines) {
                	if (!invalidLines.contains(m_lineNum))
                		invalidLines.add(m_lineNum);
                }
                //System.exit(1);
                return;
            }
            
            long currentCount = inCount.incrementAndGet();
            System.out.println("<xin> put line " + inCount.get() + " to databse");
             
            if (currentCount % reportEveryNRows == 0) {
                System.out.println("Inserted " + currentCount + " rows");
            }
        }
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
        if (args.length < 2) {
            System.err.println("Two arguments, csv filename and insert procedure name, required");
            System.exit(1);
        }

        final String filename = args[0];
        final String insertProcedure = args[1];
        int argsUsed = 2;

        processCommandLineOptions(argsUsed, args);

        int waits = 0;
        int shortWaits = 0;

        try {
            final CSVReader reader = new CSVReader(new FileReader(filename));
            //final ProcedureCallback oneCallbackFitsAll = new MyCallback(0);
            ProcedureCallback cb = null;

            final Client client = ClientFactory.createClient();
            client.createConnection("localhost");
            
            boolean lastOK = true;
            String line[] = null;

            for (int i = 0; i < skipRows; ++i) {
                reader.readNext();
                // Keep these sync'ed with line numbers.
                outCount.incrementAndGet();
                inCount.incrementAndGet();
            }

            while ((limitRows-- > 0) && (line = reader.readNext()) != null) {
                long counter = outCount.incrementAndGet();
                boolean queued = false;
                while (queued == false) {
                    String[] correctedLine = line;
                    if (colProjection != null) {
                    	System.out.println(String.format("colProject:%s | correctedLine: %s", colProjection, correctedLine));
                        correctedLine = projectColumns(colProjection, correctedLine);
                    }

                    if (stripQuotes) {
                        correctedLine = stripMatchingColumnQuotes(correctedLine);
                    }

                    if (auditRows > 0) {
                        --auditRows;
                        System.err.println(joinIntoString(", ", line));
                        System.err.println(joinIntoString(", ", correctedLine));
                        System.err.println("----------");
                        cb = new MyCallback(counter);
                    } else {
                        cb = new MyCallback(outCount.get());
                    }
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
            
            produceInvalidRowsFile(filename, "/home/zhengli/invalidrows.csv");            
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Inserted " + (outCount.get() - skipRows) + " and acknowledged " + (inCount.get() - skipRows) + " rows (final)");
        if (waits > 0) {
            System.out.println("Waited " + waits + " times");
            if (shortWaits > 0) {
                System.out.println("Waited too briefly? " + shortWaits + " times");
            }
        }
        
        return inCount.get() - skipRows;
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
    
	/**
	 * TODO(xin): produce the invalid row file from
	 * 
	 * @param inputFile
	 */
	private static void produceInvalidRowsFile(String inputFile,
			String outputFile) {
		Collections.sort(invalidLines);
		System.out.println("All the invalid row numbers are:" + invalidLines);
		String line = "";
		try {
			FileWriter fstream = new FileWriter(outputFile);
			BufferedWriter out = new BufferedWriter(fstream);
			BufferedReader csvfile = new BufferedReader(new FileReader(inputFile));

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
			System.err.println("CSV file '" + inputFile
					+ "' could not be found.");
		} catch (Exception x) {
			System.err.println(x.getMessage());
		}

    }
    
    private static void processCommandLineOptions(int argsUsed, String args[]) {
        final String columnsMatch = "--columns";
        final String stripMatch = "--stripquotes";
        final String waitMatch = "--wait";
        final String auditMatch = "--audit";
        final String limitMatch = "--limit";
        final String skipMatch = "--skip";
        final String reportMatch = "--report";
        final String columnsStyle = "comma-separated-zero-based-column-numbers";

        while (argsUsed < args.length) {

            final String optionPrefix = args[argsUsed++];

            if (optionPrefix.equalsIgnoreCase(columnsMatch)) {
                if (argsUsed < args.length) {
                    final String colsListed = args[argsUsed++];
                    final String[] cols = colsListed.split(",");
                    if (cols != null && cols.length > 0) {
                        colProjection = new int[cols.length];
                        for (int i = 0; i < cols.length; i++) {
                            try {
                                colProjection[i] = Integer.parseInt(cols[i]);
                                continue;
                            } catch (NumberFormatException e) {
                            }
                        }
                        if (colProjection.length == cols.length) {
                            continue;
                        }
                    }
                }
            } else if (optionPrefix.equalsIgnoreCase(waitMatch)) {
                if (argsUsed < args.length) {
                    try {
                        waitSeconds = Integer.parseInt(args[argsUsed++]);
                        if (waitSeconds >= 0) {
                            continue;
                        }
                    } catch (NumberFormatException e) {
                    }
                }
            } else if (optionPrefix.equalsIgnoreCase(auditMatch)) {
                if (argsUsed < args.length) {
                    try {
                        auditRows = Integer.parseInt(args[argsUsed++]);
                        continue;
                    } catch (NumberFormatException e) {
                    }
                }
            } else if (optionPrefix.equalsIgnoreCase(limitMatch)) {
                if (argsUsed < args.length) {
                    try {
                        limitRows = Integer.parseInt(args[argsUsed++]);
                        continue;
                    } catch (NumberFormatException e) {
                    }
                }
            } else if (optionPrefix.equalsIgnoreCase(skipMatch)) {
                if (argsUsed < args.length) {
                    try {
                        skipRows = Integer.parseInt(args[argsUsed++]);
                        continue;
                    } catch (NumberFormatException e) {
                    }
                }
            } else if (optionPrefix.equalsIgnoreCase(reportMatch)) {
                if (argsUsed < args.length) {
                    try {
                        reportEveryNRows = Integer.parseInt(args[argsUsed++]);
                        if (reportEveryNRows > 0) {
                            continue;
                        }
                    } catch (NumberFormatException e) {
                    }
                }
            } else if (optionPrefix.equalsIgnoreCase(stripMatch)) {
                stripQuotes = true;
                continue;
            }
            // Fall through means an error.
            System.err.println("Option arguments are invalid, expected csv filename and insert procedure name (required) and optionally" +
                    " '" + columnsMatch + " " + columnsStyle + "'," +
                    " '" + waitMatch + " s (default=10 seconds)'," +
                    " '" + auditMatch + " n (default=0 rows)'," +
                    " '" + limitMatch + " n (default=all rows)'," +
                    " '" + skipMatch + " n (default=0 rows)'," +
                    " '" + reportMatch + " n (default=10000)'," +
                    " and/or '" + stripMatch + " (disabled by default)'");
            System.exit(2);
        }
    }

    private static String[] stripMatchingColumnQuotes(String[] line) {
        final String[] strippedLine = new String[line.length];
        Pattern pattern = Pattern.compile("^([\"'])(.*)\\1$");
        for (int i = 0; i < line.length; i++) {
            Matcher matcher = pattern.matcher(line[i]);
            if (matcher.find()) {
                strippedLine[i] = matcher.group(2);
            } else {
                strippedLine[i] = line[i];
            }
        }
        return strippedLine;
    }

    private static String[] projectColumns(int[] colSelection, String[] line) {
        final String[] projectedLine = new String[colSelection.length];
        for (int i = 0; i < projectedLine.length; i++) {
            projectedLine[i] = line[colSelection[i]];
        }
        return projectedLine;
    }

    // This function borrowed/mutated from http:/stackoverflow.com/questions/1515437
    static String joinIntoString(String glue, Object... elements)
    {
        int k = elements.length;
        if (k == 0) {
            return null;
        }
        StringBuilder out = new StringBuilder();
        out.append(elements[0].toString());
        for (int i = 1; i < k; ++i) {
            out.append(glue).append(elements[i]);
        }
        return out.toString();
    }

    // This function borrowed/mutated from http:/stackoverflow.com/questions/1515437
    static String joinIntoString(String glue, String... elements)
    {
        int k = elements.length;
        if (k == 0) {
            return null;
        }
        StringBuilder out = new StringBuilder();
        out.append(elements[0]);
        for (int i = 1; i < k; ++i) {
            out.append(glue).append(elements[i]);
        }
        return out.toString();
    }

}
