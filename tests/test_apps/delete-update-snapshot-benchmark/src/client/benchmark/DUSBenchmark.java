/* This file is part of VoltDB.
 * Copyright (C) 2021 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package client.benchmark;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Benchmark for when Deletes, Updates, and a Snapshot, are all happening at
 *  the same time; needed especially for the DRO (deterministic row order)
 *  project (see ENG-18744, ENG-21259, ENG-21260).
 *  Note: DUS(B) stands for Delete-Update-Snapshot (Benchmark). */
public class DUSBenchmark {

    protected final DUSBConfig config;
    private Client client;
    private Random rand = new Random();
    private DUSBenchmarkStats stats;

    private static final long backpressureSleepMillis = 1000;
    private static boolean backpressure = false;
    private static long countBackpressureCalls = 0;
    private static long countBackpressureDelays = 0;

    private static long countSynchronousCalls = 0;
    private static long countAsynchronousCalls = 0;
    private static long countUpdateCalls = 0;
    private static long countDeleteCalls = 0;
    private static long countUpdateResults = 0;
    private static long countDeleteResults = 0;
    private static long countNoResponseInTime = 0;
    private static long countOtherErrors = 0;

    private long minimumId = -1;
    private long maximumId = -1;
    private long minimumModId = -1;
    private long maximumModId = -1;
    private List<String> tableNamesForDeleteAndUpdate = null;

    /** Constructor **/
    public DUSBenchmark(String[] args) throws Exception {
        config = new DUSBConfig();
        config.parse(DUSBenchmark.class.getName(), args);

        // Echo input option values (useful for debugging):
        System.out.println("\nIn DUSBenchmark constructor args:\n    "+Arrays.toString(args));
        System.out.println("  displayinterval: "+config.displayinterval);
        System.out.println("  duration       : "+config.duration);
        System.out.println("  tabletypes     : "+config.tabletypes);
        System.out.println("  columntypes    : "+config.columntypes);
        System.out.println("  insertnumrows  : "+config.insertnumrows);
        System.out.println("  insertblocksize: "+config.insertblocksize);
        System.out.println("  deleteblocksize: "+config.deleteblocksize);
        System.out.println("  updateblocksize: "+config.updateblocksize);
        System.out.println("  deleteorder    : "+config.deleteorder);
        System.out.println("  updateorder    : "+config.updateorder);
        System.out.println("  statsfile      : "+config.statsfile);
        System.out.println("  servers        : "+config.servers);

        DUSBClientListener dusbcl = new DUSBClientListener();
        ClientConfig cc = new ClientConfig(null, null, dusbcl);
        cc.setProcedureCallTimeout(10 * 60 * 1000);       // 10 minutes
        cc.setConnectionResponseTimeout(20 * 60 * 1000);  // 20 minutes
        client = ClientFactory.createClient(cc);

        String[] serverArray = config.servers.split(",");
        // Echo server array values (useful for debugging):
        System.out.println("  serverArray    : "+Arrays.toString(serverArray));
//        try {
//            Thread.sleep(60 * 1000);
//        } catch (InterruptedException e) {
//            System.err.println("Caught (& ignoring) InterruptedException:\n"
//                                +e+"\n"+e.getStackTrace());
//        }
        for (String server : serverArray) {
            try {
                client.createConnection(server);
            } catch (IOException e) {
                System.err.println("Caught (& re-throwing) IOException, for server '"
                                    +server+"':\n"+e+"\n"+e.getStackTrace());
                throw new IOException("Unable to create Connection to '"+server+"'", e);
            }
        }

        stats = new DUSBenchmarkStats(client);
    }


    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    private static class DUSBConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds; default: 5.")
        int displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds; default: 60.")
        int duration = 60;

        @Option(desc = "Comma-separated list of the form server[:port] to "
                + "connect to database for queries; default: localhost.")
        String servers = "localhost";

        @Option(desc = "Comma-separated list of one or more of the following: replicated, "
                + "partitioned, [view??]; default: 'replicated'.")
        String tabletypes = "replicated";

        @Option(desc = "Comma-separated list of one or more of the following: inline "
                + "(i.e., numeric, timestamp, short varchar), outline (i.e., non-inline: "
                + "long varchar columns), varbinary, geo (which varbinary or geo columns "
                + "are used depends on use of inline and/or outline); default: 'inline'.")
        String columntypes = "inline";

        @Option(desc = "Number of rows to be inserted, at the start; "
                + "default: 1000000 (a million).")
        long insertnumrows = 1000000;

        @Option(desc = "Size of the blocks by which row inserts will occur; e.g. if "
                + "insertnumrows is a thousand (1000), and blocksize is a hundred "
                + "(100), rows will be inserted as 10 blocks of a hundred (100) rows;"
                + "default: 1000 (a thousand).")
        long insertblocksize = 1000;

        @Option(desc = "Number of rows to be deleted, by each delete; default: insertblocksize.")
        long deleteblocksize = insertblocksize;

        @Option(desc = "Number of rows to be updated, by each update; default: deleteblocksize.")
        long updateblocksize = deleteblocksize;

        @Option(desc = "The order in which to delete rows, either: 'byid' (ascending), "
                + "'byiddesc'; or, for a more 'arbitrary' order, i.e. not in the same "
                + "order they were inserted: 'bymodid' (the default), or 'bymodiddesc';"
                + " case insensitive.")
        String deleteorder = "bymodid";

        @Option(desc = "The order in which to update rows, either: 'byid' (ascending), "
                + "'byiddesc'; or, for a more 'arbitrary' order, i.e. not in the same "
                + "order they were inserted: 'bymodid' (the default), or 'bymodiddesc';"
                + " case insensitive.")
        String updateorder = "bymodid";

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "deleteupdatesnapshot.csv";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (servers.length() == 0) servers = "localhost";
            if (statsfile.length() == 0) statsfile = "deleteupdatesnapshot.csv";
            if (tabletypes == null || tabletypes.isEmpty()) exitWithMessageAndUsage("tabletypes cannot be null or empty");
            List knownTableTypes = Arrays.asList("replicated", "partitioned");
            for (String type : tabletypes.split(",")) {
                 if (!knownTableTypes.contains(type.trim().toLowerCase())) {
                     exitWithMessageAndUsage("Unrecognized type '"+type+"' in tabletypes: "+tabletypes);
                 }
            }
            if (columntypes == null || columntypes.isEmpty()) exitWithMessageAndUsage("columntypes cannot be null or empty");
            List knownColumnTypes = Arrays.asList("inline", "outline", "varbinary", "geo");
            for (String type : columntypes.split(",")) {
                 if (!knownColumnTypes.contains(type.trim().toLowerCase())) {
                     exitWithMessageAndUsage("Unrecognized type '"+type+"' in columntypes: "+columntypes);
                 }
            }

        }
    } // end of DUSBConfig


    /** Initialize the DUSBenchmark, by adding lots of rows to the table(s). **/
    private void init() throws Exception {

        // TODO: debug print:
        System.out.println("\nStart of DUSBenchmark.init()");

        // Get the list of table names to which to add rows
        List<String> tableNamesForInsertValues = new ArrayList<String>();
        List<String> tableNamesForBulkInsert = new ArrayList<String>();
        String tableTypesLower = config.tabletypes.toLowerCase();
        if (tableTypesLower.contains("replicated")) {
            tableNamesForInsertValues.add("DUSB_R1");
            tableNamesForBulkInsert.add("DUSB_R1");
        }
        if (tableTypesLower.contains("partitioned")) {
            if (!tableNamesForInsertValues.contains("DUSB_R1")) {
                tableNamesForInsertValues.add("DUSB_R1");
            }
            tableNamesForBulkInsert.add("DUSB_P1");
        }
        tableNamesForDeleteAndUpdate = tableNamesForBulkInsert;

        // Get the list of column names to be set, for those columns that
        // will be non-null
        List<String> columnNames = getColumnNames();

        String[] colNamesArray  = columnNames.toArray(new String[0]);
        String[] colValuesArray = null;

        int minId = 0;
        for (String tableName : tableNamesForInsertValues) {
            // TODO: debug print:
            System.out.println( "\nIn DUSBenchmark.init()"
                    +": tableNames "+tableNamesForInsertValues
                    +", tableName "+tableName
                    +"\ncolumnNames: "+Arrays.toString(colNamesArray)
                    +"\nminId "+minId
                    +", insertblocksize "+config.insertblocksize
                    +", insertnumrows "+config.insertnumrows );

            for (int row=0; row < config.insertblocksize; row++) {
                // Get the array of random column values to be set, for those
                // columns that will be non-null
                colValuesArray = getColumnValues(colNamesArray);

                // TODO: debug print:
                if (row == 0 || row == config.insertblocksize / 2
                             || row == config.insertblocksize - 1) {
                    System.out.println( "\ncolValuesArray, row "+row+": "
                                        + Arrays.toString(colValuesArray) );
                }

                ClientResponse cr = client.callProcedure("InsertOneRow", tableName,
                        row, colNamesArray, colValuesArray);
                VoltTable vt = cr.getResults()[0];
                // TODO: check 'cr/vt' result ??

                // TODO: debug print:
                if (row == 0 || row == config.insertblocksize / 2
                             || row == config.insertblocksize - 1) {
                    System.out.println( "/nRow "+row
                             +": vt.getStatusCode "+vt.getStatusCode()
                             +", cr.getStatus "+cr.getStatus()
                             +" (SUCCESS="+ClientResponse.SUCCESS+")"
                             +", cr.getStatusString: "+cr.getStatusString() );
                }
            }
        }
        for (String tableName : tableNamesForBulkInsert) {
            VoltTable vt = client.callProcedure("BulkInsert", tableName, colNamesArray,
                    colValuesArray, minId, config.insertblocksize, config.insertnumrows)
                    .getResults()[0];
            // TODO: check 'cr/vt' result ??
        }

        // Set values needed by runBenchmark()
        minimumId = minimumModId = minId;
        maximumId = config.insertnumrows;
        maximumModId = config.insertblocksize;

        // TODO: debug print:
        System.out.println("\nEnd of DUSBenchmark.init()");

    } // end of init()


    /** Get the list of column names to be set, for those columns that
     *  will be non-null. **/
    private List<String> getColumnNames() {
        List<String> columnNames = new ArrayList<String>();
        String columnTypesLower = config.columntypes.toLowerCase();
        if (columnTypesLower.contains("inline")) {
            columnNames.add("TINY");
            columnNames.add("SMALL");
            columnNames.add("INTEG");
            columnNames.add("BIG");
            columnNames.add("FLOAT");
            columnNames.add("DECIMAL");
            columnNames.add("TIMESTAMP");
            columnNames.add("VCHAR_INLINE");
            columnNames.add("VCHAR_INLINE_MAX");

            if (columnTypesLower.contains("varbinary")) {
                columnNames.add("VARBIN_INLINE");
                columnNames.add("VARBIN_INLINE_MAX");
            }

            if (columnTypesLower.contains("geo")) {
                columnNames.add("POINT");
            }
        }
        if (columnTypesLower.contains("outline")) {
            columnNames.add("VCHAR_OUTLINE_MIN");
            columnNames.add("VCHAR_OUTLINE");
            columnNames.add("VCHAR_DEFAULT");

            if (columnTypesLower.contains("varbinary")) {
                columnNames.add("VARBIN_OUTLINE_MIN");
                columnNames.add("VARBIN_OUTLINE");
                columnNames.add("VARBIN_DEFAULT");
            }

            if (columnTypesLower.contains("geo")) {
                columnNames.add("POLYGON");
            }
        }
        return columnNames;
    } // end of getColumnNames()


    /** Get a list of random column values to be set, for those columns that
     *  will be non-null. **/
    private String[] getColumnValues(String[] colNamesArray) {
        int length = colNamesArray.length;
        String[] colValuesArray = new String[length];

        for (int i=0; i < length; i++) {
            try {
                switch (colNamesArray[i].toUpperCase()) {
                case "TINY":
                case "TINYINT":
                    colValuesArray[i] = Integer.valueOf(rand.nextInt(255) - 127).toString();
                    break;
                case "SMALL":
                case "SMALLINT":
                    colValuesArray[i] = Integer.valueOf(rand.nextInt(65535) - 32767).toString();
                    break;
                case "INT":
                case "INTEG":
                case "INTEGER":
                    colValuesArray[i] = Integer.valueOf(rand.nextInt()).toString();
                    break;
                case "BIG":
                case "BIGINT":
                    colValuesArray[i] = Long.valueOf(rand.nextLong()).toString();
                    break;
                case "FLOT":
                case "FLOAT":
                    colValuesArray[i] = Double.valueOf(20*rand.nextDouble() - 10).toString();
                    break;
                case "DEC":
                case "DECML":
                case "DECIMAL":
                    // A random BigDecimal with precision 38, scale 12, and
                    // non-zero digits before and after the decimal point
                    BigInteger bigInt = new BigInteger(100, rand);
                    MathContext mc = new MathContext(38);
                    colValuesArray[i] = new BigDecimal(bigInt, 12, mc).toString();
                    break;
                case "TIME":
                case "TIMESTMP":
                case "TIMESTAMP":
                    colValuesArray[i] = getRandomTimestampString();
                    break;
                case "VCHAR_INLINE":
                    colValuesArray[i] = getRandomString(14);
                    break;
                case "VCHAR_INLINE_MAX":
                    colValuesArray[i] = getRandomString(15);
                    break;
                case "VCHAR_OUTLINE_MIN":
                    colValuesArray[i] = getRandomString(16);
                    break;
                case "VCHAR_OUTLINE":
                    colValuesArray[i] = getRandomString(20);
                    break;
                case "VCHAR_DEFAULT":
                case "VARCHAR":
                    colValuesArray[i] = getRandomString(100);
                    break;
                case "VARBIN_INLINE":
                    colValuesArray[i] = getRandomVarbinaryString(32);
                    break;
                case "VARBIN_INLINE_MAX":
                    colValuesArray[i] = getRandomVarbinaryString(63);
                    break;
                case "VARBIN_OUTLINE_MIN":
                    colValuesArray[i] = getRandomVarbinaryString(64);
                    break;
                case "VARBIN_OUTLINE":
                    colValuesArray[i] = getRandomVarbinaryString(128);
                    break;
                case "VARBIN_DEFAULT":
                case "VARBINARY":
                    colValuesArray[i] = getRandomVarbinaryString(100);
                    break;
                case "POINT":
                case "GEOGRAPHY_POINT":
                    colValuesArray[i] = getRandomGeographyPointString();
                    break;
                case "POLYGON":
                case "GEOGRAPHY":
                    colValuesArray[i] = getRandomGeographyString(8);
                    break;
                default:
                    throw new RuntimeException("Unknown column name: '"
                                                +colNamesArray[i]+"'.");
                }  // end of switch

            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Failed to set random value, for column name '"
                                            +colNamesArray[i]+"'.", e);
            } // end of try/catch
        } // end of for loop

        return colValuesArray;

    } // end of getColumnValues(...)


    // Constant for getRandomString():
    private static final String RANDOM_CHARS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";

    /** Get a random String, with the specified length, at most. **/
    private String getRandomString(int maxLength) {
        StringBuffer stringValue = new StringBuffer();
        for (int i=0; i < maxLength; i++) {
            stringValue.append(RANDOM_CHARS.charAt(rand.nextInt(RANDOM_CHARS.length())));
            // There's some chance that you'll get a string shorter than maxLength
            if (rand.nextInt(100) < 1) {
                break;
            }
        }
        return stringValue.toString();
    } // end of getRandomString(...)


    /** Get a random Varbinary, as a String. **/
    private String getRandomVarbinaryString(int maxLength) {
        // TODO
        return null;
    } // end of getRandomVarbinaryString(...)


    // Constants for getRandomTimestampString():
    // These are the minimum & maximum valid years for a TIMESTAMP
    static final int MIN_YEAR = 1583;
    static final int MAX_YEAR = 9999;
    // 30 days has September,
    // April, June, and November ...
    static final List<String> MONTHS_WITH_30_DAYS = Arrays.asList("04", "06", "09", "11");

    /** Get a random Timestamp, as a String. **/
    private String getRandomTimestampString() {
        String year = Integer.valueOf(MIN_YEAR + rand.nextInt(MAX_YEAR+1 - MIN_YEAR)).toString();
        String month = getTwoDigitString( Integer.valueOf(rand.nextInt(12) + 1) );

        int maxDay = 31;           // default, good for 7 of the 12 months)
        if (month.equals("02")) {  // February
            maxDay = 28;
            if ( Integer.valueOf(year) % 4 == 0 && !(Integer.valueOf(year) % 100 == 0) ) {
                maxDay = 29;       // February in leap years
            }
        } else if (MONTHS_WITH_30_DAYS.contains(month)) {
            maxDay = 30;
        }

        String day    = getTwoDigitString( Integer.valueOf(rand.nextInt(maxDay) + 1) );
        String hour   = getTwoDigitString( Integer.valueOf(rand.nextInt(24)) );
        String minute = getTwoDigitString( Integer.valueOf(rand.nextInt(60)) );
        String second = getTwoDigitString( Integer.valueOf(rand.nextInt(60)) );
        String micros = getNDigitString(6, Integer.valueOf(rand.nextInt(1000000)) );

        return (year+"-"+month+"-"+day+" "+hour+":"+minute+":"+second+"."+micros);
    } // end of getRandomTimestampString()


    /** Convert an int into a N-digit String; e.g., for N=2, 9 would become
     *  "09", while 10 would become "10"; whereas for N=3, they would become
     *  "009" and "010", respectively. **/
    private String getNDigitString(int numDigits, int intValue) {
        String stringValue = "";
        for (int digit = numDigits-1; digit > 0 ; digit--) {
            if (intValue < Math.pow(10, digit)) {
                stringValue += "0";
            } else {
                break;
            }
        }
        stringValue += Integer.valueOf(intValue).toString();
        return stringValue;
    } // end of getNDigitString(...)


    /** Convert an int into a two-digit String; e.g., 9 would become "09",
     *  while 10 would become "10". **/
    private String getTwoDigitString(int intValue) {
        return getNDigitString(2, intValue);
    }


    /** Get a random GeographyPointValue, as a String. **/
    private String getRandomGeographyPointString() {
        // TODO
        return null;
    } // end of getRandomGeographyPointString()


    /** Get a random GeographyValue, with the specified number of points, at most, as a String. **/
    private String getRandomGeographyString(int maxNumPoints) {
        // TODO
        return null;
    } // end of getRandomGeographyString(...)


    /** Quickly process the results of a call to a stored procedure
     *  (UpdateRows or DeleteRows). **/
//    private static void processResult(String procName,
//            ClientResponse clientResponse, long count) {
//        if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
//            String Status = clientResponse.getStatusString();
//            if (Status.startsWith("No response received in the allotted time")) {
//                if (countNoResponseInTime % 99 == 0) {
//                    System.err.println("\n"+procName+" Error (#"
//                                        +countNoResponseInTime+"):\n"+Status);
//                }
//                countNoResponseInTime++;
//            } else {
//                System.err.println("\n"+procName+" Error:\n"+Status);
//                countOtherErrors++;
//            }
//
//        // TODO: debug print:
//        } else if (count % 100 == 0 || count % 100 == 99) {
//            VoltTable vt = clientResponse.getResults()[0];
//            System.out.println("\n"+procName+" Result"
//                                +": cr.getStatus "+clientResponse.getStatus()
//                                +" (SUCCESS="+ClientResponse.SUCCESS+")"
//                                +", cr.getStatusString "+clientResponse.getStatusString()
//                                +", vt.getSerializedSize "+vt.getSerializedSize()
//                                +", vt.getColumnCount "+vt.getColumnCount()
//                                +", vt.getRowCount "+vt.getRowCount()
//                                +", vt.getColumnType(0) "+vt.getColumnType(0)
//                                +", vt.advanceRow "+vt.advanceRow()
//                                +", vt.getColumnName(0) "+vt.getColumnName(0)
//                                +", vt.get(0) "+vt.get(0) );
//        }
//    }


    /** Check for backpressure */
    public class DUSBClientListener extends ClientStatusListenerExt {
        private final Object BACKPRESSURE_LOCK = new Object();

        @Override
        public void backpressure(boolean status) {
            synchronized (BACKPRESSURE_LOCK) {
                countBackpressureCalls++;
                backpressure = status;
            }
            System.out.println("\nDUSBClientListener.backpressure called, with status: "+status+"\n");
        }
    }


    /** Just a wrapper around Client.callProcedure, to deal with error
     *  conditions, backpressure, and debug-print. */
    private void callStoredProc(DUSBenchmarkCallback callBack, String procName,
            String tableName, String columnName, long minValue, long maxValue,
            String inlineOrOutline, boolean debugPrint) throws Exception {

        // Debug print, when called for
        if (debugPrint || backpressure) {
            System.out.println("Calling "+procName+" with"
                    +": table "+tableName+", column "+columnName
                    +", minValue "+minValue+", maxValue "+maxValue
                    +" (inlineOrOutline "+inlineOrOutline
                    +", backpressure "+backpressure+")." );
        }

        // Check for backpressure: if it exists, wait until it passes before
        // calling anymore stored procedures
        while (backpressure) {
            countBackpressureDelays++;
            try {
                Thread.sleep(backpressureSleepMillis);
            } catch (InterruptedException e) {
                System.err.println( "Caught (backpressure) InterruptedException:\n"
                        +e+"\n"+e.getStackTrace() );
            }
        }

        if ("UpdateRows".equals(procName)) {
            countUpdateCalls++;
        } else if ("DeleteRows".equals(procName)) {
            countDeleteCalls++;
        } else {
            throw new IllegalArgumentException("Unrecognized 'procName': "+procName+".");
        }

        boolean useInlineOrOutline = (inlineOrOutline != null && !inlineOrOutline.isEmpty());
        boolean queued = true;

        if (callBack == null) {
            // This doesn't normally happen
            countSynchronousCalls++;
            ClientResponse cr = null;
            if (useInlineOrOutline) {
                cr = client.callProcedure(procName, tableName, columnName, minValue, maxValue,
                                            inlineOrOutline);
            } else {
                cr = client.callProcedure(procName, tableName, columnName, minValue, maxValue);
            }
            callBack = new DUSBenchmarkCallback(procName+"-synchronous");
            callBack.clientCallback(cr);

        } else {
            if (useInlineOrOutline) {
                queued = client.callProcedure(callBack, procName, tableName, columnName,
                                                minValue, maxValue, inlineOrOutline);
            } else {
                queued = client.callProcedure(callBack, procName, tableName, columnName,
                                                minValue, maxValue);
            }
            countAsynchronousCalls++;
        }

        if (!queued) {
            System.out.println(procName+" not queued successfully, for table "
                +tableName+", column "+columnName+", minValue "+minValue
                +", maxValue "+maxValue+" (inlineOrOutline "+inlineOrOutline+").");
        }
    }


    /** Run the DUSBenchmark, by simultaneously running a snapshot, while
     *  updating and deleting lots of table rows. **/
    private void runBenchmark() throws Exception {

        System.out.println("\nStart of DUSBenchmark.runBenchmark()\n");

        // Get the blockSize, that is, the number of table rows to be deleted
        // (or updated) by each stored procedure call
        if (config.updateblocksize != config.deleteblocksize) {
            throw new UnsupportedOperationException("Using a different updateblocksize ("
                    +config.updateblocksize+") and deleteblocksize ("+config.deleteblocksize
                    +") is not yet supported");
        }
        long blockSize = config.deleteblocksize;

        // If we are deleting (& updating) by MOD_ID values rather than ID values,
        // then we need an equivalent to 'blockSize' for the MOD_ID values
        long modIdIncrement = 1;
        if (config.deleteorder != null &&
                config.deleteorder.toLowerCase().startsWith("bymodid")) {

            // The MOD_ID value increment, in order to delete (or update) roughly
            // the intended number of rows (blockSize) with each stored proc call,
            // should be the blockSize times the ratio of the total number of rows
            // and the total number of distinct MOD_ID values
            modIdIncrement = Math.round( (maximumModId - minimumModId) * blockSize
                                          / (maximumId - minimumId) );
            // Cannot be less than one
            modIdIncrement = Math.max(1, modIdIncrement);
        }

        String columnName = "UNSPECIFIED";
        long initMinValue      = -1, lowestMinValue  = -1,
             minValueIncrement = -1, highestMinValue = -1,
             startSnapshotAt   = -1;

        if ("byid".equals(config.deleteorder.toLowerCase())) {
            columnName = "ID";
            lowestMinValue    = minimumId;
            highestMinValue   = maximumId;
            initMinValue      = minimumId;
            startSnapshotAt   = minimumId + blockSize;
            minValueIncrement = blockSize;
        } else if ("byiddesc".equals(config.deleteorder.toLowerCase())) {
            columnName = "ID";
            lowestMinValue    = minimumId;
            highestMinValue   = maximumId;
            initMinValue      = maximumId - blockSize;
            startSnapshotAt   = maximumId - blockSize*2;
            minValueIncrement = -blockSize;
        } else if ("bymodid".equals(config.deleteorder.toLowerCase())) {
            columnName = "MOD_ID";
            lowestMinValue    = minimumModId;
            highestMinValue   = maximumModId;
            initMinValue      = minimumModId;
            startSnapshotAt   = minimumModId + modIdIncrement;
            minValueIncrement = modIdIncrement;
        } else if ("bymodiddesc".equals(config.deleteorder.toLowerCase())) {
            columnName = "MOD_ID";
            lowestMinValue    = minimumModId;
            highestMinValue   = maximumModId;
            initMinValue      = maximumModId - modIdIncrement;
            startSnapshotAt   = minimumModId - modIdIncrement*2;
            minValueIncrement = -modIdIncrement;
        } else {
            throw new IllegalArgumentException("Unknown 'deleteorder' config "
                    + "parameter value ("+config.deleteorder+") is not allowed.");
        }

        // These values are just used to determine when to do debug-print:
        // 3 or 4 times total, including the first and last iterations
        long lastMinValue = initMinValue + minValueIncrement * (long)
                ( Math.ceil( 1.0D*(highestMinValue - lowestMinValue)/Math.abs(minValueIncrement) ) - 1 );
        long minValueDebugFrequency = Math.abs(lastMinValue - initMinValue) / 3;
        // Cannot be less than one
        minValueDebugFrequency = Math.max(1, minValueDebugFrequency);

        // Which group of columns should get updated, in the UpdateRows procedure?
        String updateInlineOrOutlineColumns = "INLINE";
        if (config.columntypes.toLowerCase().contains("outline")) {
            updateInlineOrOutlineColumns = "OUTLINE";
        }

        // TODO: debug print:
        System.out.println("\nValues used in the main for loop"
                +": minimumId "+minimumId
                +", maximumId "+maximumId
                +", blockSize "+blockSize
                +", minimumModId "+minimumModId
                +", maximumModId "+maximumModId
                +", modIdIncrement "+modIdIncrement
                +";\ncolumnName "+columnName
                +", lowestMinValue "+lowestMinValue
                +", highestMinValue "+highestMinValue
                +", initMinValue "+initMinValue
                +", startSnapshotAt "+startSnapshotAt
                +", minValueIncrement "+minValueIncrement
                +";\nlastMinValue "+lastMinValue
                +", minValueDebugFrequency "+minValueDebugFrequency
                +", updateInlineOrOutlineColumns "+updateInlineOrOutlineColumns+"\n" );

        stats.startBenchmark(config.displayinterval);

        for ( long minValue = initMinValue;
                minValue >= lowestMinValue && minValue < highestMinValue;
                minValue += minValueIncrement ) {
            long maxValue = minValue + Math.abs(minValueIncrement);

            boolean debugPrint = false;
            if (minValue == lastMinValue ||
                    (Math.abs(lastMinValue - minValue) % minValueDebugFrequency == 0) ) {
                debugPrint = true;
            }

            if (minValue == startSnapshotAt) {
                // TODO: implement this!
                System.out.println("\nStarting the snapshot is not yet implemented\n");
            }

            for (String procName : Arrays.asList("UpdateRows", "DeleteRows")) {
                String inlineOrOutline = "UpdateRows".equals(procName) ?
                                    updateInlineOrOutlineColumns : null;
                for (String tableName : tableNamesForDeleteAndUpdate) {
                    callStoredProc( new DUSBenchmarkCallback(procName), procName,
                            tableName, columnName, minValue, maxValue,
                            inlineOrOutline, debugPrint );
                }
            }
        }

        // End of the DUS Benchmark
        stats.endBenchmark(config.statsfile);
        client.drain();
        DUSBenchmarkCallback.printAllResults();
        client.close();

        System.out.println("\nTotal counts:"
                            +"\n  Synchronous calls "+countSynchronousCalls
                            +", and Asynchronous "+countAsynchronousCalls
                            +"\n  Update calls "+countUpdateCalls
                            +", and Results "+countUpdateResults
                            +"\n  Delete calls "+countDeleteCalls
                            +", and Results "+countDeleteResults
                            +"\n  Backpressure Calls "+countBackpressureCalls
                            +", and Delays "+countBackpressureDelays
                            +"\n  No response received in the allotted time: "
                            +countNoResponseInTime
                            +"\n  Other Errors: "+countOtherErrors );

        System.out.println("\nEnd of DUSBenchmark.runBenchmark()\n");

    } // end of runBenchmark()


    public static void main(String[] args) throws Exception {
        DUSBenchmark benchmark = new DUSBenchmark(args);
        benchmark.init();
        benchmark.runBenchmark();
    }
}
