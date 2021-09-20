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
import org.voltdb.CLIConfig.Option;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Random;

/** Benchmark for when Deletes, Updates, and a Snapshot, are all happening at
 *  the same time; needed especially for the DRO (deterministic row order)
 *  project (see ENG-18744, ENG-21259, ENG-21260).
 *  Note: DUS(B) stands for Delete-Update-Snapshot (Benchmark). */
public class DUSBenchmark {

    // Define this in just one place, besides the DDL file
    public static final List<String> PARTITIONED_TABLES = Arrays.asList("DUSB_P1");

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
    private static long countNoResponseInTime = 0;
    private static long countOtherErrors = 0;
    private static Map<String,Long> countStoredProcCalls = new ConcurrentHashMap<String,Long>();
    private static Map<String,Long> countStoredProcResults = new ConcurrentHashMap<String,Long>();

    private long minimumId = -1;
    private long maximumId = -1;
    private long minimumModId = -1;
    private long maximumModId = -1;
    private List<String> tableNamesForDeleteAndUpdate = null;

    /** Constructor **/
    public DUSBenchmark(DUSBConfig config) throws Exception {
        this.config = config;

        // Echo input option values (useful for debugging):
        System.out.println("  displayinterval: "+config.displayinterval);
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
        System.out.println("  rate limit     : "+config.ratelimit);

        DUSBClientListener dusbcl = new DUSBClientListener(config.insertnumrows);
        ClientConfig cc = new ClientConfig(null, null, dusbcl);
        cc.setMaxTransactionsPerSecond(config.ratelimit);
        cc.setProcedureCallTimeout(10 * 60 * 1000);       // 10 minutes
        cc.setConnectionResponseTimeout(20 * 60 * 1000);  // 20 minutes
        client = ClientFactory.createClient(cc);

        String[] serverArray = config.servers.split(",");
        // Echo server array values (useful for debugging):
        System.out.println("  serverArray    : "+Arrays.toString(serverArray));
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
        init();
    }


    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    private static class DUSBConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds; default: 5.")
        int displayinterval = 5;

        @Option(desc = "Comma-separated list of the form server[:port] to "
                + "connect to database for queries; default: 'localhost'.")
        String servers = "localhost";

        @Option(desc = "Comma-separated list of one or more of the following: replicated, "
                + "partitioned, [view??]; default: 'partitioned'.")
        String tabletypes = "replicated";

        @Option(desc = "Comma-separated list of one or more of the following: inline "
                + "(i.e., numeric, timestamp, short varchar), outline (i.e., non-inline: "
                + "long varchar columns), varbinary, geo (which varbinary or geo columns "
                + "are used depends on use of inline and/or outline); default: 'inline'.")
        String columntypes = "inline";

        @Option(desc = "Number of rows to be inserted, at the start; "
                + "default: 10,000,000 (ten million).")
        long insertnumrows = 10000000;

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
                + "'byiddesc'; or, for a more arbitrary order, i.e. not in the same "
                + "order they were inserted: 'bymodid' (the default), or 'bymodiddesc' ;"
                + "or, to delete rows one at a time 'onebyone' (ascending), or 'onebyonedesc';"
                + " case insensitive.")
        String deleteorder = "bymodid";

        @Option(desc = "The order in which to update rows, either: 'byid' (ascending), "
                + "'byiddesc'; or, for a more arbitrary order, i.e. not in the same "
                + "order they were inserted: 'bymodid', or 'bymodiddesc'; or, to update "
                + "rows one at a time 'onebyone' (ascending), or 'onebyonedesc';"
                + " case insensitive; default: deleteorder.")
        String updateorder = deleteorder;

        @Option(desc = "Number of updates/deletes to do before starting the snapshot; default: 3.")
        long startSnapshotAtIteration = 3;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "deleteupdatesnapshot.csv";

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Override
        public void validate() {
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
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
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

        long minId = 0;
        for (String tableName : tableNamesForInsertValues) {
            String procName = "InsertOneRow";
            if (tableName != null && tableName.toUpperCase().equals("DUSB_P1")) {
                procName = "InsertOneRowP";
            }

            // TODO: debug print:
            System.out.println( "\nIn DUSBenchmark.init()"
                    +": tableNames "+tableNamesForInsertValues
                    +", tableName "+tableName
                    +", tableName "+tableName
                    +"\ncolumnNames: "+Arrays.toString(colNamesArray)
                    +"\nminId "+minId
                    +", insertblocksize "+config.insertblocksize
                    +", insertnumrows "+config.insertnumrows );

            for (long row=0; row < config.insertblocksize; row++) {
                // Get the array of random column values to be set, for those
                // columns that will be non-null
                colValuesArray = getColumnValues(colNamesArray);

                // TODO: debug print:
                if (row == 0 || row == config.insertblocksize / 2
                             || row == config.insertblocksize - 1) {
                    System.out.println( "\ncolValuesArray, row "+row+": "
                                        + Arrays.toString(colValuesArray) );
                }

                ClientResponse cr = client.callProcedure(procName, row,
                                    tableName, colNamesArray, colValuesArray);
                VoltTable vt = cr.getResults()[0];
                // TODO: check 'cr/vt' result ??

                // TODO: debug print:
                if (row == 0 || row == config.insertblocksize / 2
                             || row == config.insertblocksize - 1) {
                    System.out.println( "\nRow "+row
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
                throw new RuntimeException( "Failed to set random value, for column name '"
                                            +colNamesArray[i]+"'.", e );
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


    /** Check for backpressure (and other conditions) */
    public class DUSBClientListener extends ClientStatusListenerExt {
        private long numRows = -1;

        /** Constructor **/
        public DUSBClientListener(long numRows) {
            super();
            this.numRows = numRows;
        }

        @Override
        public synchronized void backpressure(boolean status) {
            countBackpressureCalls++;
            backpressure = status;
            if (countBackpressureCalls < 10 || countBackpressureCalls > numRows - 5 ||
                    (numRows / 10) % countBackpressureCalls == 0 ) {
                System.out.println("\nDUSBClientListener.backpressure called, with status: "
                        +status+",\nafter "+countStoredProcCalls.get("total")+" calls and "
                        +countStoredProcResults.get("total")+" results (total).");
            }
        }

        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            System.out.println("\nDUSBClientListener.connectionLost called, with hostname: "+hostname
                    +", port "+port+", connectionsLeft "+connectionsLeft+", cause "+cause+"\n");
        }

        @Override
        public void connectionCreated(String hostname, int port, AutoConnectionStatus status) {
            System.out.println("\nDUSBClientListener.connectionCreated called, with hostname: "
                    +hostname+", port "+port+", status "+status+"\n");
        }

        @Override
        public void uncaughtException(ProcedureCallback callback, ClientResponse cr, Throwable e) {
            System.out.println("\nDUSBClientListener.uncaughtException called, with callback: "
                    +callback+", ClientResponse "+cr+", Throwable "+e+"\n");
        }

        @Override
        public void lateProcedureResponse(ClientResponse cr, String hostname, int port) {
            System.out.println("\nDUSBClientListener.lateProcedureResponse called, with hostname: "
                    +hostname+", port "+port+", ClientResponse "+cr+"\n");
        }
    }


    /** Counts the number of times that each stored procedure is called. */
    private static void countProcCalls(String procName) {
        Long count = countStoredProcCalls.get(procName);
        if (count == null) {
            countStoredProcCalls.put(procName, 1L);
        } else {
            countStoredProcCalls.put(procName, count + 1);
        }
        if (!"total".equals(procName)) {
            countProcCalls("total");
        }
    }


    /** Counts the number of times that each stored procedure finishes,
     *  i.e., produces results. */
    static void countProcResults(String procName) {
        Long count = countStoredProcResults.get(procName);
        if (count == null) {
            countStoredProcResults.put(procName, 1L);
        } else {
            countStoredProcResults.put(procName, count + 1);
        }
        if (!"total".equals(procName)) {
            countProcResults("total");
        }
   }


    /** Retruns a String, suitable for printing, that contains the number of
     *  times that each stored procedure was called; and produced a result. */
    private String printProcCounts() {
        SortedSet<String> sortedKeys = new TreeSet<String>();
        sortedKeys.addAll(countStoredProcCalls.keySet());
        sortedKeys.addAll(countStoredProcResults.keySet());

        StringBuffer result = new StringBuffer();
        for (String key : sortedKeys) {
            result.append( "\n  "+key+" calls "+countStoredProcCalls.get(key)
                            +", and results "+countStoredProcResults.get(key) );
        }

        return result.toString();
    }


    /** Just a wrapper around Client.callProcedure, to deal with error
     *  conditions, backpressure, and debug-print. */
    private void callStoredProc(DUSBenchmarkCallback callBack, String procName,
            String tableName, String columnName, long minValue, long maxValue,
            String inlineOrOutline, boolean debugPrint) throws Exception {

        // Debug print, when called for
        if (debugPrint || backpressure) {
            if ("@SnapshotSave".equals(procName)) {
                // For @SnapshotSave, the meanings of the parameters are different
                System.out.println("Calling "+procName+" with parameters"
                        +": directory-path "+tableName+", unique-ID "+columnName
                        +", blocking-flag "+minValue+"." );
            } else {
                System.out.println("Calling "+procName+" with parameters"
                        +": table "+tableName+", column "+columnName
                        +", minValue "+minValue+", maxValue "+maxValue
                        +" (inlineOrOutline "+inlineOrOutline
                        +", backpressure "+backpressure+")." );
            }
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

        boolean useInlineOrOutline = (inlineOrOutline != null && !inlineOrOutline.isEmpty());
        boolean queued = true;

        if (callBack == null) {
            // This doesn't normally happen
            countSynchronousCalls++;
            ClientResponse cr = null;
            if (procName != null && procName.startsWith("DeleteOneRow")) {
                cr = client.callProcedure(procName, minValue, tableName);
            } else if (procName != null && procName.startsWith("UpdateOneRow")) {
                cr = client.callProcedure(procName, minValue, tableName, inlineOrOutline);
            } else if (procName != null && procName.equals("@SnapshotSave")) {
                cr = client.callProcedure(procName, tableName, columnName, minValue);
            } else if (useInlineOrOutline) {
                cr = client.callProcedure(procName, tableName, columnName, minValue, maxValue,
                                            inlineOrOutline);
            } else {
                cr = client.callProcedure(procName, tableName, columnName, minValue, maxValue);
            }
            callBack = new DUSBenchmarkCallback(procName+"-synchronous");
            callBack.clientCallback(cr);

        } else {
            if (procName != null && procName.startsWith("DeleteOneRow")) {
                queued = client.callProcedure(callBack, procName, minValue, tableName);
            } else if (procName != null && procName.startsWith("UpdateOneRow")) {
                queued = client.callProcedure(callBack, procName, minValue, tableName,
                                inlineOrOutline);
            } else if (procName != null && procName.equals("@SnapshotSave")) {
                queued = client.callProcedure(callBack, procName, tableName, columnName,
                                minValue);
            } else if (useInlineOrOutline) {
                queued = client.callProcedure(callBack, procName, tableName, columnName,
                                minValue, maxValue, inlineOrOutline);
            } else {
                queued = client.callProcedure(callBack, procName, tableName, columnName,
                                minValue, maxValue);
            }
            countAsynchronousCalls++;
        }
        countProcCalls(procName);

        if (!queued) {
            System.out.println(procName+" not queued successfully, for table "
                +tableName+", column "+columnName+", minValue "+minValue
                +", maxValue "+maxValue+" (inlineOrOutline "+inlineOrOutline+").");
        }
    }


    /** If using one of the "...OneRow" stored procs with a partitioned table,
     *  add a "P" to the end of the proc name, to use the partitioned version. **/
    private String alterProcName(String procName, String tableName) {
        if (procName == null || tableName == null) {
            throw new RuntimeException("Illegal null tableName ("+tableName+") "
                                        +"or procName ("+procName+").");
        }

        if (procName.endsWith("OneRow") && PARTITIONED_TABLES.contains(tableName)) {
            return procName + "P";
        }

        return procName;
    }


    /** Run the DUSBenchmark, by simultaneously running a snapshot, while
     *  updating and deleting lots of table rows. **/
    private void runBenchmark() throws Exception {

        System.out.println("\n\nStart of DUSBenchmark.runBenchmark()");

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
        long initMinValue = -1, minValueIncrement = -1, numIterations = -1;

        if ("byid".equals(config.deleteorder.toLowerCase())) {
            columnName = "ID";
            initMinValue      = minimumId;
            minValueIncrement = blockSize;
            numIterations = (long) Math.ceil( (maximumId - minimumId)*1.0D / blockSize );
        } else if ("byiddesc".equals(config.deleteorder.toLowerCase())) {
            columnName = "ID";
            initMinValue      = maximumId - blockSize;
            minValueIncrement = -blockSize;
            numIterations = (long) Math.ceil( (maximumId - minimumId)*1.0D / blockSize );
        } else if ("bymodid".equals(config.deleteorder.toLowerCase())) {
            columnName = "MOD_ID";
            initMinValue      = minimumModId;
            minValueIncrement = modIdIncrement;
            numIterations = (long) Math.ceil( (maximumModId - minimumModId)*1.0D / modIdIncrement );
        } else if ("bymodiddesc".equals(config.deleteorder.toLowerCase())) {
            columnName = "MOD_ID";
            initMinValue      = maximumModId - modIdIncrement;
            minValueIncrement = -modIdIncrement;
            numIterations = (long) Math.ceil( (maximumModId - minimumModId)*1.0D / modIdIncrement );
        } else if ("onebyone".equals(config.deleteorder.toLowerCase())) {
            columnName = "ID";
            initMinValue = minimumId;
            minValueIncrement = 1;
            numIterations = maximumId - minimumId;
        } else if ("onebyonedesc".equals(config.deleteorder.toLowerCase())) {
            columnName = "ID";
            initMinValue = maximumId - 1;
            minValueIncrement = -1;
            numIterations = maximumId - minimumId;
        } else {
            throw new IllegalArgumentException("Unknown 'deleteorder' config "
                    + "parameter value ("+config.deleteorder+") is not allowed.");
        }

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
                +", initMinValue "+initMinValue
                +", minValueIncrement "+minValueIncrement
                +", startSnapshotAtIteration "+config.startSnapshotAtIteration
                +", updateInlineOrOutlineColumns "+updateInlineOrOutlineColumns
                +";\ntableNamesForDeleteAndUpdate: "+tableNamesForDeleteAndUpdate+"\n" );

        stats.startBenchmark(config.displayinterval);
        for (long i=0; i < numIterations; i++) {
            long minValue = initMinValue + i*minValueIncrement;
            long maxValue = minValue + Math.abs(minValueIncrement);

            boolean debugPrint = false;
            if (i == 0 || i == numIterations/2 || i == numIterations - 1) {
                debugPrint = true;
            }

            if (i == config.startSnapshotAtIteration) {
                try {
                    // This is the equivalent of running, in 'sqlcmd':
                    //     @SnapshotSave . dusbsnapshot0 0
                    // where the first parameter is the directory-path, the second a
                    // unique-ID, and the third the blocking-flag (0 is non-blocking);
                    // the next 2 args to callStoredProc are ignored, in this case,
                    // while the last is the debugPrint arg
                    callStoredProc( new DUSBenchmarkCallback("@SnapshotSave"),
                            "@SnapshotSave", ".", "dusbsnapshot0", 0,
                            -1, "", true );
                } catch (Exception e) {
                    throw new RuntimeException( "Failed to start snapshot, by "
                            + "calling '@SnapshotSave', with parameters: "
                            + "'.', 'dusbsnapshot0', 0.", e );
                }
            }

            List<String> procNames = new ArrayList<String>();
            if (config.deleteorder.toLowerCase().startsWith("onebyone")) {
                procNames.add("UpdateOneRow");
                procNames.add("DeleteOneRow");
            } else {
                procNames.add("UpdateRows");
                procNames.add("DeleteRows");
            }

            for (String procName : procNames) {
                String inlineOrOutline = (procName != null && procName.startsWith("Update"))
                                          ? updateInlineOrOutlineColumns : null;
                for (String tableName : tableNamesForDeleteAndUpdate) {
                    String finalProcName = alterProcName(procName, tableName);
                    callStoredProc( new DUSBenchmarkCallback(finalProcName),
                            finalProcName, tableName, columnName, minValue,
                            maxValue, inlineOrOutline, debugPrint );
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
                            +printProcCounts()
                            +"\n  Backpressure Calls "+countBackpressureCalls
                            +", and Delays "+countBackpressureDelays
                            +"\n  No response received in the allotted time: "
                            +countNoResponseInTime
                            +"\n  Other Errors: "+countOtherErrors );

        System.out.println("\nEnd of DUSBenchmark.runBenchmark()\n");

    } // end of runBenchmark()


    public static void main(String[] args) throws Exception {
        DUSBConfig config = new DUSBConfig();
        config.parse(DUSBenchmark.class.getName(), args);

        DUSBenchmark benchmark = new DUSBenchmark(config);
        benchmark.runBenchmark();
    }
}
