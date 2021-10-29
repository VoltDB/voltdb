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
import org.voltdb.CLIConfig.Option;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.VoltTable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.io.IOException;
import java.time.LocalDateTime;
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

    // Define these in just one place, besides the DDL file
    public static final List<String> PARTITIONED_TABLES      = Arrays.asList("DUSB_P1", "DUSB_P2", "DUSB_P3");
    public static final List<String> PARTITIONED_BY_ID       = Arrays.asList("DUSB_P1");
    public static final List<String> PARTITIONED_BY_MOD_ID   = Arrays.asList("DUSB_P2");
    public static final List<String> PARTITIONED_BY_BLOCK_ID = Arrays.asList("DUSB_P3");
    public static final List<String> REPLICATED_TABLES       = Arrays.asList("DUSB_R1");

    protected final DUSBConfig config;
    private Client client;
    private Random rand = new Random();
    private DUSBenchmarkStats stats;

    private static final int backpressureSleepMillis = 1000;
    private static boolean backpressure = false;
    private static int countBackpressureCalls = 0;
    private static int countBackpressureDelays = 0;

    private static int countSynchronousCalls = 0;
    private static int countAsynchronousCalls = 0;
    private static int countNoResponseInTime = 0;
    private static int countOtherErrors = 0;
    private static Map<String,Long> countStoredProcCalls = new ConcurrentHashMap<String,Long>();
    private static Map<String,Long> countStoredProcResults = new ConcurrentHashMap<String,Long>();
    private static Map<String,List<Long>> randomValues = new ConcurrentHashMap<String,List<Long>>();

    private long insertBlockSize = -1;
    private long minimumId = -1;
    private long maximumId = -1;
    private long minimumModId = -1;
    private long maximumModId = -1;
    private long minimumBlockId = -1;
    private long maximumBlockId = -1;
    private List<String> tableNamesForDeleteAndUpdate = null;

    /** Constructor **/
    public DUSBenchmark(DUSBConfig config) throws Exception {
        this.config = config;

        // Echo input option values (useful for debugging):
        System.out.println("\nIn DUSBenchmark constructor:");
        System.out.println("  displayinterval: "+config.displayinterval);
        System.out.println("  seed           : "+config.seed);
        System.out.println("  tabletypes     : "+config.tabletypes);
        System.out.println("  columntypes    : "+config.columntypes);
        System.out.println("  insertnumrows  : "+config.insertnumrows);
        System.out.println("  deleteupdatenumrows: "+config.deleteupdatenumrows);
        System.out.println("  deleteupdatevalues: "+config.deleteupdatevalues);
        System.out.println("  deleteupdatecolumn: "+config.deleteupdatecolumn);
        System.out.println("  deleteupdateorder : "+config.deleteupdateorder);
        System.out.println("  rate limit     : "+config.ratelimit);
        System.out.println("  statsfile      : "+config.statsfile);
        System.out.println("  servers        : "+config.servers);

        rand.setSeed(config.seed);
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

        @Option(desc = "Benchmark maximum duration, in seconds; default: 180 (3 minutes).")
        int duration = 180;

        @Option(desc = "Comma-separated list of the form server[:port] to "
                + "connect to database for queries; default: 'localhost'.")
        String servers = "localhost";

        @Option(desc = "Random seed, used to generate random numbers; default: "
                + "6,782,743,657,833,577,466.")
        long seed = 6782743657833577466L;  // taken from a recent run of grammar-gen

        @Option(desc = "Comma-separated list of one or more of the following: 'partitioned' "
                + "(the default), 'replicated', or 'view' [not yet implemented].")
        String tabletypes = "partitioned";

        @Option(desc = "Comma-separated list of one or more of the following: 'inline' "
                + "(i.e., numeric, timestamp, short varchar), 'outline' (i.e., non-inline: "
                + "long varchar columns), varbinary, geo (which varbinary or geo columns "
                + "are used depends on use of inline and/or outline); default: 'inline'.")
        String columntypes = "inline";

        @Option(desc = "Number of rows to be inserted, at the start; "
                + "default: 10,000,000 (ten million).")
        long insertnumrows = 10000000;

        @Option(desc = "The number of rows to be deleted or updated by each (stored proc) "
                + "transaction; note that initial inserts will typically happen in a "
                + "blocksize of roughly insertnumrows / deleteupdatenumrows rows at a "
                + "time, and the MOD_ID columns will have values running from zero to "
                + "blocksize - 1, i.e., MOD_ID = ID % blocksize; default: 1000.")
        long deleteupdatenumrows = 1000;

        @Option(desc = "The manner in which to delete and update: 'single' to indicate "
                + "just one value at a time (e.g. using 'WHERE MOD_ID = ?'); or 'range' "
                + "to indicate a range of values (e.g. using 'WHERE MOD_ID <= ? AND "
                + "MOD_ID > ?'; case insensitive, default: 'single'.")
        String deleteupdatevalues = "single";

        @Option(desc = "The column to be used for delete and update: 'id' (e.g. using "
                + "'WHERE ID = ?'); or 'modid' (e.g. using 'WHERE MOD_ID = ?'); or "
                + "'blockid' (e.g. using 'WHERE BLOCK_ID = ?'); case insensitive, "
                + "default: 'modid'.")
        String deleteupdatecolumn = "modid";

        @Option(desc = "The order in which to delete and update values, either: 'ascending' "
                + "(the default), 'descending', 'random1' (updates and deletes in the same "
                + "random order), or 'random2' (updates and deletes in 2 different random "
                + "orders); case insensitive.")
        String deleteupdateorder = "ascending";

        @Option(desc = "Number of updates/deletes to do before starting the snapshot; "
                + "default: -1, which means never start the snapshot.")
        long startSnapshotAtIteration = -1;

        @Option(desc = "Maximum TPS (transactions per second) limit for benchmark; "
                + "default: Integer.MAX_VALUE.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "deleteupdatesnapshot.csv";

        @Override
        public void validate() {
            if (displayinterval <= 0) {
                exitWithMessageAndUsage("displayinterval must be > 0");
            }
            if (servers.length() == 0) {
                servers = "localhost";
            }
            if (statsfile.length() == 0) {
                statsfile = "deleteupdatesnapshot.csv";
            }
            checkKnownValues("tabletypes",  tabletypes,  "replicated", "partitioned" /*, "view"*/ );
            checkKnownValues("columntypes", columntypes, "inline", "outline", "varbinary", "geo");
            checkKnownValues("deleteupdatevalues", deleteupdatevalues, "single", "range");
            checkKnownValues("deleteupdatecolumn", deleteupdatecolumn, "id", "modid", "blockid");
            checkKnownValues("deleteupdateorder",  deleteupdateorder,  "ascending", "descending",
                             "random", "random1", "random2");
            if (ratelimit <= 0) {
                exitWithMessageAndUsage("ratelimit must be > 0");
            }
        }

        private void checkKnownValues(String name, String values, String ... knownValues) {
            if (values == null || values.isEmpty()) {
                exitWithMessageAndUsage(name+" cannot be null or empty");
            }
            List<String> knownValuesList = Arrays.asList(knownValues);
            for (String val : values.split(",")) {
                if (!knownValuesList.contains(val.trim().toLowerCase())) {
                    exitWithMessageAndUsage("Unrecognized value '"+val+"' in "+name+": "+values);
                }
           }
        }
    } // end of DUSBConfig


    /** Initialize the DUSBenchmark, by adding lots of rows to the table(s). **/
    private void init() throws Exception {

        // Debug print:
        System.out.println("\n\nStart of DUSBenchmark.init()");

        // Get the lists of table names to which to add, delete, and update rows
        List<String> tableNamesForInsertValues = new ArrayList<String>();
        List<String> tableNamesForInsertFromSelect = new ArrayList<String>();
        String tableTypesLower = config.tabletypes.toLowerCase();
        tableNamesForInsertValues.add("DUSB_R1");
        if (tableTypesLower.contains("replicated")) {
            tableNamesForInsertFromSelect.add("DUSB_R1");
        }
        String delUpdColumnLowerCase = config.deleteupdatecolumn.toLowerCase();
        if (tableTypesLower.contains("partitioned")) {
            if ("modid".equals(delUpdColumnLowerCase)) {
                tableNamesForInsertFromSelect.add("DUSB_P2");
            } else if ("blockid".equals(delUpdColumnLowerCase)) {
                tableNamesForInsertFromSelect.add("DUSB_P3");
            } else {
                tableNamesForInsertFromSelect.add("DUSB_P1");
            }
        }
        tableNamesForDeleteAndUpdate = tableNamesForInsertFromSelect;

        // Get the list of column names to be set, for those columns that
        // will be non-null
        List<String> columnNames = getColumnNames();
        String[] colNamesArray  = columnNames.toArray(new String[0]);
        String[] colValuesArray = null;

        // Compute the number of "blocks" of rows to insert, from the total
        // number of rows, and the delete/update block size (num. rows):
        long minId = 0;
        long minBlockId = 0;
        long numRows = config.insertnumrows;
        long numInsertBlocks = config.deleteupdatenumrows;
        // If it divides evenly, it's simple
        insertBlockSize = numRows / numInsertBlocks;
        // Otherwise, round to one decimal place, i.e., to the nearest 0.1;
        // then take the "ceiling" (nearest integer above or equal to)
        if (numRows % numInsertBlocks != 0) {
            insertBlockSize = (long) Math.ceil( Math.round(10.0D * numRows / numInsertBlocks)
                                                         / 10.0D );
        }

        // If we are deleting (& updating) by block, then the above values are reversed
        if ("blockid".equals(delUpdColumnLowerCase)) {
            numInsertBlocks = insertBlockSize;
            insertBlockSize = config.deleteupdatenumrows;
        }


        // Insert the initial rows (1 "block"), one at a time
        for (String tableName : tableNamesForInsertValues) {
            String procName = "InsertOneRow";
            if (tableName != null && PARTITIONED_BY_ID.contains(tableName.toUpperCase())) {
                procName = "InsertOneRowPid";
            }

            // Debug print:
            System.out.println( "\nIn DUSBenchmark.init()"
                    +": tableNames(insert) "+tableNamesForInsertValues
                    +", tableNames(select) "+tableNamesForInsertFromSelect
                    +", tableNames(del&upd) "+tableNamesForDeleteAndUpdate
                    +", tableName "+tableName
                    +", duration(max) "+config.duration
                    +"\ncolumnNames: "+Arrays.toString(colNamesArray)
                    +"\nminId "+minId
                    +", insertnumrows "+numRows
                    +", deleteupdatenumrows "+config.deleteupdatenumrows
                    +", numInsertBlocks "+numInsertBlocks
                    +", insertBlockSize "+insertBlockSize );

            for (long rowId=0; rowId < insertBlockSize; rowId++) {
                // Get the array of random column values to be set, for those
                // columns that will be non-null
                colValuesArray = getColumnValues(colNamesArray);

                // Occasional debug print:
                boolean debugPrint = false;
                if (rowId == 0 || rowId == insertBlockSize / 2
                               || rowId == insertBlockSize - 1) {
                    debugPrint = true;
                    System.out.println( "\ncolValuesArray, row "+rowId+": "
                                        + Arrays.toString(colValuesArray) );
                }

                // Insert one row
                ClientResponse cr = client.callProcedure(procName, rowId, minBlockId,
                                    tableName, colNamesArray, colValuesArray);
                VoltTable vt = cr.getResults()[0];

                // Debug print: occasionally, or when something goes wrong
                if  (debugPrint || cr.getStatus() != ClientResponse.SUCCESS ||
                        vt.getStatusCode() != -128 ) {
                    System.out.println( "\nRow "+rowId
                             +": vt.getStatusCode "+vt.getStatusCode()
                             +", cr.getStatus "+cr.getStatus()
                             +" (SUCCESS="+ClientResponse.SUCCESS+")"
                             +", cr.getStatusString: "+cr.getStatusString() );
                }
            }
        }

        for (String tableName : tableNamesForInsertFromSelect) {

            // Due to limits  on Partioned tables, they need to be populated by
            // INSERT FROM SELECT queries that select from a Replicated table; so
            // a Partioned table needs to copy one extra "block" at the beginning,
            // whereas a Replicated table is copying from itself, so the first
            // "block" has already been populated
            long minBlock = minBlockId;
            if (REPLICATED_TABLES.contains(tableName.toUpperCase())) {
                minBlock = minBlockId + 1;
            }

            // Insert lots more rows, using InsertFromSelect, i.e., queries of the form:
            // INSERT INTO <table1> SELECT ...  FROM <table2> WHERE ...
            long maxId = insertBlockSize;
            for (long block = minBlock; block < numInsertBlocks + minBlockId; block++) {
                long addToId = block * insertBlockSize;

                // For the last block, only in the case where the insertBlockSize
                // did not divide evenly
                if (block == numInsertBlocks - 1 && numRows % numInsertBlocks != 0) {
                    maxId = numInsertBlocks*insertBlockSize - numRows;
                }

                // Insert many rows
                ClientResponse cr = client.callProcedureWithTimeout( 60000,
                        "InsertFromSelect", tableName, addToId, block, minId, maxId );
                VoltTable vt = cr.getResults()[0];

                // Debug print: occasionally, or when something goes wrong
                if ( block <= 1 || block == numInsertBlocks / 2 ||
                        block == numInsertBlocks - 1  ||
                        cr.getStatus() != ClientResponse.SUCCESS ||
                        vt.getStatusCode() != -128 ) {
                    System.out.println( "\nBlock "+block
                            +": addToId "+addToId+", numInsertBlocks "+numInsertBlocks
                            +", vt.getStatusCode "+vt.getStatusCode()
                            +", cr.getStatus "+cr.getStatus()
                            +" (SUCCESS="+ClientResponse.SUCCESS+"), "
                            +"cr.getStatusString: "+cr.getStatusString() );
                }
            }
        }

        // Set values needed by runBenchmark()
        minimumId = minimumModId = minId;
        minimumBlockId = minBlockId;
        // Technically, these are one more than the maximum allowed value
        maximumId = numRows + minId;
        maximumModId = insertBlockSize + minId;
        maximumBlockId = numInsertBlocks + minBlockId;

        // Debug print:
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
                    throwIllegalArgException("column name", colNamesArray[i]);
                }  // end of switch

            } catch (Exception e) {
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


    /** Throw an IllegalArgumentException, for the specified argument name and
     *  value, with an additional error message. **/
    private void throwIllegalArgException(String argName, String argValue,
            String additionalMessage) {
        throw new IllegalArgumentException("Illegal or Unknown "+argName+", '"
            +argValue+"' is not allowed"+additionalMessage);
    }

    /** Throw an IllegalArgumentException, for the specified argument name and value. **/
    private void throwIllegalArgException(String argName, String argValue) {
        throwIllegalArgException(argName, argValue, ".");
    }


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
            System.out.println("\nDUSBClientListener.connectionLost called, with: hostname "+hostname
                    +", port "+port+", connectionsLeft "+connectionsLeft+", cause "+cause+"\n");
        }

        @Override
        public void connectionCreated(String hostname, int port, AutoConnectionStatus status) {
            System.out.println("\nDUSBClientListener.connectionCreated called, with: hostname "
                    +hostname+", port "+port+", status "+status+"\n");
        }

        @Override
        public void uncaughtException(ProcedureCallback callback, ClientResponse cr, Throwable e) {
            System.out.println("\nDUSBClientListener.uncaughtException called, with callback: "
                    +callback+", ClientResponse "+cr+", Throwable "+e+"\n");
        }

        @Override
        public void lateProcedureResponse(ClientResponse cr, String hostname, int port) {
            System.out.println("\nDUSBClientListener.lateProcedureResponse called, with: hostname "
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


    /** Returns a String, suitable for printing, that contains the number of
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

        if (procName == null) {
            throwIllegalArgException("procName", procName, ", with tableName '"
                    +tableName+"', columnName '"+columnName+"', minValue "
                    +minValue+", maxValue "+maxValue+", inlineOrOutline '"
                    +inlineOrOutline+"', debugPrint "+debugPrint+". " );
        }

        // Debug print, occasionally or when there is backpressure
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

        boolean queued = true;

        if (callBack == null) {
            // synchronous calls (this doesn't normally happen)
            countSynchronousCalls++;
            ClientResponse cr = null;

            if (procName.startsWith("Delete")) {
                if (procName.equals("DeleteMultiValues")) {
                    cr = client.callProcedure(procName, tableName, columnName, minValue, maxValue);
                } else if (procName.equals("DeleteOneValue")) {
                    cr = client.callProcedure(procName, minValue, tableName, columnName);
                } else {
                    cr = client.callProcedure(procName, minValue, maxValue, tableName);
                }

            } else if (procName.startsWith("Update")) {
                if (procName.equals("UpdateMultiValues")) {
                    cr = client.callProcedure(procName, tableName, columnName, minValue, maxValue, inlineOrOutline);
                } else if (procName.equals("UpdateOneValue")) {
                    cr = client.callProcedure(procName, minValue, tableName, columnName, inlineOrOutline);
                } else {
                    cr = client.callProcedure(procName, minValue, tableName, inlineOrOutline);
                }
            } else if (procName != null && procName.equals("@SnapshotSave")) {
                cr = client.callProcedure(procName, tableName, columnName, minValue);

            } else {
                throwIllegalArgException("(synch) procName", procName, " (with tableName '"
                        +tableName+"', columnName '"+columnName+"', minValue "+minValue
                        +", maxValue "+maxValue+", inlineOrOutline '"+inlineOrOutline
                        +"', debugPrint "+debugPrint+"). " );
            }
            callBack = new DUSBenchmarkCallback(procName+"-synchronous");
            callBack.clientCallback(cr);

        } else {
            // asynchronous calls (as usual)
            if (procName.startsWith("Delete")) {
                if (procName.equals("DeleteMultiValues")) {
                    queued = client.callProcedure(callBack, procName, tableName, columnName, minValue, maxValue);
                } else if (procName.equals("DeleteOneValue")) {
                    queued = client.callProcedure(callBack, procName, minValue, tableName, columnName);
                } else {
                    queued = client.callProcedure(callBack, procName, minValue, tableName);
                }

            } else if (procName.startsWith("Update")) {
                if (procName.equals("UpdateMultiValues")) {
                    queued = client.callProcedure(callBack, procName, tableName, columnName, minValue, maxValue, inlineOrOutline);
                } else if (procName.equals("UpdateOneValue")) {
                    queued = client.callProcedure(callBack, procName, minValue, tableName, columnName, inlineOrOutline);
                } else {
                    queued = client.callProcedure(callBack, procName, minValue, tableName, inlineOrOutline);
                }
            } else if (procName != null && procName.equals("@SnapshotSave")) {
                queued = client.callProcedure(callBack, procName, tableName, columnName, minValue);

            } else {
                throwIllegalArgException("(asynch) procName", procName, " (with tableName '"
                        +tableName+"', columnName '"+columnName+"', minValue "+minValue
                        +", maxValue "+maxValue+", inlineOrOutline '"+inlineOrOutline
                        +"', debugPrint "+debugPrint+"). " );
            }
            countAsynchronousCalls++;
        }
        countProcCalls(procName);

        if (!queued) {
            System.err.println(procName+" not queued successfully, for table "
                +tableName+", column "+columnName+", minValue "+minValue
                +", maxValue "+maxValue+" (inlineOrOutline "+inlineOrOutline+").");
        }
    }


    /** Get the Stored Procedure name you want, based on various factors. **/
    private String getProcName(String procType, String tableName,
            String delUpdValues, String delUpdColumn) {

        if (procType == null || tableName == null || delUpdValues == null || delUpdColumn == null) {
            throw new IllegalArgumentException("Illegal null procType ("+procType+"), "
                    +"tableName ("+tableName+"), delUpdValues ("+delUpdValues+"), "
                    +"or delUpdColumn ("+delUpdColumn+")." );
        }

        String oneOrMultiValues = "OneValue";
        String partitioning = "";

        if ("range".equals(delUpdValues.toLowerCase())) {
            oneOrMultiValues = "MultiValues";

        } else if ("id".equals(delUpdColumn.toLowerCase()) &&
                PARTITIONED_BY_ID.contains(tableName.toUpperCase())) {
            partitioning = "Pid";

        } else if ("modid".equals(delUpdColumn.toLowerCase()) &&
                PARTITIONED_BY_MOD_ID.contains(tableName.toUpperCase())) {
            partitioning = "PmodId";

        } else if ("blockid".equals(delUpdColumn.toLowerCase()) &&
                PARTITIONED_BY_MOD_ID.contains(tableName.toUpperCase())) {
            partitioning = "PblockId";
        }

        return procType + oneOrMultiValues + partitioning;
    }


    /** Returns a list containing the Long values from 0 (inclusive) to
     *  <i>size</i> (exclusive), in a random order. */
    private List<Long> getRandomValuesList(int size) {
        List<Long> orderedList = new ArrayList<Long>(size);
        for (long i=0; i < size; i++) {
            orderedList.add(i);
        }

        List<Long> randomList = new ArrayList<Long>(size);
        for (long i=0; i < size; i++) {
            int next = rand.nextInt(orderedList.size());
            randomList.add(orderedList.remove(next));
        }

        return randomList;
    }


    /** Returns the next Long value in a named List of random (Long) values;
     *  the first time called with a new <i>name</i>, it generates the List. */
    private long getNextRandomValue(int i, int numIterations, String name) {
        if (randomValues.get(name) == null) {
            randomValues.put(name, getRandomValuesList(numIterations));

            // TODO: temp. debug:
            System.out.println("In getNextRandomValue:\n    name '"+name+"', numIterations "
                    +numIterations+", i "+i+", randomValues.get("+name+"):");
            if (numIterations <= 100) {
                // Print the whole (fairly short) List of random (Long) values
                System.out.println(randomValues.get(name));
            } else {
                // Print part of the (quite long) List of random (Long) values
                int halfway = numIterations/2 - 1;
                List<Long> all = randomValues.get(name);
                List<Long> begin  = all.subList(0,10);
                List<Long> middle = all.subList(halfway-5,halfway+5);
                List<Long> end    = all.subList(numIterations-10,numIterations);
                System.out.println(begin.toString().replace("]", ", ...") + "\n"
                                 +middle.toString().replace("[", " ").replace("]", ", ...") + "\n"
                                    +end.toString().replace("[", " ") );
            }

        }
        return randomValues.get(name).get(i);
    }


    /** Run the DUSBenchmark, by simultaneously running a snapshot, while
     *  updating and deleting lots of table rows. **/
    private void runBenchmark() throws Exception {
        System.out.println("\n\nStart of DUSBenchmark.runBenchmark()");

        // Prepare values used in the for loop below
        String columnName = null;
        long numIterations = -1, initMinValue = -1, minValueIncrement = 0,
            modIdIncrement = 0, blockIdIncrement = 0;
        String delUpdColumnLowerCase = config.deleteupdatecolumn.toLowerCase();
        String delUpdValuesLowerCase = config.deleteupdatevalues.toLowerCase();
        String delUpdOrderLowerCase  = config.deleteupdateorder.toLowerCase();
        boolean randomOrder = false;
        boolean random2Orders = false;
        if (delUpdOrderLowerCase.startsWith("random")) {
            randomOrder = true;
            if ("random2".equals(delUpdOrderLowerCase)) {
                random2Orders = true;
            }
        }

        if ("id".equals(delUpdColumnLowerCase)) {
            columnName = "ID";

            if ("single".equals(delUpdValuesLowerCase)) {
                numIterations = maximumId - minimumId;
                if (randomOrder || "ascending".equals(delUpdOrderLowerCase)) {
                    initMinValue = minimumId;
                    minValueIncrement = 1;
                } else if ("descending".equals(delUpdOrderLowerCase)) {
                    initMinValue = maximumId - 1;
                    minValueIncrement = -1;
                } else {
                    throwIllegalArgException("'deleteupdateorder' config "
                            +"parameter value", config.deleteupdateorder);
                }

            } else if ("range".equals(delUpdValuesLowerCase)) {
                numIterations = (int) Math.ceil( (maximumId - minimumId)*1.0D / insertBlockSize );
                if (randomOrder || "ascending".equals(delUpdOrderLowerCase)) {
                    initMinValue = minimumId;
                    minValueIncrement = insertBlockSize;
                } else if ("descending".equals(delUpdOrderLowerCase)) {
                    initMinValue = maximumId - insertBlockSize;
                    minValueIncrement = -insertBlockSize;
                } else {
                    throwIllegalArgException("'deleteupdateorder' config "
                            +"parameter value", config.deleteupdateorder);
                }

            } else {
                throwIllegalArgException("'deleteupdatecolumn' config "
                        +"parameter value", config.deleteupdatecolumn);
            }

        } else if ("modid".equals(delUpdColumnLowerCase)) {
            columnName = "MOD_ID";

            if ("single".equals(delUpdValuesLowerCase)) {
                numIterations = maximumModId - minimumModId;
                if (randomOrder || "ascending".equals(delUpdOrderLowerCase)) {
                    initMinValue = minimumModId;
                    minValueIncrement = 1;
                } else if ("descending".equals(delUpdOrderLowerCase)) {
                    initMinValue = maximumModId - 1;
                    minValueIncrement = -1;
                } else {
                    throwIllegalArgException("'deleteupdateorder' config "
                            +"parameter value", config.deleteupdateorder);
                }

            } else if ("range".equals(delUpdValuesLowerCase)) {
                // When deleting (& updating) by a range of MOD_ID values, we need an
                // equivalent to the 'insertBlockSize' used for ID values.  This MOD_ID
                // value increment, which is used to delete (or update) roughly the
                // intended number of rows with each stored proc call, should be the
                // insertBlockSize times the ratio of the total number of rows and
                // the total number of distinct MOD_ID values
                modIdIncrement = Math.round( (maximumModId - minimumModId) * insertBlockSize * 1.0D
                                              / (maximumId - minimumId) );
                // Cannot be less than one
                modIdIncrement = Math.max(1, modIdIncrement);

                numIterations = (long) Math.ceil( (maximumModId - minimumModId)*1.0D / modIdIncrement );
                if (randomOrder || "ascending".equals(delUpdOrderLowerCase)) {
                    initMinValue = minimumModId;
                    minValueIncrement = modIdIncrement;
                } else if ("descending".equals(delUpdOrderLowerCase)) {
                    initMinValue = maximumModId - modIdIncrement;
                    minValueIncrement = -modIdIncrement;
                } else {
                    throwIllegalArgException("'deleteupdateorder' config "
                            +"parameter value", config.deleteupdateorder);
                }

            } else {
                throwIllegalArgException("'deleteupdatecolumn' config "
                        +"parameter value", config.deleteupdatecolumn);
            }

        } else if ("blockid".equals(delUpdColumnLowerCase)) {
            columnName = "BLOCK_ID";

            if ("single".equals(delUpdValuesLowerCase)) {
                numIterations = maximumBlockId - minimumBlockId;
                if (randomOrder || "ascending".equals(delUpdOrderLowerCase)) {
                    initMinValue = minimumBlockId;
                    minValueIncrement = 1;
                } else if ("descending".equals(delUpdOrderLowerCase)) {
                    initMinValue = maximumBlockId - 1;
                    minValueIncrement = -1;
                } else {
                    throwIllegalArgException("'deleteupdateorder' config "
                            +"parameter value", config.deleteupdateorder);
                }

            } else if ("range".equals(delUpdValuesLowerCase)) {
                // When deleting (& updating) by a range of BLOCK_ID values, we need an
                // TODO
                // equivalent to the 'insertBlockSize' used for ID values.  This MOD_ID
                // value increment, which is used to delete (or update) roughly the
                // intended number of rows with each stored proc call, should be the
                // insertBlockSize times the ratio of the total number of rows and
                // the total number of distinct MOD_ID values
                blockIdIncrement = Math.round( (maximumBlockId - minimumBlockId) * 1.0D
                                              / (maximumId - minimumId) );
                // Cannot be less than one
                blockIdIncrement = Math.max(1, blockIdIncrement);

                numIterations = (long) Math.ceil( (maximumBlockId - minimumBlockId)*1.0D / blockIdIncrement );
                if (randomOrder || "ascending".equals(delUpdOrderLowerCase)) {
                    initMinValue = minimumBlockId;
                    minValueIncrement = blockIdIncrement;
                } else if ("descending".equals(delUpdOrderLowerCase)) {
                    initMinValue = maximumBlockId - blockIdIncrement;
                    minValueIncrement = -blockIdIncrement;
                } else {
                    throwIllegalArgException("'deleteupdateorder' config "
                            +"parameter value", config.deleteupdateorder);
                }

            } else {
                throwIllegalArgException("'deleteupdatecolumn' config "
                        +"parameter value", config.deleteupdatecolumn);
            }

        } else {
            throwIllegalArgException("'deleteupdatevalues' config "
                    + "parameter value", config.deleteupdatevalues);
        }

        // Which group of columns should get updated, in the various Update procs?
        String updateInlineOrOutlineColumns = "INLINE";
        if (config.columntypes.toLowerCase().contains("outline")) {
            updateInlineOrOutlineColumns = "OUTLINE";
        }

        // TODO: debug print:
        System.out.println("\nValues used in the main for loop"
                +": minimumId "+minimumId
                +", maximumId "+maximumId
                +", insertBlockSize "+insertBlockSize
                +", minimumModId "+minimumModId
                +", maximumModId "+maximumModId
                +", modIdIncrement "+modIdIncrement
                +", minimumBlockId "+minimumBlockId
                +", maximumBlockId "+maximumBlockId
                +", blockIdIncrement "+blockIdIncrement
                +": randomOrder "+randomOrder
                +": random2Orders "+random2Orders
                +";\ncolumnName "+columnName
                +", initMinValue "+initMinValue
                +", minValueIncrement "+minValueIncrement
                +", numIterations "+numIterations
                +", startSnapshotAtIteration "+config.startSnapshotAtIteration
                +", updateInlineOrOutlineColumns "+updateInlineOrOutlineColumns
                +";\ntableNamesForDeleteAndUpdate: "+tableNamesForDeleteAndUpdate+"\n" );

        // Set a few final values before starting the actual benchmark
        long maxValueIncrement = Math.abs(minValueIncrement);
        long halfway = numIterations/2 - 1;
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime maxEndTime = startTime.plusSeconds(config.duration);

        stats.startBenchmark(config.displayinterval);
        for (long i=0; i < numIterations; i++) {
            // Do not exceed the maximum duration
            if (LocalDateTime.now().isAfter(maxEndTime)) {
                break;
            }

            long next = i;
            if (randomOrder) {
                next = getNextRandomValue((int)i, (int)numIterations, "Update");
            }
            long minValue = initMinValue + next*minValueIncrement;
            long maxValue = minValue + maxValueIncrement;

            boolean debugPrint = false;
            if (i < 3 || i > numIterations - 3 ||
                    (i >= halfway-1 && i <= halfway+1)) {
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

            for (String procType : Arrays.asList("Update", "Delete")) {
                String inlineOrOutline = "Update".equals(procType) ? updateInlineOrOutlineColumns : null;
                if (random2Orders) {
                    next = getNextRandomValue((int)i, (int)numIterations, procType);
                    minValue = initMinValue + next*minValueIncrement;
                    maxValue = minValue + maxValueIncrement;
                }
                for (String tableName : tableNamesForDeleteAndUpdate) {
                    String procName = getProcName(procType, tableName, config.deleteupdatevalues,
                            config.deleteupdatecolumn);
                    if (debugPrint) {
                        System.out.println("\nIn runBenchmark(), calling callStoredProc, for i "+i+", next "+next+":");
                    }
                    callStoredProc( new DUSBenchmarkCallback(procName),
                            procName, tableName, columnName, minValue,
                            maxValue, inlineOrOutline, debugPrint );
                }
            }

        }

        // End of the DUS Benchmark
        LocalDateTime endTime = LocalDateTime.now();
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
