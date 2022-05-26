/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
import org.voltdb.ClientResponseImpl;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;


/** Benchmark for when Deletes, Updates, and a Snapshot */
public class DUSBenchmark {

    protected final DUSBConfig config;
    private Client client;
    private Random rand = new Random();
    private DUSBenchmarkStats stats;

    static String SNAPSHOT_ID = "DUSBenchmark";
    static String SNAPSHOT_DIR = "/tmp/DUSBenchmark";

    private static final int backpressureSleepMillis = 1000;
    private static Boolean backpressure = null;
    private static int countBackpressureCalls = 0;
    private static int countBackpressureDelays = 0;
    private static String JUST_DASHES = "----------------------------------------";
    private static String JUST_DOUBLE_DASHES = JUST_DASHES+JUST_DASHES;
    private static String DASHES = "\n"+JUST_DASHES+"\n";
    private static String DOUBLE_DASHES = "\n"+JUST_DOUBLE_DASHES;
    private static final String RANDOM_CHARS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";

    private  List<Long> batchIds = new ArrayList<>();

    public DUSBenchmark(DUSBConfig config) throws Exception {
        this.config = config;
        rand.setSeed(config.seed);
        DUSBClientListener dusbcl = new DUSBClientListener(config.insertnumrows);
        ClientConfig cc = new ClientConfig(null, null, dusbcl);
        cc.setMaxTransactionsPerSecond(config.ratelimit);
        cc.setProcedureCallTimeout(10 * 60 * 1000);       // 10 minutes
        cc.setConnectionResponseTimeout(20 * 60 * 1000);  // 20 minutes
        client = ClientFactory.createClient(cc);

        String[] serverArray = config.servers.split(",");
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

        @Option(desc = "Benchmark maximum duration, in seconds; default: 600 (10 minutes).")
        int duration = 600;

        @Option(desc = "Comma-separated list of the form server[:port] to "
                + "connect to database for queries; default: 'localhost'.")
        String servers = "localhost";

        @Option(desc = "Random seed, used to generate random numbers")
        long seed = 6782743657833577466L;

        @Option(desc = "Comma-separated list of one or more of the following: 'inline' "
                + "(i.e., numeric, timestamp, short varchar), 'outline' (i.e., non-inline: "
                + "long varchar columns), varbinary, geo (which varbinary or geo columns "
                + "are used depends on use of inline and/or outline)")
        String columntypes = "inline,outline";

        @Option(desc = "Number of rows to be inserted, at the start.")
        long insertnumrows = 10000000;

        @Option(desc = "The number of rows to be deleted or updated by each (stored proc) ")
        long deleteupdatenumrows = 100;

        @Option(desc = "Trigger snapshot during benchmark")
        boolean snapshot = false;

        @Option(desc = "Maximum TPS (transactions per second) limit.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Filename to write raw summary statistics to")
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
            checkKnownValues("columntypes", columntypes, "inline", "outline", "varbinary", "geo");
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
    }

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
    }

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
            }
        }
        return colValuesArray;
    }

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
    }

    /** Get a random Varbinary, as a String. **/
    private String getRandomVarbinaryString(int maxLength) {
        // TODO
        return null;
    }

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
    }

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
    }


    /** Convert an int into a two-digit String; e.g., 9 would become "09",
     *  while 10 would become "10". **/
    private String getTwoDigitString(int intValue) {
        return getNDigitString(2, intValue);
    }


    /** Get a random GeographyPointValue, as a String. **/
    private String getRandomGeographyPointString() {
        // TODO
        return null;
    }


    /** Get a random GeographyValue, with the specified number of points, at most, as a String. **/
    private String getRandomGeographyString(int maxNumPoints) {
        // TODO
        return null;
    }

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
        }
     }

    /**
     * Preload partitioned table
     * @throws Exception
     */
    private void initialize() throws Exception {
        System.out.println("Preparing......................");
        long totalInsertRows = config.insertnumrows;
        long updateDeleteBatchSize = config.deleteupdatenumrows;
        CountDownLatch latch = new CountDownLatch((int)totalInsertRows);
        List<String> columnNames = getColumnNames();
        String[] colNamesArray  = columnNames.toArray(new String[0]);
        String[] colValuesArray = null;
        for (long rowId = 0; rowId < totalInsertRows; rowId++) {
            colValuesArray = getColumnValues(colNamesArray);
            long blockId = rowId/updateDeleteBatchSize;
            if (!batchIds.contains(blockId)) {
                batchIds.add(blockId);
            }
            client.callProcedure(new DUSBenchmarkCallback(latch),
                        "InsertByBlock", blockId, rowId,
                                      colNamesArray, colValuesArray);
        }
        latch.await();
    }

    /**
     * Update or delete by block id
     * @throws Exception
     */
    private void runBenchmark() throws Exception {

         System.out.println( JUST_DOUBLE_DASHES + "\nBenchmark beginning " + DOUBLE_DASHES );
         int outline = 0;
         if (config.columntypes.toLowerCase().contains("outline")) {
             outline = 1;
         }

         long start = System.currentTimeMillis();
         final long benchmarkEndTime = start + (1000l * config.duration);
         stats.startBenchmark(config.displayinterval);
         //Request a snapshot after 1/3 benchmark is done if requested.
         int snapshotAtBatch = (config.snapshot ? batchIds.size()/3 : -1);
         //Declare it done if the designated duration is used or all blocks are deleted
         while (benchmarkEndTime > System.currentTimeMillis() && batchIds.size() > 0) {

             //Randomly get an index of block IDs with which update is done.
             int idx = rand.nextInt(batchIds.size());
             client.callProcedure(new DUSBenchmarkCallback(),
                     "UpdateByBlock", batchIds.get(idx), outline);

             //Randomly get an index of block IDs with which delete is done.
             idx = rand.nextInt(batchIds.size());
             client.callProcedure(new DUSBenchmarkCallback(),
                     "DeleteByBlock", batchIds.get(idx));

             //Remove the blockId so it won't be used again
             batchIds.remove(idx);
             snapshotAtBatch--;
             if (snapshotAtBatch == 0) {
                 client.callProcedure(new DUSBenchmarkCallback(),
                         "@SnapshotSave", SNAPSHOT_DIR, SNAPSHOT_ID, 0);
             }
         }

         stats.endBenchmark(config.statsfile);

         long timeLast = (System.currentTimeMillis() - start) /1000;
         System.out.println( JUST_DOUBLE_DASHES + "\nBenchmark ended after " + timeLast + " seconds."+ DOUBLE_DASHES);
         client.close();
    }

    public static void main(String[] args) throws Exception {
        DUSBConfig config = new DUSBConfig();
        config.parse(DUSBenchmark.class.getName(), args);

        DUSBenchmark benchmark = new DUSBenchmark(config);
        benchmark.initialize();
        benchmark.runBenchmark();
    }
}

