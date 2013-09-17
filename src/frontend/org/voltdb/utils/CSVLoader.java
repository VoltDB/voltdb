/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv_voltpatches.tokenizer.Tokenizer;
import org.voltcore.logging.VoltLogger;

import org.voltdb.CLIConfig;
import org.voltdb.LegacyHashinator;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;

/**
 * CSVLoader is a simple utility to load data from a CSV formatted file to a table.
 *
 * This utility processes partitioned data efficiently and creates as many partition processors. For partitioned data
 * each processor calls
 * @LoadSinglepartitionTable
 *
 * For multi-partitioned data it uses a single processor which call
 * @LoadMultipartitionTable
 *
 * The maxerror indicates maximum number of errors it can tolerate. Its a threshold but since processors are processing
 * in parallel we may process rows beyond maxerror and additional errors may occur. Only first maxerror indicated errors
 * will be reported.
 */
public class CSVLoader {

    public static String pathInvalidrowfile = "";
    public static String pathReportfile = "csvloaderReport.log";
    public static String pathLogfile = "csvloaderLog.log";
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");
    private static CSVConfig config = null;
    private static long latency = 0;
    private static long start = 0;
    private static boolean standin = false;
    private static BufferedWriter out_invaliderowfile;
    private static BufferedWriter out_logfile;
    private static BufferedWriter out_reportfile;
    private static String insertProcedure = "";
    private static CsvPreference csvPreference = null;
    public static final char DEFAULT_SEPARATOR = ',';
    public static final char DEFAULT_QUOTE_CHARACTER = '\"';
    public static final char DEFAULT_ESCAPE_CHARACTER = '\\';
    public static final boolean DEFAULT_STRICT_QUOTES = false;
    public static final int DEFAULT_SKIP_LINES = 0;
    public static final boolean DEFAULT_NO_WHITESPACE = false;
    public static final long DEFAULT_COLUMN_LIMIT_SIZE = 16777216;
    //Intialized data for CSVLoader
    private static List<VoltType> typeList = new ArrayList<VoltType>();
    private static int partitionedColumnIndex = -1;
    private static int columnCnt = 0;
    private static VoltType partitionColumnType = VoltType.NULL;
    private static VoltTable.ColumnInfo colInfo[];
    private static Map<Integer, VoltType> columnTypes;
    private static Map<Integer, String> colNames;
    private static boolean isMP;
    private static int numProcessors = 1;

    public static class CSVConfig extends CLIConfig {

        @Option(shortOpt = "f", desc = "location of CSV input file")
        String file = "";
        @Option(shortOpt = "p", desc = "procedure name to insert the data into the database")
        String procedure = "";
        @Option(desc = "maximum rows to be read from the CSV file")
        int limitrows = Integer.MAX_VALUE;
        @Option(shortOpt = "r", desc = "directory path for report files")
        String reportdir = System.getProperty("user.dir");
        @Option(shortOpt = "m", desc = "maximum errors allowed")
        int maxerrors = 100;
        @Option(desc = "different ways to handle blank items: {error|null|empty} (default: error)")
        String blank = "error";
        @Option(desc = "delimiter to use for separating entries")
        char separator = DEFAULT_SEPARATOR;
        @Option(desc = "character to use for quoted elements (default: \")")
        char quotechar = DEFAULT_QUOTE_CHARACTER;
        @Option(desc = "character to use for escaping a separator or quote (default: \\)")
        char escape = DEFAULT_ESCAPE_CHARACTER;
        @Option(desc = "require all input values to be enclosed in quotation marks", hasArg = false)
        boolean strictquotes = DEFAULT_STRICT_QUOTES;
        @Option(desc = "number of lines to skip before inserting rows into the database")
        long skip = DEFAULT_SKIP_LINES;
        @Option(desc = "do not allow whitespace between values and separators", hasArg = false)
        boolean nowhitespace = DEFAULT_NO_WHITESPACE;
        @Option(desc = "max size of a quoted column in bytes(default: 16777216 = 16MB)")
        long columnsizelimit = DEFAULT_COLUMN_LIMIT_SIZE;
        @Option(shortOpt = "s", desc = "list of servers to connect to (default: localhost)")
        String servers = "localhost";
        @Option(desc = "username when connecting to the servers")
        String user = "";
        @Option(desc = "password to use when connecting to servers")
        String password = "";
        @Option(desc = "port to use when connecting to database (default: 21212)")
        int port = Client.VOLTDB_SERVER_PORT;
        @Option(desc = "Batch Size for processing.")
        public int batch = 200;
        @AdditionalArgs(desc = "insert the data into database by TABLENAME.insert procedure by default")
        public String table = "";
        boolean useSuppliedProcedure = false;

        @Override
        public void validate() {
            if (maxerrors < 0) {
                exitWithMessageAndUsage("abortfailurecount must be >=0");
            }
            if (procedure.equals("") && table.equals("")) {
                exitWithMessageAndUsage("procedure name or a table name required");
            }
            if (!procedure.equals("") && !table.equals("")) {
                exitWithMessageAndUsage("Only a procedure name or a table name required, pass only one please");
            }
            if (skip < 0) {
                exitWithMessageAndUsage("skipline must be >= 0");
            }
            if (limitrows > Integer.MAX_VALUE) {
                exitWithMessageAndUsage("limitrows to read must be < "
                        + Integer.MAX_VALUE);
            }
            if (port < 0) {
                exitWithMessageAndUsage("port number must be >= 0");
            }
            if (batch < 0) {
                exitWithMessageAndUsage("batch size number must be >= 0");
            }
            if ((blank.equalsIgnoreCase("error")
                    || blank.equalsIgnoreCase("null")
                    || blank.equalsIgnoreCase("empty")) == false) {
                exitWithMessageAndUsage("blank configuration specified must be one of {error|null|empty}");
            }
            if ((procedure != null) && (procedure.trim().length() > 0)) {
                useSuppliedProcedure = true;
            }
        }

        @Override
        public void printUsage() {
            System.out
                    .println("Usage: csvloader [args] tablename");
            System.out
                    .println("       csvloader [args] -p procedurename");
            super.printUsage();
        }
    }

    private static boolean isProcedureMp(Client csvClient)
            throws IOException, org.voltdb.client.ProcCallException {
        boolean procedure_is_mp = false;
        VoltTable procInfo = csvClient.callProcedure("@SystemCatalog",
                "PROCEDURES").getResults()[0];
        while (procInfo.advanceRow()) {
            if (insertProcedure.matches(procInfo.getString("PROCEDURE_NAME"))) {
                String remarks = procInfo.getString("REMARKS");
                if (remarks.contains("\"singlePartition\":false")) {
                    procedure_is_mp = true;
                }
                break;
            }
        }
        return procedure_is_mp;
    }

    // Based on method of loading do sanity check and setup info for loading.
    private static void setupCSVLoader(Client csvClient) throws IOException, ProcCallException, InterruptedException {
        VoltTable procInfo;
        if (config.useSuppliedProcedure) {
            // -p mode where user specified a procedure name could be standard CRUD or written from scratch.
            boolean isProcExist = false;
            procInfo = csvClient.callProcedure("@SystemCatalog",
                    "PROCEDURECOLUMNS").getResults()[0];
            while (procInfo.advanceRow()) {
                if (insertProcedure.matches((String) procInfo.get(
                        "PROCEDURE_NAME", VoltType.STRING))) {
                    columnCnt++;
                    isProcExist = true;
                    String typeStr = (String) procInfo.get("TYPE_NAME", VoltType.STRING);
                    typeList.add(VoltType.typeFromString(typeStr));
                }
            }
            if (isProcExist == false) {
                m_log.error("No matching insert procedure available");
                close_cleanup();
                System.exit(-1);
            }
            isMP = isProcedureMp(csvClient);
        } else {
            //Table mode get table details and then build partition and column information.
            procInfo = csvClient.callProcedure("@SystemCatalog",
                    "COLUMNS").getResults()[0];
            columnTypes = new TreeMap<Integer, VoltType>();
            colNames = new TreeMap<Integer, String>();
            while (procInfo.advanceRow()) {
                String table = procInfo.getString("TABLE_NAME");
                if (config.table.equalsIgnoreCase(table)) {
                    VoltType vtype = VoltType.typeFromString(procInfo.getString("TYPE_NAME"));
                    int idx = (int) procInfo.getLong("ORDINAL_POSITION") - 1;
                    columnTypes.put(idx, vtype);
                    colNames.put(idx, procInfo.getString("COLUMN_NAME"));
                    String remarks = procInfo.getString("REMARKS");
                    if (remarks != null && remarks.equalsIgnoreCase("PARTITION_COLUMN")) {
                        partitionColumnType = vtype;
                        partitionedColumnIndex = idx;
                        m_log.info("Table " + config.table + " Partition Column Name is: " + procInfo.getString("COLUMN_NAME"));
                        m_log.info("Table " + config.table + " Partition Column Type is: " + vtype.toString());
                    }
                }
            }

            if (columnTypes.isEmpty()) {
                m_log.error("Table " + config.table + " Not found");
                close_cleanup();
                System.exit(-1);
            }
            columnCnt = columnTypes.size();
            //Build column info so we can build VoltTable
            colInfo = new VoltTable.ColumnInfo[columnTypes.size()];
            for (int i = 0; i < columnTypes.size(); i++) {
                VoltType type = columnTypes.get(i);
                String cname = colNames.get(i);
                VoltTable.ColumnInfo ci = new VoltTable.ColumnInfo(cname, type);
                colInfo[i] = ci;
            }
            typeList = new ArrayList<VoltType>(columnTypes.values());
            int sitesPerHost = 1;
            int kfactor = 0;
            int hostcount = 1;
            procInfo = csvClient.callProcedure("@SystemInformation",
                    "deployment").getResults()[0];
            while (procInfo.advanceRow()) {
                String prop = procInfo.getString("PROPERTY");
                if (prop != null && prop.equalsIgnoreCase("sitesperhost")) {
                    sitesPerHost = Integer.parseInt(procInfo.getString("VALUE"));
                }
                if (prop != null && prop.equalsIgnoreCase("hostcount")) {
                    hostcount = Integer.parseInt(procInfo.getString("VALUE"));
                }
                if (prop != null && prop.equalsIgnoreCase("kfactor")) {
                    kfactor = Integer.parseInt(procInfo.getString("VALUE"));
                }
            }
            isMP = (partitionedColumnIndex == -1 ? true : false);
            if (!isMP) {
                numProcessors = (hostcount * sitesPerHost) / (kfactor + 1);
                m_log.info("Number of Partitions: " + numProcessors);
                m_log.info("Batch Size is: " + config.batch);

                TheHashinator.initialize(LegacyHashinator.class, LegacyHashinator.getConfigureBytes(numProcessors));
            }
        }

        if (isMP) {
            m_log.warn("Using a multi-partitioned procedure to load data will be slow. "
                    + "If loading a partitioned table, use a single-partitioned procedure "
                    + "for best performance.");
        }
    }

    public static void main(String[] args) throws IOException,
            InterruptedException {
        start = System.currentTimeMillis();
        long insertTimeStart = start;
        long insertTimeEnd;

        final CSVConfig cfg = new CSVConfig();
        cfg.parse(CSVLoader.class.getName(), args);

        config = cfg;
        configuration();
        final Tokenizer tokenizer;
        ICsvListReader listReader = null;
        try {
            if (CSVLoader.standin) {
                tokenizer = new Tokenizer(new BufferedReader(new InputStreamReader(System.in)), csvPreference,
                        config.strictquotes, config.escape, config.columnsizelimit,
                        config.skip);
                listReader = new CsvListReader(tokenizer, csvPreference);
            } else {
                tokenizer = new Tokenizer(new FileReader(config.file), csvPreference,
                        config.strictquotes, config.escape, config.columnsizelimit,
                        config.skip);
                listReader = new CsvListReader(tokenizer, csvPreference);
            }
        } catch (FileNotFoundException e) {
            m_log.error("CSV file '" + config.file + "' could not be found.");
            System.exit(-1);
        }
        // Split server list
        final String[] serverlist = config.servers.split(",");

        // Create connection
        final ClientConfig c_config = new ClientConfig(config.user, config.password);
        c_config.setProcedureCallTimeout(0); // Set procedure all to infinite
        Client csvClient = null;
        try {
            csvClient = CSVLoader.getClient(c_config, serverlist, config.port);
        } catch (Exception e) {
            m_log.error("Error connecting to the servers: "
                    + config.servers);
            close_cleanup();
            System.exit(-1);
        }
        assert (csvClient != null);

        try {
            setupCSVLoader(csvClient);

            CountDownLatch processor_cdl = new CountDownLatch(numProcessors);
            CSVPartitionProcessor.processor_cdl = processor_cdl;
            CSVPartitionProcessor.colInfo = colInfo;
            CSVPartitionProcessor.columnTypes = columnTypes;
            CSVPartitionProcessor.insertProcedure = insertProcedure;
            CSVPartitionProcessor.isMP = isMP;
            CSVPartitionProcessor.config = config;
            CSVPartitionProcessor.tableName = config.table;

            //Create a launch processor threads. If Multipartitioned onle 1 processor is launched.
            List<Thread> spawned = new ArrayList<Thread>(numProcessors);
            CSVLineWithMetaData endOfData = new CSVLineWithMetaData(null, null, -1);

            Map<Integer, BlockingQueue<CSVLineWithMetaData>> lineq = new HashMap<Integer, BlockingQueue<CSVLineWithMetaData>>(numProcessors);
            List<CSVPartitionProcessor> processors = new ArrayList<CSVPartitionProcessor>(numProcessors);
            for (int i = 0; i < numProcessors; i++) {
                LinkedBlockingQueue<CSVLineWithMetaData> partitionQueue = new LinkedBlockingQueue<CSVLineWithMetaData>(Integer.MAX_VALUE);
                lineq.put(i, partitionQueue);
                CSVPartitionProcessor processor = new CSVPartitionProcessor(csvClient, i, partitionedColumnIndex,
                        partitionQueue, endOfData);
                processors.add(processor);

                Thread th = new Thread(processor);
                th.setName(processor.m_processorName);
                spawned.add(th);
            }

            CSVFileReader.config = config;
            CSVFileReader.listReader = listReader;
            CSVFileReader.partitionedColumnIndex = partitionedColumnIndex;
            CSVFileReader.partitionColumnType = partitionColumnType;
            CSVFileReader.typeList = typeList;
            CSVFileReader.columnCnt = columnCnt;
            CSVFileReader.csvClient = csvClient;
            CSVFileReader.processorQueues = lineq;
            CSVFileReader.endOfData = endOfData;
            CSVFileReader.processor_cdl = processor_cdl;

            CSVFileReader csvReader = new CSVFileReader();
            Thread th = new Thread(csvReader);
            th.setName("CSVFileReader");
            th.setDaemon(true);

            //Start the processor threads.
            for (Thread th2 : spawned) {
                th2.start();
            }

            //Wait for reader to finish it count downs the procesors.
            th.start();
            th.join();

            long readerTime = (csvReader.m_parsingTimeEnd - csvReader.m_parsingTimeSt) / 1000000;

            insertTimeEnd = System.currentTimeMillis();

            csvClient.drain();
            csvClient.close();
            long insertCount = 0;
            for (CSVPartitionProcessor pp : processors) {
                insertCount += pp.m_partitionProcessedCount.get();
            }
            long ackCount = CSVPartitionProcessor.partitionAcknowledgedCount.get();
            m_log.info("Parsing CSV file took " + readerTime + " milliseconds.");
            m_log.info("Inserting Data took " + ((insertTimeEnd - insertTimeStart) - readerTime) + " milliseconds.");
            m_log.info("Inserted " + insertCount + " and acknowledged "
                    + ackCount + " rows (final)");
            produceFiles(ackCount, insertCount);
            close_cleanup();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void configuration() {
        csvPreference = new CsvPreference.Builder(config.quotechar, config.separator, "\n").build();
        if (config.file.equals("")) {
            standin = true;
        }
        if (!config.table.equals("")) {
            insertProcedure = config.table.toUpperCase() + ".insert";
        } else {
            insertProcedure = config.procedure;
        }
        if (!config.reportdir.endsWith("/")) {
            config.reportdir += "/";
        }
        try {
            File dir = new File(config.reportdir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
        } catch (Exception x) {
            m_log.error(x.getMessage(), x);
            System.exit(-1);
        }

        String myinsert = insertProcedure;
        myinsert = myinsert.replaceAll("\\.", "_");
        pathInvalidrowfile = config.reportdir + "csvloader_" + myinsert + "_"
                + "invalidrows.csv";
        pathLogfile = config.reportdir + "csvloader_" + myinsert + "_"
                + "log.log";
        pathReportfile = config.reportdir + "csvloader_" + myinsert + "_"
                + "report.log";

        try {
            out_invaliderowfile = new BufferedWriter(new FileWriter(
                    pathInvalidrowfile));
            out_logfile = new BufferedWriter(new FileWriter(pathLogfile));
            out_reportfile = new BufferedWriter(new FileWriter(pathReportfile));
        } catch (IOException e) {
            m_log.error(e.getMessage());
            System.exit(-1);
        }
    }

    public static Client getClient(ClientConfig config, String[] servers,
            int port) throws Exception {
        final Client client = ClientFactory.createClient(config);

        for (String server : servers) {
            client.createConnection(server.trim(), port);
        }
        return client;
    }

    private static void produceFiles(long ackCount, long insertCount) {
        Map<Long, String[]> errorInfo = CSVFileReader.errorInfo;
        latency = System.currentTimeMillis() - start;
        m_log.info("CSVLoader elapsed: " + latency / 1000F
                + " seconds");

        int bulkflush = 300; // by default right now
        try {
            long linect = 0;
            for (Long irow : errorInfo.keySet()) {
                String info[] = errorInfo.get(irow);
                if (info.length != 2) {
                    System.out
                            .println("internal error, information is not enough");
                }
                linect++;
                out_invaliderowfile.write(info[0] + "\n");
                String message = "Invalid input on line " + irow + ".\n  Contents:" + info[0];
                m_log.error(message);
                out_logfile.write(message + "\n  " + info[1] + "\n");
                if (linect % bulkflush == 0) {
                    out_invaliderowfile.flush();
                    out_logfile.flush();
                }
            }

            // Get elapsed time in seconds
            float elapsedTimeSec = latency / 1000F;
            out_reportfile.write("CSVLoader elaspsed: " + elapsedTimeSec
                    + " seconds\n");
            long trueSkip = 0;
            //get the actuall number of lines skipped
            if (config.skip < CSVFileReader.totalLineCount.get()) {
                trueSkip = config.skip;
            } else {
                trueSkip = CSVFileReader.totalLineCount.get();
            }
            out_reportfile.write("Number of input lines skipped: "
                    + trueSkip + "\n");
            out_reportfile.write("Number of lines read from input: "
                    + (CSVFileReader.totalLineCount.get() - trueSkip) + "\n");
            if (config.limitrows == -1) {
                out_reportfile.write("Input stopped after " + CSVFileReader.totalRowCount.get() + " rows read" + "\n");
            }
            out_reportfile.write("Number of rows discovered: "
                    + CSVFileReader.totalLineCount.get() + "\n");
            out_reportfile.write("Number of rows successfully inserted: "
                    + ackCount + "\n");
            // if prompted msg changed, change it also for test case
            out_reportfile.write("Number of rows that could not be inserted: "
                    + errorInfo.size() + "\n");
            out_reportfile.write("CSVLoader rate: " + insertCount
                    / elapsedTimeSec + " row/s\n");

            m_log.info("invalid row file is generated to:" + pathInvalidrowfile);
            m_log.info("log file is generated to:" + pathLogfile);
            m_log.info("report file is generated to:" + pathReportfile);

            out_invaliderowfile.flush();
            out_logfile.flush();
            out_reportfile.flush();
        } catch (FileNotFoundException e) {
            m_log.error("CSV report directory '" + config.reportdir
                    + "' does not exist.");
        } catch (Exception x) {
            m_log.error(x.getMessage());
        }

    }

    private static void close_cleanup() throws IOException,
            InterruptedException {
        //Reset all this for tests which uses main to load csv data.
        CSVFileReader.errorInfo.clear();
        CSVFileReader.errored = false;
        CSVLoader.numProcessors = 1;
        CSVLoader.columnCnt = 0;
        CSVFileReader.totalLineCount = new AtomicLong(0);
        CSVFileReader.totalRowCount = new AtomicLong(0);

        CSVPartitionProcessor.partitionAcknowledgedCount = new AtomicLong(0);
        out_invaliderowfile.close();
        out_logfile.close();
        out_reportfile.close();
    }
}
