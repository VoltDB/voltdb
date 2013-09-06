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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

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

/**
 * CSVLoaderMT is a simple utility to load data from a CSV formatted file to a
 * table (or pass it to any stored proc, but ignoring any result other than the
 * success code.).
 */
public class CSVLoaderMT {

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
    private static Map<VoltType, String> blankValues = new HashMap<VoltType, String>();

    static {
        blankValues.put(VoltType.NUMERIC, "0");
        blankValues.put(VoltType.TINYINT, "0");
        blankValues.put(VoltType.SMALLINT, "0");
        blankValues.put(VoltType.INTEGER, "0");
        blankValues.put(VoltType.BIGINT, "0");
        blankValues.put(VoltType.FLOAT, "0.0");
        blankValues.put(VoltType.TIMESTAMP, "0");
        blankValues.put(VoltType.STRING, "");
        blankValues.put(VoltType.DECIMAL, "0");
        blankValues.put(VoltType.VARBINARY, "");
    }
    private static List<VoltType> typeList = new ArrayList<VoltType>();

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
        @Option(desc = "Use @Ping")
        boolean ping = false;
        @Option(desc = "Batch Size for processing.")
        public long batch = 200;
        @Option(desc = "Use Load Table?")
        public boolean loadTable = false;
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

    public static void main(String[] args) throws IOException,
            InterruptedException {
        start = System.currentTimeMillis();
        long insertTimeStart = start;
        long insertTimeEnd;

        final CSVConfig cfg = new CSVConfig();
        cfg.parse(CSVLoaderMT.class.getName(), args);

        config = cfg;
        configuration();
        final Tokenizer tokenizer;
        ICsvListReader listReader = null;
        try {
            if (CSVLoaderMT.standin) {
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
            csvClient = CSVLoaderMT.getClient(c_config, serverlist, config.port);
        } catch (Exception e) {
            m_log.error("Error to connect to the servers:"
                    + config.servers);
            close_cleanup();
            System.exit(-1);
        }
        assert (csvClient != null);

        int partitionedColumnIndex = -1;
        VoltType partitionColumnType = VoltType.NULL;
        try {
            int columnCnt = 0;
            VoltTable procInfo;
            boolean isProcExist = false;
            try {
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
            } catch (Exception e) {
                m_log.error(e.getMessage(), e);
                close_cleanup();
                System.exit(-1);
            }
            if (isProcExist == false) {
                m_log.error("No matching insert procedure available");
                close_cleanup();
                System.exit(-1);
            }
            try {
                if (isProcedureMp(csvClient)) {
                    m_log.warn("Using a multi-partitioned procedure to load data will be slow. "
                            + "If loading a partitioned table, use a single-partitioned procedure "
                            + "for best performance.");
                }
            } catch (Exception e) {
                m_log.fatal(e.getMessage(), e);
                close_cleanup();
                System.exit(-1);
            }

            ArrayList<VoltType> columnTypes = new ArrayList<VoltType>();
            ArrayList<String> colNames = new ArrayList<String>();
            procInfo = csvClient.callProcedure("@SystemCatalog",
                    "COLUMNS").getResults()[0];
            while (procInfo.advanceRow()) {
                String table = procInfo.getString("TABLE_NAME");
                if (config.table.equalsIgnoreCase(table)) {
                    VoltType vtype = VoltType.typeFromString(procInfo.getString("TYPE_NAME"));
                    columnTypes.add(vtype);
                    colNames.add(procInfo.getString("COLUMN_NAME"));
                    String remarks = procInfo.getString("REMARKS");
                    if (remarks != null && remarks.equalsIgnoreCase("PARTITION_COLUMN")) {
                        partitionedColumnIndex = (int) procInfo.getLong("ORDINAL_POSITION");
                        partitionColumnType = vtype;
                        System.out.println("Partition Column Name is: " + procInfo.getString("COLUMN_NAME"));
                        System.out.println("Partition Column Type is: " + vtype.toString());
                    }
                }
            }

            VoltTable.ColumnInfo colInfo[] = new VoltTable.ColumnInfo[columnTypes.size()];
            for (int i = 0; i < columnTypes.size(); i++) {
                VoltType type = columnTypes.get(i);
                String cname = colNames.get(i);
                VoltTable.ColumnInfo ci = new VoltTable.ColumnInfo(cname, type);
                colInfo[i] = ci;
            }

            int numPartitions = -1;
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
            numPartitions = (hostcount * sitesPerHost) / (kfactor + 1);
            System.out.println("Number of Partitions: " + numPartitions);
            System.out.println("Batch Size is: " + config.batch);

            TheHashinator.initialize(LegacyHashinator.class, LegacyHashinator.getConfigureBytes(numPartitions));

            CSVPartitionProcessor.colInfo = colInfo;
            CSVPartitionProcessor.columnTypes = columnTypes;
            CSVPartitionProcessor.insertProcedure = insertProcedure;
            CSVPartitionProcessor.isMP = (partitionedColumnIndex == -1 ? true : false);
            CSVPartitionProcessor.isLoadTable = config.loadTable;
            CSVPartitionProcessor.config = config;

            List<Thread> spawned = new ArrayList<Thread>(numPartitions);
            CSVLineWithMetaData dummy = new CSVLineWithMetaData();
            Map<Integer, BlockingQueue<CSVLineWithMetaData>> lineq = new HashMap<Integer, BlockingQueue<CSVLineWithMetaData>>(numPartitions);
            CountDownLatch pcount = new CountDownLatch(numPartitions);
            List<CSVPartitionProcessor> processors = new ArrayList<CSVPartitionProcessor>(numPartitions);
            for (int i = 0; i < numPartitions; i++) {
                ArrayBlockingQueue<CSVLineWithMetaData> q = new ArrayBlockingQueue<CSVLineWithMetaData>((int) config.batch * 2);
                lineq.put(i, q);
                CSVPartitionProcessor pp = new CSVPartitionProcessor();
                processors.add(pp);
                pp.csvClient = csvClient;
                pp.partitionId = i;
                pp.tableName = config.table;
                pp.columnCnt = columnCnt;
                pp.lineq = q;
                pp.dummy = dummy;
                Thread th = new Thread(pp);
                th.setName("PartitionProcessor-" + 0);
                spawned.add(th);
            }
            CSVPartitionProcessor.pcount = pcount;

            CSVFileReader.config = config;
            CSVFileReader.columnCnt = columnCnt;
            CSVFileReader.listReader = listReader;
            CSVFileReader.partitionedColumnIndex = partitionedColumnIndex;
            CSVFileReader.partitionColumnType = partitionColumnType;
            CSVFileReader.tableName = config.table;
            CSVFileReader.typeList = typeList;
            CSVFileReader.csvClient = csvClient;
            CSVFileReader.lineq = lineq;
            CSVFileReader.dummy = dummy;
            CSVFileReader.pcount = pcount;

            CSVFileReader rdr = new CSVFileReader();
            Thread th = new Thread(rdr);
            th.setName("CSVReader");
            th.setDaemon(true);
            th.start();

            for (Thread th2 : spawned) {
                th2.start();
            }

            th.join();

            long readerTime = 0;
            readerTime += (rdr.parsingTimeEnd - rdr.parsingTimeSt);
            readerTime = readerTime / 1000000;

            for (Thread th2 : spawned) {
                try {
                    th2.join();
                } catch (InterruptedException ex) {
                }
            }
            insertTimeEnd = System.currentTimeMillis();

            csvClient.drain();
            csvClient.close();
            long inCount = 0, outCount = 0;
            for (CSVPartitionProcessor pp : processors) {
                inCount += pp.partitionProcessedCount;
                outCount += pp.partitionAcknowledgedCount.get();
            }
            m_log.info("Parsing CSV file took " + readerTime + " milliseconds.");
            m_log.info("Inserting Data took " + ((insertTimeEnd - insertTimeStart) - readerTime) + " milliseconds.");
            m_log.info("Inserted " + inCount + " and acknowledged "
                    + outCount + " rows (final)");
            produceFiles(rdr, inCount, outCount);
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

    private static void produceFiles(CSVFileReader rdr, long inCount, long outCount) {
        Map<Long, String[]> errorInfo = CSVFileReader.errorInfo;
        latency = System.currentTimeMillis() - start;
        m_log.info("CSVLoader elapsed: " + latency
                + " milliseconds");

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
                    + CSVFileReader.totalRowCount.get() + "\n");
            out_reportfile.write("Number of rows successfully inserted: "
                    + inCount + "\n");
            // if prompted msg changed, change it also for test case
            out_reportfile.write("Number of rows that could not be inserted: "
                    + errorInfo.size() + "\n");
            out_reportfile.write("CSVLoader rate: " + outCount
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
        out_invaliderowfile.close();
        out_logfile.close();
        out_reportfile.close();
    }
}
