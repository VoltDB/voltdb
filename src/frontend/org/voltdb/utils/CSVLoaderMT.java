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
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv_voltpatches.tokenizer.Tokenizer;
import org.voltcore.logging.VoltLogger;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ProcedureCallback;

/**
 * CSVLoader is a simple utility to load data from a CSV formatted file to a
 * table (or pass it to any stored proc, but ignoring any result other than the
 * success code.).
 */
public class CSVLoaderMT {
    public static String pathInvalidrowfile = "";
    public static String pathReportfile = "csvloaderReport.log";
    public static String pathLogfile = "csvloaderLog.log";

    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");

    private static final AtomicLong inCount = new AtomicLong(0);
    private static final AtomicLong outCount = new AtomicLong(0);
    private static final AtomicLong totalLineCount = new AtomicLong(0);
    private static final AtomicLong totalRowCount = new AtomicLong(0);
    private static final int reportEveryNRows = 10000;
    private static final int waitSeconds = 10;
    private static CSVConfig config = null;
    private static long latency = 0;
    private static long start = 0;
    private static boolean standin = false;
    private static BufferedWriter out_invaliderowfile;
    private static BufferedWriter out_logfile;
    private static BufferedWriter out_reportfile;
    private static String insertProcedure = "";
    private static Map<Long, String[]> errorInfo = new TreeMap<Long, String[]>();
    private static CsvPreference csvPreference = null;

    public static final char DEFAULT_SEPARATOR = ',';
    public static final char DEFAULT_QUOTE_CHARACTER = '\"';
    public static final char DEFAULT_ESCAPE_CHARACTER = '\\';
    public static final boolean DEFAULT_STRICT_QUOTES = false;
    public static final int DEFAULT_SKIP_LINES = 0;
    public static final boolean DEFAULT_NO_WHITESPACE = false;
    public static final long DEFAULT_COLUMN_LIMIT_SIZE = 16777216;


    private static Map <VoltType, String> blankValues = new HashMap<VoltType, String>();
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
    private static List <VoltType> typeList = new ArrayList<VoltType>();

    private static final class MyCallback implements ProcedureCallback {
        private final long m_lineNum;
        private final CSVConfig m_config;
        private final List<String> m_rowdata;

        MyCallback(long lineNumber, CSVConfig cfg, List<String> rowdata) {
            m_lineNum = lineNumber;
            m_config = cfg;
            m_rowdata = rowdata;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                m_log.error( response.getStatusString() );
                String[] info = { m_rowdata.toString(), response.getStatusString() };
                synchronizeErrorInfo( m_lineNum , info );
                return;
            }

            long currentCount = inCount.incrementAndGet();

//            if (currentCount % reportEveryNRows == 0) {
//                m_log.info( "Inserted " + currentCount + " rows" );
//            }
        }
    }

    private static class CSVConfig extends CLIConfig {
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
        @Option(desc = "Check CSV Input Data Only.")
        boolean check = false;
        @Option(desc = "Call Ping only")
        boolean ping = false;
        @Option(desc = "Use DoNothingProcedure which does full round trip but no transaction.")
        boolean dnp = false;
        @AdditionalArgs(desc = "insert the data into database by TABLENAME.insert procedure by default")
        String table = "";

        @Override
        public void validate() {
            if (maxerrors < 0)
                exitWithMessageAndUsage("abortfailurecount must be >=0");
            if (procedure.equals("") && table.equals(""))
                exitWithMessageAndUsage("procedure name or a table name required");
            if (!procedure.equals("") && !table.equals(""))
                exitWithMessageAndUsage("Only a procedure name or a table name required, pass only one please");
            if (skip < 0)
                exitWithMessageAndUsage("skipline must be >= 0");
            if (limitrows > Integer.MAX_VALUE)
                exitWithMessageAndUsage("limitrows to read must be < "
                        + Integer.MAX_VALUE);
            if (port < 0)
                exitWithMessageAndUsage("port number must be >= 0");
            if ((blank.equalsIgnoreCase("error") ||
                    blank.equalsIgnoreCase("null") ||
                    blank.equalsIgnoreCase("empty")) == false)
                exitWithMessageAndUsage("blank configuration specified must be one of {error|null|empty}");
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
        throws IOException, org.voltdb.client.ProcCallException
    {
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
        int waits = 0;
        int shortWaits = 0;
        ClientStatsContext periodicStatsContext = null;
        Timer timer = null;

        CSVConfig cfg = new CSVConfig();
        cfg.parse(CSVLoaderMT.class.getName(), args);

        config = cfg;
        configuration();
        Tokenizer tokenizer = null;
        ICsvListReader listReader = null;

        try {
            long st = System.currentTimeMillis();
            if (CSVLoaderMT.standin) {
                tokenizer = new Tokenizer(new BufferedReader( new InputStreamReader(System.in)), csvPreference,
                        config.strictquotes, config.escape, config.columnsizelimit,
                        config.skip) ;
                listReader = new CsvListReader(tokenizer, csvPreference);
            }
            else {
                tokenizer = new Tokenizer(new FileReader(config.file), csvPreference,
                        config.strictquotes, config.escape, config.columnsizelimit,
                        config.skip) ;
                listReader = new CsvListReader(tokenizer, csvPreference);
            }
        } catch (FileNotFoundException e) {
            m_log.error("CSV file '" + config.file + "' could not be found.");
            System.exit(-1);
        }

        // Split server list
        String[] serverlist = config.servers.split(",");

        // Create connection
        ClientConfig c_config = new ClientConfig(config.user, config.password);
        c_config.setProcedureCallTimeout(0); // Set procedure all to infinite
                                             // timeout, see ENG-2670
        Client csvClient = null;
        if (!config.check) {
            try {
                csvClient = CSVLoaderMT.getClient(c_config, serverlist, config.port);
                periodicStatsContext = csvClient.createStatsContext();
                timer = schedulePeriodicStats(periodicStatsContext, start);
            } catch (Exception e) {
                m_log.error("Error to connect to the servers:"
                        + config.servers);
                close_cleanup();
                System.exit(-1);
            }
            assert (csvClient != null);
        }

        try {
            int columnCnt = 0;
            ProcedureCallback cb = null;
            boolean lastOK = true;
            if (!cfg.check) {

                VoltTable procInfo = null;
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
            }

            if (config.ping) {
                m_log.info("Running Roundtrip PING only. No Data will be inserted.");
            }

            class CSVFileReader implements Runnable {
                public CSVConfig config;
                public ICsvListReader listReader;
                public BlockingQueue<List<String>> lineq;
                public boolean done = false;
                long parsingTimeSt = System.nanoTime();
                long parsingTimeEnd = System.nanoTime();
                @Override
                public void run() {
                    List<String> lineList = new ArrayList<String>();
                    while ((config.limitrows-- > 0)) {
                        try {
                            //Initial setting of totalLineCount
                            if (listReader.getLineNumber() == 0) {
                                totalLineCount.set(config.skip);
                            } else {
                                totalLineCount.set(listReader.getLineNumber());
                            }

                            long st = System.nanoTime();
                            lineList = listReader.read();
                            long end = System.nanoTime();
                            if (lineList == null) {
                                if (totalLineCount.get() > listReader.getLineNumber()) {
                                    totalLineCount.set(listReader.getLineNumber());
                                }
                                break;
                            }
                            lineq.put(lineList);
                            parsingTimeEnd += (end - st);
                            totalRowCount.getAndIncrement();
                        } catch (SuperCsvException e) {
                            //Catch rows that can not be read by superCSV listReader. E.g. items without quotes when strictquotes is enabled.
                            totalRowCount.getAndIncrement();
                            String[] info = {e.getMessage(), ""};
                            try {
                                synchronizeErrorInfo(totalLineCount.get() + 1, info);
                            } catch (IOException ex) {
                            } catch (InterruptedException ex) {
                            }
                        } catch (Throwable ex) {
                            //Catch rows that can not be read by superCSV listReader. E.g. items without quotes when strictquotes is enabled.
                            totalRowCount.getAndIncrement();
                            String[] info = {ex.getMessage(), ""};
                            try {
                                synchronizeErrorInfo(totalLineCount.get() + 1, info);
                            } catch (IOException ex1) {
                            } catch (InterruptedException ex2) {
                            }
                            break;
                        }
                    }
                    try {
                        listReader.close();
                    } catch (IOException ex) {
                        m_log.error("Error cloging Reader: " + ex);
                    }
                    done = true;
                    m_log.info("Rows Queued by Reader: " + totalRowCount.get());
                }
            }

            CSVFileReader rdr = new CSVFileReader();
            BlockingQueue<List<String>> lineq = new ArrayBlockingQueue<List<String>>(20000);
            rdr.config = config;
            rdr.lineq = lineq;
            rdr.listReader = listReader;
            Thread th = new Thread(rdr);
            th.setName("CSVReader");
            th.setDaemon(true);
            th.start();

            int pcnt = 0;
            while (true) {
                    if (rdr.done && lineq.size() == 0) {
                        break;
                    }
                    List<String> lineList = lineq.poll(100, TimeUnit.MILLISECONDS);
                    if (lineList == null) {
                        m_log.info("Waited no data to pull reader is slower than processor.");
                        continue;
                }
                pcnt++;
                    boolean queued = false;
                    while (queued == false) {
                        String[] correctedLine = lineList.toArray(new String[0]);
                        if (!config.check) {
                            cb = new MyCallback(pcnt, config,
                                    lineList);
                            String lineCheckResult;

                            if ((lineCheckResult = checkparams_trimspace(correctedLine,
                                    columnCnt)) != null) {
                                String[] info = {lineList.toString(), lineCheckResult};
                                synchronizeErrorInfo(pcnt, info);
                                break;
                            }
                            if (config.ping) {
                                queued = csvClient.callProcedure(cb, "@Ping",
                                        (Object[]) correctedLine);
                            } else if (config.dnp) {
                                queued = csvClient.callProcedure(cb, "DoNothingProcedure",
                                        (Object[]) correctedLine);
                            } else {
                                queued = csvClient.callProcedure(cb, insertProcedure,
                                        (Object[]) correctedLine);
                            }
                            outCount.incrementAndGet();
                        } else {
                            queued = true;
                        }
                    }
            }
            if (csvClient != null) {
                csvClient.drain();
            }
            insertTimeEnd = System.currentTimeMillis();
            m_log.info("Parsing CSV file took " + (rdr.parsingTimeEnd - rdr.parsingTimeSt) / 1000000 + " milliseconds.");
            if (!config.check) {
                m_log.info("Inserting Data took " + ((insertTimeEnd - insertTimeStart) - ((rdr.parsingTimeEnd - rdr.parsingTimeSt) / 1000000)) + " milliseconds.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (!config.check) {
            m_log.info("Inserted " + outCount.get() + " and acknowledged "
                    + inCount.get() + " rows (final)");
        } else {
            m_log.info("Verification of CSV input completed.");
        }
        if (!config.check) {
            produceFiles();
        }
        close_cleanup();
        if (timer != null) {
            timer.cancel();
            //Print stats one last time.
            printStatistics(periodicStatsContext, start);
        }
        if (csvClient != null) {
            csvClient.close();
        }
    }

    /**
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
     */
    public static Timer schedulePeriodicStats(final ClientStatsContext periodicStatsContext, final long startTime) {
        Timer timer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() {
                printStatistics(periodicStatsContext, startTime);
            }
        };
        timer.scheduleAtFixedRate(statsPrinting,
                2 * 1000,
                2 * 1000);
        return timer;
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public static synchronized void printStatistics(ClientStatsContext periodicStatsContext, long startTime) {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - startTime) / 1000.0);

        m_log.info(String.format("%02d:%02d:%02d Throughput %d/s, Aborts/Failures %d/%d", time / 3600,
                (time / 60) % 60, time % 60, stats.getTxnThroughput(), stats.getInvocationAborts(), stats.getInvocationErrors()));
    }

    private static void synchronizeErrorInfo( long errLineNum, String[] info ) throws IOException, InterruptedException {
        synchronized (errorInfo) {
            if (!errorInfo.containsKey(errLineNum)) {
                errorInfo.put(errLineNum, info);
            }
            if (errorInfo.size() >= config.maxerrors) {
                m_log.error("The number of Failure row data exceeds "
                        + config.maxerrors);
                produceFiles();
                close_cleanup();
                System.exit(-1);
            }
        }
    }

    private static String checkparams_trimspace(String[] slot, int columnCnt) {
        if (slot.length != columnCnt) {
            return "Error: Incorrect number of columns. " + slot.length
                    + " found, " + columnCnt + " expected.";
        }
        for (int i = 0; i < slot.length; i++) {
            //supercsv read "" to null
            if( slot[i] == null )
            {
                if(config.blank.equalsIgnoreCase("error"))
                    return "Error: blank item";
                else if (config.blank.equalsIgnoreCase("empty"))
                    slot[i] = blankValues.get(typeList.get(i));
              //else config.blank == null which is already the case
            }

            // trim white space in this line. SuperCSV preserves all the whitespace by default
            else {
                if( config.nowhitespace &&
                        ( slot[i].charAt(0) == ' ' || slot[i].charAt( slot[i].length() - 1 ) == ' ') ) {
                    return "Error: White Space Detected in nowhitespace mode.";
                }
                else
                    slot[i] = ((String) slot[i]).trim();
                // treat NULL, \N and "\N" as actual null value
                if ( slot[i].equals("NULL") ||
                        slot[i].equals(VoltTable.CSV_NULL) ||
                        slot[i].equals(VoltTable.QUOTED_CSV_NULL) )
                    slot[i] = null;
            }
        }
        return null;
    }

    private static void configuration() {
        csvPreference = new CsvPreference.Builder(config.quotechar, config.separator, "\n").build();
        if (config.file.equals(""))
            standin = true;
        if (!config.table.equals("")) {
            insertProcedure = config.table.toUpperCase() + ".insert";
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

    private static Client getClient(ClientConfig config, String[] servers,
            int port) throws Exception {
        final Client client = ClientFactory.createClient(config);

        for (String server : servers)
            client.createConnection(server.trim(), port);
        return client;
    }

    private static void produceFiles() {
        latency = System.currentTimeMillis() - start;
        m_log.info("CSVLoader elapsed: " + latency
                + " milliseconds");

        int bulkflush = 300; // by default right now
        try {
            long linect = 0;
            for (Long irow : errorInfo.keySet()) {
                String info[] = errorInfo.get(irow);
                if (info.length != 2)
                    System.out
                            .println("internal error, information is not enough");
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
            out_reportfile.write("csvloader elaspsed: " + elapsedTimeSec
                    + " seconds\n");
            long trueSkip = 0;
            //get the actuall number of lines skipped
            if( config.skip < totalLineCount.get() )
                trueSkip = config.skip;
            else
                trueSkip = totalLineCount.get();
            out_reportfile.write("Number of input lines skipped: "
                    + trueSkip + "\n");
            out_reportfile.write("Number of lines read from input: "
                    + (totalLineCount.get() - trueSkip) + "\n");
            if( config.limitrows == -1 ) {
                out_reportfile.write("Input stopped after "+ totalRowCount.get() +" rows read"+"\n");
            }
            out_reportfile.write("Number of rows discovered: "
                    + totalRowCount.get() + "\n");
            out_reportfile.write("Number of rows successfully inserted: "
                    + inCount.get() + "\n");
            // if prompted msg changed, change it also for test case
            out_reportfile.write("Number of rows that could not be inserted: "
                    + errorInfo.size() + "\n");
            out_reportfile.write("CSVLoader rate: " + outCount.get()
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
        inCount.set(0);
        outCount.set(0);
        errorInfo.clear();

        typeList.clear();

        out_invaliderowfile.close();
        out_logfile.close();
        out_reportfile.close();
    }
}
