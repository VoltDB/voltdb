/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;

import com.google_voltpatches.common.net.HostAndPort;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv_voltpatches.tokenizer.Tokenizer;

import org.voltdb.CLIConfig;
import org.voltdb.client.AutoReconnectListener;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;

/**
 * CSVLoader is a simple utility to load data from a CSV formatted file to a table.
 *
 * This utility processes partitioned data efficiently and creates as many partition processors.
 * For partitioned data each processor calls
 * @LoadSinglepartitionTable
 *
 * For multi-partitioned data it uses a single processor which call
 * @LoadMultipartitionTable
 *
 * The maxerror indicates maximum number of errors it can tolerate.
 * Its a threshold but since processors are processing in parallel we may process rows beyond
 * maxerror and additional errors may occur. Only first maxerror indicated errors will be reported.
 *
 */
public class CSVLoader implements BulkLoaderErrorHandler {

    /**
     * Path of invalid row file that will be created.
     */
    static String pathInvalidrowfile = "";
    /**
     * report file name
     */
    static String pathReportfile = "csvloaderReport.log";
    /**
     * log file name
     */
    static String pathLogfile = "csvloaderLog.log";
    private static CSVConfig config = null;
    private static long start = 0;
    private static boolean standin = false;
    private static BufferedWriter out_invaliderowfile;
    private static BufferedWriter out_logfile;
    private static BufferedWriter out_reportfile;
    private static CsvPreference csvPreference = null;
    /**
     * default CSV separator
     */
    public static final char DEFAULT_SEPARATOR = ',';
    /**
     * default quote char
     */
    public static final char DEFAULT_QUOTE_CHARACTER = '\"';
    /**
     * default escape char
     */
    public static final char DEFAULT_ESCAPE_CHARACTER = '\\';
    /**
     * Are we using strict quotes
     */
    public static final boolean DEFAULT_STRICT_QUOTES = false;
    /**
     * Number of lines to skip in CSV
     */
    public static final int DEFAULT_SKIP_LINES = 0;
    /**
     * Allow whitespace?
     */
    public static final boolean DEFAULT_NO_WHITESPACE = false;
    /**
     * Size limit for each column.
     */
    public static final long DEFAULT_COLUMN_LIMIT_SIZE = 16777216;
    /**
     * Using upsert instead of insert
     */
    public static final boolean DEFAULT_UPSERT_MODE = false;
    /**
     * First line is column name?
     */
    public static final boolean DEFAULT_HEADER = false;
    /**
     * Stop when all connections are lost?
     */
    public static final boolean DEFAULT_STOP_ON_DISCONNECT = false;
    /**
     * Used for testing only.
     */
    public static boolean testMode = false;

    private class ErrorInfoItem {
        public long lineNumber;
        public String[] errorInfo;
        ErrorInfoItem(long line, String[] info) {
            lineNumber = line;
            errorInfo = info;
        }
    }

    private static final int ERROR_INFO_QUEUE_SIZE = Integer.getInteger("ERROR_INFO_QUEUE_SIZE", 500);
    //Errors we keep track only upto maxerrors
    private final LinkedBlockingQueue<ErrorInfoItem> m_errorInfo = new LinkedBlockingQueue<ErrorInfoItem>(ERROR_INFO_QUEUE_SIZE);
    private volatile long m_errorCount = 0;

    private class ErrorInfoFlushProcessor extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    ErrorInfoItem currItem;
                    currItem = m_errorInfo.take();

                    if (currItem.lineNumber == -1) {
                        return;
                    }

                    if (currItem.errorInfo.length != 2) {
                        System.out.println("internal error, information is not enough");
                    }
                    out_invaliderowfile.write(currItem.errorInfo[0] + "\n");
                    String message = "Invalid input on line " + currItem.lineNumber + ". " + currItem.errorInfo[1];
                    out_logfile.write(message + "\n  Content: " + currItem.errorInfo[0] + "\n");
                    m_errorCount++;

                } catch (Exception x) {
                    System.err.println(x.getMessage());
                }
            }
        }
    }

    private ErrorInfoFlushProcessor m_errorinfoProcessor = null;

    public void launchErrorFlushProcessor() {
        m_errorinfoProcessor = new ErrorInfoFlushProcessor();
        m_errorinfoProcessor.start();
    }

    public void waitForErrorFlushComplete() throws InterruptedException {

        //Put an empty ErrorInfoItem
        ErrorInfoItem emptyErrorInfo = new ErrorInfoItem(-1, null);

        if (!m_errorInfo.offer(emptyErrorInfo)) {
            m_errorInfo.put(emptyErrorInfo);
        }

        if (m_errorinfoProcessor != null) {
            m_errorinfoProcessor.join();
        }
    }

    @Override
    public boolean handleError(RowWithMetaData metaData, ClientResponse response, String error) {
        synchronized (m_errorInfo) {
            //Don't collect more than we want to report.
            if (m_errorCount + m_errorInfo.size() >= config.maxerrors) {
                return true;
            }

            String rawLine;
            if (metaData.rawLine == null) {
                rawLine = "Unknown line content";
            } else {
                rawLine = metaData.rawLine.toString();
            }
            String infoStr = (response != null) ? response.getStatusString() : error;
            String[] info = {rawLine, infoStr};

            ErrorInfoItem newErrorInfo = new ErrorInfoItem(metaData.lineNumber, info);

            try {
                if (!m_errorInfo.offer(newErrorInfo)) {
                    m_errorInfo.put(newErrorInfo);
                }
            } catch (InterruptedException e) {
            }

            if (response != null) {
                byte status = response.getStatus();
                if (status != ClientResponse.USER_ABORT && status != ClientResponse.GRACEFUL_FAILURE) {
                    System.out.println("Fatal Response from server for: " + response.getStatusString()
                            + " for: " + rawLine);
                    System.exit(1);
                }
            }

            return false;
        }
    }

    @Override
    public boolean hasReachedErrorLimit()
    {
        return m_errorCount + m_errorInfo.size() >= config.maxerrors;
    }

    /**
     * Configuration options.
     */
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

        @Option(desc = "different ways to handle blank items: {error|null|empty} (default: null)")
        String blank = "null";

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

        @Option(desc = "credentials that contains username and password information")
        String credentials = "";

        @Option(desc = "port to use when connecting to database (default: 21212)")
        int port = Client.VOLTDB_SERVER_PORT;

        @Option(shortOpt = "z", desc = "timezone for interpreting date and time strings")
        String timezone = "";

        @Option(shortOpt = "n", desc = "Custom null string, overrides all other Null pattern matching")
        String customNullString = "";

        // add a charset flag as "c"
        @Option(shortOpt = "c", desc = "character set , default system character set")
        String charset = "utf-8";

        @Option(desc = "Disables the quote character. All characters between delimiters, including quote characters, are included in the input.",
                hasArg = false)
        boolean noquotechar = false;

        @Option(desc = "Enable SSL, Optionally provide configuration file.")
        String ssl = "";

        @Option(desc = "Enable Kerberos and use provided JAAS login configuration entry key.")
        String kerberos = "";

        @Option(desc = "Priority for VoltDB client requests (0=none/default)")
        int priority = 0;

        /**
         * Batch size for processing batched operations.
         */
        @Option(desc = "Batch Size for processing.")
        public int batch = 200;

        @Option(desc = "First line of csv file is column name.", hasArg = false)
        boolean header = DEFAULT_HEADER;

        /**
         * Table name to insert CSV data into.
         */
        @AdditionalArgs(desc = "insert the data into database by TABLENAME.insert procedure by default")
        public String table = "";
        // This is set to true when -p option us used.
        boolean useSuppliedProcedure = false;

        @Option(desc = "Use upsert instead of insert", hasArg = false)
        boolean update = DEFAULT_UPSERT_MODE;

        @Option(desc = "Stop when all connections are lost", hasArg = false)
        boolean stopondisconnect = DEFAULT_STOP_ON_DISCONNECT;
        /**
         * Validate command line options.
         */
        @Override
        public void validate() {
            if (header && !procedure.equals("")) {
                exitWithMessageAndUsage("--header and --procedure options are mutually exclusive.");
            }
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
            if (port < 0) {
                exitWithMessageAndUsage("port number must be >= 0");
            }
            if (batch < 0) {
                exitWithMessageAndUsage("batch size number must be >= 0");
            }
            if(!customNullString.isEmpty() && !blank.equals("error")){
                blank = "empty";
            }
            if (!blank.equalsIgnoreCase("error") &&
                !blank.equalsIgnoreCase("null") &&
                !blank.equalsIgnoreCase("empty")) {
                exitWithMessageAndUsage("blank configuration specified must be one of {error|null|empty}");
            }
            if ((procedure != null) && (procedure.trim().length() > 0)) {
                useSuppliedProcedure = true;
            }
            if ((useSuppliedProcedure) && (update)){
                update = false;
                exitWithMessageAndUsage("update is not applicable when stored procedure specified");
            }
            if(!timezone.equals("")){
                boolean isValidTimezone = false;
                for (String tzId : TimeZone.getAvailableIDs()) {
                    if(tzId.equals(timezone)) {
                        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
                        isValidTimezone = true;
                        break;
                    }
                }
                if(!isValidTimezone){
                    exitWithMessageAndUsage("specified timezone \"" + timezone + "\" is invalid");
                }
            }
        }

        /**
         * Usage
         */
        @Override
        public void printUsage() {
            System.out
                    .println("Usage: csvloader [args] tablename");
            System.out
                    .println("       csvloader [args] -p procedurename");
            super.printUsage();
        }
    }

    /**
     * csvloader main. (main is directly used by tests as well be sure to reset statics that you need to start over)
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     *
     */
    public static void main(String[] args) throws IOException,
            InterruptedException {
        start = System.currentTimeMillis();
        long insertTimeStart = start;
        long insertTimeEnd;
        FileReader fr = null;
        BufferedReader br = null;
        final CSVConfig cfg = new CSVConfig();
        cfg.parse(CSVLoader.class.getName(), args);
        config = cfg;
        if (config.noquotechar) {
            config.quotechar = '\u0000';
        }

        configuration();

        final Tokenizer tokenizer;
        ICsvListReader listReader = null;
        try {
            if (CSVLoader.standin) {
                tokenizer = new Tokenizer(new BufferedReader(new InputStreamReader(System.in)), csvPreference,
                        config.strictquotes, config.escape, config.columnsizelimit,
                        config.skip, config.header);
                listReader = new CsvListReader(tokenizer, csvPreference);
            } else {
                FileInputStream fis = new FileInputStream(config.file);
                InputStreamReader isr = new InputStreamReader(fis, config.charset);
                tokenizer = new Tokenizer(isr,
                          csvPreference,
                          config.strictquotes,
                          config.escape,
                          config.columnsizelimit,
                          config.skip,
                          config.header);

                listReader = new CsvListReader(tokenizer, csvPreference);
            }
        } catch (FileNotFoundException e) {
            System.err.println("CSV file '" + config.file + "' could not be found.");
            System.exit(-1);
        }

        // read username and password from txt file
        if (config.credentials != null && !config.credentials.trim().isEmpty()) {
            Properties props = MiscUtils.readPropertiesFromCredentials(config.credentials);
            config.user = props.getProperty("username");
            config.password = props.getProperty("password");
        }

        // If we need to prompt the user for a password, do so.
        config.password = CLIConfig.readPasswordIfNeeded(config.user, config.password, "Enter password: ");

        // Create connection
        AutoReconnectListener listener = config.stopondisconnect ? null : new AutoReconnectListener();
        final ClientConfig c_config = new ClientConfig(config.user, config.password, listener);
        if (config.ssl != null && !config.ssl.trim().isEmpty()) {
            c_config.setTrustStoreConfigFromPropertyFile(config.ssl);
            c_config.enableSSL();
        }
        if (!config.kerberos.trim().isEmpty()) {
            c_config.enableKerberosAuthentication(config.kerberos);
        }
        c_config.setProcedureCallTimeout(0); // 0 => infinite
        if (config.priority > 0) {
            c_config.setRequestPriority(config.priority);
        }
        Client csvClient = null;
        try {
            csvClient = CSVLoader.getClient(c_config, config.servers, config.port);
        } catch (Exception e) {
            System.err.println("Error connecting to the servers: "
                    + config.servers);
            System.exit(-1);
        }
        assert (csvClient != null);

        try {
            long readerTime;
            long insertCount;
            long ackCount;
            long rowsQueued;
            final CSVLoader errHandler = new CSVLoader();
            final CSVDataLoader dataLoader;

            errHandler.launchErrorFlushProcessor();

            if (config.useSuppliedProcedure) {
                dataLoader = new CSVTupleDataLoader(csvClient, config.procedure, errHandler);
            } else {
                dataLoader = new CSVBulkDataLoader(csvClient, config.table, config.batch, config.update, errHandler);
            }
            if (!config.stopondisconnect) {
                listener.setLoader(dataLoader);
            }

            CSVFileReader.initializeReader(cfg, listReader);

            CSVFileReader csvReader = new CSVFileReader(dataLoader, errHandler);

            Thread readerThread = new Thread(csvReader);
            readerThread.setName("CSVFileReader");
            readerThread.setDaemon(true);

            //Wait for reader to finish.
            readerThread.start();
            readerThread.join();

            insertTimeEnd = System.currentTimeMillis();

            csvClient.close();

            errHandler.waitForErrorFlushComplete();

            readerTime = (csvReader.m_parsingTime) / 1000000;
            insertCount = dataLoader.getProcessedRows();
            ackCount = insertCount - dataLoader.getFailedRows();
            rowsQueued = CSVFileReader.m_totalRowCount.get();

            //Close the reader.
            try {
               listReader.close();
            } catch (Exception ex) {
                //Do nothing here.
            }

            if (errHandler.hasReachedErrorLimit()) {
               System.out.println("The number of failed rows exceeds the configured maximum failed rows: "
                                  + config.maxerrors);
            }
            System.out.println("Read " + insertCount + " rows from file and successfully inserted "
                               + ackCount + " rows (final)");
            errHandler.produceFiles(ackCount, insertCount);
            close_cleanup();
            //In test junit mode we let it continue for reuse
            if (!CSVLoader.testMode) {
                System.exit(errHandler.m_errorInfo.isEmpty() ? 0 : -1);
            }
        } catch (Exception ex) {
            System.err.println("Exception Happened while loading CSV data: " + ex);
            System.exit(1);
        }
    }

    private static void configuration() {
        csvPreference = new CsvPreference.Builder(config.quotechar, config.separator, "\n").build();
        if (config.file.equals("")) {
            standin = true;
        }
        String insertProcedure;
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
            System.err.println(x.getMessage());
            System.exit(-1);
        }

        insertProcedure = insertProcedure.replaceAll("\\.", "_");
        pathInvalidrowfile = config.reportdir + "csvloader_" + insertProcedure + "_"
                + "invalidrows.csv";
        pathLogfile = config.reportdir + "csvloader_" + insertProcedure + "_"
                + "log.log";
        pathReportfile = config.reportdir + "csvloader_" + insertProcedure + "_"
                + "report.log";

        try {
            out_invaliderowfile = new BufferedWriter(new FileWriter(
                    pathInvalidrowfile));
            out_logfile = new BufferedWriter(new FileWriter(pathLogfile));
            out_reportfile = new BufferedWriter(new FileWriter(pathReportfile));
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    /**
     * Get connection to servers in cluster.
     * Connects to the first available server, and uses
     * the topology-awareness features to connect to the
     * rest.
     *
     * @param config
     * @param servers
     * @param port
     * @return client
     * @throws IOException
     */
    private static Client getClient(ClientConfig config, String servers,
                                    int port) throws IOException, InterruptedException {
        config.setTopologyChangeAware(true);
        Client client = ClientFactory.createClient(config);
        try {
            client.createAnyConnection(makeServerList(servers, port));
            return client;
        }
        catch (IOException | RuntimeException ex) {
            client.close();
            throw ex;
        }
    }

    // Adjust conventions: we want a list of server:port values
    private static String makeServerList(String servers, int port) {
        if (port == Client.VOLTDB_SERVER_PORT) {
            return servers; // implied default
        }
        String[] list = servers.split(",");
        for (int i=0; i<list.length; i++) {
            list[i] = HostAndPort.fromString(list[i].trim())
                                 .withDefaultPort(port)
                                 .requireBracketsForIPv6()
                                 .toString();
        }
        return String.join(",", list);
    }

    private void produceFiles(long ackCount, long insertCount) {
        long latency = System.currentTimeMillis() - start;
        System.out.println("Elapsed time: " + latency / 1000F
                           + " seconds");
        try {
            // Get elapsed time in seconds
            float elapsedTimeSec = latency / 1000F;
            out_reportfile.write("CSVLoader elaspsed: " + elapsedTimeSec + " seconds\n");
            long trueSkip;
            long totolLineCnt;
            long totalRowCnt;

            if (config.useSuppliedProcedure) {
                totolLineCnt = CSVFileReader.m_totalLineCount.get();
                totalRowCnt = CSVFileReader.m_totalRowCount.get();
            } else {
                totolLineCnt = CSVFileReader.m_totalLineCount.get();
                totalRowCnt = CSVFileReader.m_totalRowCount.get();
            }

            //get the actual number of lines skipped
            if (config.skip < totolLineCnt) {
                trueSkip = config.skip;
            } else {
                trueSkip = totolLineCnt;
            }
            out_reportfile.write("Number of input lines skipped: "
                    + trueSkip + "\n");
            out_reportfile.write("Number of lines read from input: "
                    + (totolLineCnt - trueSkip) + "\n");
            if (config.limitrows == -1) {
                out_reportfile.write("Input stopped after "
                        + totalRowCnt + " rows read" + "\n");
            }
            out_reportfile.write("Number of rows discovered: "
                    + totolLineCnt + "\n");
            out_reportfile.write("Number of rows successfully inserted: "
                    + ackCount + "\n");
            // if prompted msg changed, change it also for test case
            out_reportfile.write("Number of rows that could not be inserted: "
                    + m_errorCount + "\n");
            out_reportfile.write("CSVLoader rate: " + insertCount
                    / elapsedTimeSec + " row/s\n");

            System.out.println("Invalid row file: " + pathInvalidrowfile);
            System.out.println("Log file: " + pathLogfile);
            System.out.println("Report file: " + pathReportfile);

            out_invaliderowfile.flush();
            out_logfile.flush();
            out_reportfile.flush();
        } catch (Exception x) {
            System.err.println(x.getMessage());
        }
    }

    private static void close_cleanup() throws IOException,
            InterruptedException {
        out_invaliderowfile.close();
        out_logfile.close();
        out_reportfile.close();
    }
}
