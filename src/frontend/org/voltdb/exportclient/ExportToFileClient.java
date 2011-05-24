/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.exportclient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.voltdb.VoltType;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.logging.VoltLogger;
import org.voltdb.types.TimestampType;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

/**
 * Uses the Export feature of VoltDB to write exported tables to files.
 *
 * command line args: --servers {comma-separated list of VoltDB server to which to connect} --type [csv|tsv] csv for
 * comma-separated values, tsv for tab-separated values --outdir {path where output files should be written} --nonce
 * {string-to-unique-ify output files} --user {username for cluster export user} --password {password for cluster export
 * user} --period {period (in minutes) to use when rolling the file over} --dateformat {format of the date/time stamp
 * added to each new rolling file}
 *
 */
public class ExportToFileClient extends ExportClientBase {
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    protected final char m_delimiter;
    protected final String m_nonce;
    protected final File m_outDir;
    protected final HashMap<Long, HashMap<String, ExportToFileDecoder>> m_tableDecoders;
    protected final int m_period;
    protected final SimpleDateFormat m_dateformat;
    protected final int m_firstfield;
    protected final List<String> m_commandLineServerArgs = new ArrayList<String>();
    protected final String m_dateFormatOriginalString;

    // This class outputs exported rows converted to CSV or TSV values
    // for the table named in the constructor's AdvertisedDataSource
    class ExportToFileDecoder extends ExportDecoderBase {
        protected String m_currentFilename;
        protected String m_extension;
        protected String m_prefix;
        protected SimpleDateFormat m_dateformat;
        protected int m_period;
        protected Date m_lastWriterCreation;
        protected CSVWriter m_writer;
        protected final SimpleDateFormat m_sdf;
        protected final int m_firstfield;
        protected final char m_delimiter;
        private final long m_generation;
        private final String m_tableName;
        private final HashSet<AdvertisedDataSource> m_sources = new HashSet<AdvertisedDataSource>();

        private void EnsureFileStream() {

            Calendar calendar = Calendar.getInstance();
            Date now = calendar.getTime();
            calendar.add(Calendar.MINUTE, -m_period);
            Date periodCheck = calendar.getTime();
            if ((m_lastWriterCreation != null) && m_lastWriterCreation.after(periodCheck)) {
                return;
            }
            String filename = m_prefix + m_dateformat.format(now) + m_extension;
            if (filename == m_currentFilename) {
                return;
            } else {
                if (m_writer != null) {
                    try {
                        m_writer.flush();
                        m_writer.close();
                    } catch (Exception e) {
                        m_logger.error(e.getMessage());
                        throw new RuntimeException();
                    }
                }
                m_currentFilename = filename;
                m_lastWriterCreation = now;
            }

            m_logger.info("Opening filename " + m_currentFilename);
            try {
                m_writer = new CSVWriter(
                        new BufferedWriter(
                                new OutputStreamWriter(new FileOutputStream(m_currentFilename, true), "UTF-8"),
                                1048576),
                        m_delimiter);
            } catch (Exception e) {
                m_logger.error(e.getMessage());
                m_logger.error("Error: Failed to create output file: " + m_currentFilename);
                throw new RuntimeException();
            }
        }

        public ExportToFileDecoder(AdvertisedDataSource source, String tableName, String nonce, File outdir,
                char delimiter, int period, SimpleDateFormat dateformat, int firstfield, long generation) {
            super(source);
            m_delimiter = delimiter;
            m_currentFilename = "";
            m_writer = null;
            m_lastWriterCreation = null;
            m_generation = generation;
            m_tableName = tableName;
            // ODBC Datetime Format
            // if you need microseconds, you'll have to change this code or
            //  export a bigint representing microseconds since an epoch
            m_sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            m_firstfield = firstfield;

            // Create the output file for this table
            m_period = period;
            m_dateformat = dateformat;
            m_prefix = outdir.getPath() +
                        File.separator + m_generation + "-" + nonce + "-" + source.tableName + ".";
            m_extension = m_delimiter == ',' ? ".csv" : ".tsv";
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) {
            // Grab the data row
            Object[] row = null;
            try {
                row = decodeRow(rowData);
            } catch (IOException e) {
                m_logger.error("Unable to decode row for table: " + m_source.tableName);
                return false;
            }

            try {
                String[] fields = new String[m_tableSchema.size()];

                for (int i = m_firstfield; i < m_tableSchema.size(); i++) {
                    if (row[i] == null) {
                        fields[i] = "NULL";
                    } else if (m_tableSchema.get(i) == VoltType.STRING) {
                        fields[i] = (String) row[i];
                    } else if (m_tableSchema.get(i) == VoltType.TIMESTAMP) {
                        TimestampType timestamp = (TimestampType) row[i];
                        fields[i] = m_sdf.format(timestamp.asApproximateJavaDate());
                        fields[i] += String.valueOf(timestamp.getUSec());
                    } else {
                        fields[i] = row[i].toString();
                    }
                }
                m_writer.writeNext(fields);
            }
            catch (Exception x) {
                x.printStackTrace();
                return false;
            }
            return true;
        }

        @Override
        public void onBlockStart() {
            // Ensure we have a valid file stream to push data to - no
            // try/catch: a failure there is final
            EnsureFileStream();
        }

        @Override
        public void onBlockCompletion() {
            if (m_writer != null) {
                try {
                    m_writer.flush();
                } catch (Exception e) {
                    m_logger.error(e.getMessage());
                    throw new RuntimeException();
                }
            }
        }

        @Override
        protected void finalize() throws Throwable {
            if (m_writer != null) {
                try {
                    m_writer.flush();
                    m_writer.close();
                } catch (Exception e) {
                    String msg = e.getMessage();
                    // ignore the boring "stream closed" error
                    if (msg.compareToIgnoreCase("stream closed") != 0)
                        m_logger.error(e.getMessage());
                }
            }
            super.finalize();
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            m_sources.remove(source);
            if (m_sources.isEmpty()) {
                if (m_writer != null) {
                    try {
                        m_writer.flush();
                        m_writer.close();
                    } catch (Exception e) {
                        m_logger.error(e.getMessage());
                        throw new RuntimeException();
                    }
                }
                HashMap<String, ExportToFileDecoder> decoders = m_tableDecoders.get(m_generation);
                if (decoders != null) {
                    decoders.remove(m_tableName);
                    if (decoders.isEmpty()) {
                        m_tableDecoders.remove(m_generation);
                    }
                }
            }
        }

    }

    public ExportToFileClient(char delimiter,
                              String nonce,
                              File outdir,
                              int period,
                              String dateformatString,
                              int firstfield,
                              boolean useAdminPorts) {
        super(useAdminPorts);
        m_delimiter = delimiter;
        m_nonce = nonce;
        m_outDir = outdir;
        m_tableDecoders = new HashMap<Long, HashMap<String, ExportToFileDecoder>>();
        m_period = period;
        m_dateformat = new SimpleDateFormat(dateformatString);
        m_dateFormatOriginalString = dateformatString;
        m_firstfield = firstfield;
    }

    @Override
    public ExportToFileDecoder constructExportDecoder(AdvertisedDataSource source) {
        // For every source that provides part of a table, use the same
        // export decoder.
        String table_name = source.tableName;
        HashMap<String, ExportToFileDecoder> decoders = m_tableDecoders.get(source.m_generation);
        if (decoders == null) {
            decoders = new HashMap<String, ExportToFileDecoder>();
            m_tableDecoders.put(source.m_generation, decoders);
        }
        ExportToFileDecoder decoder = decoders.get(table_name);
        if (decoder == null) {
            decoder = new ExportToFileDecoder(source, table_name, m_nonce, m_outDir, m_delimiter,
                    m_period, m_dateformat, m_firstfield, source.m_generation);
            decoders.put(table_name, decoder);
        }
        decoder.m_sources.add(source);
        return decoders.get(table_name);
    }

    protected void logConfigurationInfo() {

        StringBuilder sb = new StringBuilder();
        sb.append("Address for ").append(m_commandLineServerArgs.size());
        sb.append(" given as command line arguments:");
        for (String server : m_commandLineServerArgs) {
            sb.append("\n  ").append(server);
        }
        m_logger.info(sb.toString());

        m_logger.info(String.format("Connecting to cluster on %s ports",
                m_useAdminPorts ? "admin" : "client"));
        if ((m_username != null) && (m_username.length() > 0)) {
            m_logger.info("Connecting as user " + m_username);
        }
        else {
            m_logger.info("Connecting anonymously");
        }

        m_logger.info(String.format("Writing to disk in %s format",
                (m_delimiter == ',') ? "CSV" : "TSV"));
        m_logger.info(String.format("Prepending export data files with nonce: %s",
                m_nonce));
        m_logger.info(String.format("Using date format for file names: %s",
                m_dateFormatOriginalString));
        m_logger.info(String.format("Rotate export files every %d minute%s",
                m_period, m_period == 1 ? "" : "s"));
        m_logger.info(String.format("Writing export files to dir: %s",
                m_outDir));
        if (m_firstfield == 0) {
            m_logger.info("Including VoltDB export metadata");
        }
        else {
            m_logger.info("Not including VoltDB export metadata");
        }
    }

    protected static void printHelpAndQuit(int code) {
        System.out.println("java -cp <classpath> org.voltdb.exportclient.ExportToFileClient "
                        + "--help");
        System.out.println("java -cp <classpath> org.voltdb.exportclient.ExportToFileClient "
                        + "--servers server1[,server2,...,serverN] "
                        + "--connect (admin|client) "
                        + "--type (csv|tsv) "
                        + "--nonce file_prefix "
                        + "[--period rolling_period_in_minutes] "
                        + "[--dateformat date_pattern_for_file_name] "
                        + "[--outdir target_directory] "
                        + "[--skipinternals] "
                        + "[--user export_username] "
                        + "[--password export_password]");
        System.out.println("Note that server hostnames may be appended with a specific port:");
        System.out.println("  --servers server1:port1[,server2:port2,...,serverN:portN]");

        System.exit(code);
    }

    public static void main(String[] args) {
        String[] volt_servers = null;
        String user = null;
        String password = null;
        String nonce = null;
        char delimiter = '\0';
        File outdir = null;
        int firstfield = 0;
        int period = 60;
        char connect = ' '; // either ' ', 'c' or 'a'
        String dateformatString = "yyyyMMddHHmmss";

        for (int ii = 0; ii < args.length; ii++) {
            String arg = args[ii];
            if (arg.equals("--help")) {
                printHelpAndQuit(0);
            }
            else if (arg.equals("--discard")) {
                System.err.println("Option \"--discard\" is no longer supported.");
                System.err.println("Try org.voltdb.exportclient.DiscardingExportClient.");
                printHelpAndQuit(-1);
            }
            else if (arg.equals("--skipinternals")) {
                firstfield = 6;
            }
            else if (arg.equals("--connect")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --connect");
                    printHelpAndQuit(-1);
                }
                String connectStr = args[ii + 1];
                if (connectStr.equalsIgnoreCase("admin")) {
                    connect = 'a';
                } else if (connectStr.equalsIgnoreCase("client")) {
                    connect = 'c';
                } else {
                    System.err.println("Error: --type must be one of \"admin\" or \"client\"");
                    printHelpAndQuit(-1);
                }
                ii++;
            }
            else if (arg.equals("--servers")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --servers");
                    printHelpAndQuit(-1);
                }
                volt_servers = args[ii + 1].split(",");
                ii++;
            }
            else if (arg.equals("--type")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --type");
                    printHelpAndQuit(-1);
                }
                String type = args[ii + 1];
                if (type.equalsIgnoreCase("csv")) {
                    delimiter = ',';
                } else if (type.equalsIgnoreCase("tsv")) {
                    delimiter = '\t';
                } else {
                    System.err.println("Error: --type must be one of CSV or TSV");
                    printHelpAndQuit(-1);
                }
                ii++;
            }
            else if (arg.equals("--outdir")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --outdir");
                    printHelpAndQuit(-1);
                }
                boolean invalidDir = false;
                outdir = new File(args[ii + 1]);
                if (!outdir.exists()) {
                    System.err.println("Error: " + outdir.getPath() + " does not exist");
                    invalidDir = true;
                }
                if (!outdir.canRead()) {
                    System.err.println("Error: " + outdir.getPath() + " does not have read permission set");
                    invalidDir = true;
                }
                if (!outdir.canExecute()) {
                    System.err.println("Error: " + outdir.getPath() + " does not have execute permission set");
                    invalidDir = true;
                }
                if (!outdir.canWrite()) {
                    System.err.println("Error: " + outdir.getPath() + " does not have write permission set");
                    invalidDir = true;
                }
                if (invalidDir) {
                    System.exit(-1);
                }
                ii++;
            }
            else if (arg.equals("--nonce")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --nonce");
                    printHelpAndQuit(-1);
                }
                nonce = args[ii + 1];
                ii++;
            }
            else if (arg.equals("--user")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --user");
                    printHelpAndQuit(-1);
                }
                user = args[ii + 1];
                ii++;
            }
            else if (arg.equals("--password")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --password");
                    printHelpAndQuit(-1);
                }
                password = args[ii + 1];
                ii++;
            }
            else if (arg.equals("--period")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --period");
                    printHelpAndQuit(-1);
                }
                period = Integer.parseInt(args[ii + 1]);
                if (period < 1) {
                    System.err.println("Error: Specified value for --period must be >= 1.");
                    printHelpAndQuit(-1);
                }
                ii++;
            }
            else if (arg.equals("--dateformat")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --dateformat");
                    printHelpAndQuit(-1);
                }
                dateformatString = args[ii + 1].trim();
                ii++;
            }
        }
        // Check args for validity
        if (volt_servers == null || volt_servers.length < 1) {
            System.err.println("ExportToFile: must provide at least one VoltDB server");
            printHelpAndQuit(-1);
        }
        if (connect == ' ') {
            System.err.println("ExportToFile: must specify connection type as admin or client using --connect argument");
            printHelpAndQuit(-1);
        }
        assert ((connect == 'c') || (connect == 'a'));
        if (user == null) {
            user = "";
        }
        if (password == null) {
            password = "";
        }
        if (nonce == null) {
            System.err.println("ExportToFile: must provide a filename nonce");
            printHelpAndQuit(-1);
        }
        if (outdir == null) {
            outdir = new File(".");
        }
        if (delimiter == '\0') {
            System.err.println("ExportToFile: must provide an output type");
            printHelpAndQuit(-1);
        }

        // create the export to file client
        ExportToFileClient client = new ExportToFileClient(delimiter,
                                                           nonce,
                                                           outdir,
                                                           period,
                                                           dateformatString,
                                                           firstfield,
                                                           connect == 'a');

        // add all of the servers specified
        for (String server : volt_servers) {
            server = server.trim();
            client.m_commandLineServerArgs.add(server);
            client.addServerInfo(server, connect == 'a');
        }

        // add credentials (default blanks used if none specified)
        client.addCredentials(user, password);

        // main loop
        try {
            client.run();
        }
        catch (ExportClientException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Override
    public void run() throws ExportClientException {
        logConfigurationInfo();
        super.run();
    }
}
