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
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.logging.VoltLogger;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.DelimitedDataWriterUtil.CSVWriter;
import org.voltdb.utils.DelimitedDataWriterUtil.DelimitedDataWriter;
import org.voltdb.utils.DelimitedDataWriterUtil.TSVWriter;

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

public class ExportToRollingFileClient extends ExportClientBase {
    private static final VoltLogger m_logger = new VoltLogger("ExportToRollingFileClient");

    private final DelimitedDataWriter m_escaper;
    private final String m_nonce;
    private final File m_outDir;
    private final HashMap<String, ExportToRollingFileDecoder> m_tableDecoders;
    private final int m_period;
    private final SimpleDateFormat m_dateformat;
    private final int m_firstfield;

    // This class outputs exported rows converted to CSV or TSV values
    // for the table named in the constructor's AdvertisedDataSource
    class ExportToRollingFileDecoder extends ExportDecoderBase {
        private final DelimitedDataWriter m_escaper;
        private String m_currentFilename;
        private String m_extension;
        private String m_prefix;
        private boolean m_discard;
        private SimpleDateFormat m_dateformat;
        private int m_period;
        private Date m_lastWriterCreation;
        private BufferedWriter m_Writer;
        private final SimpleDateFormat m_sdf;
        private final int m_firstfield;

        private void EnsureFileStream() {

            if (m_discard) {
                return;
            } else {
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
                    if (m_Writer != null) {
                        try {
                            m_Writer.flush();
                            m_Writer.close();
                        } catch (Exception e) {
                            m_logger.error(e.getMessage());
                            throw new RuntimeException();
                        }
                    }
                    m_currentFilename = filename;
                    m_lastWriterCreation = now;
                }
            }

            m_logger.info("Opening filename " + m_currentFilename);
            try {
                m_Writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(m_currentFilename), "UTF-8"),
                        1048576);
            } catch (Exception e) {
                m_logger.error(e.getMessage());
                m_logger.error("Error: Failed to create output file: " + m_currentFilename);
                throw new RuntimeException();
            }
        }

        public ExportToRollingFileDecoder(AdvertisedDataSource source, String nonce, File outdir,
                DelimitedDataWriter escaper, int period, SimpleDateFormat dateformat, int firstfield) {
            super(source);
            m_escaper = escaper;
            m_currentFilename = "";
            m_Writer = null;
            m_lastWriterCreation = null;
            m_sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss:SSS:");
            m_firstfield = firstfield;

            // Create the output file for this table
            if (outdir != null) {
                m_period = period;
                m_dateformat = dateformat;
                m_prefix = outdir.getPath() + File.separator + nonce + "-" + source.tableName() + ".";
                m_extension = "." + escaper.getExtension();
                m_discard = false;
            } else {
                m_logger.error("--discard provided, data will be dumped to /dev/null");
                m_discard = true;
            }
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) {
            // Return immediately if we're in discard mode
            if (m_discard) return true;

            // Ensure we have a valid file stream to push data to - no
            // try/catch: a failure there is final
            EnsureFileStream();

            // Grab the data row
            Object[] row = null;
            try {
                row = decodeRow(rowData);
            } catch (IOException e) {
                m_logger.error("Unable to decode row for table: " + m_source.tableName());
                return false;
            }

            try {
                for (int i = m_firstfield; i < m_tableSchema.size(); i++) {
                    if (row[i] == null) {
                        m_escaper.writeRawField(m_Writer, "NULL", i > m_firstfield);
                    } else if (m_tableSchema.get(i) == VoltType.STRING) {
                        m_escaper.writeEscapedField(m_Writer, (String) row[i], i > m_firstfield);
                    } else if (m_tableSchema.get(i) == VoltType.TIMESTAMP) {
                        TimestampType timestamp = (TimestampType) row[i];
                        m_escaper.writeRawField(m_Writer, m_sdf.format(timestamp.asApproximateJavaDate()),
                                i > m_firstfield);
                        m_escaper.writeRawField(m_Writer, String.valueOf(timestamp.getUSec()), false);
                    } else {
                        m_escaper.writeRawField(m_Writer, row[i].toString(), i > m_firstfield);
                    }
                }
                m_Writer.write("\n");
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            } catch (Exception x) {
                x.printStackTrace();
                return false;
            }
            return true;
        }

        @Override
        public void onBlockCompletion() {
            if (m_Writer != null) {
                try {
                    m_Writer.flush();
                } catch (Exception e) {
                    m_logger.error(e.getMessage());
                    throw new RuntimeException();
                }
            }
        }

        @Override
        protected void finalize() throws Throwable {
            if (m_Writer != null) {
                try {
                    m_Writer.flush();
                    m_Writer.close();
                } catch (Exception e) {
                    m_logger.error(e.getMessage());
                    throw new RuntimeException();
                }
            }
            super.finalize();
        }

    }

    public void setVoltServers(String[] voltServers) {
        for (int i = 0; i < voltServers.length; i++) {
            InetSocketAddress server = new InetSocketAddress(voltServers[i], VoltDB.DEFAULT_PORT);
            addServerInfo(server);
        }
    }

    public ExportToRollingFileClient(DelimitedDataWriter escaper, String nonce, File outdir, int period,
            SimpleDateFormat dateformat, int firstfield) {
        m_escaper = escaper;
        m_nonce = nonce;
        m_outDir = outdir;
        m_tableDecoders = new HashMap<String, ExportToRollingFileDecoder>();
        m_period = period;
        m_dateformat = dateformat;
        m_firstfield = firstfield;
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        // For every source that provides part of a table, use the same
        // export decoder.
        String table_name = source.tableName();
        if (!m_tableDecoders.containsKey(table_name)) {
            m_tableDecoders.put(table_name, new ExportToRollingFileDecoder(source, m_nonce, m_outDir, m_escaper,
                    m_period, m_dateformat, m_firstfield));
        }
        return m_tableDecoders.get(table_name);
    }

    private static void printHelpAndQuit(int code) {
        System.out
                .println("java -cp <classpath> -Djava.library.path=<library path> org.voltdb.exportclient.ExportToRollingFileClient "
                        + "--help");
        System.out
                .println("java -cp <classpath> -Djava.library.path=<library path> org.voltdb.exportclient.ExportToRollingFileClient "
                        + "--servers server1[,server2,...,serverN]"
                        + "--discard");
        System.out
                .println("java -cp <classpath> -Djava.library.path=<library path> org.voltdb.exportclient.ExportToRollingFileClient "
                        + "--servers server1[,server2,...,serverN]"
                        + "--type (csv|tsv) "
                        + "--nonce file_prefix "
                        + "[--period rolling_period_in_minutes]"
                        + "[--dateformat date_pattern_for_file_name]"
                        + "[--outdir target_directory] "
                        + "[--skipinternals] "
                        + "[--user export_username]"
                        + "[--password export_password]");
        System.exit(code);
    }

    public static void main(String[] args) {
        String[] volt_servers = null;
        String user = null;
        String password = null;
        String nonce = null;
        DelimitedDataWriter escaper = null;
        File outdir = null;
        boolean discard = false;
        int firstfield = 0;
        int period = 60;
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMddHHmmss");

        for (int ii = 0; ii < args.length; ii++) {
            String arg = args[ii];
            if (arg.equals("--help")) {
                printHelpAndQuit(0);
            } else if (arg.equals("--discard")) {
                discard = true;
            } else if (arg.equals("--skipinternals")) {
                firstfield = 6;
            } else if (arg.equals("--servers")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --servers");
                    printHelpAndQuit(-1);
                }
                volt_servers = args[ii + 1].split(",");
                ii++;
            } else if (arg.equals("--type")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --type");
                    printHelpAndQuit(-1);
                }
                String type = args[ii + 1];
                if (type.equalsIgnoreCase("csv")) {
                    escaper = new CSVWriter();
                } else if (type.equalsIgnoreCase("tsv")) {
                    escaper = new TSVWriter();
                } else {
                    System.err.println("Error: --type must be one of CSV or TSV");
                    printHelpAndQuit(-1);
                }
                ii++;
            } else if (arg.equals("--outdir")) {
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
            } else if (arg.equals("--nonce")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --nonce");
                    printHelpAndQuit(-1);
                }
                nonce = args[ii + 1];
                ii++;
            } else if (arg.equals("--user")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --user");
                    printHelpAndQuit(-1);
                }
                user = args[ii + 1];
                ii++;
            } else if (arg.equals("--password")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --password");
                    printHelpAndQuit(-1);
                }
                password = args[ii + 1];
                ii++;
            } else if (arg.equals("--period")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --period");
                    printHelpAndQuit(-1);
                }
                period = Integer.parseInt(args[ii + 1]);
                ii++;
            } else if (arg.equals("--dateformat")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --dateformat");
                    printHelpAndQuit(-1);
                }
                dateformat = new SimpleDateFormat(args[ii + 1]);
                ii++;
            }
        }
        // Check args for validity
        if (volt_servers == null || volt_servers.length < 1) {
            System.err.println("ExportToRollingFile: must provide at least one VoltDB server");
            printHelpAndQuit(-1);
        }
        if (user == null) {
            user = "";
        }
        if (password == null) {
            password = "";
        }
        if (!discard) {
            if (nonce == null) {
                System.err
                        .println("ExportToFile: must provide a filename nonce");
                printHelpAndQuit(-1);
            }
            if (outdir == null) {
                outdir = new File(".");
            }
        } else {
            outdir = null;
            nonce = null;
            escaper = new CSVWriter();
        }
        if (escaper == null) {
            System.err.println("ExportToRollingFile: must provide an output type");
            printHelpAndQuit(-1);
        }
        ExportToRollingFileClient client = new ExportToRollingFileClient(escaper, nonce, outdir, period, dateformat,
                firstfield);
        client.setVoltServers(volt_servers);
        try {
            client.addCredentials(user, password);
            client.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
