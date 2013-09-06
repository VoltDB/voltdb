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

package org.voltdb.exportclient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringEscapeUtils;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.VoltFile;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

import com.google.common.base.Throwables;

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
    public static final int INTERNAL_FIELD_COUNT = 6;

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    // These get put in from of the batch folders
    // active means the folder is being written to
    private static final String ACTIVE_PREFIX = "active-";

    // use thread-local to avoid SimpleDateFormat thread-safety issues
    protected ThreadLocal<SimpleDateFormat> m_ODBCDateformat;
    protected char m_delimiter;
    protected char[] m_fullDelimiters;
    protected String m_extension;
    protected String m_nonce;
    // outDir is the folder that will contain the raw files or batch folders
    protected File m_outDir;
    // the set of active decoders
    protected HashMap<Long, HashMap<String, ExportToFileDecoder>> m_tableDecoders;
    // how often to roll batches / files
    protected int m_period;
    // use thread-local to avoid SimpleDateFormat thread-safety issues
    protected ThreadLocal<SimpleDateFormat> m_dateformat;
    protected String m_dateFormatOriginalString;
    protected int m_firstfield;

    // record the original command line args for servers
    protected final List<String> m_commandLineServerArgs = new ArrayList<String>();

    protected final Set<String> m_globalSchemasWritten = new HashSet<String>();

    protected PeriodicExportContext m_current = null;

    protected boolean m_batched;
    protected boolean m_withSchema;

    protected final ReentrantReadWriteLock m_batchLock = new ReentrantReadWriteLock();

    private static final Object m_batchDirNamingLock = new Object();

    // timer used to roll batches
    protected ScheduledExecutorService m_ses;

    public static enum BinaryEncoding {
        BASE64,
        HEX
    }

    private BinaryEncoding m_binaryEncoding;

    /**
    *
    */
    public void notifyRollIsComplete(File[] files) {}

    class PeriodicExportContext {
        File m_dirContainingFiles;
        final Map<FileHandle, CSVWriter> m_writers = new TreeMap<FileHandle, CSVWriter>();
        boolean m_hasClosed = false;
        protected Date start;
        protected final Set<String> m_batchSchemasWritten = new HashSet<String>();

        class FileHandle implements Comparable<FileHandle> {
            final String tableName;
            final long generation;

            FileHandle(String tableName, long generation) {
                this.tableName = tableName;
                this.generation = generation;
            }

            String getPath(String prefix) {
                if (m_batched) {
                    return m_dirContainingFiles.getPath() +
                           File.separator +
                           generation +
                           "-" +
                           tableName +
                           m_extension;
                }
                else {
                    return m_dirContainingFiles.getPath() +
                           File.separator +
                           prefix +
                           m_nonce +
                           "-" +
                           generation +
                           "-" +
                           tableName +
                           "-" +
                           m_dateformat.get().format(start) +
                           m_extension;
                }
            }

            String getPathForSchema() {
                if (m_batched) {
                    return m_dirContainingFiles.getPath() +
                           File.separator +
                           generation +
                           "-" +
                           tableName +
                           "-schema.json";
                }
                else {
                    return m_dirContainingFiles.getPath() +
                           File.separator +
                           m_nonce +
                           "-" +
                           generation +
                           "-" +
                           tableName +
                           "-schema.json";
                }
            }

            @Override
            public int compareTo(FileHandle obj) {
                int first = tableName.compareTo(obj.tableName);
                if (first != 0) return first;
                long second = generation - obj.generation;
                if (second > 0) return 1;
                if (second < 0) return -1;
                return 0;
            }
        }

        PeriodicExportContext() {
            if (m_batched) {
                /*
                 * The batch dir name is by default only named to second granularity.
                 * What can happen is that when using on server export the client
                 * can be rapidly cycled sub-second resulting in collisions with the previous
                 * client. Spin here until a new one can be generated.
                 *
                 * Going to do this under a global lock to ensure that if this ends
                 * up being done in parallel it is properly sequenced
                 */
                synchronized (m_batchDirNamingLock) {
                    File dirContainingFiles = null;
                    do {
                        start = new Date();
                        dirContainingFiles = new VoltFile(getPathOfBatchDir(ACTIVE_PREFIX));
                        if (dirContainingFiles.exists()) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                Throwables.propagate(e);
                            }
                        }
                    } while (dirContainingFiles.exists());
                    m_dirContainingFiles = dirContainingFiles;
                }
                m_logger.trace(String.format("Creating dir for batch at %s", m_dirContainingFiles.getPath()));
                m_dirContainingFiles.mkdirs();
                if (m_dirContainingFiles.exists() == false) {
                    m_logger.error("Error: Unable to create batch directory at path: " + m_dirContainingFiles.getPath());
                    throw new RuntimeException("Unable to create batch directory.");
                }
            }
            else {
                start = new Date();
                m_dirContainingFiles = m_outDir;
            }
        }

        String getPathOfBatchDir(String prefix) {
            assert(m_batched);
            return m_outDir.getPath() + File.separator + prefix + m_nonce + "-" + m_dateformat.get().format(start);
        }

        /**
         * Flush and close all active writers, allowing the batch to
         * move from the active state into the finished state. This is
         * done as part of the roll process, or as part of the
         * unexpected shutdown process.
         *
         * Note this method is idempotent.
         *
         * @param clean True if we expect all writer have finished. False
         * if we just need to be done.
         */
        void closeAllWriters() {
            // only need to run this once per batch
            if (m_hasClosed) return;

            // flush and close any files that are open
            for (Entry<FileHandle, CSVWriter> entry : m_writers.entrySet()) {
                CSVWriter writer = entry.getValue();
                if (writer == null) continue;
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (m_batched)
                closeBatch();
            else
                closeFiles();

            // empty the writer set (probably not needed)
            m_writers.clear();

            // note that we're closed now
            m_hasClosed = true;
        }

        void closeBatch() {
            // rename the file appropriately
            m_logger.trace("Renaming batch.");

            String oldPath = getPathOfBatchDir(ACTIVE_PREFIX);
            String newPath = getPathOfBatchDir("");

            File oldDir = new VoltFile(oldPath);
            assert(oldDir.exists());
            assert(oldDir.isDirectory());
            assert(oldDir.canWrite());

            if (oldDir.listFiles().length > 0) {
                File newDir = new VoltFile(newPath);
                oldDir.renameTo(newDir);
                notifyRollIsComplete(new File[] { newDir });
            }
            else {
                oldDir.delete();
            }
        }

        void closeFiles() {
            File[] notifySet = new VoltFile[m_writers.size()];

            int i = 0;
            // Sort the open files by TXN ID so that we can close and rename
            // them in the order in which they were created.  This allows
            // apps interested in the files to know that whenever a new file
            // is closed, it will be the next file temporally in the export stream
            FileHandle[] keys = m_writers.keySet().toArray(new FileHandle[] {});
            Arrays.sort(keys, new Comparator<FileHandle>(){
                @Override
                public int compare(FileHandle f1, FileHandle f2)
                {
                    long first_txnid = Long.parseLong(f1.getPath("").split("-")[1]);
                    long second_txnid = Long.parseLong(f2.getPath("").split("-")[1]);
                    if (first_txnid < second_txnid)
                    {
                        return -1;
                    }
                    else if (first_txnid > second_txnid)
                    {
                        return 1;
                    }
                    else
                    {
                        return 0;
                    }
                }
            });

            for (FileHandle handle : keys)
            {
                String oldPath = handle.getPath(ACTIVE_PREFIX);
                String newPath = handle.getPath("");

                File oldFile = new VoltFile(oldPath);
                assert(oldFile.exists());
                assert(oldFile.isFile());
                assert(oldFile.canWrite());

                File newFile = new VoltFile(newPath);
                assert(!newFile.exists());
                oldFile.renameTo(newFile);

                notifySet[i] = newFile;
                i++;
            }

            notifyRollIsComplete(notifySet);
        }

        CSVWriter getWriter(String tableName, long generation) {
            FileHandle handle = new FileHandle(tableName, generation);
            CSVWriter writer = m_writers.get(handle);
            if (writer != null)
                return writer;

            String path = handle.getPath(ACTIVE_PREFIX);
            File newFile = new VoltFile(path);
            if (newFile.exists()) {
                m_logger.error("Error: Output file for next period already exists at path: " + newFile.getPath());
                m_logger.error("Consider using a more specific timestamp in your filename or cleaning up your export data directory.");
                m_logger.error("ExportToFileClient will stop to prevent data loss.");
                throw new RuntimeException();
            }
            try {
                OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(newFile, false), "UTF-8");
                if (m_fullDelimiters != null) {
                    writer = new CSVWriter(new BufferedWriter(osw, 4096 * 4),
                            m_fullDelimiters[0], m_fullDelimiters[1], m_fullDelimiters[2], String.valueOf(m_fullDelimiters[3]));
                }
                else if (m_delimiter == ',')
                    // CSV
                    writer = new CSVWriter(new BufferedWriter(osw, 4096 * 4), m_delimiter);
                else {
                    // TSV
                    writer = CSVWriter.getStrictTSVWriter(new BufferedWriter(osw, 4096 * 4));
                }
            }
            catch (Exception e) {
                m_logger.error(e.getMessage());
                m_logger.error("Error: Failed to create output file: " + path);
                throw new RuntimeException();
            }
            m_writers.put(handle, writer);
            return writer;
        }

        void writeSchema(String tableName, long generation, String schema) {
            // if no schema's enabled pretend like this worked
            if (!m_withSchema) return;

            FileHandle handle = new FileHandle(tableName, generation);
            String path = handle.getPathForSchema();

            Set<String> targetSet = m_batched ? m_batchSchemasWritten : m_globalSchemasWritten;
            synchronized (targetSet) {
                // only write the schema once per batch
                if (m_batched) {
                    if (m_batchSchemasWritten.contains(path)) {
                        return;
                    }
                }
                else {
                    if (m_globalSchemasWritten.contains(path)) {
                        return;
                    }
                }

                File newFile = new VoltFile(path);
                try {
                    OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(newFile, false), "UTF-8");
                    BufferedWriter writer = new BufferedWriter(osw);
                    writer.write(schema);
                    writer.flush();
                    writer.close();
                    if (m_batched) {
                        m_batchSchemasWritten.add(path);
                    }
                    else {
                        m_globalSchemasWritten.add(path);
                    }
                } catch (Exception e) {
                    m_logger.error(e.getMessage());
                    m_logger.error("Error: Failed to create output file: " + path);
                    throw new RuntimeException();
                }
            }
        }

        /**
         * Try to ensure file descriptors are closed.
         * This probably only really is useful for the crl-c case.
         * Still, it's not very well tested.
         */
        @Override
        protected void finalize() throws Throwable {
            closeAllWriters();
            super.finalize();
        }
    }


    // This class outputs exported rows converted to CSV or TSV values
    // for the table named in the constructor's AdvertisedDataSource
    class ExportToFileDecoder extends ExportDecoderBase {
        private final long m_generation;
        private final String m_tableName;
        protected String m_schemaString = "ERROR SERIALIZING SCHEMA";
        private final HashSet<AdvertisedDataSource> m_sources = new HashSet<AdvertisedDataSource>();
        private FutureTask<CSVWriter> m_firstBlockTask;
        private CSVWriter m_writer;

        private void resetWriter() {
            m_firstBlockTask = new FutureTask<CSVWriter>(new Callable<CSVWriter>() {
                @Override
                public CSVWriter call() throws Exception {
                    CSVWriter writer = m_current.getWriter(m_tableName, m_generation);
                    m_current.writeSchema(m_tableName, m_generation, m_schemaString);
                    return writer;
                }
            });;
        }

        public ExportToFileDecoder(
                AdvertisedDataSource source,
                String tableName,
                long generation) {
            super(source);
            m_generation = generation;
            m_tableName = tableName;

            setSchemaForSource(source);
            resetWriter();
        }

        /**
         * Given the data source, construct a JSON serialization
         * of its schema to be written to disk with the export
         * data.
         */
        void setSchemaForSource(AdvertisedDataSource source) {
            try {
                JSONStringer json = new JSONStringer();
                json.object();
                json.key("table name").value(source.tableName);
                json.key("generation id").value(source.m_generation);
                json.key("columns").array();

                for (int i = 0; i < source.columnNames.size(); i++) {
                    json.object();
                    json.key("name").value(source.columnNames.get(i));
                    json.key("type").value(source.columnTypes.get(i).name());
                    json.endObject();
                }

                json.endArray();
                json.endObject();

                // get the json string
                m_schemaString = json.toString();
                // the next two lines pretty print the json string
                JSONObject jsonObj = new JSONObject(m_schemaString);
                m_schemaString = jsonObj.toString(4);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
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
                String[] fields = new String[m_tableSchema.size() - m_firstfield];

                for (int i = m_firstfield; i < m_tableSchema.size(); i++) {
                    if (row[i] == null) {
                        fields[i - m_firstfield] = "NULL";
                    } else if (m_tableSchema.get(i) == VoltType.VARBINARY) {
                        if (m_binaryEncoding == BinaryEncoding.HEX) {
                            fields[i - m_firstfield] = Encoder.hexEncode((byte[]) row[i]);
                        } else {
                            fields[i - m_firstfield] = Encoder.base64Encode((byte[]) row[i]);
                        }
                    } else if (m_tableSchema.get(i) == VoltType.STRING) {
                        fields[i - m_firstfield] = (String) row[i];
                    } else if (m_tableSchema.get(i) == VoltType.TIMESTAMP) {
                        TimestampType timestamp = (TimestampType) row[i];
                        fields[i - m_firstfield] = m_ODBCDateformat.get().format(timestamp.asApproximateJavaDate());
                    } else {
                        fields[i - m_firstfield] = row[i].toString();
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

        /**
         * Get and hold the current batch folder.
         * Ask the batch object for a stream to write to.
         */
        @Override
        public void onBlockStart() {
            m_batchLock.readLock().lock();
            m_firstBlockTask.run();
            try {
                m_writer = m_firstBlockTask.get();
            } catch (Throwable e) {
                Throwables.propagate(e);
            }
        }

        /**
         * Release the current batch folder.
         */
        @Override
        public void onBlockCompletion() {
            try {
                m_writer.flush();
            } catch (Throwable t) {
                Throwables.propagate(t);
            } finally {
                m_batchLock.readLock().unlock();
            }
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            m_batchLock.writeLock().lock();
            try {
                HashMap<String, ExportToFileDecoder> decoders = m_tableDecoders.get(m_generation);
                if (decoders != null) {
                    decoders.remove(m_tableName);
                    if (decoders.isEmpty()) {
                        m_tableDecoders.remove(m_generation);
                    }
                }
            } finally {
                m_batchLock.writeLock().unlock();
            }
        }
    }

    public ExportToFileClient(char delimiter,
            String nonce,
            File outdir,
            int period,
            String dateformatString,
            String fullDelimiters,
            int firstfield,
            boolean useAdminPorts,
            boolean batched,
            boolean withSchema,
            int throughputMonitorPeriod,
            BinaryEncoding be) {
        this(delimiter, nonce, outdir, period, dateformatString, fullDelimiters,
                firstfield, useAdminPorts, batched, withSchema, throughputMonitorPeriod,
                true, TimeZone.getDefault(), be);
    }

    public ExportToFileClient() {
        super(false, 0, false);
    }

    public ExportToFileClient(char delimiter,
                              String nonce,
                              File outdir,
                              int period,
                              String dateformatString,
                              String fullDelimiters,
                              int firstfield,
                              boolean useAdminPorts,
                              boolean batched,
                              boolean withSchema,
                              int throughputMonitorPeriod,
                              boolean autodiscoverTopology,
                              TimeZone tz,
                              BinaryEncoding be) {
        super(useAdminPorts, throughputMonitorPeriod, autodiscoverTopology);
        configureInternal(
                delimiter,
                nonce,
                outdir,
                period,
                dateformatString,
                fullDelimiters,
                firstfield,
                batched,
                withSchema,
                tz,
                be);
    }

    @Override
    public ExportToFileDecoder constructExportDecoder(AdvertisedDataSource source) {
        m_batchLock.writeLock().lock();
        try {
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
                decoder = new ExportToFileDecoder(source, table_name, source.m_generation);
                decoders.put(table_name, decoder);
            }
            decoder.m_sources.add(source);
            return decoders.get(table_name);
        } finally {
            m_batchLock.writeLock().unlock();
        }
    }

    @Override
    public void shutdown() {
        m_ses.shutdown();
        try {
            m_ses.awaitTermination(365, TimeUnit.DAYS);
        } catch( InterruptedException iex) {
            Throwables.propagate(iex);
        }
        m_batchLock.writeLock().lock();
        m_current.closeAllWriters();
    }

    /**
     * Deprecate the current batch and create a new one. The old one will still
     * be active until all writers have finished writing their current blocks
     * to it.
     */
    void roll() {
        m_batchLock.writeLock().lock();
        final PeriodicExportContext previous = m_current;
        try {
            m_current = new PeriodicExportContext();

            m_logger.trace("Rolling batch.");

            for( Map<String, ExportToFileDecoder> genDecoders: m_tableDecoders.values()) {
                for( ExportToFileDecoder decoder: genDecoders.values()) {
                    decoder.resetWriter();
                }
            }

        } finally {
            m_batchLock.writeLock().unlock();
        }
        previous.closeAllWriters();
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
                        + "[--batched] "
                        + "[--with-schema] "
                        + "[--period rolling_period_in_minutes] "
                        + "[--dateformat date_pattern_for_file_name] "
                        + "[--outdir target_directory] "
                        + "[--skipinternals] "
                        + "[--delimiters html-escaped delimiter set (4 chars)] "
                        + "[--user export_username] "
                        + "[--password export_password]"
                        + "[--timezone GMT+0]"
                        + "[--binaryencoding [ HEX | BASE64 ]]");
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
        boolean batched = false;
        boolean withSchema = false;
        String fullDelimiters = null;
        int throughputMonitorPeriod = 0;
        boolean autodiscoverTopolgy = true;
        TimeZone tz = VoltDB.GMT_TIMEZONE;
        BinaryEncoding be = BinaryEncoding.HEX;

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
                firstfield = INTERNAL_FIELD_COUNT;
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
                outdir = new VoltFile(args[ii + 1]);
                if (!outdir.exists()) {
                    if (!outdir.mkdir()) {
                        System.err.println("Error: " + outdir.getPath() + " cannot be created");
                        invalidDir = true;
                    }
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
            } else if (arg.equals("--timezone")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --timezone");
                    printHelpAndQuit(-1);
                }
                String tzId = args[ii + 1];
                ii++;
                tz = TimeZone.getTimeZone(tzId);
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
            else if (arg.equals("--batched")) {
                batched = true;
            }
            else if (arg.equals("--with-schema")) {
                withSchema = true;
            }
            else if (arg.equals("--delimiters")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --delimiters");
                    printHelpAndQuit(-1);
                }
                fullDelimiters = args[ii + 1].trim();
                ii++;
                String charsAsStr = StringEscapeUtils.unescapeHtml4(fullDelimiters.trim());
                if (charsAsStr.length() != 4) {
                    System.err.println("The delimiter set must contain exactly 4 characters (after any html escaping).");
                    printHelpAndQuit(-1);
                }
            }
            else if (arg.equals("--binaryencoding")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --binaryencoding");
                    printHelpAndQuit(-1);
                }
                try {
                    be = BinaryEncoding.valueOf(args[ii + 1].trim().toUpperCase());
                } catch (IllegalArgumentException e) {
                    System.err.println(
                            "The binary encoding \"" + args[ii + 1].trim().toUpperCase() +
                            "\" is unsupported. Must be one of [ HEX | BASE64 ]");
                    printHelpAndQuit(-1);
                }
                ii++;
            }
            else if (arg.equals("--disable-topology-autodiscovery")) {
                autodiscoverTopolgy = false;
            }
            else if (arg.equals("--throughput-monitor-period")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --throughput-monitor-period");
                    printHelpAndQuit(-1);
                }
                throughputMonitorPeriod = Integer.parseInt(args[ii + 1].trim());
                ii++;
            }
            else {
                System.err.println("Unrecognized parameter " + arg);
                System.exit(-1);
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
            outdir = new VoltFile(".");
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
                                                           fullDelimiters,
                                                           firstfield,
                                                           connect == 'a',
                                                           batched,
                                                           withSchema,
                                                           throughputMonitorPeriod,
                                                           autodiscoverTopolgy,
                                                           tz,
                                                           be);

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

    @Override
    public void configure( Properties conf) throws Exception {
        String nonce = conf.getProperty("nonce");

        if (nonce == null) {
            throw new IllegalArgumentException("ExportToFile: must provide a filename nonce");
        }
        char delimiter = '\0';
        String type = conf.getProperty("type", "").trim();
        if (type != null) {
            if (type.equalsIgnoreCase("csv")) {
                delimiter = ',';
            } else if (type.equalsIgnoreCase("tsv")) {
                delimiter = '\t';
            } else {
                throw new IllegalArgumentException("Error: --type must be one of CSV or TSV");
            }
        }
        if (delimiter == '\0') {
            throw new IllegalArgumentException("ExportToFile: must provide an output type");
        }

        File outdir = new VoltFile(conf.getProperty("outdir", "."));
        if (!outdir.exists()) {
            if (!outdir.mkdir()) {
                throw new IllegalArgumentException("Error: " + outdir.getPath() + " cannot be created");
            }
        }
        if (!outdir.canRead()) {
            throw new IllegalArgumentException("Error: " + outdir.getPath() + " does not have read permission set");
        }
        if (!outdir.canExecute()) {
            throw new IllegalArgumentException("Error: " + outdir.getPath() + " does not have execute permission set");
        }
        if (!outdir.canWrite()) {
            throw new IllegalArgumentException("Error: " + outdir.getPath() + " does not have write permission set");
        }

        int firstfield = Boolean.parseBoolean(conf.getProperty("skipinternals","false")) ? INTERNAL_FIELD_COUNT : 0;

        int period = Integer.parseInt(conf.getProperty("period", "60"));
        if (period < 1) {
            throw new IllegalArgumentException("Error: Specified value for --period must be >= 1.");
        }

        String dateformatString = conf.getProperty("dateformat", "yyyyMMddHHmmss").trim();
        boolean batched = Boolean.parseBoolean(conf.getProperty("batched", "false"));
        boolean withSchema = Boolean.parseBoolean(conf.getProperty("with-schema", "false"));

        String fullDelimiters = conf.getProperty("delimiters");
        if (fullDelimiters != null) {
            fullDelimiters = fullDelimiters.trim();
            String charsAsStr = StringEscapeUtils.unescapeJava(fullDelimiters.trim());
            if (charsAsStr.length() != 4) {
                throw new IllegalArgumentException("The delimiter set must contain exactly 4 characters (after any string escaping).");
            }
        }

        TimeZone tz = TimeZone.getTimeZone(conf.getProperty("timezone", VoltDB.GMT_TIMEZONE.getID()));

        BinaryEncoding encoding = BinaryEncoding.valueOf(
                conf.getProperty("binaryencoding", "HEX").trim().toUpperCase());

        configureInternal(
                delimiter,
                nonce,
                outdir,
                period,
                dateformatString,
                fullDelimiters,
                firstfield,
                batched,
                withSchema,
                tz,
                encoding);
    }

    private void configureInternal(
                              char delimiter,
                              String nonce,
                              File outdir,
                              int period,
                              String dateformatString,
                              String fullDelimiters,
                              int firstfield,
                              boolean batched,
                              boolean withSchema,
                              final TimeZone tz,
                              final BinaryEncoding be) {
        m_delimiter = delimiter;
        m_extension = (delimiter == ',') ? ".csv" : ".tsv";
        m_nonce = nonce;
        m_outDir = outdir;
        m_tableDecoders = new HashMap<Long, HashMap<String, ExportToFileDecoder>>();
        m_period = period;
        m_dateFormatOriginalString = dateformatString;
        // SimpleDateFormat isn't threadsafe
        // ThreadLocal variables should protect them, lamely.
        m_dateformat = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(m_dateFormatOriginalString);
            }
        };
        m_ODBCDateformat = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                SimpleDateFormat sdf = new SimpleDateFormat(VoltDB.ODBC_DATE_FORMAT_STRING);
                sdf.setTimeZone(tz);
                return sdf;
            }
        };
        m_binaryEncoding = be;
        m_firstfield = firstfield;
        m_batched = batched;
        m_withSchema = withSchema;

        if (fullDelimiters != null) {
            fullDelimiters = StringEscapeUtils.unescapeHtml4(fullDelimiters);
            m_fullDelimiters = new char[4];
            for (int i = 0; i < 4; i++) {
                m_fullDelimiters[i] = fullDelimiters.charAt(i);
            }
        }
        else {
            m_fullDelimiters = null;
        }

        // init the batch system with the first batch
        assert(m_current == null);
        m_current = new PeriodicExportContext();


        // schedule rotations every m_period minutes
        Runnable rotator = new Runnable() {
            @Override
            public void run() {
                try {
                    roll();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        };
        m_ses =
                CoreUtils.getScheduledThreadPoolExecutor(
                        "Export file rotate timer for nonce " + nonce, 1, CoreUtils.SMALL_STACK_SIZE);
        m_ses.scheduleWithFixedDelay(rotator, m_period, m_period, TimeUnit.MINUTES);
    }
}
