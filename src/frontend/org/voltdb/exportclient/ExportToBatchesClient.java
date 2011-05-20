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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.logging.VoltLogger;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.DelimitedDataWriterUtil.CSVWriter;
import org.voltdb.utils.DelimitedDataWriterUtil.DelimitedDataWriter;

/**
 * Uses the Export feature of VoltDB to write exported tables to files in format for easy consumption by
 * batch ingestors.
 *
 * command line args:
 *     --servers {comma-separated list of VoltDB server to which to connect}
 *     --outdir {path where output files should be written}
 *     --nonce {string-to-unique-ify output files}
 *     --user {username for cluster export user}
 *     --password {password for cluster export user}
 *     --period {period (in minutes) to use when building a batch}
 *
 */
public class ExportToBatchesClient extends ExportClientBase {
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    // These get put in from of the batch folders
    // active means the folder is being written to
    private static final String ACTIVE_PREFIX = "active-";
    // dirty is rare, but means the folder wasn't cleanly closed
    private static final String DIRTY_PREFIX = "dirty-";

    // ODBC Datetime Format
    // if you need microseconds, you'll have to change this code or
    //  export a bigint representing microseconds since an epoch
    protected final SimpleDateFormat m_sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    // Batches only supports CSV
    protected final DelimitedDataWriter m_escaper = new CSVWriter();
    protected final String m_nonce;

    // outDir is the folder that will contain the batch folders
    protected final File m_outDir;
    // the set of active decoders
    protected final HashMap<Long, HashMap<String, ExportToBatchDecoder>> m_tableDecoders;
    // how often to roll batches
    protected final int m_period;

    // whether to skip the initial metadata in the output
    protected final int m_firstfield;
    // timer used to roll batches
    protected final Timer m_timer = new Timer();

    // record the original command line args for servers
    protected final List<String> m_commandLineServerArgs = new ArrayList<String>();

    protected final static SimpleDateFormat m_dateformat = new SimpleDateFormat("yyyyMMddHHmmss");
    static private ExportBatch m_current = null;

    /**
     *
     */
    public void notifyBatchIsDone(File batch) {

    }

    /**
     * Represents all of the export data (and accompanying schema)
     * for the export client for the specified rolling period.
     *
     * Each instance is responsible for a single dated folder of
     * export data.
     *
     */
    class ExportBatch {
        protected final String pathToDirectory;
        protected final String nonce;
        protected final Date start;
        protected Date end = null;
        protected HashSet<ExportToBatchDecoder> m_decoders = new HashSet<ExportToBatchDecoder>();
        protected final Set<String> m_schemasWritten = new HashSet<String>();

        HashMap<String, BufferedWriter> m_writers = new HashMap<String, BufferedWriter>();
        boolean m_hasClosed = false;

        ExportBatch(String pathToDirectory, String nonce, Date batchStart) {
            start = batchStart;
            this.pathToDirectory = pathToDirectory;
            this.nonce = nonce;

            File dirForBatch = new File(getDirPath(ACTIVE_PREFIX));
            m_logger.trace(String.format("Creating dir for batch at %s", dirForBatch.getPath()));
            boolean created = dirForBatch.mkdirs();
            assert(created);
        }

        String getDirPath(String prefix) {
            return pathToDirectory + File.separator + prefix + nonce + "-" + m_dateformat.format(start);
        }

        /**
         * Release a hold on the batch. When all holds are done
         * and the roll time has passed, the batch can move out
         * of the active state.
         */
        void decref(ExportToBatchDecoder decoder) {
            synchronized (getClass()) {
                m_decoders.remove(decoder);
                if ((end != null) && (m_decoders.size() == 0)) {
                    closeAllWriters(true);
                }
            }
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
        private void closeAllWriters(boolean clean) {
            // only need to run this once per batch
            if (m_hasClosed)
                return;

            // flush and close any files that are open
            for (Entry<String, BufferedWriter> entry : m_writers.entrySet()) {
                BufferedWriter writer = entry.getValue();
                if (writer == null) continue;
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            // empty the writer set (probably not needed)
            m_writers.clear();

            // note that we're closed now
            m_hasClosed = true;

            // rename the file appropriately
            m_logger.trace("Renaming batch.");

            String oldPath = getDirPath(ACTIVE_PREFIX);
            String newPath = clean ? getDirPath("") : getDirPath(DIRTY_PREFIX);

            File oldDir = new File(oldPath);
            assert(oldDir.exists());
            assert(oldDir.isDirectory());
            assert(oldDir.canWrite());

            if (oldDir.listFiles().length > 0) {
                File newDir = new File(newPath);
                oldDir.renameTo(newDir);
                notifyBatchIsDone(newDir);
            }
            else {
                oldDir.delete();
            }
        }

        BufferedWriter getWriter(String filename) {
            BufferedWriter writer = m_writers.get(filename);
            if (writer != null)
                return writer;

            String path = getDirPath(ACTIVE_PREFIX) + File.separator + filename;
            File newFile = new File(path);
            try {
                OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(newFile, true), "UTF-8");
                writer = new BufferedWriter(osw, 1048576);
            } catch (Exception e) {
                m_logger.error(e.getMessage());
                m_logger.error("Error: Failed to create output file: " + path);
                throw new RuntimeException();
            }
            m_writers.put(filename, writer);
            return writer;
        }

        void writeSchema(String filename, String schema) {
            // only write the schema once per batch
            if (m_schemasWritten.contains(filename))
                return;
            String path = getDirPath(ACTIVE_PREFIX) + File.separator + filename;
            File newFile = new File(path);
            try {
                OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(newFile, true), "UTF-8");
                BufferedWriter writer = new BufferedWriter(osw, 1048576);
                writer.write(schema);
                writer.flush();
                writer.close();
                m_schemasWritten.add(filename);
            } catch (Exception e) {
                m_logger.error(e.getMessage());
                m_logger.error("Error: Failed to create output file: " + path);
                throw new RuntimeException();
            }
        }

        /**
         * Try to ensure file descriptors are closed.
         * This probably only really is useful for the crl-c case.
         * Still, it's not very well tested.
         */
        @Override
        protected void finalize() throws Throwable {
            synchronized (getClass()) {
                closeAllWriters(false);
            }
            super.finalize();
        }
    }

    /**
     * This class outputs, using the ExportBatch class, exported rows
     * converted to CSV for the table named in the constructor's AdvertisedDataSource
     */
    class ExportToBatchDecoder extends ExportDecoderBase {
        protected String m_filename;
        private final long m_generation;
        private final String m_tableName;
        protected String m_schemaString = "ERROR SERIALIZING SCHEMA";

        // transient per-block state
        protected ExportBatch m_batch = null;
        protected BufferedWriter m_writer = null;

        public ExportToBatchDecoder(AdvertisedDataSource source, String tableName) {
            super(source);
            m_generation = source.m_generation;
            m_tableName = tableName;
            // Create the output file for this table
            m_filename = source.tableName + "-" + String.valueOf(m_generation) + "." + m_escaper.getExtension();

            setSchemaForSource(source);
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
                for (int i = m_firstfield; i < m_tableSchema.size(); i++) {
                    if (row[i] == null) {
                        m_escaper.writeRawField(m_writer, "NULL", i > m_firstfield);
                    } else if (m_tableSchema.get(i) == VoltType.STRING) {
                        m_escaper.writeEscapedField(m_writer, (String) row[i], i > m_firstfield);
                    } else if (m_tableSchema.get(i) == VoltType.TIMESTAMP) {
                        TimestampType timestamp = (TimestampType) row[i];
                        m_escaper.writeRawField(m_writer, m_sdf.format(timestamp.asApproximateJavaDate()),
                                i > m_firstfield);
                        m_escaper.writeRawField(m_writer, String.valueOf(timestamp.getUSec()), false);
                    } else {
                        m_escaper.writeRawField(m_writer, row[i].toString(), i > m_firstfield);
                    }
                }
                m_writer.write("\n");
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            } catch (Exception x) {
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
            // get the current batch
            m_batch = getCurrentBatchAndAddref(this);
            if (m_batch == null) {
                m_logger.error("NULL batch object found.");
                throw new RuntimeException("NULL batch object found.");
            }

            m_writer = m_batch.getWriter(m_filename);
            if (m_writer == null) {
                m_logger.error("NULL writer object from batch.");
                throw new RuntimeException("NULL writer object from batch.");
            }

            String schemaPath = m_tableName + "-" + String.valueOf(m_generation) + "-schema.json";
            m_batch.writeSchema(schemaPath, m_schemaString);
        }

        /**
         * Release the current batch folder.
         */
        @Override
        public void onBlockCompletion() {
            try {
                if (m_writer != null)
                    m_writer.flush();
            }
            catch (Exception e) {
                m_logger.error(e.getMessage());
                throw new RuntimeException();
            }
            finally {
                if (m_batch != null) {
                    m_batch.decref(this);
                }
                m_batch = null;
            }
        }

        @Override
        protected void finalize() throws Throwable {
            try {
                if (m_writer != null) {
                    m_writer.flush();
                    m_writer.close();
                    m_writer = null;
                }
            }
            catch (Exception e) {
                m_logger.error(e.getMessage());
                throw new RuntimeException();
            }
            finally {
                if (m_batch != null)
                    m_batch.decref(this);
            }
            super.finalize();
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            try {
                if (m_writer != null) {
                    m_writer.flush();
                    m_writer.close();
                    m_writer = null;
                }
            }
            catch (Exception e) {
                m_logger.error(e.getMessage());
                throw new RuntimeException();
            }
            finally {
                if (m_batch != null)
                    m_batch.decref(this);
            }
            HashMap<String, ExportToBatchDecoder> decoders = m_tableDecoders.get(m_generation);
            decoders.remove(m_tableName);
            if (decoders.isEmpty()) {
                m_tableDecoders.remove(m_generation);
            }
        }
    }

    public ExportToBatchesClient(String nonce,
                            File outdir,
                            int period,
                            int firstfield,
                            boolean useAdminPorts) {

        super(useAdminPorts);
        m_nonce = nonce;
        m_outDir = outdir;
        m_tableDecoders = new HashMap<Long, HashMap<String, ExportToBatchDecoder>>();
        m_period = period;
        m_firstfield = firstfield;

        // init the batch system with the first batch
        assert(m_current == null);
        m_current = new ExportBatch(outdir.getPath(), m_nonce, new Date());

        // schedule rotations every m_period minutes
        TimerTask rotateTask = new TimerTask() {
            @Override
            public void run() {
                roll(new Date());
            }
        };
        m_timer.scheduleAtFixedRate(rotateTask, 1000 * 60 * m_period, 1000 * 60 * m_period);
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

        if (m_outDir == null) {
            m_logger.info("Discarding all export data.");
        }
        else {
            m_logger.info(String.format("Prepending export data folders with nonce: %s",
                    m_nonce));
            m_logger.info(String.format("Rotate export batches every %d minute%s",
                    m_period, m_period == 1 ? "" : "s"));
            m_logger.info(String.format("Writing export batches to dir: %s",
                    m_outDir));
            if (m_firstfield == 0) {
                m_logger.info("Including VoltDB export metadata");
            }
            else {
                m_logger.info("Not including VoltDB export metadata");
            }
        }
    }

    protected static void printHelpAndQuit(int code) {
        System.out
                .println("java -cp <classpath> org.voltdb.exportclient.ExportToSqClient "
                        + "--help");
        System.out
                .println("java -cp <classpath> org.voltdb.exportclient.ExportToSqClient "
                        + "--servers server1[,server2,...,serverN] "
                        + "--connect (admin|client) "
                        + "--nonce file_prefix "
                        + "[--period batch_period_in_minutes] "
                        + "[--outdir target_directory] "
                        + "[--skipinternals] "
                        + "[--user export_username] "
                        + "[--password export_password]");
        System.out.println("Note that server hostnames may be appended with a specific port:");
        System.out.println("  --servers server1:port1[,server2:port2,...,serverN:portN]");

        System.exit(code);
    }

    synchronized ExportBatch getCurrentBatchAndAddref(ExportToBatchDecoder decoder) {
        m_current.m_decoders.add(decoder);
        return m_current;
    }

    /**
     * Deprecate the current batch and create a new one. The old one will still
     * be active until all writers have finished writing their current blocks
     * to it.
     */
    synchronized void roll(Date rollTime) {
        m_logger.trace("Rolling batch.");
        m_current.end = rollTime;
        if (m_current.m_decoders.size() == 0)
            m_current.closeAllWriters(true);
        m_current = new ExportBatch(m_current.pathToDirectory, m_current.nonce, rollTime);
    }

    /**
     * Get the decoder for the table described by source and add this partition
     * to it, creating a decoder if necessary.
     *
     * @param source AdvertisedDataSource sent over the wire to this client.
     */
    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        // For every source that provides part of a table, use the same
        // export decoder.
        String table_name = source.tableName;
        HashMap<String, ExportToBatchDecoder> decoders = m_tableDecoders.get(source.m_generation);
        if (decoders == null) {
            decoders = new HashMap<String, ExportToBatchDecoder>();
            m_tableDecoders.put(source.m_generation, decoders);
        }
        if (!decoders.containsKey(table_name)) {
            decoders.put(table_name, new ExportToBatchDecoder(source, table_name));
        }
        return decoders.get(table_name);
    }

    public static void main(String[] args) {
        String[] volt_servers = null;
        String user = null;
        String password = null;
        String nonce = null;
        File outdir = null;
        int firstfield = 0;
        int period = 60;
        char connect = ' '; // either ' ', 'c' or 'a'

        for (int ii = 0; ii < args.length; ii++) {
            String arg = args[ii];
            if (arg.equals("--help")) {
                printHelpAndQuit(0);
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
            System.err
                    .println("ExportToSq: must provide a filename nonce");
            printHelpAndQuit(-1);
        }
        if (outdir == null) {
            outdir = new File(".");
        }


        // create the export to file client
        ExportToBatchesClient client = new ExportToBatchesClient(nonce,
                                                       outdir,
                                                       period,
                                                       firstfield,
                                                       connect == 'a');

        // add all of the servers specified
        for (String server : volt_servers)
            client.addServerInfo(server, connect == 'a');

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
