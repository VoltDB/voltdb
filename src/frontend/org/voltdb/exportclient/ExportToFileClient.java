/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
import org.voltdb.common.Constants;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportManager;
import org.voltdb.exportclient.ExportDecoderBase.BinaryEncoding;
import org.voltdb.exportclient.decode.CSVWriterDecoder;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

/**
 * Uses the Export feature of VoltDB to write exported tables to files.
 *
 */
public class ExportToFileClient extends ExportClientBase {

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    private static final TimeUnit TIME_PERIOD_UNIT =
            TimeUnit.valueOf(System.getProperty("__EXPORT_FILE_ROTATE_PERIOD_UNIT__", TimeUnit.MINUTES.name()));
    private static final int EXPORT_DELIM_NUM_CHARACTERS = 4;

    // These get put in from of the batch folders
    // active means the folder is being written to
    private static final String ACTIVE_PREFIX = "active-";

    protected char m_delimiter;
    protected char[] m_fullDelimiters;
    protected String m_extension;
    protected String m_nonce;
    // outDir is the folder that will contain the raw files or batch folders
    protected File m_outDir;
    // map of active decoders
    protected HashMap<DecoderMetaData, ExportToFileDecoder> m_tableDecoders;
    // map of executor service to tables
    protected HashMap<String, ListeningExecutorService> m_decoderExecutor;
    // how often to roll batches / files
    protected int m_period;
    // use thread-local to avoid SimpleDateFormat thread-safety issues
    protected ThreadLocal<SimpleDateFormat> m_dateformat;
    protected String m_dateFormatOriginalString;
    protected boolean m_skipinternal;

    protected final Set<String> m_globalSchemasWritten = new HashSet<>();

    protected PeriodicExportContext m_current = null;

    protected boolean m_batched;
    protected boolean m_withSchema;
    protected boolean m_uniquenames;

    protected final ReentrantReadWriteLock m_batchLock = new ReentrantReadWriteLock();

    private static final Object m_batchDirNamingLock = new Object();

    // timer used to roll batches
    protected ScheduledExecutorService m_scheduledFileRotatorService;

    protected BinaryEncoding m_binaryEncoding;

    // date formatter time zone
    protected TimeZone m_timeZone;

    //For test
    public static String TEST_VOLTDB_ROOT = null;

    class DecoderMetaData {
        final String tableName;
        final long generation;
        final int partitionId;

        public DecoderMetaData (String tableName, long generation, int partition) {
            this.tableName = tableName;
            this.generation = generation;
            this.partitionId = partition;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 23 * hash + Objects.hashCode(tableName);
            hash = 23 * hash + partitionId;
            hash = 23 * hash + (int) (generation ^ generation >>> 32);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            final DecoderMetaData other = (DecoderMetaData)obj;
            if (generation == other.generation && partitionId == other.partitionId
                    && tableName.equals(tableName)) {
                return true;
            }

            return false;
        }

    }

    class PeriodicExportContext {
        File m_dirContainingFiles;
        final Map<FileHandle, CSVWriter> m_writers = Collections.synchronizedMap(new TreeMap<FileHandle, CSVWriter>());
        boolean m_hasClosed = false;
        protected Date start;
        protected final Set<String> m_batchSchemasWritten = new HashSet<>();

        class FileHandle implements Comparable<FileHandle> {
            final String tableName;
            final long generation;
            final long creationTime;

            FileHandle(String tableName, long generation) {
                this.tableName = tableName;
                this.generation = generation;
                this.creationTime = System.currentTimeMillis();
            }

            String getPathUtility(String extension, String hostId, String prefix) {
                String res = "";
                if(m_batched) {
                    res = m_dirContainingFiles.getPath() +
                          File.separator +
                          generation +
                          "-" +
                          tableName +
                          hostId +
                          extension;
                }
                else {
                    res = m_dirContainingFiles.getPath() +
                          File.separator +
                          prefix +
                          m_nonce +
                          "-" +
                          generation +
                          "-" +
                          tableName +
                          "-" +
                          m_dateformat.get().format(start) +
                          hostId +
                          extension;
                }
                return res;
            }

            String getPath(String prefix) {
                String hostId = "";
                if(m_uniquenames) {
                    hostId = "-("+VoltDB.instance().getHostMessenger().getHostId()+")";
                }
                return getPathUtility(m_extension, hostId, prefix);
            }

            String getPathForSchema() {
                String hostId = "";
                if(m_uniquenames) {
                    hostId = "-("+VoltDB.instance().getHostMessenger().getHostId()+")";
                }
                return getPathUtility("-schema.json", hostId, "");
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

            @Override
            public String toString() {
                return "FileHandle for " + tableName + " Generation " + generation + " Creation time: " + creationTime;
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
         */
        void closeAllWriters() {
            // only need to run this once per batch
            if (m_hasClosed) return;

            // flush and close any files that are open
            for (Entry<FileHandle, CSVWriter> entry : m_writers.entrySet()) {
                CSVWriter writer = entry.getValue();
                if (writer == null) {
                    m_logger.info("Null writer found for: " + entry.getKey().toString());
                    continue;
                }
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    m_logger.error("Failed to flush or close file, export file may be unavailable/unwritable, or not enough space.", e);
                } finally {
                    if (writer.checkError()) {
                        m_logger.error("Failed to flush or close file, export file may be unavailable/unwritable, or not enough space.");
                    }
                }
            }

            if (m_batched) {
                closeBatch();
            }
            else {
                closeFiles();
            }

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
                if (!oldDir.renameTo(newDir)) {
                    m_logger.error("Failed to rename export directory from " + oldPath + " to " + newPath);
                }
            }
            else {
                if (!oldDir.delete()) {
                    m_logger.warn(String.format("Failed to delete export directory %s", oldPath));
                }
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
                    return Long.compare(f1.creationTime, f2.creationTime);
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
                if (!oldFile.renameTo(newFile)) {
                    m_logger.error("Failed to rename export file from " + oldPath + " to " + newPath);
                }

                notifySet[i] = newFile;
                i++;
            }
        }

        CSVWriter getWriter(String tableName, long generation) throws IOException {
            FileHandle handle = new FileHandle(tableName, generation);
            CSVWriter writer = m_writers.get(handle);
            if (writer != null)
                return writer;

            String path = handle.getPath(ACTIVE_PREFIX);
            File newFile = new VoltFile(path);
            if (newFile.exists()) {
                m_logger.error("Error: Output file for next period already exists at path: " + newFile.getPath()
                        + " Consider using a more specific timestamp in your filename or cleaning up your export data directory."
                        + " ExportToFileClient will stop to prevent data loss.");
                throw new RuntimeException();
            }
            try {
                OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(newFile, false), StandardCharsets.UTF_8);
                if (m_fullDelimiters != null) {
                    writer = new CSVWriter(new BufferedWriter(osw, 4096 * 4),
                            m_fullDelimiters[0], m_fullDelimiters[1], m_fullDelimiters[2], String.valueOf(m_fullDelimiters[3]));
                }
                else if (m_delimiter == ',') {
                    // CSV
                    writer = new CSVWriter(new BufferedWriter(osw, 4096 * 4), m_delimiter);
                }
                else {
                    // TSV
                    writer = CSVWriter.getStrictTSVWriter(new BufferedWriter(osw, 4096 * 4));
                }
            }
            catch (Exception e) {
                if (e instanceof IOException) {
                    rateLimitedLogError(m_logger, "Failed to create output file: " + path + " , file may be unavailable/unwritable, or not enough space.");
                    throw e;
                } else {
                    m_logger.error("Error: Failed to create output file: " + path + " " + Throwables.getStackTraceAsString(e));
                    throw new RuntimeException();
                }

            }
            m_writers.put(handle, writer);
            return writer;
        }

        void writeSchema(String tableName, long generation, String schema) throws IOException {
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
                    OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(newFile, false), StandardCharsets.UTF_8);
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
                }
                catch (Exception e) {
                    if (e instanceof IOException) {
                        rateLimitedLogError(m_logger, "Failed to write schema file: " + path + " , file may be unavailable/unwritable, or not enough space.");
                        throw e;
                    } else {
                        m_logger.error("Error: Failed to create output file: " + path + " " + Throwables.getStackTraceAsString(e));
                        throw new RuntimeException();
                    }
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
        DecoderMetaData m_metaData;
        protected String m_schemaString = "ERROR SERIALIZING SCHEMA";
        private FutureTask<CSVWriter> m_firstBlockTask;
        private CSVWriter m_writer;
        private final CSVWriterDecoder m_csvWriterDecoder;
        private ListeningExecutorService m_es;

        public ExportToFileDecoder(AdvertisedDataSource source) {
            super(source);

            // file export exports data to a file per table (per generation in the periodic window)
            // irrespective of number of partitions. Decoder instances gets spawned for number of
            // tables and partitions as table information is not available at time of creation. To
            // avoid synchronization and contention on the file resource, the creation of executor
            // is delayed till block start notification when the table information is available
            // and instead use the current thread available in base export client as the executor
            m_es = null;
            m_metaData = new DecoderMetaData("", Long.MIN_VALUE, Integer.MIN_VALUE);

            CSVWriterDecoder.Builder builder = new CSVWriterDecoder.Builder();
            builder
                .dateFormatter(Constants.ODBC_DATE_FORMAT_STRING)
                .timeZone(m_timeZone.getID())
                .binaryEncoding(m_binaryEncoding)
                .skipInternalFields(m_skipinternal)
                ;
            m_csvWriterDecoder = builder.build();
        }

        private void resetWriter() {
            m_firstBlockTask = new FutureTask<>(new Callable<CSVWriter>() {
                @Override
                public CSVWriter call() throws Exception {
                    assert !m_metaData.tableName.isEmpty() : "Table not initialized";
                    CSVWriter writer = m_current.getWriter(m_metaData.tableName, m_metaData.generation);
                    m_current.writeSchema(m_metaData.tableName, m_metaData.generation, m_schemaString);
                    return writer;
                }
            });;
        }

        @Override
        public ListeningExecutorService getExecutor() {
            if (m_es == null) {
                return super.getExecutor();
            }
            return m_es;
        }

        /**
         * Given the data source, construct a JSON serialization
         * of its schema to be written to disk with the export
         * data.
         */
        void setSchemaForSource(final List<String> columnNames, final List<VoltType> columnTypes) {
            try {
                JSONStringer json = new JSONStringer();
                json.object();
                json.key("table name").value(m_metaData.tableName);
                json.key("generation id").value(m_metaData.generation);
                json.key("columns").array();

                for (int i = 0; i < columnNames.size(); i++) {
                    json.object();
                    json.key("name").value(columnNames.get(i));
                    json.key("type").value(columnTypes.get(i).name());
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

        /**
         * this routine needs to be called in synchronized way through writer lock
         * @param rd
         */
        void registerSelf(ExportRow rd) {
                ExportToFileDecoder decoder = m_tableDecoders.get(m_metaData);
                if (decoder == null) {
                    m_tableDecoders.put(m_metaData, this);
                }
        }

        void setSchemaSourceNWriter(final ExportRow row) throws RestartBlockException {
            if (m_metaData.generation != row.generation) {
                // generation change, update the meta-data and writers
                m_metaData = new DecoderMetaData(row.tableName, row.generation, row.partitionId);
                setSchemaForSource(row.names, row.types);
                resetWriter();
                m_batchLock.writeLock().lock();
                try {
                    // TODO: if same export client is getting used, unregisterSelf(not implemented) during generation change
                    registerSelf(row);
                    if (m_es == null) {
                        ListeningExecutorService executor = m_decoderExecutor.get(row.tableName);
                        if (executor == null) {
                            executor = CoreUtils.getListeningSingleThreadExecutor(
                                "File Export decoder for table " + row.tableName + CoreUtils.MEDIUM_STACK_SIZE
                                );
                            m_decoderExecutor.put(row.tableName, executor);
                        }
                        m_es = executor;

                        // force fetch the writer ahead for fresh start to avoid multiple threads race for same file creation
                        m_firstBlockTask.run();
                        m_writer = m_firstBlockTask.get();
                    }
                } catch (Exception e) {
                    // if writeSchema or getWriter method fails, it will throw IOException
                    // try reset writer and restart the block
                    if (e.getCause() instanceof IOException) {
                        resetWriter();
                        throw new RestartBlockException("Fail to start the block", e.getCause(),true);
                    } else {
                        Throwables.propagate(e);
                    }
                } finally {
                    m_batchLock.writeLock().unlock();
                }
            }
        }

        @Override
        public void onBlockStart(ExportRow row) throws RestartBlockException {
            setSchemaSourceNWriter(row);
            m_batchLock.readLock().lock();

            try {
                m_firstBlockTask.run();
                m_writer = m_firstBlockTask.get();
            }
            catch (Exception e) {
                // if writeSchema or getWriter method fails, it will throw IOException
                // try reset writer and restart the block
                if (e.getCause() instanceof IOException) {
                    m_logger.error("Failed to get writer, export file may be unavailable/unwritable, or not enough space.", e);

                    resetWriter();
                    //This means we will not reach onBlockCompletion and thus not release and roll will block
                    //not creating new files and export may appear not working.
                    m_batchLock.readLock().unlock();
                    throw new RestartBlockException("Fail to start the block", e.getCause(),true);
                }
                else {
                    Throwables.propagate(e);
                }
            }
        }


        @Override
        public boolean processRow(ExportRow rd) throws RestartBlockException {
            // reader lock is acquired in on-block start
            try {
                m_csvWriterDecoder.decode(rd.generation, rd.tableName, rd.types, rd.names, m_writer,rd.values);
            }
            catch (IOException io) {
                rateLimitedLogError(m_logger, "failed to to process export row %s", Throwables.getStackTraceAsString(io));
                return false;
            }
            return true;
        }

        /**
         * Release the current batch folder.
         * @throws RestartBlockException
         */
        @Override
        public void onBlockCompletion(ExportRow row) throws RestartBlockException {
            try {
                m_writer.flush();
            }
            catch (Throwable t) {
                Throwables.propagate(t);
            }
            finally {
                m_batchLock.readLock().unlock();
                if (m_writer.checkError()) {
                    rateLimitedLogError(m_logger, "Failed to flush, export file may be unavailable/unwritable, or not enough space.");
                    m_writer.resetWriter();
                    throw new RestartBlockException("Failed to complete the block.", true);
                }
            }
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            m_batchLock.writeLock().lock();
            try {
                ExportToFileDecoder decoder = m_tableDecoders.get(m_metaData);
                if (decoder != null) {
                    m_tableDecoders.remove(m_metaData);
                }
            }
            finally {
                m_batchLock.writeLock().unlock();
            }

            if (m_es != null) {
                m_es.shutdown();
                try {
                    m_es.awaitTermination(365, TimeUnit.DAYS);
                    m_es = null;
                }
                catch (InterruptedException e) {
                    Throwables.propagate(e);
                }
            }
        }
    }

    @Override
    public ExportToFileDecoder constructExportDecoder(AdvertisedDataSource source) {
        return new ExportToFileDecoder(source);
    }

    @Override
    public void shutdown() {
        m_scheduledFileRotatorService.shutdown();
        try {
            m_scheduledFileRotatorService.awaitTermination(365, TimeUnit.DAYS);
        }
        catch( InterruptedException iex) {
            Throwables.propagate(iex);
        }
        m_batchLock.writeLock().lock();
        try {
            m_current.closeAllWriters();
        }
        finally {
            m_batchLock.writeLock().unlock();
        }
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

            for( ExportToFileDecoder decoder : m_tableDecoders.values()) {
                decoder.resetWriter();
            }

        }
        finally {
            m_batchLock.writeLock().unlock();
        }
        previous.closeAllWriters();
    }

    public String getVoltDBRootPath() {
        return TEST_VOLTDB_ROOT != null ? TEST_VOLTDB_ROOT : VoltDB.instance().getVoltDBRootPath();
    }

    @Override
    public void configure( Properties conf) throws Exception {
        String nonce = conf.getProperty("nonce");

        if (nonce == null) {
            throw new IllegalArgumentException("ExportToFile: must provide a filename nonce");
        }
        char delimiter = '\0';
        // Default to CSV if missing
        String type = conf.getProperty("type", "csv").trim();
        if (type.equalsIgnoreCase("csv")) {
            delimiter = ',';
        }
        else if (type.equalsIgnoreCase("tsv")) {
            delimiter = '\t';
        }
        else {
            throw new IllegalArgumentException("Error: --type must be one of CSV or TSV");
        }
        if (delimiter == '\0') {
            throw new IllegalArgumentException("ExportToFile: must provide an output type");
        }
        String dir = conf.getProperty("outdir");
        if (dir == null || dir.length() == 0) {
            //This is so that tests dont end up calling getVoltDBRootPath which gives NPE when no cluster.
            dir = getVoltDBRootPath() + File.separator + "file_export";
        }
        File outdir = new VoltFile(dir);
        if (!outdir.isAbsolute()) {
            outdir = new File(getVoltDBRootPath(), outdir.getPath());
        }

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

        boolean skipinternal = Boolean.parseBoolean(conf.getProperty("skipinternals", "false"));

        int period = Integer.parseInt(conf.getProperty("period", "60"));
        if (period < 1) {
            throw new IllegalArgumentException("Error: Specified value for --period must be >= 1.");
        }

        String dateformatString = conf.getProperty("dateformat", "yyyyMMddHHmmss").trim();
        boolean batched = Boolean.parseBoolean(conf.getProperty("batched", "false"));
        boolean withSchema = Boolean.parseBoolean(conf.getProperty("with-schema", "false"));

        String fullDelimiters = conf.getProperty("delimiters");
        if (fullDelimiters != null) {
            fullDelimiters = StringEscapeUtils.unescapeJava(fullDelimiters);
            if (fullDelimiters.length() != EXPORT_DELIM_NUM_CHARACTERS) {
                throw new IllegalArgumentException(
                        String.format("The delimiter set must contain exactly %d characters (after any string escaping).",
                                      EXPORT_DELIM_NUM_CHARACTERS));
            }
        }
        TimeZone tz = TimeZone.getTimeZone(conf.getProperty("timezone", VoltDB.GMT_TIMEZONE.getID()));

        BinaryEncoding encoding = BinaryEncoding.valueOf(
                conf.getProperty("binaryencoding", "HEX").trim().toUpperCase());
        boolean uniquenames = Boolean.parseBoolean(conf.getProperty("uniquenames"));

        //Dont do actual config in check mode.
        boolean configcheck = Boolean.parseBoolean(conf.getProperty(ExportManager.CONFIG_CHECK_ONLY, "false"));
        if (configcheck) {
            return;
        }
        setRunEverywhere(Boolean.parseBoolean(conf.getProperty("replicated", "false")));
        configureInternal(
                delimiter,
                nonce,
                outdir,
                period,
                dateformatString,
                fullDelimiters,
                skipinternal,
                batched,
                withSchema,
                tz,
                encoding,
                uniquenames);
    }

    private void configureInternal(
                              final char delimiter,
                              final String nonce,
                              final File outdir,
                              final int period,
                              final String dateformatString,
                              String fullDelimiters,
                              final boolean skipinternal,
                              final boolean batched,
                              final boolean withSchema,
                              final TimeZone tz,
                              final BinaryEncoding be,
                              final boolean uniquenames) {
        m_delimiter = delimiter;
        m_extension = (delimiter == ',') ? ".csv" : ".tsv";
        m_nonce = nonce;
        m_outDir = outdir;
        m_tableDecoders = new HashMap<>();
        m_decoderExecutor = new HashMap<>();
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
        m_timeZone = tz;
        m_binaryEncoding = be;
        m_skipinternal = skipinternal;
        m_batched = batched;
        m_withSchema = withSchema;
        m_uniquenames = uniquenames;

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
                    m_logger.warn("Failed to roll file periodically.", t);
                }
            }
        };
        m_scheduledFileRotatorService =
                CoreUtils.getScheduledThreadPoolExecutor(
                        "Export file rotate timer for nonce " + nonce, 1, CoreUtils.SMALL_STACK_SIZE);
        m_scheduledFileRotatorService.scheduleWithFixedDelay(rotator, m_period, m_period, TIME_PERIOD_UNIT);
    }
}
