/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.sysprocs.saverestore;

import java.io.BufferedInputStream;
import java.io.CharArrayWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.InstanceId;
import org.voltcore.utils.Pair;
import org.voltdb.ClientInterface;
import org.voltdb.ClientResponseImpl;
import org.voltdb.SimpleClientResponseAdapter;
import org.voltdb.SnapshotCompletionInterest;
import org.voltdb.SnapshotDaemon;
import org.voltdb.SnapshotDaemon.ForwardClientException;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotInitiationInfo;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TheHashinator;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.common.Constants;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

public class SnapshotUtil {

    public final static String HASH_EXTENSION = ".hash";
    public final static String COMPLETION_EXTENSION = ".finished";

    public static final String JSON_PATH = "path";
    public static final String JSON_NONCE = "nonce";
    public static final String JSON_DUPLICATES_PATH = "duplicatesPath";
    public static final String JSON_HASHINATOR = "hashinator";

    public static final ColumnInfo nodeResultsColumns[] =
    new ColumnInfo[] {
        new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID),
        new ColumnInfo("HOSTNAME", VoltType.STRING),
        new ColumnInfo("TABLE", VoltType.STRING),
        new ColumnInfo("RESULT", VoltType.STRING),
        new ColumnInfo("ERR_MSG", VoltType.STRING)
    };

    public static final ColumnInfo partitionResultsColumns[] =
    new ColumnInfo[] {
        new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID),
        new ColumnInfo("HOSTNAME", VoltType.STRING),
        new ColumnInfo(VoltSystemProcedure.CNAME_SITE_ID, VoltSystemProcedure.CTYPE_ID),
        new ColumnInfo("RESULT", VoltType.STRING),
        new ColumnInfo("ERR_MSG", VoltType.STRING)
    };

    public static final VoltTable constructNodeResultsTable()
    {
        return new VoltTable(nodeResultsColumns);
    }

    public static final VoltTable constructPartitionResultsTable()
    {
        return new VoltTable(partitionResultsColumns);
    }

    /**
     * Create a digest for a snapshot
     * @param txnId   transaction ID when snapshot was initiated
     * @param path    path to which snapshot files will be written
     * @param nonce   nonce used to distinguish this snapshot
     * @param tables   List of tables present in this snapshot
     * @param hostId   Host ID where this is happening
     * @param exportSequenceNumbers  ???
     * @throws IOException
     */
    public static Runnable writeSnapshotDigest(
        long txnId,
        long catalogCRC,
        String path,
        String nonce,
        List<Table> tables,
        int hostId,
        Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
        Map<Integer, Long> partitionTransactionIds,
        InstanceId instanceId,
        long timestamp,
        int newPartitionCount)
    throws IOException
    {
        final File f = new VoltFile(path, constructDigestFilenameForNonce(nonce, hostId));
        if (f.exists()) {
            if (!f.delete()) {
                throw new IOException("Unable to write table list file " + f);
            }
        }
        boolean success = false;
        try {
            final FileOutputStream fos = new FileOutputStream(f);
            StringWriter sw = new StringWriter();
            JSONStringer stringer = new JSONStringer();
            try {
                stringer.object();
                stringer.key("version").value(1);
                stringer.key("txnId").value(txnId);
                stringer.key("timestamp").value(timestamp);
                stringer.key("timestampString").value(SnapshotUtil.formatHumanReadableDate(timestamp));
                stringer.key("newPartitionCount").value(newPartitionCount);
                stringer.key("tables").array();
                for (int ii = 0; ii < tables.size(); ii++) {
                    stringer.value(tables.get(ii).getTypeName());
                }
                stringer.endArray();
                stringer.key("exportSequenceNumbers").array();
                for (Map.Entry<String, Map<Integer, Pair<Long, Long>>> entry : exportSequenceNumbers.entrySet()) {
                    stringer.object();

                    stringer.key("exportTableName").value(entry.getKey());

                    stringer.key("sequenceNumberPerPartition").array();
                    for (Map.Entry<Integer, Pair<Long,Long>> sequenceNumber : entry.getValue().entrySet()) {
                        stringer.object();
                        stringer.key("partition").value(sequenceNumber.getKey());
                        //First value is the ack offset which matters for pauseless rejoin, but not persistence
                        stringer.key("exportSequenceNumber").value(sequenceNumber.getValue().getSecond());
                        stringer.endObject();
                    }
                    stringer.endArray();

                    stringer.endObject();
                }
                stringer.endArray();

                stringer.key("partitionTransactionIds").object();
                for (Map.Entry<Integer, Long> entry : partitionTransactionIds.entrySet()) {
                    stringer.key(entry.getKey().toString()).value(entry.getValue());
                }
                stringer.endObject();

                stringer.key("catalogCRC").value(catalogCRC);
                stringer.key("instanceId").value(instanceId.serializeToJSONObject());
                stringer.endObject();
            } catch (JSONException e) {
                throw new IOException(e);
            }

            sw.append(stringer.toString());

            final byte tableListBytes[] = sw.getBuffer().toString().getBytes("UTF-8");
            final PureJavaCrc32 crc = new PureJavaCrc32();
            crc.update(tableListBytes);
            ByteBuffer fileBuffer = ByteBuffer.allocate(tableListBytes.length + 4);
            fileBuffer.putInt((int)crc.getValue());
            fileBuffer.put(tableListBytes);
            fileBuffer.flip();
            fos.getChannel().write(fileBuffer);
            success = true;
            return new Runnable() {
                @Override
                public void run() {
                    try {
                        fos.getChannel().force(true);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            fos.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            };
        } finally {
            if (!success) {
                f.delete();
            }
        }
    }

    /**
     * Write the hashinator config file for a snapshot
     * @param instId    instance ID
     * @param path      path to which snapshot files will be written
     * @param nonce     nonce used to distinguish this snapshot
     * @param hostId    host ID where this is happening
     * @param hashData  serialized hash configuration data
     * @return  Runnable object for asynchronous write flushing
     * @throws IOException
     */
    public static Runnable writeHashinatorConfig(
        InstanceId instId,
        String path,
        String nonce,
        int hostId,
        HashinatorSnapshotData hashData)
    throws IOException
    {
        final File file = new VoltFile(path, constructHashinatorConfigFilenameForNonce(nonce, hostId));
        if (file.exists()) {
            if (!file.delete()) {
                throw new IOException("Unable to replace existing hashinator config " + file);
            }
        }

        boolean success = false;
        try {
            final FileOutputStream fos = new FileOutputStream(file);
            ByteBuffer fileBuffer = hashData.saveToBuffer(instId);
            fos.getChannel().write(fileBuffer);
            success = true;
            return new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        fos.getChannel().force(true);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    finally {
                        try {
                            fos.close();
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            };
        }
        finally {
            if (!success) {
                file.delete();
            }
        }
    }

    /**
     * Get the nonce from the filename of the digest file.
     * @param filename The filename of the digest file
     * @return The nonce
     */
    public static String parseNonceFromDigestFilename(String filename) {
        if (filename == null || !filename.endsWith(".digest")) {
            throw new IllegalArgumentException("Bad digest filename: " + filename);
        }

        return parseNonceFromSnapshotFilename(filename);
    }

    /**
     * Get the nonce from the filename of the hashinator config file.
     * @param filename The filename of the hashinator config file
     * @return The nonce
     */
    public static String parseNonceFromHashinatorConfigFilename(String filename) {
        if (filename == null || !filename.endsWith(HASH_EXTENSION)) {
            throw new IllegalArgumentException("Bad hashinator config filename: " + filename);
        }

        return parseNonceFromSnapshotFilename(filename);
    }

    /**
     * Get the nonce from any snapshot-related file.
     */
    public static String parseNonceFromSnapshotFilename(String filename)
    {
        if (filename == null) {
            throw new IllegalArgumentException("Bad snapshot filename: " + filename);
        }

        // For the snapshot catalog
        if (filename.endsWith(".jar")) {
            return filename.substring(0, filename.indexOf(".jar"));
        }
        // for everything else valid in new format or volt1.2 or earlier table files
        else if (filename.indexOf("-") > 0) {
           return filename.substring(0, filename.indexOf("-"));
        }
        // volt 1.2 and earlier digest filename
        else if (filename.endsWith(".digest")) {
            return filename.substring(0, filename.indexOf(".digest"));
        }
        // Hashinator config filename.
        else if (filename.endsWith(HASH_EXTENSION)) {
            return filename.substring(0, filename.indexOf(HASH_EXTENSION));
        }

        throw new IllegalArgumentException("Bad snapshot filename: " + filename);
    }

    public static List<JSONObject> retrieveDigests(String path,
            String nonce, VoltLogger logger) throws Exception {
        VoltFile directoryWithDigest = new VoltFile(path);
        ArrayList<JSONObject> digests = new ArrayList<JSONObject>();
        if (directoryWithDigest.listFiles() == null) {
            return digests;
        }
        for (File f : directoryWithDigest.listFiles()) {
            if ( f.getName().equals(nonce + ".digest") || //old style digest name
                    (f.getName().startsWith(nonce + "-host_") && f.getName().endsWith(".digest"))) {//new style
                JSONObject retval = CRCCheck(f, logger);
                if (retval != null) {
                    digests.add(retval);
                }
            }
        }
        return digests;
    }

    /**
     * Read hashinator snapshots into byte buffers.
     * @param path base snapshot path
     * @param nonce unique snapshot name
     * @param maxConfigs max number of good configs to return (0 for all)
     * @param logger log writer
     * @return byte buffers for each host
     * @throws IOException
     */
    public static List<ByteBuffer> retrieveHashinatorConfigs(
        String path,
        String nonce,
        int maxConfigs,
        VoltLogger logger) throws IOException
   {
        VoltFile directory = new VoltFile(path);
        ArrayList<ByteBuffer> configs = new ArrayList<ByteBuffer>();
        if (directory.listFiles() == null) {
            return configs;
        }
        for (File file : directory.listFiles()) {
            if (file.getName().startsWith(nonce + "-host_") && file.getName().endsWith(HASH_EXTENSION)) {
                byte[] rawData = new byte[(int) file.length()];
                FileInputStream fis = null;
                DataInputStream dis = null;
                try {
                    fis = new FileInputStream(file);
                    dis = new DataInputStream(fis);
                    dis.readFully(rawData);
                    configs.add(ByteBuffer.wrap(rawData));
                }
                finally {
                    if (dis != null) {
                        dis.close();
                    }
                    if (fis != null) {
                        fis.close();
                    }
                }
            }
        }
        return configs;
    }

    /**
     * Write the current catalog associated with the database snapshot
     * to the snapshot location
     */
    public static Runnable writeSnapshotCatalog(String path, String nonce)
    throws IOException
    {
        String filename = SnapshotUtil.constructCatalogFilenameForNonce(nonce);
        try
        {
            return VoltDB.instance().getCatalogContext().writeCatalogJarToFile(path, filename);
        }
        catch (IOException ioe)
        {
            throw new IOException("Unable to write snapshot catalog to file: " +
                                  path + File.separator + filename, ioe);
        }
    }

    /**
     * Write the .complete file for finished snapshot
     */
    public static Runnable writeSnapshotCompletion(String path, String nonce, int hostId, final VoltLogger logger) throws IOException {

        final File f = new VoltFile(path, constructCompletionFilenameForNonce(nonce, hostId));
        if (f.exists()) {
            if (!f.delete()) {
                throw new IOException("Failed to replace existing " + f.getName());
            }
        }
        return new Runnable() {
            @Override
            public void run() {
                try {
                    f.createNewFile();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create .complete file for " + f.getName(), e);
                }
            }
        };
    }

    /**
     *
     * This isn't just a CRC check. It also loads the file and returns it as
     * a JSON object.
     * Check if the CRC of the snapshot digest. Note that this only checks if
     * the CRC at the beginning of the digest file matches the CRC of the digest
     * file itself.
     *
     * @param f
     *            The snapshot digest file object
     * @return The table list as a string
     * @throws IOException
     *             If CRC does not match
     */
    public static JSONObject CRCCheck(File f, VoltLogger logger) throws IOException {
        final FileInputStream fis = new FileInputStream(f);
        try {
            final BufferedInputStream bis = new BufferedInputStream(fis);
            ByteBuffer crcBuffer = ByteBuffer.allocate(4);
            if (4 != bis.read(crcBuffer.array())) {
                logger.warn(
                        "EOF while attempting to read CRC from snapshot digest " + f +
                        " on host " + CoreUtils.getHostnameOrAddress());
                return null;
            }
            final int crc = crcBuffer.getInt();
            final InputStreamReader isr = new InputStreamReader(bis, "UTF-8");
            CharArrayWriter caw = new CharArrayWriter();
            while (true) {
                int nextChar = isr.read();
                if (nextChar == -1) {
                    break;
                }
                //The original format had a single newline terminate the file.
                //Still here for backwards compatiblity with previously generated
                //digests
                if (nextChar == '\n') {
                    break;
                }
                caw.write(nextChar);
            }

            /*
             * Try and parse the contents as a JSON object. If it succeeds then assume
             * it is a the new version of the digest file. It is unlikely the old version
             * will successfully parse as JSON because it starts with a number
             * instead of an open brace.
             */
            JSONObject obj = null;
            try {
                obj = new JSONObject(caw.toString());
            } catch (JSONException e) {
                //assume it is the old format
            }

            /*
             * Convert the old style file to a JSONObject so it can be presented
             * via a consistent interface.
             */
            if (obj == null) {
                String tableList = caw.toString();
                byte tableListBytes[] = tableList.getBytes("UTF-8");
                PureJavaCrc32 tableListCRC = new PureJavaCrc32();
                tableListCRC.update(tableListBytes);
                tableListCRC.update("\n".getBytes("UTF-8"));
                final int calculatedValue = (int)tableListCRC.getValue();
                if (crc != calculatedValue) {
                    logger.warn("CRC of snapshot digest " + f + " did not match digest contents");
                    return null;
                }

                String tableNames[] = tableList.split(",");
                long txnId = Long.valueOf(tableNames[0]);

                obj = new JSONObject();
                try {
                    obj.put("version", 0);
                    obj.put("txnId", txnId);
                    for (int ii = 1; ii < tableNames.length; ii++) {
                        obj.append("tables", tableNames[ii]);
                    }
                } catch (JSONException e) {
                    logger.warn("Exception parsing JSON of digest " + f, e);
                    return null;
                }
                return obj;
            } else {
                /*
                 * Verify the CRC and then return the data as a JSON object.
                 */
                String tableList = caw.toString();
                byte tableListBytes[] = tableList.getBytes("UTF-8");
                PureJavaCrc32 tableListCRC = new PureJavaCrc32();
                tableListCRC.update(tableListBytes);
                final int calculatedValue = (int)tableListCRC.getValue();
                if (crc != calculatedValue) {
                    logger.warn("CRC of snapshot digest " + f + " did not match digest contents");
                    return null;
                }
                return obj;
            }
        } catch (Exception e) {
            logger.warn("Exception while parsing snapshot digest " + f, e);
            return null;
        } finally {
            try {
                if (fis != null)
                    fis.close();
            } catch (IOException e) {}
        }
    }

    /**
     * Storage for information about files that are part of a specific snapshot
     */
    public static class Snapshot {
        public Snapshot(String nonce)
        {
            m_nonce = nonce;
            m_txnId = Long.MIN_VALUE;
        }

        public void setInstanceId(InstanceId id)
        {
            if (m_instanceId == null) {
                m_instanceId = id;
            }
            else if (!m_instanceId.equals(id)) {
                throw new RuntimeException("Snapshot named " + m_nonce +
                        " has digests with conflicting cluster instance IDs." +
                        " Please ensure that there is only one snapshot named " + m_nonce + " in your" +
                        " cluster nodes' VOLTDBROOT directories and try again.");
            }
        }

        public InstanceId getInstanceId()
        {
            return m_instanceId;
        }

        public void setTxnId(long txnId)
        {
            if (m_txnId != Long.MIN_VALUE) {
                assert(txnId == m_txnId);
            }
            m_txnId = txnId;
        }

        public long getTxnId()
        {
            return m_txnId;
        }

        public String getNonce()
        {
            return m_nonce;
        }

        public final List<File> m_digests = new ArrayList<File>();
        public File m_hashConfig = null;
        public final List<Set<String>> m_digestTables = new ArrayList<Set<String>>();
        public final Map<String, TableFiles> m_tableFiles = new TreeMap<String, TableFiles>();
        public File m_catalogFile = null;

        private final String m_nonce;
        private InstanceId m_instanceId = null;
        private long m_txnId;
    }

    /**
     * Description of all the files for a specific table that is part of a specific snapshot
     */
    public static class TableFiles {
        public final boolean m_isReplicated;
        TableFiles(boolean isReplicated) {
            m_isReplicated = isReplicated;
        }

        public final List<File> m_files = new ArrayList<File>();
        public final List<Boolean> m_completed = new ArrayList<Boolean>();
        public final List<Set<Integer>> m_validPartitionIds = new ArrayList<Set<Integer>>();
        public final List<Set<Integer>> m_corruptParititionIds = new ArrayList<Set<Integer>>();
        public final List<Integer> m_totalPartitionCounts = new ArrayList<Integer>();
    }

    /**
     * Simple filter that includes directories and files that end in .digest or .vpt
     */
    public static class SnapshotFilter implements FileFilter {
        @Override
        public boolean accept(File pathname) {
            if (pathname.isDirectory()) {
                return true;
            }
            if (pathname.getName().endsWith(".digest") || pathname.getName().endsWith(".vpt")) {
                return true;
            }
            if (pathname.getName().endsWith(".jar")) {
                return true;
            }
            if (pathname.getName().endsWith(HASH_EXTENSION)) {
                return true;
            }
            return false;
        }
    };

    /**
     * Filter that looks for files related to a specific snapshot.
     */
    public static class SpecificSnapshotFilter extends SnapshotFilter {
        private final Set<String> snapshotNames;

        public SpecificSnapshotFilter(Set<String> snapshotNames) {
            this.snapshotNames = snapshotNames;
        }

        @Override
        public boolean accept(File pathname) {
            if (!super.accept(pathname)) {
                return false;
            }

            if (pathname.isDirectory()) {
                return true;
            }

            for (String snapshotName : snapshotNames) {
                // izzy: change this to use parseNonceFromSnapshotFilename at some point
                if (pathname.getName().startsWith(snapshotName + "-")  ||
                    pathname.getName().equals(snapshotName + ".digest") ||
                    pathname.getName().equals(snapshotName + HASH_EXTENSION) ||
                    pathname.getName().equals(snapshotName + ".jar")) {
                    return true;
                }
            }

            return false;
        }
    }

    /**
     * Convenience class to manage the named snapshot map for retrieveSnapshotFiles().
     */
    private static class NamedSnapshots {

        private final Map<String, Snapshot> m_map;

        public NamedSnapshots(Map<String, Snapshot> map) {
            m_map = map;
        }

        public Snapshot get(String nonce) {
            Snapshot named_s = m_map.get(nonce);
            if (named_s == null) {
                named_s = new Snapshot(nonce);
                m_map.put(nonce, named_s);
            }
            return named_s;
        }
    }

    /**
     * Spider the provided directory applying the provided FileFilter. Optionally validate snapshot
     * files. Return a summary of partition counts, partition information, files, digests etc.
     * that can be used to determine if a valid restore plan exists.
     * @param directory
     * @param snapshots
     * @param filter
     * @param validate
     */
    public static void retrieveSnapshotFiles(
            File directory,
            Map<String, Snapshot> namedSnapshotMap,
            FileFilter filter,
            boolean validate,
            VoltLogger logger) {

        NamedSnapshots namedSnapshots = new NamedSnapshots(namedSnapshotMap);
        retrieveSnapshotFilesInternal(directory, namedSnapshots, filter, validate, logger, 0);
    }

    private static void retrieveSnapshotFilesInternal(
            File directory,
            NamedSnapshots namedSnapshots,
            FileFilter filter,
            boolean validate,
            VoltLogger logger,
            int recursion) {

        if (recursion == 32) {
            return;
        }

        if (!directory.exists()) {
            System.err.println("Error: Directory " + directory.getPath() + " doesn't exist");
            return;
        }
        if (!directory.canRead()) {
            System.err.println("Error: Directory " + directory.getPath() + " is not readable");
            return;
        }
        if (!directory.canExecute()) {
            System.err.println("Error: Directory " + directory.getPath() + " is not executable");
            return;
        }

        for (File f : directory.listFiles(filter)) {
            if (f.isDirectory()) {
                if (!f.canRead() || !f.canExecute()) {
                    System.err.println("Warning: Skipping directory " + f.getPath()
                            + " due to lack of read permission");
                } else {
                    retrieveSnapshotFilesInternal(f, namedSnapshots, filter, validate, logger, recursion++);
                }
                continue;
            }
            if (!f.canRead()) {
                System.err.println("Warning: " + f.getPath() + " is not readable");
                continue;
            }

            FileInputStream fis = null;
            try {
                fis = new FileInputStream(f);
            } catch (FileNotFoundException e1) {
                System.err.println(e1.getMessage());
                continue;
            }

            try {
                if (f.getName().endsWith(".digest")) {
                    JSONObject digest = CRCCheck(f, logger);
                    if (digest == null) continue;
                    Long snapshotTxnId = digest.getLong("txnId");
                    String nonce = parseNonceFromSnapshotFilename(f.getName());
                    Snapshot named_s = namedSnapshots.get(nonce);
                    named_s.setTxnId(snapshotTxnId);
                    InstanceId iid = new InstanceId(0,0);
                    if (digest.has("instanceId")) {
                        iid = new InstanceId(digest.getJSONObject("instanceId"));
                    }
                    named_s.setInstanceId(iid);
                    TreeSet<String> tableSet = new TreeSet<String>();
                    JSONArray tables = digest.getJSONArray("tables");
                    for (int ii = 0; ii < tables.length(); ii++) {
                        tableSet.add(tables.getString(ii));
                    }
                    named_s.m_digestTables.add(tableSet);
                    named_s.m_digests.add(f);
                } else if (f.getName().endsWith(".jar")) {
                    String nonce = parseNonceFromSnapshotFilename(f.getName());
                    Snapshot named_s = namedSnapshots.get(nonce);
                    named_s.m_catalogFile = f;
                } else if (f.getName().endsWith(HASH_EXTENSION)) {
                    String nonce = parseNonceFromSnapshotFilename(f.getName());
                    Snapshot named_s = namedSnapshots.get(nonce);
                    if (validate) {
                        try {
                            // Retrieve hashinator config data for validation only.
                            // Throws IOException when the CRC check fails.
                            HashinatorSnapshotData hashData = new HashinatorSnapshotData();
                            hashData.restoreFromFile(f);
                            named_s.m_hashConfig = f;
                        }
                        catch (IOException e) {
                            logger.warn(String.format("Skipping bad hashinator snapshot file '%s'",
                                                      f.getPath()));
                            // Skip bad hashinator files.
                            continue;
                        }
                    }
                } else {
                    HashSet<Integer> partitionIds = new HashSet<Integer>();
                    TableSaveFile saveFile = new TableSaveFile(fis, 1, null, true);
                    try {
                        for (Integer partitionId : saveFile.getPartitionIds()) {
                            partitionIds.add(partitionId);
                        }
                        if (validate && saveFile.getCompleted()) {
                            while (saveFile.hasMoreChunks()) {
                                BBContainer cont = saveFile.getNextChunk();
                                if (cont != null) {
                                    cont.discard();
                                }
                            }
                        }
                        partitionIds.removeAll(saveFile.getCorruptedPartitionIds());
                        String nonce = parseNonceFromSnapshotFilename(f.getName());
                        Snapshot named_s = namedSnapshots.get(nonce);
                        named_s.setTxnId(saveFile.getTxnId());
                        TableFiles namedTableFiles = named_s.m_tableFiles.get(saveFile.getTableName());
                        if (namedTableFiles == null) {
                            namedTableFiles = new TableFiles(saveFile.isReplicated());
                            named_s.m_tableFiles.put(saveFile.getTableName(), namedTableFiles);
                        }
                        namedTableFiles.m_files.add(f);
                        namedTableFiles.m_completed.add(saveFile.getCompleted());
                        namedTableFiles.m_validPartitionIds.add(partitionIds);
                        namedTableFiles.m_corruptParititionIds.add(saveFile.getCorruptedPartitionIds());
                        namedTableFiles.m_totalPartitionCounts.add(saveFile.getTotalPartitions());
                    } finally {
                        saveFile.close();
                    }
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
                System.err.println("Error: Unable to process " + f.getPath());
            } catch (JSONException e) {
                System.err.println(e.getMessage());
                System.err.println("Error: Unable to process " + f.getPath());
            }
            finally {
                try {
                    if (fis != null) {
                        fis.close();
                    }
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * Returns a detailed report and a boolean indicating whether the snapshot can be successfully loaded
     * @param snapshotTime
     * @param snapshot
     */
    public static Pair<Boolean, String> generateSnapshotReport(Long snapshotTxnId, Snapshot snapshot) {
        return generateSnapshotReport(snapshotTxnId, snapshot, true);
    }

    /**
     * Returns a detailed report and a boolean indicating whether the snapshot can be successfully loaded
     * The implementation supports disabling the hashinator check, e.g. for old snapshots in tests.
     * @param snapshotTime
     * @param snapshot
     * @param expectHashinator
     */
    public static Pair<Boolean, String> generateSnapshotReport(
            Long snapshotTxnId, Snapshot snapshot, boolean expectHashinator) {
        CharArrayWriter caw = new CharArrayWriter();
        PrintWriter pw = new PrintWriter(caw);
        boolean snapshotConsistent = true;
        String indentString = "";
        pw.println(indentString + "TxnId: " + snapshotTxnId);
        pw.println(indentString + "Date: " +
                new Date(
                        org.voltdb.TransactionIdManager.getTimestampFromTransactionId(snapshotTxnId)));

        pw.println(indentString + "Digests:");
        indentString = "\t";
        TreeSet<String> digestTablesSeen = new TreeSet<String>();

        if (snapshot.m_digests.isEmpty()) {
            pw.println(indentString + "No digests found.");
            snapshotConsistent = false;
        } else {
            boolean inconsistent = false;

            /*
             * Iterate over the digests and ensure that they all contain the same list of tables
             */
            Map<Integer, List<Integer>> inconsistentDigests = new HashMap<Integer, List<Integer>>();
            for (int ii = 0; ii < snapshot.m_digests.size(); ii++) {
                inconsistentDigests.put( ii, new ArrayList<Integer>());
                Set<String> tables = snapshot.m_digestTables.get(ii);
                for (int zz = 0; zz < snapshot.m_digests.size(); zz++) {
                    if (zz == ii) {
                        continue;
                    }
                    if (!tables.equals(snapshot.m_digestTables.get(zz))) {
                        snapshotConsistent = false;
                        inconsistent = true;
                        inconsistentDigests.get(ii).add(zz);
                    }
                }
            }

            /*
             * Summarize what was inconsistent/consistent
             */
            if (!inconsistent) {
                for (int ii = 0; ii < snapshot.m_digests.size(); ii++) {
                    pw.println(indentString + snapshot.m_digests.get(ii).getPath());
                }
            } else {
                pw.println(indentString + "Not all digests are consistent");
                indentString = indentString + "\t";
                for (Map.Entry<Integer, List<Integer>> entry : inconsistentDigests.entrySet()) {
                    File left = snapshot.m_digests.get(entry.getKey());
                    pw.println(indentString + left.getPath() + " is inconsistent with:");
                    indentString = indentString + "\t";
                    for (Integer id : entry.getValue()) {
                        File right = snapshot.m_digests.get(id);
                        pw.println(indentString + right.getPath());
                    }
                    indentString = indentString.substring(1);
                }
            }

            /*
             * Print the list of tables found in the digests
             */
            indentString = indentString.substring(1);
            pw.print(indentString + "Tables: ");
            int ii = 0;

            for (int jj = 0; jj < snapshot.m_digestTables.size(); jj++) {
                for (String table : snapshot.m_digestTables.get(jj)) {
                    digestTablesSeen.add(table);
                }
            }

            for (String table : digestTablesSeen) {
                if (ii != 0) {
                    pw.print(", ");
                }
                ii++;
                pw.print(table);
            }
            pw.print("\n");
        }

        /*
         * Check the hash data (if expected).
         */
        if (expectHashinator) {
            pw.print(indentString + "Hash configuration: ");
            if (snapshot.m_hashConfig != null) {
                pw.println(indentString + "present");
            } else {
                pw.println(indentString + "not present");
                snapshotConsistent = false;
            }
        }

        /*
         * Check that the total partition count is the same in every table file
         */
        Integer totalPartitionCount = null;
        indentString = indentString + "\t";
        for (Map.Entry<String, TableFiles> entry : snapshot.m_tableFiles.entrySet()) {
            if (entry.getValue().m_isReplicated) {
                continue;
            }
            for (Integer partitionCount : entry.getValue().m_totalPartitionCounts){
                if (totalPartitionCount == null) {
                    totalPartitionCount = partitionCount;
                } else if (!totalPartitionCount.equals(partitionCount)) {
                    snapshotConsistent = false;
                    pw.println(indentString + "Partition count is not consistent throughout snapshot files for "
                            + entry.getKey() + ". Saw "
                            + partitionCount + " and " + totalPartitionCount);
                }
            }
        }

        /*
         * Now check that each individual table has enough information to be restored.
         * It is possible for a valid partition set to be available and still have a restore
         * fail because the restore plan loads a save file with a corrupt partition.
         */
        TreeSet<String> consistentTablesSeen = new TreeSet<String>();
        for (Map.Entry<String, TableFiles> entry : snapshot.m_tableFiles.entrySet()) {
            TableFiles tableFiles = entry.getValue();

            /*
             * Calculate the set of visible partitions not corrupted partitions
             */
            TreeSet<Integer> partitionsAvailable = new TreeSet<Integer>();
            int kk = 0;
            for (Set<Integer> validPartitionIds : tableFiles.m_validPartitionIds) {
                if (tableFiles.m_completed.get(kk++)) {
                    partitionsAvailable.addAll(validPartitionIds);
                }
            }

            /*
             * Ensure the correct range of partition ids is present
             */
            boolean partitionsPresent = false;
            if ((partitionsAvailable.size() == (tableFiles.m_isReplicated ? 1 : totalPartitionCount)) &&
                (partitionsAvailable.first() == 0) &&
                (partitionsAvailable.last() == (tableFiles.m_isReplicated ? 1 : totalPartitionCount) - 1)) {
                partitionsPresent = true;
            }

            /*
             * Report if any of the files have corrupt partitions
             */
            boolean hasCorruptPartitions = false;
            for (Set<Integer> corruptIds : tableFiles.m_corruptParititionIds) {
                if (!corruptIds.isEmpty()) {
                    hasCorruptPartitions = true;
                    snapshotConsistent = false;
                }
            }

            pw.println(indentString + "Table name: " + entry.getKey());
            indentString = indentString + "\t";
            pw.println(indentString + "Replicated: " + entry.getValue().m_isReplicated);
            pw.println(indentString + "Valid partition set available: " + partitionsPresent);
            pw.println(indentString + "Corrupt partitions present: " + hasCorruptPartitions);

            /*
             * Print information about individual files such as the partitions present and whether
             * they are corrupted
             */
            pw.println(indentString + "Files: ");
            indentString = indentString + "\t";
            for (int ii = 0; ii < tableFiles.m_files.size(); ii++) {
                String corruptPartitionIdString = "";
                int zz = 0;
                for (Integer partitionId : tableFiles.m_corruptParititionIds.get(ii)) {
                    if (zz != 0) {
                        corruptPartitionIdString = corruptPartitionIdString + ", ";
                    }
                    zz++;
                    corruptPartitionIdString = corruptPartitionIdString + partitionId;
                }

                String validPartitionIdString = "";
                zz = 0;
                for (Integer partitionId : tableFiles.m_validPartitionIds.get(ii)) {
                    if (zz != 0) {
                        validPartitionIdString = validPartitionIdString + ", ";
                    }
                    zz++;
                    validPartitionIdString = validPartitionIdString + partitionId;
                }

                if (corruptPartitionIdString.isEmpty()) {
                    consistentTablesSeen.add(entry.getKey());
                    pw.println(indentString + tableFiles.m_files.get(ii).getPath() +
                            " Completed: " + tableFiles.m_completed.get(ii) + " Partitions: " +
                            validPartitionIdString);
                } else {
                    pw.println(indentString + tableFiles.m_files.get(ii).getPath() +
                            " Completed: " + tableFiles.m_completed.get(ii) +
                            " Valid Partitions: " +
                            validPartitionIdString +
                            " Corrupt Partitions: " +
                            corruptPartitionIdString);
                }
            }
            indentString = indentString.substring(2);
        }
        indentString = indentString.substring(1);

        StringBuilder missingTables = new StringBuilder(8192);
        if (!consistentTablesSeen.containsAll(digestTablesSeen)) {
            snapshotConsistent = false;
            missingTables.append("Missing tables: ");
            Set<String> missingTablesSet = new TreeSet<String>(digestTablesSeen);
            missingTablesSet.removeAll(consistentTablesSeen);
            int hh = 0;
            for (String tableName : missingTablesSet) {
                if (hh > 0) {
                    missingTables.append(", ");
                }
                missingTables.append(tableName);
                hh++;
            }
            missingTables.append('\n');
        }

        /*
         * Tack on a summary at the beginning to indicate whether a restore is guaranteed to succeed
         * with this file set.
         */
        if (snapshotConsistent) {
            return Pair.of( true, "Snapshot valid\n" + caw.toString());
        } else {
            StringBuilder sb = new StringBuilder(8192);
            sb.append("Snapshot corrupted\n").append(missingTables).append(caw.toCharArray());
            return Pair.of( false,  sb.toString());
        }
    }

    /**
     * Generates a Filename to the snapshot file for the given table.
     * @param table
     * @param fileNonce
     * @param hostId
     */
    public static final String constructFilenameForTable(Table table,
                                                         String fileNonce,
                                                         SnapshotFormat format,
                                                         int hostId)
    {
        String extension = ".vpt";
        if (format == SnapshotFormat.CSV) {
            extension = ".csv";
        }

        StringBuilder filename_builder = new StringBuilder(fileNonce);
        filename_builder.append("-");
        filename_builder.append(table.getTypeName());
        if (!table.getIsreplicated())
        {
            filename_builder.append("-host_");
            filename_builder.append(hostId);
        }
        filename_builder.append(extension);//Volt partitioned table
        return filename_builder.toString();
    }

    public static final File constructFileForTable(Table table,
            String filePath,
            String fileNonce,
            SnapshotFormat format,
            int hostId)
    {
        return new VoltFile(filePath, SnapshotUtil.constructFilenameForTable(
            table, fileNonce, format, hostId));
    }

    /**
     * Generates the digest filename for the given nonce.
     * @param nonce
     * @param hostId
     */
    public static final String constructDigestFilenameForNonce(String nonce, int hostId) {
        return (nonce + "-host_" + hostId + ".digest");
    }

    /**
     * Generates the hashinator config filename for the given nonce.
     * @param nonce
     * @param hostId
     */
    public static final String constructHashinatorConfigFilenameForNonce(String nonce, int hostId) {
        return (nonce + "-host_" + hostId + HASH_EXTENSION);
    }

    public static final String constructCompletionFilenameForNonce(String nonce, int hostId) {
        return (nonce + "-host_" + hostId + COMPLETION_EXTENSION);
    }

    /**
     * Generates the catalog filename for the given nonce.
     * @param nonce
     */
    public static final String constructCatalogFilenameForNonce(String nonce) {
        return (nonce + ".jar");
    }

    public static final List<Table> getTablesToSave(Database database)
    {
        CatalogMap<Table> all_tables = database.getTables();
        ArrayList<Table> my_tables = new ArrayList<Table>();
        for (Table table : all_tables)
        {
            // Make a list of all non-materialized, non-export only tables
            if ((table.getMaterializer() != null) ||
                    (CatalogUtil.isTableExportOnly(database, table)))
            {
                continue;
            }
            my_tables.add(table);
        }
        return my_tables;
    }

    public static File[] retrieveRelevantFiles(String filePath,
                                               final String fileNonce)
    {
        FilenameFilter has_nonce = new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String file)
            {
                return file.startsWith(fileNonce) && file.endsWith(".vpt");
            }
        };

        File save_dir = new VoltFile(filePath);
        File[] save_files = save_dir.listFiles(has_nonce);
        return save_files;
    }

    public static String didSnapshotRequestFailWithErr(VoltTable results[]) {
        if (results.length < 1) return "HAD NO RESULT TABLES";
        final VoltTable result = results[0];
        result.resetRowPosition();
        //Crazy old code would return one column with an error message.
        //Not sure any of it exists anymore
        if (result.getColumnCount() == 1) {
            if (result.advanceRow()) {
                return result.getString(0);
            } else {
                return "UNKNOWN ERROR WITH ONE COLUMN NO ROW RESULT TABLE";
            }
        }

        //assert(result.getColumnName(1).equals("TABLE"));
        String err = null;
        while (result.advanceRow()) {
            if (!result.getString("RESULT").equals("SUCCESS")) {
                err = result.getString("ERR_MSG");
            }
        }
        result.resetRowPosition();
        return err;
    }

    public static boolean didSnapshotRequestSucceed(VoltTable result[]) {
        return didSnapshotRequestFailWithErr(result) == null;
    }

    public static boolean isSnapshotInProgress(VoltTable results[]) {
        final VoltTable result = results[0];
        result.resetRowPosition();
        if (result.getColumnCount() == 1) {
            return false;
        }

        boolean inprogress = false;
        while (result.advanceRow()) {
            if (result.getString("ERR_MSG").contains("IN PROGRESS")) {
                inprogress = true;
            }
        }
        return inprogress;
    }

    public static boolean isSnapshotQueued(VoltTable results[]) {
        final VoltTable result = results[0];
        result.resetRowPosition();
        if (result.getColumnCount() == 1) {
            return false;
        }

        boolean queued = false;
        while (result.advanceRow()) {
            if (result.getString("ERR_MSG").contains("SNAPSHOT REQUEST QUEUED")) {
                queued = true;
            }
        }
        return queued;
    }

    /**
     * Handles response from asynchronous snapshot requests.
     */
    public static interface SnapshotResponseHandler {
        /**
         *
         * @param resp could be null
         */
        public void handleResponse(ClientResponse resp);
    }

    /*
     * fatalSnapshotResponseHandler is called when a SnapshotUtil.requestSnapshot response occurs.
     * This callback runs on the snapshot daemon thread.
     */
    public static final SnapshotUtil.SnapshotResponseHandler fatalSnapshotResponseHandler =
        new SnapshotUtil.SnapshotResponseHandler() {
            @Override
            public void handleResponse(ClientResponse resp)
            {
                if (resp == null) {
                    VoltDB.crashLocalVoltDB("Failed to initiate snapshot", false, null);
                } else if (resp.getStatus() != ClientResponseImpl.SUCCESS) {
                    final String statusString = resp.getStatusString();
                    if (statusString != null && statusString.contains("Failure while running system procedure @SnapshotSave")) {
                        VoltDB.crashLocalVoltDB("Failed to initiate snapshot due to node failure, aborting", false, null);
                    }
                    VoltDB.crashLocalVoltDB("Failed to initiate snapshot: "
                                            + resp.getStatusString(), true, null);
                }

                assert resp != null;
                VoltTable[] results = resp.getResults();
                if (SnapshotUtil.didSnapshotRequestSucceed(results)) {
                    String appStatus = resp.getAppStatusString();
                    if (appStatus == null) {
                        VoltDB.crashLocalVoltDB("Snapshot request failed: "
                                                + resp.getStatusString(), false, null);
                    }
                    // else success
                } else {
                    VoltDB.crashLocalVoltDB("Snapshot request failed: " + results[0].toJSONString(),
                                            false, null);
                }
            }
        };

    /**
     * Request a new snapshot. It will retry for a couple of times. If it
     * doesn't succeed in the specified time, an error response will be sent to
     * the response handler, otherwise a success response will be passed to the
     * handler.
     *
     * The request process runs in a separate thread, this method call will
     * return immediately.
     *
     * @param clientHandle
     * @param path
     * @param nonce
     * @param blocking
     * @param format
     * @param data Any data that needs to be passed to the snapshot target
     * @param handler
     */
    public static void requestSnapshot(final long clientHandle,
                                       final String path,
                                       final String nonce,
                                       final boolean blocking,
                                       final SnapshotFormat format,
                                       final String data,
                                       final SnapshotResponseHandler handler,
                                       final boolean notifyChanges)
    {
        final SnapshotInitiationInfo snapInfo = new SnapshotInitiationInfo(path, nonce, blocking, format, data);
        final SimpleClientResponseAdapter adapter =
                new SimpleClientResponseAdapter(ClientInterface.SNAPSHOT_UTIL_CID, "SnapshotUtilAdapter", true);
        final LinkedBlockingQueue<ClientResponse> responses = new LinkedBlockingQueue<ClientResponse>();
        adapter.registerCallback(clientHandle, new SimpleClientResponseAdapter.Callback() {
            @Override
            public void handleResponse(ClientResponse response)
            {
                responses.offer(response);
            }
        });

        final SnapshotDaemon sd = VoltDB.instance().getClientInterface().getSnapshotDaemon();
        Runnable work = new Runnable() {
            @Override
            public void run() {
                ClientResponse response = null;
                // abort if unable to succeed in 2 hours
                final long startTime = System.currentTimeMillis();
                boolean hasRequested = false;
                while (System.currentTimeMillis() - startTime <= TimeUnit.HOURS.toMillis(2)) {
                    try {
                        if (!hasRequested) {
                            sd.createAndWatchRequestNode(clientHandle, adapter, snapInfo, notifyChanges);
                            hasRequested = true;
                        }

                        try {
                            response = responses.poll(
                                    TimeUnit.HOURS.toMillis(2) - (System.currentTimeMillis() - startTime),
                                    TimeUnit.MILLISECONDS);
                            if (response == null) break;
                        } catch (InterruptedException e) {
                            VoltDB.crashLocalVoltDB("Should never happen", true, e);
                        }

                        VoltTable[] results = response.getResults();
                        if (response.getStatus() != ClientResponse.SUCCESS) {
                            break;
                        } else if (isSnapshotInProgress(results)) {
                            // retry after a second
                            Thread.sleep(1000);
                            // Request again
                            hasRequested = false;
                            continue;
                        } else if (isSnapshotQueued(results) && notifyChanges) {
                            //Wait for an update on the queued state via ZK
                            continue;
                        } else {
                            // other errors are not recoverable
                            break;
                        }
                    } catch (ForwardClientException e) {
                        //This happens when something goes wrong in the snapshot daemon
                        //I think it will always be that there was an existing snapshot request
                        //It should eventually terminate and then we can submit one.
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e1) {}
                        new VoltLogger("SNAPSHOT").warn("Partition detection was unable to submit a snapshot request" +
                                "because one already existed. Retrying.");
                        continue;
                    } catch (InterruptedException ignore) {}
                }

                handler.handleResponse(response);
            }
        };

        // Use an executor service here to avoid explosion of threads???
        ThreadFactory factory = CoreUtils.getThreadFactory("Snapshot Request - " + nonce);
        Thread workThread = factory.newThread(work);
        workThread.start();
    }

    public static String formatHumanReadableDate(long timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING + "z");
        sdf.setTimeZone(VoltDB.VOLT_TIMEZONE);
        return sdf.format(new Date(timestamp));
    }

    public static byte[] OutputBuffersToBytes(Collection<BBContainer> outputContainers)
    {
        ByteBuffer buf = ByteBuffer.allocate(4 + // buffer count
                                             (8 + 4 + 4) * outputContainers.size()); // buffer info

        buf.putInt(outputContainers.size());
        for (DBBPool.BBContainer container : outputContainers) {
            buf.putLong(container.address());
            buf.putInt(container.b().position());
            buf.putInt(container.b().remaining());
        }

        return buf.array();
    }

    /**
     * Watch for the completion of a snapshot
     * @param nonce    The snapshot nonce to watch for
     * @return A future that will return the SnapshotCompletionEvent
     */
    public static ListenableFuture<SnapshotCompletionInterest.SnapshotCompletionEvent>
    watchSnapshot(final String nonce)
    {
        final SettableFuture<SnapshotCompletionInterest.SnapshotCompletionEvent> result =
            SettableFuture.create();

        SnapshotCompletionInterest interest = new SnapshotCompletionInterest() {
            @Override
            public CountDownLatch snapshotCompleted(SnapshotCompletionEvent event)
            {
                if (event.nonce.equals(nonce) && event.didSucceed) {
                    VoltDB.instance().getSnapshotCompletionMonitor().removeInterest(this);
                    result.set(event);
                }
                return null;
            }
        };
        VoltDB.instance().getSnapshotCompletionMonitor().addInterest(interest);

        return result;
    }

    /**
     * Retrieve hashinator config for restore.
     * @param path snapshot base directory
     * @param nonce unique snapshot ID
     * @param hostId host ID
     * @return hashinator shapshot data
     * @throws Exception
     */
    public static HashinatorSnapshotData retrieveHashinatorConfig(
            String path, String nonce, int hostId, VoltLogger logger) throws IOException {
        HashinatorSnapshotData hashData = null;
        String expectedFileName = constructHashinatorConfigFilenameForNonce(nonce, hostId);
        File[] files = new VoltFile(path).listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().equals(expectedFileName)) {
                    hashData = new HashinatorSnapshotData();
                    hashData.restoreFromFile(file);
                    break;
                }
            }
        }
        if (hashData == null && TheHashinator.getConfiguredHashinatorType() == HashinatorType.ELASTIC) {
            throw new IOException("Missing hashinator data in snapshot");
        }
        return hashData;
    }

    /*
     * Do parameter checking for the pre-JSON version of @SnapshotRestore old version
     */
    public static ClientResponseImpl transformRestoreParamsToJSON(StoredProcedureInvocation task) {
        Object params[] = task.getParams().toArray();
        if (params.length == 1) {
            return null;
        } else if (params.length == 2) {
            if (params[0] == null) {
                return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                                              new VoltTable[0],
                                              "@SnapshotRestore parameter 0 was null",
                                              task.getClientHandle());
            }
            if (params[1] == null) {
                return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                                              new VoltTable[0],
                                              "@SnapshotRestore parameter 1 was null",
                                              task.getClientHandle());
            }
            if (!(params[0] instanceof String)) {
                return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                                              new VoltTable[0],
                                              "@SnapshotRestore param 0 (path) needs to be a string, but was type "
                                              + params[0].getClass().getSimpleName(),
                                              task.getClientHandle());
            }
            if (!(params[1] instanceof String)) {
                return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                                              new VoltTable[0],
                                              "@SnapshotRestore param 1 (nonce) needs to be a string, but was type "
                                              + params[1].getClass().getSimpleName(),
                                              task.getClientHandle());
            }
            JSONObject jsObj = new JSONObject();
            try {
                jsObj.put(SnapshotUtil.JSON_PATH, params[0]);
                jsObj.put(SnapshotUtil.JSON_NONCE, params[1]);
            } catch (JSONException e) {
                Throwables.propagate(e);
            }
            task.setParams( jsObj.toString() );
            return null;
        } else {
            return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                                          new VoltTable[0],
                                          "@SnapshotRestore supports a single json document parameter or two parameters (path, nonce), " +
                                          params.length + " parameters provided",
                                          task.getClientHandle());
        }
    }
}
