/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.UnaryOperator;
import java.util.zip.CRC32;

import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.VoltSnapshotFile;

import com.google_voltpatches.common.annotations.VisibleForTesting;
import com.google_voltpatches.common.util.concurrent.UnsynchronizedRateLimiter;

public abstract class NativeSnapshotDataTarget implements SnapshotDataTarget {
    protected static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    public static final int SNAPSHOT_SYNC_FREQUENCY = Integer.getInteger("SNAPSHOT_SYNC_FREQUENCY", 500);
    public static final int SNAPSHOT_FADVISE_BYTES = Integer.getInteger("SNAPSHOT_FADVISE_BYTES", 1024 * 1024 * 2);
    private static final boolean s_enableDirectIoSnapshots = Boolean
            .parseBoolean(System.getProperty("SNAPSHOT_DIRECT_IO", Boolean.FALSE.toString()));
    private static volatile boolean s_enforceSnapshotRatelimit = true;
    private static final UnsynchronizedRateLimiter s_snapshotRatelimiter;

    protected static final int s_offsetCrc = 0;
    protected static final int s_offsetHeaderSize = s_offsetCrc + Integer.BYTES;
    protected static final int s_offsetComplete = s_offsetHeaderSize + Integer.BYTES;
    protected static final int s_offsetVersion = s_offsetComplete + Byte.BYTES;
    protected static final int s_offsetJsonSize = s_offsetVersion + Integer.BYTES * 4;
    protected static final int s_offsetJson = s_offsetJsonSize + Integer.BYTES;

    private static final int s_staticHeaderSize = s_offsetJson;
    private static final int s_staticHeaderSizeAfterSize = s_staticHeaderSize - s_offsetComplete;

    static final int[] s_currentVersion = { 0, 0, 0, 2 };

    // Test hook for injecting errors through a FileChanel
    static volatile UnaryOperator<FileChannel> SNAPSHOT_FILE_CHANEL_OPERATER = UnaryOperator.identity();

    private Runnable m_onCloseHandler = null;
    private Runnable m_inProgressHandler = null;

    volatile IOException m_reportedSerializationFailure = null;

    private final boolean m_needsFinalClose;

    static {
        int snapshotRatelimitMb;

        int limit = Integer.getInteger("SNAPSHOT_RATELIMIT_MEGABYTES", Integer.MAX_VALUE);
        if (limit < 1) {
            SNAP_LOG.warn("Invalid snapshot rate limit " + limit + ", no limit will be applied");
            snapshotRatelimitMb = Integer.MAX_VALUE;
        } else {
            snapshotRatelimitMb = limit;
        }
        if (snapshotRatelimitMb < Integer.MAX_VALUE) {
            SNAP_LOG.info("Rate limiting snapshots to " + snapshotRatelimitMb + " megabytes/second");
            s_snapshotRatelimiter = UnsynchronizedRateLimiter.create(snapshotRatelimitMb * 1024.0 * 1024.0, 1,
                    TimeUnit.SECONDS);
        } else {
            s_snapshotRatelimiter = null;
        }
    }

    public static void enforceSnapshotRateLimit(int permits) {
        if (s_snapshotRatelimiter != null && s_enforceSnapshotRatelimit) {
            s_snapshotRatelimiter.acquire(permits);
        }
    }

    public static void enforceSnapshotRateLimit(boolean enforce) {
        s_enforceSnapshotRatelimit = enforce;
    }

    /**
     * Construct a factory for creating {@link NativeSnapshotDataTarget} instances
     *
     * @param directory     where snapshot files will be written
     * @param hostId        of this host
     * @param clusterName   for this cluster
     * @param databaseName  for this database
     * @param numPartitions in the cluster
     * @param partitions    list of partitions on this host
     * @param txnId         of snapshot
     * @param timestamp     of snapshot
     * @return {@link Factory} to create {@link NativeSnapshotDataTarget} instances
     */
    public static Factory getFactory(String directory, String pathType, String nonce, int hostId, final String clusterName,
            final String databaseName, int numPartitions, List<Integer> partitions, long txnId, long timestamp, boolean isTruncationSnapshot) {
        // Grab the currently configured operator and replace it with a no-op so only the targets returned by this
        // factory are affected
        UnaryOperator<FileChannel> channelOperator = SNAPSHOT_FILE_CHANEL_OPERATER;
        SNAPSHOT_FILE_CHANEL_OPERATER = UnaryOperator.identity();

        return getFactory(directory, pathType, nonce, hostId, clusterName, databaseName, numPartitions, partitions, txnId, timestamp,
                isTruncationSnapshot, channelOperator);
    }

    /**
     * Factory method for tests to allow them to inject misbehaving {@link FileChannel}s into targets
     *
     * @see #getFactory(String, String, String, int, String, String, int, List, long, long, boolean, UnaryOperator<FileChannel>)
     */
    @VisibleForTesting
    public static Factory getFactory(String directory, String pathType, String nonce, int hostId, final String clusterName, final String databaseName,
            int numPartitions, List<Integer> partitions, long txnId, long timestamp, boolean isTruncationSnapshot,
            UnaryOperator<FileChannel> channelOperator) {
        // If direct IO is enabled and supported in directory return a direct IO target
        if (s_enableDirectIoSnapshots && DirectIoSnapshotDataTarget.directIoSupported(directory)) {
            return DirectIoSnapshotDataTarget.factory(directory, hostId, clusterName, databaseName, numPartitions,
                    partitions, txnId, timestamp, s_currentVersion, isTruncationSnapshot, channelOperator);
        }

        return (fileName, tableName, isReplicated, schema) -> {
            File f = (SnapshotUtil.isCommandLogOrTerminusSnapshot(pathType, nonce)) ? new File(directory, fileName) : new VoltSnapshotFile(directory, fileName);
            return new DefaultSnapshotDataTarget(
                    f, hostId, clusterName, databaseName, tableName, numPartitions,
                    isReplicated, partitions, schema, txnId, timestamp, s_currentVersion, isTruncationSnapshot, channelOperator);
        };
    }

    NativeSnapshotDataTarget(boolean isReplicated) {
        m_needsFinalClose = !isReplicated;
    }

    @Override
    public int getHeaderSize() {
        return 0;
    }

    @Override
    public SnapshotFormat getFormat() {
        return SnapshotFormat.NATIVE;
    }

    @Override
    public boolean needsFinalClose() {
        return m_needsFinalClose;
    }

    @Override
    public void setOnCloseHandler(Runnable onClose) {
        m_onCloseHandler = onClose;
    }

    /**
     * Get the row count if any, of the content wrapped in the given {@link BBContainer}
     *
     * @param tupleData
     * @return the numbers of tuple data rows contained within a container
     */
    @Override
    public int getInContainerRowCount(BBContainer tupleData) {
        return SnapshotDataTarget.ROW_COUNT_UNSUPPORTED;
    }

    @Override
    public void setInProgressHandler(Runnable inProgress) {
        m_inProgressHandler = inProgress;
    }

    @Override
    public void trackProgress() {
        m_inProgressHandler.run();
    }

    @Override
    public Exception getSerializationException() {
        return m_reportedSerializationFailure;
    }

    @Override
    public void reportSerializationFailure(IOException ex) {
        m_reportedSerializationFailure = ex;
    }

    void postClose() throws IOException {
        if (m_onCloseHandler!=null) {
            m_onCloseHandler.run();
        }
        if (m_reportedSerializationFailure != null) {
            throw  m_reportedSerializationFailure;
        }
    }

    /**
     * Allocate a container from {@code containerFactory} and serialize the snapshot header into the container. The
     * returned container will have position at the start of the header and limit at the end of the header.
     *
     * @return A container with the serialized header
     * @throws IOException If an error occurs
     */
    DBBPool.BBContainer serializeHeader(IntFunction<DBBPool.BBContainer> containerFactory, int hostId,
            final String clusterName, final String databaseName, final String tableName, final int numPartitions,
            final boolean isReplicated, final List<Integer> partitionIds, final byte[] schemaBytes, final long txnId,
            final long timestamp, int version[]) throws IOException {
        String hostname = CoreUtils.getHostnameOrAddress();
        JSONStringer stringer = new JSONStringer();

        // Generate the json header first so the size can be determined
        byte jsonBytes[];
        try {
            stringer.object();
            stringer.keySymbolValuePair("txnId", txnId);
            stringer.keySymbolValuePair("hostId", hostId);
            stringer.keySymbolValuePair("hostname", hostname);
            stringer.keySymbolValuePair("clusterName", clusterName);
            stringer.keySymbolValuePair("databaseName", databaseName);
            stringer.keySymbolValuePair("tableName", tableName.toUpperCase());
            stringer.keySymbolValuePair("isReplicated", isReplicated);
            stringer.keySymbolValuePair("isCompressed", true);
            stringer.keySymbolValuePair("checksumType", "CRC32C");
            stringer.keySymbolValuePair("timestamp", timestamp);
            /*
             * The timestamp string is for human consumption, automated stuff should use the actual timestamp
             */
            stringer.keySymbolValuePair("timestampString", SnapshotUtil.formatHumanReadableDate(timestamp));
            if (!isReplicated) {
                stringer.key("partitionIds").array();
                for (int partitionId : partitionIds) {
                    stringer.value(partitionId);
                }
                stringer.endArray();

                stringer.keySymbolValuePair("numPartitions", numPartitions);
            }
            stringer.endObject();
            jsonBytes = stringer.toString().getBytes("UTF-8");
        } catch (Exception e) {
            throw new IOException(e);
        }

        DBBPool.BBContainer container = containerFactory
                .apply(s_staticHeaderSize + jsonBytes.length + schemaBytes.length);
        ByteBuffer buffer = container.b();

        buffer.position(s_offsetHeaderSize);
        // Size of static header after size value + json data
        buffer.putInt(s_staticHeaderSizeAfterSize + jsonBytes.length);
        buffer.put((byte) 1); // isComplete set to true for the CRC calculation, false later
        for (int ii = 0; ii < 4; ii++) {
            buffer.putInt(version[ii]);
        }
        buffer.putInt(jsonBytes.length);
        buffer.put(jsonBytes);
        buffer.put(schemaBytes);

        buffer.flip();
        buffer.position(s_offsetHeaderSize);

        CRC32 crc32 = new CRC32();
        crc32.update(buffer);
        final int crcValue = (int) crc32.getValue();

        buffer.rewind();
        buffer.putInt(s_offsetCrc, crcValue);
        buffer.put(s_offsetComplete, (byte) 0); // Haven't actually finished writing file

        return container;
    }

    /**
     * Simple factory that is used to create instances of {@link NativeSnapshotDataTarget}
     */
    @FunctionalInterface
    public interface Factory {
        default NativeSnapshotDataTarget create(String fileName, SnapshotTableInfo table) throws IOException {
            return create(fileName, table.getName(), table.isReplicated(), table.getSchema());
        }

        NativeSnapshotDataTarget create(String fileName, String tableName, boolean isReplicated, byte[] schemaBytes)
                throws IOException;
    }
}
