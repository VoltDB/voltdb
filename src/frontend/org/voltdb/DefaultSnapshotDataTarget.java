/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import org.apache.log4j.Logger;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.VoltLoggerFactory;
import org.voltdb.utils.DBBPool.BBContainer;

public class DefaultSnapshotDataTarget implements SnapshotDataTarget {

    public static volatile boolean m_simulateFullDiskWritingHeader = false;
    public static volatile boolean m_simulateFullDiskWritingChunk = false;

    private final FileChannel m_channel;
    private final FileOutputStream m_fos;
    private static final Logger hostLog = Logger.getLogger("HOST", VoltLoggerFactory.instance());
    private final ExecutorService m_es;
    private Runnable m_onCloseHandler = null;

    /*
     * If a write fails then this snapshot is hosed.
     * Set the flag so all writes return immediately. The system still
     * needs to scan the all table to clear the dirty bits
     * so the process continues as if the writes are succeeding.
     * A more efficient failure mode would do the scan but not the
     * extra serialization work.
     */
    private volatile boolean m_writeFailed = false;
    private volatile IOException m_writeException = null;
    private volatile long m_bytesWritten = 0;

    /*
     * Accept a single write even though simulating a full disk is enabled;
     */
    private volatile boolean m_acceptOneWrite = false;

    private final String m_tableName;

    public DefaultSnapshotDataTarget(
            final File file,
            final int hostId,
            final String clusterName,
            final String databaseName,
            final String tableName,
            final int numPartitions,
            final boolean isReplicated,
            final int partitionIds[],
            final VoltTable schemaTable,
            final long createTime) throws IOException {
            this(
                file,
                hostId,
                clusterName,
                databaseName,
                tableName,
                numPartitions,
                isReplicated,
                partitionIds,
                schemaTable,
                createTime,
                new int[] { 0, 0, 0, 0 });
    }

    public DefaultSnapshotDataTarget(
            final File file,
            final int hostId,
            final String clusterName,
            final String databaseName,
            final String tableName,
            final int numPartitions,
            final boolean isReplicated,
            final int partitionIds[],
            final VoltTable schemaTable,
            final long createTime,
            int version[]
            ) throws IOException {
        m_tableName = tableName;
        m_fos = new FileOutputStream(file);
        m_channel = m_fos.getChannel();
        m_es = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {

                return new Thread(
                        Thread.currentThread().getThreadGroup(),
                        r,
                        "Snapshot target for " + file.getName() + " host " + hostId,
                        131072);
            }
        });
        final FastSerializer fs = new FastSerializer();
        fs.writeInt(0);//CRC
        fs.writeInt(0);//Header length placeholder
        fs.writeByte(1);//Indicate the snapshot was not completed, set to true for the CRC calculation, false later
        for (int ii = 0; ii < 4; ii++) {
            fs.writeInt(version[ii]);//version
        }
        fs.writeLong(createTime);
        fs.writeInt(hostId);
        fs.writeString(clusterName);
        fs.writeString(databaseName);
        fs.writeString(tableName);
        fs.writeBoolean(isReplicated);
        if (!isReplicated) {
            fs.writeArray(partitionIds);
            fs.writeInt(numPartitions);
        }
        final BBContainer container = fs.getBBContainer();
        container.b.position(4);
        container.b.putInt(container.b.remaining() - 4);
        container.b.position(0);

        FastSerializer schemaSerializer = new FastSerializer();
        schemaTable.writeExternal(schemaSerializer);
        final BBContainer schemaContainer = schemaSerializer.getBBContainer();
        schemaContainer.b.limit(schemaContainer.b.limit() - 4);//Don't want the row count
        schemaContainer.b.position(schemaContainer.b.position() + 4);//Don't want total table length

        final CRC32 crc = new CRC32();
        ByteBuffer aggregateBuffer = ByteBuffer.allocate(container.b.remaining() + schemaContainer.b.remaining());
        aggregateBuffer.put(container.b);
        aggregateBuffer.put(schemaContainer.b);
        aggregateBuffer.flip();
        crc.update(aggregateBuffer.array(), 4, aggregateBuffer.capacity() - 4);

        final int crcValue = (int) crc.getValue();
        aggregateBuffer.putInt(crcValue).position(8);
        aggregateBuffer.put((byte)0).position(0);//Haven't actually finished writing file

        if (m_simulateFullDiskWritingHeader) {
            m_writeException = new IOException("Disk full");
            m_writeFailed = true;
            m_fos.close();
            m_es.shutdown();
            throw m_writeException;
        }

        /*
         * Be completely sure the write succeeded. If it didn't
         * the disk is probably full or the path is bunk etc.
         */
        m_acceptOneWrite = true;
        Future<?> writeFuture = write(DBBPool.wrapBB(aggregateBuffer), false);
        try {
            writeFuture.get();
        } catch (InterruptedException e) {
            m_fos.close();
            m_es.shutdown();
            return;
        } catch (ExecutionException e) {
            m_fos.close();
            m_es.shutdown();
            throw m_writeException;
        }
        if (m_writeFailed) {
            m_fos.close();
            m_es.shutdown();
            throw m_writeException;
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        m_es.shutdown();
        m_es.awaitTermination( 1, TimeUnit.DAYS);
        m_channel.force(true);
        m_fos.getFD().sync();
        m_channel.position(8);
        ByteBuffer completed = ByteBuffer.allocate(1);
        completed.put((byte)1).flip();
        m_channel.write(completed);
        m_fos.getFD().sync();
        m_channel.close();
        if (m_onCloseHandler != null) {
            m_onCloseHandler.run();
        }
    }

    @Override
    public int getHeaderSize() {
        return 4;
    }

    private Future<?> write(final BBContainer tupleData, final boolean prependLength) {
        if (m_writeFailed) {
            tupleData.discard();
            return null;
        }

        if (prependLength) {
            tupleData.b.putInt(tupleData.b.remaining() - 4);
            tupleData.b.position(0);
        }

        return m_es.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    if (m_acceptOneWrite) {
                        m_acceptOneWrite = false;
                    } else {
                        if (m_simulateFullDiskWritingChunk) {
                            throw new IOException("Disk full");
                        }
                    }
                    while (tupleData.b.hasRemaining()) {
                        m_bytesWritten += m_channel.write(tupleData.b);
                    }
                } catch (IOException e) {
                    m_writeException = e;
                    hostLog.error("Error while attempting to write snapshot data to disk", e);
                    m_writeFailed = true;
                    throw e;
                } finally {
                    tupleData.discard();
                }
                return null;
            }
        });
    }

    @Override
    public Future<?> write(final BBContainer tupleData) {
        return write(tupleData, true);
    }

    @Override
    public long getBytesWritten() {
        return m_bytesWritten;
    }

    @Override
    public void setOnCloseHandler(Runnable onClose) {
        m_onCloseHandler = onClose;
    }

    @Override
    public IOException getLastWriteException() {
        return m_writeException;
    }

}
