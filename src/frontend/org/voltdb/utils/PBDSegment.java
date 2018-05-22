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

package org.voltdb.utils;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DeferredSerialization;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public abstract class PBDSegment {

    /**
     * Represents a reader for a segment. Multiple readers may be active
     * at any point in time, reading from different locations in the segment.
     */
    public interface PBDSegmentReader {
        /**
         * Are there any more entries to read from this segment for this reader
         *
         * @return true if there are still more entries to be read. False otherwise.
         * @throws IOException if the reader was closed or on any error trying to read from the segment file.
         */
        public boolean hasMoreEntries() throws IOException;

        /**
         * Have all the entries in this segment been read by this reader and
         * acknowledged as ready for discarding.
         *
         * @return true if all entries have been read and discarded by this reader. False otherwise.
         * @throws IOException if the reader was closed
         */
        public boolean allReadAndDiscarded() throws IOException;

        /**
         * Read the next entry from the segment for this reader.
         * Returns null if all entries in this segment were already read by this reader.
         *
         * @param factory
         * @return BBContainer with the bytes read
         * @throws IOException
         */
        public DBBPool.BBContainer poll(BinaryDeque.OutputContainerFactory factory) throws IOException;

        //Don't use size in bytes to determine empty, could potentially
        //diverge from object count on crash or power failure
        //although incredibly unlikely
        /**
         * Returns the number of bytes that are left to read in this segment
         * for this reader.
         */
        public int uncompressedBytesToRead();

        /**
         * Returns the current read offset for this reader in this segment.
         */
        public long readOffset();

        /**
         * Entry that this reader will read next.
         * @return
         */
        public int readIndex();

        /**
         * Rewinds the read offset for this reader by the specified number of bytes.
         */
        public void rewindReadOffset(int byBytes);

        /**
         * Close this reader and release any resources.
         * <code>getReader</code> will still return this reader until the segment is closed.
         */
        public void close() throws IOException;

        /**
         * Has this reader been closed.
         */
        public boolean isClosed();
    }

    private static final String TRUNCATOR_CURSOR = "__truncator__";
    static final int NO_FLAGS = 0;
    static final int FLAG_COMPRESSED = 1;

    static final int COUNT_OFFSET = 0;
    static final int SIZE_OFFSET = 4;

    // Has to be able to hold at least one object (compressed or not)
    public static final int CHUNK_SIZE = Integer.getInteger("PBDSEGMENT_CHUNK_SIZE", 1024 * 1024 * 64);
    static final int OBJECT_HEADER_BYTES = 8;
    static final int SEGMENT_HEADER_BYTES = 8;
    protected final File m_file;

    protected boolean m_closed = true;
    protected RandomAccessFile m_ras;
    protected FileChannel m_fc;
    //Avoid unecessary sync with this flag
    protected boolean m_syncedSinceLastEdit = true;

    public PBDSegment(File file)
    {
        m_file = file;
    }

    abstract long segmentId();
    abstract File file();

    abstract void reset();

    abstract int getNumEntries() throws IOException;

    abstract boolean isBeingPolled();

    abstract boolean isOpenForReading(String cursorId);

    abstract PBDSegmentReader openForRead(String cursorId) throws IOException;

    /**
     * Returns the reader opened for the given cursor id.
     * This may return a closed reader if the reader has already finished reading this segment.
     */
    abstract PBDSegmentReader getReader(String cursorId);

    /**
     * @param forWrite    Open the file in read/write mode
     * @param emptyFile   true to overwrite the header with 0 entries, essentially emptying the file
     * @throws IOException
     */
    abstract protected void openForWrite(boolean emptyFile) throws IOException;

    abstract void initNumEntries(int count, int size) throws IOException;

    abstract void closeAndDelete() throws IOException;

    abstract boolean isClosed();

    abstract void close() throws IOException;

    abstract void sync() throws IOException;

    abstract boolean hasAllFinishedReading() throws IOException;

    abstract boolean offer(DBBPool.BBContainer cont, boolean compress) throws IOException;

    abstract int offer(DeferredSerialization ds) throws IOException;

    // TODO: javadoc
    abstract int size();

    abstract protected int writeTruncatedEntry(BinaryDeque.TruncatorResponse entry) throws IOException;

    /**
     * Parse the segment and truncate the file if necessary.
     * @param truncator    A caller-supplied truncator that decides where in the segment to truncate
     * @return The number of objects that was truncated. This number will be subtracted from the total number
     * of available objects in the PBD. -1 means that this whole segment should be removed.
     * @throws IOException
     */
    int parseAndTruncate(BinaryDeque.BinaryDequeTruncator truncator) throws IOException {
        if (!m_closed) throw new IOException(("Segment should not be open before truncation"));

        openForWrite(false);
        PBDSegmentReader reader = openForRead(TRUNCATOR_CURSOR);

        // Do stuff
        final int initialEntryCount = getNumEntries();
        int entriesTruncated = 0;
        int sizeInBytes = 0;

        DBBPool.BBContainer cont;
        while (true) {
            final long beforePos = reader.readOffset();

            cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            if (cont == null) {
                break;
            }

            final int compressedLength = (int) (reader.readOffset() - beforePos - OBJECT_HEADER_BYTES);
            final int uncompressedLength = cont.b().limit();

            try {
                //Handoff the object to the truncator and await a decision
                BinaryDeque.TruncatorResponse retval = truncator.parse(cont);
                if (retval == null) {
                    //Nothing to do, leave the object alone and move to the next
                    sizeInBytes += uncompressedLength;
                } else {
                    //If the returned bytebuffer is empty, remove the object and truncate the file
                    if (retval.status == BinaryDeque.TruncatorResponse.Status.FULL_TRUNCATE) {
                        if (reader.readIndex() == 1) {
                            /*
                             * If truncation is occuring at the first object
                             * Whammo! Delete the file.
                             */
                            entriesTruncated = -1;
                        } else {
                            entriesTruncated = initialEntryCount - (reader.readIndex() - 1);
                            //Don't forget to update the number of entries in the file
                            initNumEntries(reader.readIndex() - 1, sizeInBytes);
                            m_fc.truncate(reader.readOffset() - (compressedLength + OBJECT_HEADER_BYTES));
                        }
                    } else {
                        assert retval.status == BinaryDeque.TruncatorResponse.Status.PARTIAL_TRUNCATE;
                        entriesTruncated = initialEntryCount - reader.readIndex();
                        //Partial object truncation
                        reader.rewindReadOffset(compressedLength + OBJECT_HEADER_BYTES);
                        final long partialEntryBeginOffset = reader.readOffset();
                        m_fc.position(partialEntryBeginOffset);

                        final int written = writeTruncatedEntry(retval);
                        sizeInBytes += written;

                        initNumEntries(reader.readIndex(), sizeInBytes);
                        m_fc.truncate(partialEntryBeginOffset + written + OBJECT_HEADER_BYTES);
                    }

                    break;
                }
            } finally {
                cont.discard();
            }
        }

        close();

        return entriesTruncated;
    }
}
