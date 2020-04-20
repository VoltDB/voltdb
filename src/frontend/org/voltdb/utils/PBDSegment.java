/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.List;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DeferredSerialization;

public abstract class PBDSegment<M> {
    private static final String IS_FINAL_ATTRIBUTE = "VoltDB.PBDSegment.isFinal";

    // Has to be able to hold at least one object (compressed or not)
    public static final int CHUNK_SIZE = Integer.getInteger("PBDSEGMENT_CHUNK_SIZE", 1024 * 1024 * 64);
    static final long INVALID_ID = Long.MIN_VALUE;
    static final long INVALID_TIMESTAMP = -1;

    // Segment Header layout:
    // - version of segment headers (4 bytes)
    //  - crc of segment header (4 bytes),
    //  - total number of entries (4 bytes),
    //  - total bytes of data (4 bytes, uncompressed size),
    //  - id of the first data entry (8 bytes)
    //  - id of the last data entry (8 bytes)
    // - timestamp last entry was added (8 bytes)
    //  - random id assigned to segment ( 4 bytes )
    //  - size in bytes of extra header ( 4 bytes )
    //  - crc for the extra header ( 4 bytes )
    public static final int HEADER_START_OFFSET = 0;
    public static final int HEADER_CRC_OFFSET = HEADER_START_OFFSET;
    public static final int HEADER_VERSION_OFFSET = HEADER_CRC_OFFSET + 4;
    public static final int HEADER_NUM_OF_ENTRY_OFFSET = HEADER_VERSION_OFFSET + 4;
    public static final int HEADER_TOTAL_BYTES_OFFSET = HEADER_NUM_OF_ENTRY_OFFSET + 4;
    public static final int HEADER_START_ID_OFFSET = HEADER_TOTAL_BYTES_OFFSET + 8;
    public static final int HEADER_END_ID_OFFSET = HEADER_START_ID_OFFSET + 8;
    public static final int HEADER_LAST_TIMESTAMP = HEADER_END_ID_OFFSET + 8;
    public static final int HEADER_RANDOM_ID_OFFSET = HEADER_LAST_TIMESTAMP + 4;
    public static final int HEADER_EXTRA_HEADER_SIZE_OFFSET = HEADER_RANDOM_ID_OFFSET + 4;
    public static final int HEADER_EXTRA_HEADER_CRC_OFFSET = HEADER_EXTRA_HEADER_SIZE_OFFSET + 4;
    static final int SEGMENT_HEADER_BYTES = HEADER_EXTRA_HEADER_CRC_OFFSET + 4;
    static final int HEADER_EXTRA_HEADER_OFFSET = SEGMENT_HEADER_BYTES;

    static final char NO_FLAGS = 0;
    static final char FLAG_COMPRESSED = 1;

    // Export Segment Entry Header layout (each segment has multiple entries):
    //  - crc of segment entry (4 bytes),
    //  - total bytes of the entry (4 bytes, compressed size if compression is enable),
    //  - entry id (random segment id + entry number) (4 bytes),
    //  - entry flag (2 bytes)
    public static final int ENTRY_HEADER_START_OFFSET = 0;
    public static final int ENTRY_HEADER_CRC_OFFSET = ENTRY_HEADER_START_OFFSET;
    public static final int ENTRY_HEADER_TOTAL_BYTES_OFFSET = ENTRY_HEADER_CRC_OFFSET + 4;
    public static final int ENTRY_HEADER_ENTRY_ID_OFFSET = ENTRY_HEADER_TOTAL_BYTES_OFFSET + 4;
    public static final int ENTRY_HEADER_FLAG_OFFSET = ENTRY_HEADER_ENTRY_ID_OFFSET + 4;
    public static final int ENTRY_HEADER_BYTES = ENTRY_HEADER_FLAG_OFFSET + 2;

    final File m_file;
    // Persistent ID of this segment. This is monotonically increasing for PBDs with requiresId=false.
    // It is the starting id in the segment otherwise, but still increasing value
    final long m_id;
    boolean m_deleteOnAck;

    private long m_fileSize = -1;

    PBDSegment(File file, long id) {
        super();
        m_file = file;
        m_id = id;
    }

    long segmentId() {
        return m_id;
    }

    File file() {
        return m_file;
    }

    public void saveFileSize() {
        m_fileSize = m_file.length();
    }

    public long getFileSize() {
        return (m_fileSize > 0) ? m_fileSize : m_file.length();
    }

    /**
     * Returns the start id stored in this segment's header.
     *
     * @throws IOException if an IO error occurs trying to read the header.
     */
    abstract long getStartId() throws IOException;

    /**
     * Returns the end id stored in this segment's header.
     *
     * @throws IOException if an IO error occurs trying to read the header.
     */
    abstract long getEndId() throws IOException ;

    /**
     * @return the timestamp of the last entry or {@code -1} if there is no timestamp
     * @throws IOException if an IO error occurs trying to read the header.
     */
    abstract long getTimestamp() throws IOException;

    abstract int getNumEntries() throws IOException;

    abstract boolean isBeingPolled();

    abstract boolean isOpenForReading(String cursorId);

    abstract PBDSegmentReader<M> openForRead(String cursorId) throws IOException;

    /**
     * Returns the reader opened for the given cursor id. This may return a closed reader if the reader has already
     * finished reading this segment.
     */
    abstract PBDSegmentReader<M> getReader(String cursorId);

    /**
     * Open and initialize this segment as a new segment
     *
     * @param compress whether or not entries should be compressed by default
     * @throws IOException
     */
    abstract void openNewSegment(boolean compress) throws IOException;

    /**
     * Open the segment for read and possible truncation
     *
     * @throws IOException
     */
    abstract void openForTruncate() throws IOException;

    abstract void closeAndDelete() throws IOException;

    abstract boolean isClosed();

    abstract void close() throws IOException;

    abstract void sync() throws IOException;

    abstract boolean hasAllFinishedReading() throws IOException;

    /**
     * Writes passed in bytes to this segment as next entry in the PBD.
     * @param cont BBContainer with the bytes to be written
     * @param startId the starting id of the data that is being offered. This should be <code>INVALID_ID</code>
     *        if the PBD does not require ids
     * @param endId the ending id of the data that is being offered. This should be <code>INVALID_ID</code>
     *        if the PBD does not require ids
     * @return the number of bytes written. If compression is enables, bytes written will be
     *         different from the number passed in. -1 will be returned if the bytes cannot
     *         fit into this segment.
     * @throws IOException if any IO error occurs trying to write to the pbd segment file.
     */
    abstract int offer(DBBPool.BBContainer cont, long startId, long endId, long timestamp) throws IOException;

    abstract int offer(DeferredSerialization ds) throws IOException;

    // TODO: javadoc
    abstract int size();

    abstract void writeExtraHeader(M extraHeader) throws IOException;

    /**
     * Update the segment to be read only
     *
     * @throws IOException If there was an error updating the segment to read only
     */
    abstract void setReadOnly() throws IOException;

    /**
     * Parse the segment and truncate the file if necessary.
     *
     * @param truncator A caller-supplied truncator that decides where in the segment to truncate
     * @return The number of objects that was truncated. This number will be subtracted from the total number of
     *         available objects in the PBD. {@link Integer#MAX_VALUE} means that this whole segment should be removed.
     *         A negative value means that the entries were truncated because of corruption and not {@code truncator}
     * @throws IOException
     */
    abstract int parseAndTruncate(BinaryDeque.BinaryDequeTruncator truncator) throws IOException;

    /**
     * Scan over all entries in a segment possibly truncating the segment if corruption is detected
     *
     * @param truncator A caller-supplied {@link BinaryDeque.BinaryDequeScanner} to scan the individual entries
     * @return The number of objects that was truncated. This number will be subtracted from the total number of
     *         available objects in the PBD.
     * @throws IOException
     */
    abstract int scan(BinaryDeque.BinaryDequeScanner scanner) throws IOException;


    abstract int validate(BinaryDeque.BinaryDequeValidator<M> validator) throws IOException;

    /**
     * Returns whether the file is final
     *
     * @see notes on {@code setFinal}
     * @return true if file is final, false otherwise
     */
    abstract boolean isFinal();

    abstract M getExtraHeader() throws IOException;

    /**
     * If this segment is in a good condition the data will be flushed to disk and the segment will either be closed or
     * converted to read only depending on the argument {@code close}
     *
     * @param close If {@code true} this segment will be closed otherwise it will be made read only
     * @throws IOException
     */
    abstract void finalize(boolean close) throws IOException;

    /**
     * Is this segment currently active for writing.
     *
     * @return returns true if this segment is currently active for writing, false otherwise
     */
    abstract boolean isActive();

    public static boolean setFinal(File file, boolean isFinal) {

        try {
            UserDefinedFileAttributeView view = getFileAttributeView(file);
            if (view != null) {
                view.write(IS_FINAL_ATTRIBUTE, Charset.defaultCharset().encode(Boolean.toString(isFinal)));
                return true;
            }
        } catch (IOException e) {
            // No-op
        }
        return false;
    }

    public static boolean isFinal(File file) {

        boolean ret = false;
        try {
            UserDefinedFileAttributeView view = getFileAttributeView(file);

            if (view != null) {
                List<String> attrList = view.list();
                if (attrList.contains(IS_FINAL_ATTRIBUTE)) {
                    ByteBuffer buf = ByteBuffer.allocate(view.size(IS_FINAL_ATTRIBUTE));
                    view.read(IS_FINAL_ATTRIBUTE, buf);
                    buf.flip();
                    ret = Boolean.parseBoolean(Charset.defaultCharset().decode(buf).toString());
                }
            }
        } catch (IOException e) {
            // No-op
        }
        return ret;
    }

    static UserDefinedFileAttributeView getFileAttributeView(File file) {
        return Files.getFileAttributeView(file.toPath(), UserDefinedFileAttributeView.class);
    }
}
