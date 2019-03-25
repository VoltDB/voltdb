/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.List;
import java.util.zip.CRC32;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DeferredSerialization;

public abstract class PBDSegment {

    private static final String TRUNCATOR_CURSOR = "__truncator__";
    private static final String SCANNER_CURSOR = "__scanner__";
    protected static final String IS_FINAL_ATTRIBUTE = "VoltDB.PBDSegment.isFinal";

    // Has to be able to hold at least one object (compressed or not)
    public static final int CHUNK_SIZE = Integer.getInteger("PBDSEGMENT_CHUNK_SIZE", 1024 * 1024 * 64);

    // Segment Header layout:
    // - version of segment headers (4 bytes)
    //  - crc of segment header (4 bytes),
    //  - total number of entries (4 bytes),
    //  - total bytes of data (4 bytes, uncompressed size),
    //  - size in bytes of extra header ( 4 bytes )
    //  - crc for the extra header ( 4 bytes )
    public static final int HEADER_START_OFFSET = 0;
    public static final int HEADER_VERSION_OFFSET = HEADER_START_OFFSET;
    public static final int HEADER_CRC_OFFSET = HEADER_VERSION_OFFSET + 4;
    public static final int HEADER_NUM_OF_ENTRY_OFFSET = HEADER_CRC_OFFSET + 4;
    public static final int HEADER_TOTAL_BYTES_OFFSET = HEADER_NUM_OF_ENTRY_OFFSET + 4;
    public static final int HEADER_EXTRA_HEADER_SIZE_OFFSET = HEADER_TOTAL_BYTES_OFFSET + 4;
    public static final int HEADER_EXTRA_HEADER_CRC_OFFSET = HEADER_EXTRA_HEADER_SIZE_OFFSET + 4;
    static final int SEGMENT_HEADER_BYTES = HEADER_EXTRA_HEADER_CRC_OFFSET + 4;
    static final int HEADER_EXTRA_HEADER_OFFSET = SEGMENT_HEADER_BYTES;

    static final char NO_FLAGS = 0;
    static final char FLAG_COMPRESSED = 1;

    // Export Segment Entry Header layout (each segment has multiple entries):
    //  - crc of segment entry (4 bytes),
    //  - total bytes of the entry (4 bytes, compressed size if compression is enable),
    //  - entry flag (2 bytes)
    public static final int ENTRY_HEADER_START_OFFSET = 0;
    public static final int ENTRY_HEADER_CRC_OFFSET = ENTRY_HEADER_START_OFFSET;
    public static final int ENTRY_HEADER_TOTAL_BYTES_OFFSET = ENTRY_HEADER_CRC_OFFSET + 4;
    public static final int ENTRY_HEADER_FLAG_OFFSET = ENTRY_HEADER_TOTAL_BYTES_OFFSET + 4;
    public static final int ENTRY_HEADER_BYTES = ENTRY_HEADER_FLAG_OFFSET + 2;

    protected final File m_file;

    protected boolean m_closed = true;
    protected FileChannel m_fc;
    //Avoid unnecessary sync with this flag
    protected boolean m_syncedSinceLastEdit = true;
    protected CRC32 m_crc;

    public PBDSegment(File file)
    {
        m_file = file;
        m_crc = new CRC32();
    }

    abstract long segmentIndex();
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

    abstract void initNumEntries(int count, int size) throws IOException;

    abstract void closeAndDelete() throws IOException;

    abstract boolean isClosed();

    abstract void close() throws IOException;

    abstract void sync() throws IOException;

    abstract boolean hasAllFinishedReading() throws IOException;

    abstract boolean offer(DBBPool.BBContainer cont) throws IOException;

    abstract int offer(DeferredSerialization ds) throws IOException;

    // TODO: javadoc
    abstract int size();

    abstract protected int writeTruncatedEntry(BinaryDeque.TruncatorResponse entry) throws IOException;

    abstract void writeExtraHeader(DeferredSerialization ds) throws IOException;

    /**
     * Force a read and validation of the header and any extra header metadata which might exist
     *
     * @throws IOException If there was an error reading or validating the header
     */
    abstract void validateHeader() throws IOException;

    /**
     * Parse the segment and truncate the file if necessary.
     * @param truncator    A caller-supplied truncator that decides where in the segment to truncate
     * @return The number of objects that was truncated. This number will be subtracted from the total number
     * of available objects in the PBD. -1 means that this whole segment should be removed.
     * @throws IOException
     */
    int parseAndTruncate(BinaryDeque.BinaryDequeTruncator truncator) throws IOException {
        if (!m_closed) {
            close();
        }
        openForTruncate();
        PBDSegmentReader reader = openForRead(TRUNCATOR_CURSOR);

        // Do stuff
        validateHeader();
        final int initialEntryCount = getNumEntries();
        int entriesTruncated = 0;
        // Zero entry count means the segment is empty or corrupted, in both cases
        // the segment can be deleted.
        if (initialEntryCount == 0) {
            reader.close();
            close();
            return -1;
        }
        int sizeInBytes = 0;

        DBBPool.BBContainer cont;
        while (true) {
            final long beforePos = reader.readOffset();

            cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY, !isFinal());
            if (cont == null) {
                break;
            }

            final int compressedLength = (int) (reader.readOffset() - beforePos - ENTRY_HEADER_BYTES);
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
                             * If truncation is occurring at the first object
                             * Whammo! Delete the file.
                             */
                            entriesTruncated = -1;
                        } else {
                            entriesTruncated = initialEntryCount - (reader.readIndex() - 1);

                            //Don't forget to update the number of entries in the file
                            initNumEntries(reader.readIndex() - 1, sizeInBytes);
                            m_fc.truncate(reader.readOffset() - (compressedLength + ENTRY_HEADER_BYTES));
                        }
                    } else {
                        assert retval.status == BinaryDeque.TruncatorResponse.Status.PARTIAL_TRUNCATE;
                        entriesTruncated = initialEntryCount - reader.readIndex();
                        //Partial object truncation
                        reader.rewindReadOffset(compressedLength + ENTRY_HEADER_BYTES);
                        final long partialEntryBeginOffset = reader.readOffset();
                        m_fc.position(partialEntryBeginOffset);

                        final int written = writeTruncatedEntry(retval);
                        sizeInBytes += written;
                        initNumEntries(reader.readIndex(), sizeInBytes);
                        m_fc.truncate(partialEntryBeginOffset + written + ENTRY_HEADER_BYTES);
                    }

                    break;
                }
            } finally {
                cont.discard();
            }
        }
        int entriesScanned = reader.readIndex();
        reader.close();
        close();
        if (entriesTruncated == 0) {
            int entriesNotScanned = initialEntryCount - entriesScanned;
            // If we checksum the file and it looks good, mark as final
            if (!isFinal() && entriesNotScanned == 0) {
                setFinal(true);
            }
            return entriesNotScanned;
        }

        return entriesTruncated;
    }

    /**
     * Scan over all entries in a segment possibly truncating the segment if corruption is detected
     *
     * @param truncator A caller-supplied {@link BinaryDeque.BinaryDequeScanner} to scan the individual entries
     * @return The number of objects that was truncated. This number will be subtracted from the total number of
     *         available objects in the PBD.
     * @throws IOException
     */
    int scan(BinaryDeque.BinaryDequeScanner scanner) throws IOException {
        PBDSegmentReader reader = openForRead(SCANNER_CURSOR);
        try {
            validateHeader();
            DBBPool.BBContainer cont = null;
            int initialEntryCount = getNumEntries();
            if (initialEntryCount == 0) {
                return 0;
            }
            while (true) {
                cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY, true);
                if (cont == null) {
                    break;
                }
                try {
                    scanner.scan(cont);
                } finally {
                    cont.discard();
                }
            }
            int entriesScanned = reader.readIndex();

            // Scan through entire file, everything looks good
            int entriesTruncated = initialEntryCount - entriesScanned;
            if (!isFinal() && entriesTruncated == 0) {
                setFinal(true);
            }

            return entriesTruncated;
        } finally {
            reader.purge();
        }
    }

    /**
     * Set or clear segment as 'final', i.e. whether segment is complete and logically immutable.
     *
     * NOTES:
     *
     * This is a best-effort feature: On any kind of I/O failure, the exception is swallowed and the
     * operation is a no-op: this will be the case on filesystems that do no support extended file
     * attributes. Also note that the {@code FileStore.supportsFileAttributeView} method does not provide
     * a reliable way to test for the availability of the extended file attributes.
     *
     * Must be called with 'true' by segment owner when it has filled the segment, written all segment
     * metadata, and after it has either closed or sync'd the segment file.
     *
     * Must be called with 'false' whenever opening segment for writing new data.
     *
     * Note that all calls to 'setFinal' are done by the class owning the segment because the segment
     * itself generally lacks context to decide whether it's final or not.
     *
     * @param isFinal   true if segment is set to final, false otherwise
     */
    public void setFinal(boolean isFinal) {
        setFinal(m_file, isFinal);
    }

    public static void setFinal(File file, boolean isFinal) {

        try {
            UserDefinedFileAttributeView view = getFileAttributeView(file);
            if (view != null) {
                view.write(IS_FINAL_ATTRIBUTE, Charset.defaultCharset().encode(new Boolean(isFinal).toString()));
            }
        } catch (IOException e) {
            // No-op
        }
    }

    /**
     * Returns whether the file is final
     *
     * @see notes on {@code setFinal}
     * @return true if file is final, false otherwise
     */
    public boolean isFinal() {
        return isFinal(m_file);
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

    protected static UserDefinedFileAttributeView getFileAttributeView(File file) {
        return Files.getFileAttributeView(file.toPath(), UserDefinedFileAttributeView.class);
    }
}
