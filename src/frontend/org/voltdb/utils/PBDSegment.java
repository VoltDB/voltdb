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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.List;
import java.util.zip.CRC32;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.export.ExportSequenceNumberTracker;

public abstract class PBDSegment {

    private static final String TRUNCATOR_CURSOR = "__truncator__";
    private static final String SCANNER_CURSOR = "__scanner__";
    protected static final String IS_FINAL_ATTRIBUTE = "VoltDB.PBDSegment.isFinal";

    // Has to be able to hold at least one object (compressed or not)
    public static final int CHUNK_SIZE = Integer.getInteger("PBDSEGMENT_CHUNK_SIZE", 1024 * 1024 * 64);

    // Segment Header layout:
    //  - crc of segment header (4 bytes),
    //  - total number of entries (4 bytes),
    //  - total bytes of data (4 bytes, uncompressed size),
    public static final int HEADER_CRC_OFFSET = 0;
    public static final int HEADER_NUM_OF_ENTRY_OFFSET = 4;
    public static final int HEADER_TOTAL_BYTES_OFFSET = 8;
    public static final int HEADER_EXTRA_HEADER_SIZE_OFFSET = 12;
    static final int SEGMENT_HEADER_BYTES = 16;

    static final int NO_FLAGS = 0;
    static final int FLAG_COMPRESSED = 1;

    // Export Segment Entry Header layout (each segment has multiple entries):
    //  - crc of segment entry (4 bytes),
    //  - total bytes of the entry (4 bytes, compressed size if compression is enable),
    //  - entry flag (4 bytes)
    public static final int ENTRY_HEADER_CRC_OFFSET = 0;
    public static final int ENTRY_HEADER_TOTAL_BYTES_OFFSET = 4;
    public static final int ENTRY_HEADER_FLAG_OFFSET = 8;
    public static final int ENTRY_HEADER_BYTES = 12;

    protected final File m_file;

    protected boolean m_closed = true;
    protected RandomAccessFile m_ras;
    protected FileChannel m_fc;
    //Avoid unnecessary sync with this flag
    protected boolean m_syncedSinceLastEdit = true;
    protected CRC32 m_segmentHeaderCRC;
    protected CRC32 m_entryCRC;

    public PBDSegment(File file)
    {
        m_file = file;
        m_segmentHeaderCRC = new CRC32();
        m_entryCRC = new CRC32();
    }

    abstract long segmentIndex();
    abstract long segmentId();
    abstract File file();

    abstract void reset();

    abstract int getNumEntries(boolean crcCheck) throws IOException;

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

    abstract void writeExtraHeader(DeferredSerialization ds) throws IOException;

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
        openForWrite(false);
        PBDSegmentReader reader = openForRead(TRUNCATOR_CURSOR);

        // Do stuff
        final int initialEntryCount = getNumEntries(true);
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
        boolean isFinal = isFinal();
        while (true) {
            final long beforePos = reader.readOffset();
            cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY, !isFinal);
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
        // If we checksum the file and it looks good, mark as final
        if (!isFinal() && entriesScanned == initialEntryCount) {
            setFinal(true);
        }

        return entriesTruncated;
    }

    /**
     * Parse the segment and truncate the file if necessary.
     * @param truncator    A caller-supplied truncator that decides where in the segment to truncate
     * @return The number of objects that was truncated. This number will be subtracted from the total number
     * of available objects in the PBD. -1 means that this whole segment should be removed.
     * @throws IOException
     */
    ExportSequenceNumberTracker scan(BinaryDeque.BinaryDequeScanner scanner) throws IOException {
        if (!m_closed) {
            throw new IOException(("Segment should not be open before truncation"));
        }

        PBDSegmentReader reader = openForRead(SCANNER_CURSOR);
        ExportSequenceNumberTracker tracker = new ExportSequenceNumberTracker();

        DBBPool.BBContainer cont = null;
        int initialEntryCount = getNumEntries(true);
        if (initialEntryCount == 0) {
            reader.close();
            return tracker;
        }
        while (true) {
            cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY, true);
            if (cont == null) {
                break;
            }
            try {
                //Handoff the object to the truncator and await a decision
                ExportSequenceNumberTracker retval = scanner.scan(cont);
                tracker.mergeTracker(retval);

            } finally {
                cont.discard();
            }
        }
        int entriesScanned = reader.readIndex();
        reader.close();
        // Forcefully close the file
        close();
        // Scan through entire file, everything looks good
        if (!isFinal() && entriesScanned == initialEntryCount) {
            setFinal(true);
        }

        return tracker;
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
