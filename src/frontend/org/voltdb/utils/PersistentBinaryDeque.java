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

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.zip.CRC32;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.Pair;
import org.voltdb.NativeLibraryLoader;
import org.voltdb.utils.BinaryDeque.TruncatorResponse.Status;
import org.voltdb.utils.BinaryDequeReader.NoSuchOffsetException;
import org.voltdb.utils.BinaryDequeReader.SeekErrorRule;
import org.voltdb.utils.PairSequencer.CyclicSequenceException;

import com.google_voltpatches.common.base.Throwables;

/**
 * A deque that specializes in providing persistence of binary objects to disk. Any object placed in the deque will be
 * persisted to disk asynchronously. Objects placed in the deque can be persisted synchronously by invoking sync. The
 * files backing this deque all start with a nonce provided at construction time followed by a segment index that is
 * stored in the filename. Files grow to a maximum size of 64 megabytes and then a new segment is created. The index
 * starts at 0. Segments are deleted once all objects from the segment have been polled and all the containers returned
 * by poll have been discarded. Push is implemented by creating new segments at the head of the deque containing the
 * objects to be pushed.
 *
 * @param M Type of extra header metadata stored in the PBD
 */
public class PersistentBinaryDeque<M> implements BinaryDeque<M> {
    public static class UnsafeOutputContainerFactory implements OutputContainerFactory {
        private static final VoltLogger LOG = new VoltLogger("HOST");

        @Override
        public BBContainer getContainer(int minimumSize) {
              final BBContainer origin = DBBPool.allocateUnsafeByteBuffer(minimumSize);
              final BBContainer retcont = new BBContainer(origin.b()) {
                  private boolean discarded = false;

                  @Override
                  public synchronized void discard() {
                      checkDoubleFree();
                      if (discarded) {
                          LOG.error("Avoided double discard in PBD");
                          return;
                      }
                      discarded = true;
                      origin.discard();
                  }
              };
              return retcont;
        }
    }

    /**
     * Used to read entries from the PBD. Multiple readers may be active at the same time,
     * but only one read or write may happen concurrently.
     */
    class ReadCursor implements BinaryDequeReader<M> {
        private final String m_cursorId;
        private PBDSegment<M> m_segment;
        // Number of objects out of the total
        //that were deleted at the time this cursor was created
        private final int m_numObjectsDeleted;
        private int m_numRead;
        // If a rewind occurred this is set to the segment id where this cursor was before the rewind
        private long m_rewoundFromId = -1;
        private boolean m_cursorClosed = false;
        private final boolean m_isTransient;

        public ReadCursor(String cursorId, int numObjectsDeleted) {
            this(cursorId, numObjectsDeleted, false);
        }

        public ReadCursor(String cursorId, int numObjectsDeleted, boolean isTransient) {
            m_cursorId = cursorId;
            m_numObjectsDeleted = numObjectsDeleted;
            m_isTransient = isTransient;
        }

        @Override
        public BBContainer poll(OutputContainerFactory ocf) throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                if (m_cursorClosed) {
                    throw new IOException("PBD.ReadCursor.poll(): " + m_cursorId + " - Reader has been closed");
                }
                assertions();

                moveToValidSegment();
                PBDSegmentReader<M> segmentReader = m_segment.getReader(m_cursorId);
                if (segmentReader == null) {
                    segmentReader = m_segment.openForRead(m_cursorId);
                }
                long lastSegmentId = peekLastSegment().segmentIndex();
                while (!segmentReader.hasMoreEntries()) {
                    if (m_segment.segmentIndex() == lastSegmentId) { // nothing more to read
                        return null;
                    }

                    // Save closed readers until everything in the segment is acked.
                    if (m_isTransient || segmentReader.allReadAndDiscarded()) {
                        segmentReader.close();
                    } else {
                        segmentReader.closeAndSaveReaderState();
                    }
                    m_segment = m_segments.higherEntry(m_segment.segmentIndex()).getValue();
                    // push to PBD will rewind cursors. So, this cursor may have already opened this segment
                    segmentReader = m_segment.getReader(m_cursorId);
                    if (segmentReader == null) {
                        segmentReader = m_segment.openForRead(m_cursorId);
                    }
                }
                BBContainer retcont = segmentReader.poll(ocf);
                if (retcont == null) {
                    return null;
                }

                m_numRead++;
                assertions();
                assert (retcont.b() != null);
                return wrapRetCont(m_segment, segmentReader.readIndex(), retcont);
            }
        }

        @Override
        public BinaryDequeReader.Entry<M> pollEntry(OutputContainerFactory ocf) throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                BBContainer retcont = poll(ocf);
                if (retcont == null) {
                    return null;
                }

                M extraHeader = m_segment.getExtraHeader();

                return new BinaryDequeReader.Entry<M>() {
                    @Override
                    public M getExtraHeader() {
                        return extraHeader;
                    }

                    @Override
                    public ByteBuffer getData() {
                        return retcont.b();
                    }

                    @Override
                    public void release() {
                        retcont.discard();
                    }
                };
            }
        }

        PBDSegment<M> getCurrentSegment() {
            return m_segment;
        }

        @Override
        public void seekToSegment(long entryId, SeekErrorRule errorRule)
                throws NoSuchOffsetException, IOException {
            // Support this only for transient readers for now.
            // if we support this for non-transient reader, revisit wrapRetCont asserts as well.
            assert(m_isTransient);
            assert(entryId >= 0);

            synchronized(PersistentBinaryDeque.this) {
                PBDSegment<M> seekSegment = findSegmentWithEntry(entryId, errorRule);
                moveToValidSegment();

                if (m_segment.segmentIndex() == seekSegment.segmentIndex()) {
                    //Close and open to rewind reader to the beginning and reset everything
                    if (m_segment.getReader(m_cursorId) != null) {
                        m_numRead -= m_segment.getReader(m_cursorId).readIndex();
                        m_segment.getReader(m_cursorId).close();
                        m_segment.openForRead(m_cursorId);
                    }
                } else { // rewind or fastforward, adjusting the numRead accordingly
                    if (m_segment.segmentIndex() > seekSegment.segmentIndex()) { // rewind
                        for (PBDSegment<M> curr : m_segments.tailMap(seekSegment.segmentIndex(), true).values()) {
                            if (curr.segmentIndex() > m_segment.segmentIndex()) {
                                break;
                            }
                            PBDSegmentReader<M> currReader = curr.getReader(m_cursorId);
                            if (curr.segmentIndex() == m_segment.segmentIndex()) {
                                if (currReader != null) {
                                    m_numRead -= currReader.readIndex();
                                }
                            } else {
                                m_numRead -= curr.getNumEntries();
                            }
                            if (currReader != null) {
                                currReader.close();
                            }
                        }
                    } else { // fastforward
                        PBDSegmentReader<M> segmentReader = m_segment.getReader(m_cursorId);
                        m_numRead += m_segment.getNumEntries();
                        if (segmentReader != null) {
                            m_numRead -= segmentReader.readIndex();
                            segmentReader.close();
                        }
                        // increment numRead
                        // TODO: Do this only if assertions are on? Unless we use m_numRead in other places too.
                        for (PBDSegment<M> curr : m_segments.tailMap(m_segment.segmentIndex(), false).values()) {
                            if (curr.segmentIndex() == seekSegment.segmentIndex()) {
                                break;
                            }
                            m_numRead += curr.getNumEntries();
                        }
                    }
                    m_segment = seekSegment;
                }
            }
        }

        /**
         * Advance the cursor to the next segment if this segment is older than the specified time.
         * This is a method implemented specifically to make it easier to implement retention policy
         * on PBDs and hence not exposed outside the package.
         *
         * @param millis time in milliseconds
         * @return returns true if the reader advanced to the next segment. False otherwise.
         * @throws IOException if there was an error reading the segments or reading the timestamp
         */
        boolean skipToNextSegmentIfOlder(long millis) throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                if (m_cursorClosed) {
                    throw new IOException("PBD.ReadCursor.skipToNextSegmentIfOlder(): " + m_cursorId + " - Reader has been closed");
                }

                long recordTime = getSegmentLastRecordTimestamp();
                if (recordTime == 0) { // Couldn't get the last record timestamp
                    throw new IOException("Could not get last record time for segment in PBD " + m_nonce);
                }

                if (System.currentTimeMillis() - recordTime < millis) { // can't skip yet
                    return false;
                }

                long lastSegmentId = peekLastSegment().segmentIndex();
                if (m_segment.segmentIndex() == lastSegmentId) {
                    return false;
                }

                skipToNextSegment(true);
                return true;
            }
        }

        @Override
        public void skipPast(long id) throws IOException {
            moveToValidSegment();
            while (id >= m_segment.getEndId() && skipToNextSegment(false));
        }

        // This is used by retention cursor and regular skip forward.
        // With retention cursors, you want to delete the current segment,
        // while with regular skip forward, you want to delete the older segments only.
        // In regular skip, current segment is ready to be deleted only after at least
        // one entry from the next segment is read and discarded.
        private boolean skipToNextSegment(boolean deleteCurrent) throws IOException {
            PBDSegmentReader<M> segmentReader = m_segment.getReader(m_cursorId);
            if (segmentReader == null) {
                segmentReader = m_segment.openForRead(m_cursorId);
            }
            m_numRead += m_segment.getNumEntries() - segmentReader.readIndex();
            segmentReader.markAllReadAndDiscarded();

            Map.Entry<Long, PBDSegment<M>> entry = m_segments.higherEntry(m_segment.segmentIndex());
            if (entry == null) { // on the last segment
                // We are marking this one as read. So OK to delete segments before this
                deleteSegmentsBefore(m_segment, 1);
                return false;
            }

            PBDSegment<M> oldSegment = m_segment;
            m_segment = entry.getValue();
            if (deleteCurrent) {
                deleteSegmentsBefore(m_segment, 1);
            } else {
                deleteSegmentsBefore(oldSegment, 1);
            }

            return true;
        }

        /**
         * Advance the cursor to the next segment if this reader has >= bytes than the specified number
         * of bytes after advancing to the next segment.
         * This is a method implemented specifically to make it easier to implement retention policy
         * on PBDs and hence not exposed outside the package.
         *
         * @param maxBytes the minimum number of bytes this reader should have after advancing to next segment.
         * @return returns returns 0 if the reader can successfully advance to the next segment.
         *         Otherwise it returns the number of bytes that must be added to the segment before it can advance.
         * @throws IOException if the reader was closed or on other error opening the segment files.
         */
        long skipToNextSegmentIfBigger(long maxBytes) throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                if (m_cursorClosed) {
                    throw new IOException("PBD.ReadCursor.skipToNextSegmentIfBigger(): " + m_cursorId + " - Reader has been closed");
                }

                moveToValidSegment();
                long segmentSize = m_segment.getFileSize();

                long lastSegmentId = peekLastSegment().segmentIndex();
                if (m_segment.segmentIndex() == lastSegmentId) { // last segment, cannot skip
                    long needed = maxBytes - segmentSize;
                    return (needed == 0) ? 1 : needed; // To fix: 0 is a special value indicating we skipped.
                }

                long readerSize = readerFizeSize();
                long diff = readerSize - segmentSize;
                if (diff < maxBytes) {
                    return maxBytes - diff;
                }

                skipToNextSegment(true);
                return 0;
            }
        }

        private long readerFizeSize() {
            long size = 0;
            for (PBDSegment<M> segment: m_segments.tailMap(m_segment.segmentIndex()).values()) {
                size += segment.getFileSize();
            }

            return size;
        }

        void rewindTo(PBDSegment<M> segment) {
            if (m_rewoundFromId == -1 && m_segment != null) {
                m_rewoundFromId = m_segment.segmentId();
            }
            m_segment = segment;
        }

        private void moveToValidSegment() {
            PBDSegment<M> firstSegment = peekFirstSegment();
            // It is possible that m_segment got closed and removed
            if (m_segment == null || m_segment.segmentIndex() < firstSegment.segmentIndex()) {
                m_segment = firstSegment;
            }
        }

        @Override
        public int getNumObjects() throws IOException {
            synchronized(PersistentBinaryDeque.this) {
                if (m_cursorClosed) {
                    throw new IOException("Cannot compute object count of " + m_cursorId + " - Reader has been closed");
                }
                return m_numObjects - m_numObjectsDeleted - m_numRead;
            }
        }

        /*
         * Don't use size in bytes to determine empty, could potentially
         * diverge from object count on crash or power failure
         * although incredibly unlikely
         */
        @Override
        public long sizeInBytes() throws IOException {
            synchronized(PersistentBinaryDeque.this) {
                if (m_cursorClosed) {
                    throw new IOException("Cannot compute size of " + m_cursorId + " - Reader has been closed");
                }
                assertions();

                moveToValidSegment();
                long size = 0;
                boolean inclusive = true;
                if (m_segment.isOpenForReading(m_cursorId)) { //this reader has started reading from curr segment.
                    // Find out how much is left to read.
                    size = m_segment.getReader(m_cursorId).uncompressedBytesToRead();
                    inclusive = false;
                }
                // Get the size of all unread segments
                for (PBDSegment<M> currSegment : m_segments.tailMap(m_segment.segmentIndex(), inclusive).values()) {
                    size += currSegment.size();
                }
                return size;
            }
        }

        @Override
        public boolean isEmpty() throws IOException {
            synchronized(PersistentBinaryDeque.this) {
                if (m_cursorClosed) {
                    throw new IOException("Closed");
                }
                assertions();

                moveToValidSegment();
                boolean inclusive = true;
                if (m_segment.isOpenForReading(m_cursorId)) { //this reader has started reading from curr segment.
                    // Check if there are more to read.
                    if (m_segment.getReader(m_cursorId).hasMoreEntries()) {
                        return false;
                    }
                    inclusive = false;
                }

                for (PBDSegment<M> currSegment : m_segments.tailMap(m_segment.segmentIndex(), inclusive).values()) {
                    if (currSegment.getNumEntries() > 0) {
                        return false;
                    }
                }

                return true;
            }
        }

        private BBContainer wrapRetCont(PBDSegment<M> segment, int entryNumber, final BBContainer retcont) {
            return new BBContainer(retcont.b()) {
                @Override
                public void discard() {
                    synchronized(PersistentBinaryDeque.this) {
                        checkDoubleFree();
                        retcont.discard();

                        // Transient readers do not affect PBD data deletion.
                        if (m_isTransient) {
                            return;
                        }

                        assert m_cursorClosed || m_segments.containsKey(segment.segmentIndex());
                        if (m_cursorClosed) {
                            return;
                        }

                        //Close and remove segment readers that were all acked and before current PBD reader segment.
                        PBDSegmentReader<M> segmentReader = segment.getReader(m_cursorId);
                        assert(segmentReader != null); // non-transient reader is only closed and removed after all are acked

                        assert(m_segment != null);
                        // If the reader has moved past this and all have been acked close this segment reader.
                        if (segmentReader.allReadAndDiscarded() && segment.segmentIndex() < m_segment.m_index) {
                            try {
                                segmentReader.close();
                            } catch(IOException e) {
                                m_usageSpecificLog.warn("Unexpected error closing PBD file " + m_segment.m_file, e);
                            }
                        }

                        deleteSegmentsOnAck(segment, entryNumber);
                    }
                }
            };
        }

        private void deleteSegmentsOnAck(PBDSegment<M> segment, int entryNumber) {
            // Only continue if open and there is another segment
            if (m_cursorClosed || m_segments.size() == 1) {
                return;
            }

            // If this segment is already marked for deletion, check if it is the first one
            // and delete it if fully acked. All the subsequent marked ones can be checked and deleted as well.
            if (segment.m_deleteOnAck == true && segment.segmentIndex() == m_segments.firstKey()) {
                Iterator<PBDSegment<M>> itr = m_segments.values().iterator();
                while (itr.hasNext()) {
                    PBDSegment<M> toDelete = itr.next();
                    try {
                        if (toDelete.m_deleteOnAck == true && canDeleteSegment(toDelete)) {
                            itr.remove();
                            closeAndDeleteSegment(toDelete);
                        } else {
                            break;
                        }
                    } catch(IOException e) {
                        m_usageSpecificLog.error("Unexpected error deleting segment after all have been read and acked", e);
                    }
                }

                return;
            }

            deleteSegmentsBefore(segment, entryNumber);
        }

        private void deleteSegmentsBefore(PBDSegment<M> segment, int entryNumber) {
            // If this is the first entry of a segment, see if previous segments can be deleted or marked ready to delete
            if (m_cursorClosed || m_segments.size() == 1
                    || (entryNumber != 1 && m_rewoundFromId != segment.m_id)
                    || !canDeleteSegmentsBefore(segment)) {
                return;
            }

            if (m_rewoundFromId == segment.m_id) {
                m_rewoundFromId = -1;
            }

            try {
                Iterator<PBDSegment<M>> iter = m_segments.headMap(segment.segmentIndex(), false).values()
                        .iterator();
                boolean markForDel = false;
                while (iter.hasNext()) {
                    // Only delete a segment if all buffers have been acked.
                    // If not, mark this and all following segments for deletion.
                    PBDSegment<M> earlierSegment = iter.next();
                    if (markForDel) {
                        earlierSegment.m_deleteOnAck = true;
                    } else if (canDeleteSegment(earlierSegment)) {
                        iter.remove();
                        if (m_usageSpecificLog.isDebugEnabled()) {
                            m_usageSpecificLog.debug("Segment " + earlierSegment.file()
                            + " has been closed and deleted after discarding last buffer");
                        }
                        closeAndDeleteSegment(earlierSegment);
                    } else {
                        earlierSegment.m_deleteOnAck = true;
                        markForDel = true;
                    }
                }
            } catch (IOException e) {
                m_usageSpecificLog.error("Exception closing and deleting PBD segment", e);
            }
        }

        private boolean canDeleteSegment(PBDSegment<M> segment) throws IOException {
            if (segment.getNumEntries() == 0) {
                return true;
            }

            for (ReadCursor cursor : m_readCursors.values()) {
                if (cursor.m_isTransient) {
                    continue;
                }
                PBDSegmentReader<M> segmentReader = segment.getReader(cursor.m_cursorId);
                // segment reader is null if the cursor hasn't reached this segment or
                // all has been read and discarded.
                if (segmentReader == null &&
                        (cursor.m_segment == null || cursor.m_segment.segmentIndex() <= segment.segmentIndex()) ) {
                    return false;
                }

                if (segmentReader != null && !segmentReader.allReadAndDiscarded()) {
                    return false;
                }
            }

            return true;
        }

        void close() {
            if (m_segment != null) {
                PBDSegmentReader<M> reader = m_segment.getReader(m_cursorId);
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        m_usageSpecificLog.warn("Failed to close reader " + reader, e);
                    }
                }
                m_segment = null;
            }
            m_cursorClosed = true;
        }

        @Override
        public boolean isOpen() {
            return !m_cursorClosed;
        }

        /**
         * Used to find out if the current segment that this reader is on, is one that is being actively written to.
         *
         * @return Returns true if the current segment that the reader is on is also being written to. False otherwise.
         */
        boolean isCurrentSegmentActive() {
            synchronized(PersistentBinaryDeque.this) {
                moveToValidSegment();
                return m_segment.isActive();
            }
        }

        /**
         * Returns the time the last record in this segment was written.
         *
         * @return timestamp in millis of the last record in this segment.
         */
        public long getSegmentLastRecordTimestamp() {
            synchronized(PersistentBinaryDeque.this) {
                moveToValidSegment();
                return m_segment.file().lastModified();
            }
        }
    }

    public static final OutputContainerFactory UNSAFE_CONTAINER_FACTORY = new UnsafeOutputContainerFactory();

    /**
     * Processors also log using this facility.
     */
    private final VoltLogger m_usageSpecificLog;

    private final File m_path;
    private final String m_nonce;
    private final boolean m_compress;
    private final PBDSegmentFactory m_pbdSegmentFactory;
    private boolean m_initializedFromExistingFiles = false;

    private final BinaryDequeSerializer<M> m_extraHeaderSerializer;

    //Segments that are no longer being written to and can be polled
    //These segments are "immutable". They will not be modified until deletion
    private final TreeMap<Long, PBDSegment<M>> m_segments = new TreeMap<>();
    private volatile boolean m_closed = false;
    private final HashMap<String, ReadCursor> m_readCursors = new HashMap<>();
    private int m_numObjects;
    private int m_numDeleted;

    // Monotonic segment counter: note that this counter always *increases* even when
    // used for a *previous* segment (or inserting a segment *before* the others.
    private long m_segmentCounter = 0L;

    private M m_extraHeader;
    private PBDRetentionPolicy m_retentionPolicy;
    private Boolean m_requiresId;

    /**
     * Create a persistent binary deque with the specified nonce and storage back at the specified path. This is a
     * convenience method for testing so that poll with delete can be tested.
     *
     * @param m_nonce
     * @param extraHeader
     * @param m_path
     * @param deleteEmpty
     * @throws IOException
     */
    private PersistentBinaryDeque(Builder<M> builder) throws IOException {
        NativeLibraryLoader.loadVoltDB();
        m_path = builder.m_path;
        m_nonce = builder.m_nonce;
        m_usageSpecificLog = builder.m_logger;
        m_compress = builder.m_useCompression;
        m_extraHeader = builder.m_initialExtraHeader;
        m_extraHeaderSerializer = builder.m_extraHeaderSerializer;
        m_pbdSegmentFactory = builder.m_pbdSegmentFactory;

        if (!m_path.exists() || !m_path.canRead() || !m_path.canWrite() || !m_path.canExecute()
                || !m_path.isDirectory()) {
            throw new IOException(
                    m_path + " is not usable ( !exists || !readable " +
                    "|| !writable || !executable || !directory)");
        }

        parseFiles(builder.m_deleteExisting);

        // Find the first and last segment for polling and writing (after); ensure the
        // writing segment is not final

        long curId = getNextSegmentId();
        Map.Entry<Long, PBDSegment<M>> lastEntry = m_segments.lastEntry();

        // Note: the "previous" id value may be > "current" id value
        long prevId = lastEntry == null ? getNextSegmentId() : lastEntry.getValue().segmentId();
        Long writeSegmentIndex = lastEntry == null ? 1L : lastEntry.getKey() + 1;

        String fname = getSegmentFileName(curId, prevId);
        PBDSegment<M> writeSegment = initializeNewSegment(writeSegmentIndex, curId, new VoltFile(m_path, fname),
                "initialization");
        if (m_segments.put(writeSegmentIndex, writeSegment) != null) {
            // Sanity check
            throw new IllegalStateException("Overwriting segment " + writeSegmentIndex);
        }

        m_numObjects = countNumObjects();
        assertions();
    }

    TreeMap<Long, PBDSegment<M>> getSegments() {
        return m_segments;
    }

    String getNonce() {
        return m_nonce;
    }

    VoltLogger getUsageSpecificLog() {
        return m_usageSpecificLog;
    }

    /**
     * @return the next segment id
     */
    private synchronized long getNextSegmentId() {
        // reset to 0 when overflow
        if (m_segmentCounter == Long.MAX_VALUE) {
            m_segmentCounter = 0L;
        }
        long newId = ++m_segmentCounter;
        return newId;
    }

    /**
     * Return a segment file name from m_nonce and current + previous segment ids.
     *
     * @see parseFiles for file name structure
     * @param currentId   current segment id
     * @param previousId  previous segment id
     * @return  segment file name
     */
    private String getSegmentFileName(long currentId, long previousId) {
        return PbdSegmentName.createName(m_nonce, currentId, previousId, false);
    }

    /**
     * Extract the previous segment id from a file name.
     *
     * Note that the filename is assumed valid at this point.
     *
     * @see parseFiles for file name structure
     * @param file
     * @return
     */
    private long getPreviousSegmentId(File file) {
        PbdSegmentName segmentName = PbdSegmentName.parseFile(m_usageSpecificLog, file);
        if (segmentName.m_result != PbdSegmentName.Result.OK) {
            throw new IllegalStateException("Invalid file name: " + file.getName());
        }
        return segmentName.m_prevId;
    }

    /**
     * Parse files for this PBD; if creating, delete any crud left by a previous homonym.
     *
     * @param deleteExisting true if should delete any existing PBD files
     *
     * @throws IOException
     */
    private void parseFiles(boolean deleteExisting) throws IOException {

        HashMap<Long, PbdSegmentName> filesById = new HashMap<>();
        PairSequencer<Long> sequencer = new PairSequencer<>();
        List<String> invalidPbds = new ArrayList<>();
        try {
            for (File file : m_path.listFiles()) {
                if (file.isDirectory() || !file.isFile() || file.isHidden()) {
                    continue;
                }

                PbdSegmentName segmentName = PbdSegmentName.parseFile(m_usageSpecificLog, file);

                switch (segmentName.m_result) {
                case INVALID_NAME:
                case INVALID_VERSION:
                    invalidPbds.add(file.getName());
                    //$FALL-THROUGH$
                case NOT_PBD:
                    continue;
                default:
                }

                // Is this one of our PBD files?
                if (!m_nonce.equals(segmentName.m_nonce)) {
                    // Not my PBD
                    continue;
                }

                // From now on we're dealing with one of our PBD files
                if (file.length() == 0 || deleteExisting) {
                    deleteStalePbdFile(file, deleteExisting);
                    continue;
                }

                long maxCnt = Math.max(segmentName.m_id, segmentName.m_prevId);
                if (m_segmentCounter < maxCnt) {
                    m_segmentCounter = maxCnt;
                }
                filesById.put(segmentName.m_id, segmentName);
                sequencer.add(new Pair<Long, Long>(segmentName.m_prevId, segmentName.m_id));
            }

            if (!invalidPbds.isEmpty()) {
                if (m_usageSpecificLog.isDebugEnabled()) {
                    m_usageSpecificLog.debug("Found invalid PBDs in " + m_path + ": " + invalidPbds);
                } else {
                    m_usageSpecificLog.warn("Found " + invalidPbds.size() + " invalid PBD"
                            + (invalidPbds.size() > 1 ? "s" : "") + " in " + m_path);
                }
            }

            // Handle common cases: no PBD files or just one
            if (filesById.size() == 0) {
                if (m_usageSpecificLog.isDebugEnabled()) {
                    m_usageSpecificLog.debug("No PBD segments for " + m_nonce);
                }
                return;
            }

            m_initializedFromExistingFiles = true;
            if (filesById.size() == 1) {
                // Common case, only 1 PBD segment
                for (Map.Entry<Long, PbdSegmentName> entry : filesById.entrySet()) {
                    recoverSegment(1, entry.getKey(), entry.getValue());
                    break;
                }
            } else {
                // Handle the uncommon case of more than 1 PBD file, get the sequence of segments
                Deque<Deque<Long>> sequences = sequencer.getSequences();
                if (sequences.size() > 1) {
                    // FIXME: reject this case for now
                    // FIXME: we could select the sequence that has the oldest entry and delete
                    // the other files
                    StringBuilder sb = new StringBuilder();
                    for (Deque<Long> seq : sequences) {
                        sb.append("\nsequence:" + seq);
                    }
                    throw new IOException("Found " + sequences.size() + " PBD sequences for " + m_nonce +
                            sb.toString() );
                }
                Deque<Long> sequence = sequences.getFirst();
                long index = 1L;
                for (Long segmentId : sequence) {
                    PbdSegmentName segmentName = filesById.get(segmentId);
                    if (segmentName == null) {
                        // This is an Id in the sequence referring to a previous file that
                        // was deleted, so move on.
                        continue;
                    }
                    recoverSegment(index++, segmentId, segmentName);
                }
            }
        } catch (CyclicSequenceException e) {
            m_usageSpecificLog.error("Failed to parse files: " + e);
            throw new IOException(e);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException) {
                throw new IOException(e);
            }
            throw(e);
        }
    }

    /**
     * Delete a PBD segment that was identified as 'stale' i.e. produced by earlier VoltDB releases
     *
     * Note that this file may be concurrently deleted from multiple instances so we ignore
     * NoSuchFileException.
     *
     * @param file  file to delete
     * @param create true if creating PBD
     * @throws IOException
     */
    private void deleteStalePbdFile(File file, boolean create) throws IOException {
        try {
            PBDSegment.setFinal(file, false);
            if (m_usageSpecificLog.isDebugEnabled()) {
                String createStr = create ? ", forced by creation." : "";
                m_usageSpecificLog.debug("Segment " + file.getName()
                        + " (final: " + PBDSegment.isFinal(file) + "), is closed and deleted during init" + createStr);
            }
            file.delete();
        } catch (Exception e) {
            if (e instanceof NoSuchFileException) {
                // Concurrent delete, noop
            } else {
                throw e;
            }
        }
    }

    /**
     * Quarantine a segment which was already added to {@link #m_segments}
     *
     * @param prevEntry {@link Map.Entry} from {@link #m_segments} to quarantine
     * @throws IOException
     */
    void quarantineSegment(Map.Entry<Long, PBDSegment<M>> prevEntry) throws IOException {
        quarantineSegment(prevEntry, prevEntry.getValue(), prevEntry.getValue().getNumEntries());
    }

    /**
     * Quarantine a segment which has not yet been added to {@link #m_segments}
     *
     * @param segment
     * @throws IOException
     */
    private void quarantineSegment(PBDSegment<M> segment) throws IOException {
        quarantineSegment(null, segment, 0);
    }

    private void quarantineSegment(Map.Entry<Long, PBDSegment<M>> prevEntry, PBDSegment<M> segment,
            int decrementEntryCount)
            throws IOException {
        try {
            PbdSegmentName quarantinedSegment = PbdSegmentName.asQuarantinedSegment(m_usageSpecificLog, segment.file());
            if (!segment.file().renameTo(quarantinedSegment.m_file)) {
                throw new IOException("Failed to quarantine segment: " + segment.file());
            }

            PBDSegment<M> quarantined = new PbdQuarantinedSegment<>(quarantinedSegment.m_file, segment.segmentIndex(),
                    segment.segmentId());

            if (prevEntry == null) {
                PBDSegment<M> prev = m_segments.put(segment.segmentIndex(), quarantined);
                assert prev == null;
            } else {
                PBDSegment<M> prev = prevEntry.setValue(quarantined);
                assert segment == prev;
            }
            m_numObjects -= decrementEntryCount;
        } finally {
            segment.close();
        }
    }

    private PBDSegment<M> findValidSegmentFrom(PBDSegment<M> segment, boolean higher, long indexLimit) throws IOException {
        if (segment == null || segment.getNumEntries() > 0) {
            return segment;
        }

        // skip past quarantined segments
        while (segment != null && segment.getNumEntries() == 0) {
            if (segment.segmentIndex() == indexLimit) {
                segment = null;
                break;
            }
            if (higher) {
                segment = m_segments.get(segment.segmentIndex()+1);
            } else {
                segment = m_segments.get(segment.segmentIndex()-1);
            }
        }

        return segment;
    }

    private PBDSegment<M> findSegmentWithEntry(long entryId, SeekErrorRule errorRule) throws NoSuchOffsetException, IOException {
        long invalidKey = m_segments.firstKey()-1;
        PBDSegment<M> first = findValidSegmentFrom(peekFirstSegment(), true, invalidKey);
        if (first == null) {
            throw new NoSuchOffsetException("Offset " + entryId + "not found. Empty PBD");
        }

        if (!m_requiresId) {
            throw new IllegalStateException("Seek is not supported in PBDs that don't store id ranges");
        }

        PBDSegment<M> last = findValidSegmentFrom(peekLastSegment(), false, invalidKey);

        if (first.getStartId() > entryId) {
            if (errorRule == SeekErrorRule.SEEK_AFTER) {
                return first;
            } else {
                throw new NoSuchOffsetException("PBD[" + first.getStartId() + "-" +  last.getEndId() +
                        "] does not contain offset: " + entryId);
            }
        }
        if (last.getEndId() < entryId) {
            if (errorRule == SeekErrorRule.SEEK_BEFORE) {
                return last;
            } else {
                throw new NoSuchOffsetException("PBD[" + first.getStartId() + "-" +  last.getEndId() +
                        "] does not contain offset: " + entryId);
            }
        }

        // Assuming these are the common cases, eliminate these.
        if (entryId >= first.getStartId() && entryId <= first.getEndId()) {
            return first;
        }
        if (entryId >= last.getStartId() && entryId <= last.getEndId()) {
            return last;
        }

        // At this point, either the entryId is within a segment or it is in a gap between segments.
        // (As of current use of PBDs, there cannot be gaps in the middle of a segment).
        long low = first.segmentIndex();
        long high = last.segmentIndex();
        while (low <= high) {
            long mid = (low + high) / 2;
            PBDSegment<M> midSegment = m_segments.get(mid);
            // search up and down to skip over quarantined segments and find a valid segment.
            // We must find one because of checks above that verify that the entryId is within the bounds of the PBD
            PBDSegment<M> valid = findValidSegmentFrom(midSegment, true, high+1);
            midSegment = (valid == null) ? findValidSegmentFrom(valid, false, low-1) : valid;
            if (entryId >= midSegment.getStartId() && entryId <= midSegment.getEndId()) {
                return midSegment;
            } else if (entryId > midSegment.getEndId()) {
                low = mid + 1;
            } else if (entryId < midSegment.getStartId()) {
                assert(mid > first.segmentIndex()); // because of the checks above
                // entryId is at a gap or in one of the lower segments
                PBDSegment<M> prevSegment = findValidSegmentFrom(m_segments.get(mid-1), false, invalidKey);
                if (entryId > prevSegment.getEndId()) { // entryId is at a gap
                    switch(errorRule) {
                    case THROW       : throw new NoSuchOffsetException("Could not find entry with offset " + entryId);
                    case SEEK_AFTER  : return midSegment;
                    case SEEK_BEFORE : return prevSegment;
                    default          : throw new IllegalArgumentException("Unsupported SeekErrorRule: " + errorRule);
                    }
                } else {
                    high = mid - 1;
                }
            }
        }

        // Should not get here because of the initial checks
        throw new RuntimeException("Unexpected error. Could not find offset " + entryId);
    }

    /**
     * Recover a PBD segment and add it to m_segments
     *
     * @param segment
     * @param deleteEmpty
     * @throws IOException
     */
    private void recoverSegment(long segmentIndex, long segmentId, PbdSegmentName segmentName) throws IOException {
        PBDSegment<M> segment;
        if (segmentName.m_quarantined) {
            segment = new PbdQuarantinedSegment<>(segmentName.m_file, segmentIndex, segmentId);
        } else {
            segment = m_pbdSegmentFactory.create(segmentIndex, segmentId, segmentName.m_file, m_usageSpecificLog,
                    m_extraHeaderSerializer);
            segment.saveFileSize();

            try {
                // Delete preceding empty segment
                if (segment.getNumEntries() == 0 && m_segments.isEmpty()) {
                    if (m_usageSpecificLog.isDebugEnabled()) {
                        m_usageSpecificLog.debug("Found Empty Segment with entries: " + segment.getNumEntries()
                                + " For: " + segment.file().getName());
                        m_usageSpecificLog.debug("Segment " + segment.file() + " (final: " + segment.isFinal()
                                + "), will be closed and deleted during init");
                    }
                    segment.closeAndDelete();
                    return;
                }

                boolean requiresId = (segment.getStartId() >= 0);
                assert(m_requiresId == null || m_requiresId.booleanValue() == requiresId);
                assert(segment.getEndId() >= segment.getStartId());
                assert(!requiresId || peekLastSegment() == null || peekLastSegment().getEndId() < segment.getStartId());
                m_requiresId = requiresId;

                // Any recovered segment that is not final should be checked
                // for internal consistency.
                if (!segment.isFinal()) {
                    m_usageSpecificLog.warn("Segment " + segment.file() + " (final: " + segment.isFinal()
                            + "), has been recovered but is not in a final state");
                } else if (m_usageSpecificLog.isDebugEnabled()) {
                    m_usageSpecificLog.debug(
                            "Segment " + segment.file() + " (final: " + segment.isFinal() + "), has been recovered");
                }
            } catch (IOException e) {
                m_usageSpecificLog.warn(
                        "Failed to retrieve entry count from segment " + segment.file() + ". Quarantining segment", e);
                quarantineSegment(segment);
                return;
            } finally {
                segment.close();
            }
        }
        m_segments.put(segment.segmentIndex(), segment);
    }

    private int countNumObjects() throws IOException {
        int numObjects = 0;
        for (PBDSegment<M> segment : m_segments.values()) {
            numObjects += segment.getNumEntries();
        }

        return numObjects;
    }

    @Override
    public synchronized void parseAndTruncate(BinaryDequeTruncator truncator) throws IOException {
        if (m_closed) {
            throw new IOException("Cannot parseAndTruncate(): PBD has been closed");
        }

        assertions();
        if (m_segments.isEmpty()) {
            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("PBD " + m_nonce + " has no finished segments");
            }
            return;
        }

        // Close the last write segment for now, will reopen after truncation
        peekLastSegment().close();

        /*
         * Iterator all the objects in all the segments and pass them to the truncator
         * When it finds the truncation point
         */
        Long lastSegmentIndex = null;
        for (Map.Entry<Long, PBDSegment<M>> entry : m_segments.entrySet()) {
            PBDSegment<M> segment = entry.getValue();
            final long segmentIndex = segment.segmentIndex();

            final int truncatedEntries;
            try {
                truncatedEntries = segment.parseAndTruncate(truncator);
            } catch (IOException e) {
                m_usageSpecificLog.warn("Error performing parse and trunctate on segment " + segment.file()
                        + ". Marking segment quarantined", e);
                quarantineSegment(entry);
                continue;
            }

            if (truncatedEntries == Integer.MAX_VALUE) {
                // This whole segment will be truncated in the truncation loop below
                lastSegmentIndex = segmentIndex - 1;
                break;
            } else if (truncatedEntries != 0) {
                m_numObjects -= Math.abs(truncatedEntries);
                if (truncatedEntries > 0) {
                    // Set last segment and break the loop over this segment
                    lastSegmentIndex = segmentIndex;
                    break;
                } else if (segment.getNumEntries() == 0) {
                    // All entries were truncated because of corruption mark the segment as quarantined
                    quarantineSegment(entry);
                }
            }
            // truncatedEntries == 0 means nothing is truncated in this segment,
            // should move on to the next segment.
        }

        /*
         * If it was found that no truncation is necessary, lastSegmentIndex will be null.
         * Return and the parseAndTruncate is a noop, except for the finalization.
         */
        if (lastSegmentIndex == null)  {
            // Reopen the last segment for write - ensure it is not final
            PBDSegment<M> lastSegment = peekLastSegment();
            assert lastSegment.getNumEntries() == 0 : "Segment has entries: " + lastSegment.file();
            lastSegment.openNewSegment(m_compress);

            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + lastSegment.file()
                    + " (final: " + lastSegment.isFinal() + "), has been opened for writing after truncation");
            }
            return;
        }
        /*
         * Now truncate all the segments after the truncation point.
         */
        Iterator<PBDSegment<M>> iterator = m_segments.tailMap(lastSegmentIndex, false).values().iterator();
        while (iterator.hasNext()) {
            PBDSegment<M> segment = iterator.next();
            m_numObjects -= segment.getNumEntries();
            iterator.remove();

            // Ensure the file is not final before closing and truncating
            segment.closeAndDelete();

            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + segment.file()
                    + " (final: " + segment.isFinal() + "), has been closed and deleted by truncator");
            }
        }

        /*
         * Reset the poll and write segments
         */
        long curId = getNextSegmentId();

        PBDSegment<M> lastSegment = peekLastSegment();
        Long newSegmentIndex = lastSegment == null ? 1L : lastSegment.segmentIndex() + 1;
        long prevId = lastSegment == null ? getNextSegmentId() : lastSegment.segmentId();

        String fname = getSegmentFileName(curId, prevId);
        PBDSegment<M> newSegment = initializeNewSegment(newSegmentIndex, curId, new VoltFile(m_path, fname),
                "PBD truncator");
        m_segments.put(newSegment.segmentIndex(), newSegment);
        assertions();
    }

    private PBDSegment<M> initializeNewSegment(long segmentIndex, long segmentId, File file, String reason)
            throws IOException {
        return initializeNewSegment(segmentIndex, segmentId, file, reason, m_extraHeader);
    }

    private PBDSegment<M> initializeNewSegment(long segmentIndex, long segmentId, File file, String reason,
            M extraHeader)
            throws IOException {
        PBDSegment<M> segment = m_pbdSegmentFactory.create(segmentIndex, segmentId, file, m_usageSpecificLog,
                m_extraHeaderSerializer);
        try {
            segment.openNewSegment(m_compress);
            if (extraHeader != null) {
                segment.writeExtraHeader(extraHeader);
            }

            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + segment.file() + " (final: " + segment.isFinal()
                        + "), has been opened for writing because of " + reason);
            }
        } catch (Throwable t) {
            segment.close();
            throw t;
        }

        return segment;
    }

    private PBDSegment<M> peekFirstSegment() {
        Map.Entry<Long, PBDSegment<M>> entry = m_segments.firstEntry();
        // entry may be null in ctor and while we are manipulating m_segments in addSegment, for example
        return (entry==null) ? null : entry.getValue();
    }

    private PBDSegment<M> peekLastSegment() {
        Map.Entry<Long, PBDSegment<M>> entry = m_segments.lastEntry();
        // entry may be null in ctor and while we are manipulating m_segments in addSegment, for example
        return (entry==null) ? null : entry.getValue();
    }

    @Override
    public synchronized void updateExtraHeader(M extraHeader) throws IOException {
        m_extraHeader = extraHeader;
        addSegment(peekLastSegment());
    }

    @Override
    public synchronized int offer(BBContainer object) throws IOException {
        return offer(object, -1, -1);
    }

    @Override
    public synchronized int offer(BBContainer object, long startId, long endId) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Closed");
        }

        assert((startId >= 0 && endId >= startId) || (startId == -1 && endId == -1));
        boolean requires = startId >= 0;
        assert(m_requiresId == null || m_requiresId.booleanValue() == requires);
        m_requiresId = requires;

        PBDSegment<M> tail = peekLastSegment();
        int written = tail.offer(object, startId, endId);
        if (written < 0) {
            tail = addSegment(tail);
            written = tail.offer(object, startId, endId);
            if (written < 0) {
                throw new IOException("Failed to offer object in PBD");
            }
        }
        m_numObjects++;
        assertions();
        callBytesAdded(written);
        return written;
    }

    @Override
    public synchronized int offer(DeferredSerialization ds) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Cannot offer(): PBD has been Closed");
        }

        assert(m_requiresId == null || m_requiresId.booleanValue() == false);
        m_requiresId = false;

        PBDSegment<M> tail = peekLastSegment();
        int written = tail.offer(ds);
        if (written < 0) {
            tail = addSegment(tail);
            written = tail.offer(ds);
            if (written < 0) {
                throw new IOException("Failed to offer object in PBD");
            }
        }
        m_numObjects++;
        assertions();
        callBytesAdded(written);
        return written;
    }

    private void callBytesAdded(int dataBytes) {
        if (m_retentionPolicy != null) {
            m_retentionPolicy.bytesAdded(dataBytes + PBDSegment.ENTRY_HEADER_BYTES);
        }
    }

    private PBDSegment<M> addSegment(PBDSegment<M> tail) throws IOException {
        //Check to see if the tail is completely consumed so we can close it
        tail.finalize(!tail.isBeingPolled());
        tail.saveFileSize();

        if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug(
                    "Segment " + tail.file() + " (final: " + tail.isFinal() + "), has been closed by offer to PBD");
        }
        Long nextIndex = tail.segmentIndex() + 1;
        long lastId = tail.segmentId();

        long curId = getNextSegmentId();
        String fname = getSegmentFileName(curId, lastId);
        PBDSegment<M> newSegment = initializeNewSegment(nextIndex, curId, new VoltFile(m_path, fname), "an offer");
        m_segments.put(newSegment.segmentIndex(), newSegment);

        if (m_retentionPolicy != null) {
            m_retentionPolicy.newSegmentAdded();
        }

        return newSegment;
    }

    private void closeAndDeleteSegment(PBDSegment<M> segment) throws IOException {
        int toDelete = segment.getNumEntries();
        if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug("Closing and deleting segment " + segment.file()
                + " (final: " + segment.isFinal() + ")");
        }
        if (assertionsOn) { // track the numRead for transient readers, when we delete ones not read by them.
            for (ReadCursor reader : m_readCursors.values()) {
                if (!reader.m_isTransient) {
                    continue;
                }
                if (reader.m_segment == null) {
                    reader.m_numRead += toDelete;
                } else if (segment.m_index >= reader.m_segment.m_index) {
                    PBDSegmentReader<M> sreader = segment.getReader(reader.m_cursorId);
                    reader.m_numRead += toDelete - (sreader == null ? 0 : sreader.readIndex());
                }
            }
        }

        segment.closeAndDelete();
        m_numDeleted += toDelete;
    }

    @Override
    public synchronized void push(BBContainer[] objects) throws IOException {
        push(objects, m_extraHeader);
    }

    @Override
    public synchronized void push(BBContainer objects[], M extraHeader) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Cannot push(): PBD has been Closed");
        }

        ArrayDeque<ArrayDeque<BBContainer>> segments = new ArrayDeque<ArrayDeque<BBContainer>>();
        ArrayDeque<BBContainer> currentSegment = new ArrayDeque<BBContainer>();

        //Take the objects that were provided and separate them into deques of objects
        //that will fit in a single write segment
        int maxObjectSize = PBDSegment.CHUNK_SIZE - PBDSegment.SEGMENT_HEADER_BYTES;
        if (extraHeader != null) {
            maxObjectSize -= m_extraHeaderSerializer.getMaxSize(extraHeader);
        }

        int available = maxObjectSize;
        for (BBContainer object : objects) {
            int needed = PBDSegment.ENTRY_HEADER_BYTES + object.b().remaining();

            if (available < needed) {
                if (needed > maxObjectSize) {
                    throw new IOException("Maximum object size is " + maxObjectSize);
                }
                segments.offer(currentSegment);
                currentSegment = new ArrayDeque<BBContainer>();
                available = maxObjectSize;
            }
            available -= needed;
            currentSegment.add(object);
        }

        segments.offer(currentSegment);
        assert(segments.size() > 0);

        // Calculate first index to push
        PBDSegment<M> first = peekFirstSegment();
        Long nextIndex = first == null ? 1L : first.segmentIndex() - 1;

        // The first segment id is either the "previous" of the current head
        // (this avoids having to rename the file of the current head), or new id.
        long curId = first == null ? getNextSegmentId() : getPreviousSegmentId(first.file());

        while (segments.peek() != null) {
            ArrayDeque<BBContainer> currentSegmentContents = segments.poll();

            long prevId = getNextSegmentId();
            String fname = getSegmentFileName(curId, prevId);

            PBDSegment<M> writeSegment = initializeNewSegment(nextIndex, curId, new VoltFile(m_path, fname), "a push",
                    extraHeader);

            // Prepare for next file
            nextIndex--;
            curId = prevId;

            while (currentSegmentContents.peek() != null) {
                writeSegment.offer(currentSegmentContents.pollFirst(), -1, -1);
                m_numObjects++;
            }

            // If this segment is to become the writing segment, don't close and
            // finalize it.
            if (!m_segments.isEmpty()) {
                writeSegment.finalize(true);
            }

            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + writeSegment.file()
                    + " (final: " + writeSegment.isFinal() + "), has been created because of a push");
            }

            m_segments.put(writeSegment.segmentIndex(), writeSegment);
        }
        // Because we inserted at the beginning, cursors need to be rewound to the beginning
        rewindCursors();
        assertions();
    }

    private void rewindCursors() {
        PBDSegment<M> firstSegment = peekFirstSegment();
        for (ReadCursor cursor : m_readCursors.values()) {
            cursor.rewindTo(firstSegment);
        }
    }

    @Override
    public synchronized ReadCursor openForRead(String cursorId) throws IOException {
        return openForRead(cursorId, false);
    }

    @Override
    public synchronized ReadCursor openForRead(String cursorId, boolean isTransient) throws IOException {
        if (m_closed) {
            throw new IOException("Cannot openForRead(): PBD has been Closed");
        }

        ReadCursor reader = m_readCursors.get(cursorId);
        if (reader == null) {
            reader = new ReadCursor(cursorId, m_numDeleted, isTransient);
            m_readCursors.put(cursorId, reader);
        }
        assert(reader.m_isTransient == isTransient);

        return reader;
    }

    synchronized boolean isCursorOpen(String cursorId) {
        return m_readCursors.containsKey(cursorId);
    }

    @Override
    public synchronized void closeCursor(String cursorId, boolean purgeOnLastCursor) {
        if (m_closed) {
            return;
        }
        ReadCursor reader = m_readCursors.remove(cursorId);
        if (reader == null) {
            return;
        }
        reader.close();

        // Check all segments from latest to oldest to see if segments before that segment can be deleted.
        //
        // In the one-to-many DR use case, the snapshot placeholder cursor prevents purging segments that have
        // been read by the other cursors. Therefore, DR calls this method with {@code purgeOnLastCursor} == true,
        // in order to ensure that closing the last DR cursor will purge those segments.
        if (m_readCursors.isEmpty() && !purgeOnLastCursor) {
            return;
        }
        try {
            boolean deleteSegment = false;
            Iterator<PBDSegment<M>> iter = m_segments.descendingMap().values().iterator();
            while (iter.hasNext()) {
                PBDSegment<M> segment = iter.next();
                if (deleteSegment) {
                    closeAndDeleteSegment(segment);
                    iter.remove();
                } else {
                    deleteSegment = canDeleteSegmentsBefore(segment);
                }
            }
        } catch (IOException e) {
            m_usageSpecificLog.error("Exception closing and deleting PBD segment", e);
        }
    }

    /**
     * Return true if segments before this one can be deleted, i.e. no open readers on previous segments.
     * <p>
     * Note: returns true when no read cursors are open.
     *
     * @param segment
     * @return
     */
    private boolean canDeleteSegmentsBefore(PBDSegment<M> segment) {
        String retentionCursor = (m_retentionPolicy == null) ? null : m_retentionPolicy.getCursorId();
        for (ReadCursor cursor : m_readCursors.values()) {
            if (cursor.m_isTransient) {
                continue;
            }
            if (cursor.m_segment == null) {
                return false;
            }

            long segmentIndex = segment.segmentIndex();
            long cursorSegmentIndex = cursor.m_segment.segmentIndex();
            if (cursorSegmentIndex < segmentIndex) {
                return false;
            }

            PBDSegmentReader<M> segmentReader = segment.getReader(cursor.m_cursorId);
            if (Objects.equals(cursor.m_cursorId, retentionCursor)) { // retention cursor doesn't read records and discard.
                continue;                                             // It just moves to the segment and stays till the policy expires.
            }                                                         // So, just the segmentIndex check above is sufficient.
            if (segmentReader == null) {
                // segmentReader is null, if the PBD reader hasn't reached this segment OR
                // if PBD reader has moved past this and all buffers were acked.
                if (segment.segmentIndex() > cursorSegmentIndex) {
                    return false;
                }
            } else if (!segmentReader.anyReadAndDiscarded()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized void sync() throws IOException {
        if (m_closed) {
            throw new IOException("Cannot sync(): PBD has been Closed");
        }
        for (PBDSegment<M> segment : m_segments.values()) {
            if (!segment.isClosed()) {
                segment.sync();
            }
        }
    }

    @Override
    public void close() throws IOException {
        close(false);
    }

    @Override
    public synchronized Pair<Integer, Long> getBufferCountAndSize() throws IOException {
        int count = 0;
        long size = 0;
        for (PBDSegment<M> segment : m_segments.values()) {
            count += segment.getNumEntries();
            size += segment.size();
        }
        return Pair.of(count, size);
    }

    @Override
    public void closeAndDelete() throws IOException {
        close(true);
    }

    private synchronized void close(boolean delete) throws IOException {
        if (m_closed) {
            return;
        }
        m_readCursors.values().forEach(ReadCursor::close);
        m_readCursors.clear();

        for (PBDSegment<M> segment : m_segments.values()) {
            if (delete) {
                closeAndDeleteSegment(segment);
            } else {
                // When closing a PBD, all segments may be finalized because on
                // recover a new segment will be opened for writing
                segment.finalize(true);
                if (m_usageSpecificLog.isDebugEnabled()) {
                    m_usageSpecificLog.debug(
                            "Closed segment " + segment.file() + " (final: " + segment.isFinal() + "), on PBD close");
                }
            }
        }
        m_segments.clear();
        m_closed = true;
    }

    public static class ByteBufferTruncatorResponse extends TruncatorResponse {
        private final ByteBuffer m_retval;
        private final CRC32 m_crc;

        public ByteBufferTruncatorResponse(ByteBuffer retval) {
            this(retval, -1);
        }

        public ByteBufferTruncatorResponse(ByteBuffer retval, long rowId) {
            super(Status.PARTIAL_TRUNCATE, rowId);
            assert retval.remaining() > 0;
            m_retval = retval;
            m_crc = new CRC32();
        }

        @Override
        public int getTruncatedBuffSize() {
            return m_retval.remaining();
        }

        @Override
        public int writeTruncatedObject(ByteBuffer output, int entryId) {
            int objectSize = m_retval.remaining();
            // write entry header
            PBDUtils.writeEntryHeader(m_crc, output, m_retval, entryId, PBDSegment.NO_FLAGS);
            // write buffer after resetting position changed by writeEntryHeader
            // Note: cannot do this in writeEntryHeader as it breaks JUnit tests
            m_retval.position(0);
            output.put(m_retval);
            return objectSize;
        }
    }

    public static class DeferredSerializationTruncatorResponse extends TruncatorResponse {
        public static interface Callback {
            public void bytesWritten(int bytes);
        }

        private final DeferredSerialization m_ds;
        private final Callback m_truncationCallback;
        private final CRC32 m_crc32 = new CRC32();

        public DeferredSerializationTruncatorResponse(DeferredSerialization ds, Callback truncationCallback) {
            super(Status.PARTIAL_TRUNCATE);
            m_ds = ds;
            m_truncationCallback = truncationCallback;
        }

        @Override
        public int getTruncatedBuffSize() throws IOException {
            return m_ds.getSerializedSize();
        }

        @Override
        public int writeTruncatedObject(ByteBuffer output, int entryId) throws IOException {
            output.position(PBDSegment.ENTRY_HEADER_BYTES);
            int bytesWritten = MiscUtils.writeDeferredSerialization(output, m_ds);
            output.flip();
            output.position(PBDSegment.ENTRY_HEADER_BYTES);
            ByteBuffer header = output.duplicate();
            header.position(PBDSegment.ENTRY_HEADER_START_OFFSET);
            PBDUtils.writeEntryHeader(m_crc32, header, output, entryId, PBDSegment.NO_FLAGS);
            if (m_truncationCallback != null) {
                m_truncationCallback.bytesWritten(bytesWritten);
            }
            return bytesWritten;
        }
    }

    public static TruncatorResponse fullTruncateResponse() {
        return new TruncatorResponse(Status.FULL_TRUNCATE);
    }

    @Override
    public boolean initializedFromExistingFiles() {
        return m_initializedFromExistingFiles;
    }

    private static final boolean assertionsOn;
    static {
        boolean assertOn = false;
        assert(assertOn = true);
        assertionsOn = assertOn;
    }

    private void assertions() {
        if (!assertionsOn || m_closed) {
            return;
        }
        for (ReadCursor cursor : m_readCursors.values()) {
            int numObjects = 0;
            try {
                for (PBDSegment<M> segment : m_segments.values()) {
                    PBDSegmentReader<M> reader = segment.getReader(cursor.m_cursorId);
                    if (reader == null) {
                        // reader will be null if the pbd reader has not reached this segment or has passed this and all were acked.
                        if (cursor.m_segment == null || cursor.m_segment.segmentIndex() <= segment.m_index) {
                            numObjects += segment.getNumEntries();
                        }
                    } else {
                        numObjects += segment.getNumEntries() - reader.readIndex();
                    }
                }
                assert numObjects == cursor.getNumObjects() :
                    cursor.m_cursorId + " expects " + cursor.getNumObjects() + " entries but only found " + numObjects;
            } catch (Exception e) {
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
    }

    int numberOfSegments() {
        return m_segments.size();
    }

    // Used by test only
    int numOpenSegments() {
        int numOpen = 0;
        for (PBDSegment<M> segment : m_segments.values()) {
            if (!segment.isClosed()) {
                numOpen++;
            }
        }

        return numOpen;
    }

    @Override
    public synchronized void scanEntries(BinaryDequeScanner scanner) throws IOException
    {
        if (m_closed) {
            throw new IOException("Cannot scanForGap(): PBD has been closed");
        }

        assertions();
        if (m_segments.isEmpty()) {
            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("PBD " + m_nonce + " has no finished segments");
            }
            return;
        }

        /*
         * Iterator all the objects in all the segments and pass them to the scanner
         */
        for (Map.Entry<Long, PBDSegment<M>> entry : m_segments.entrySet()) {
            PBDSegment<M> segment = entry.getValue();
            try {
                int truncatedEntries = segment.scan(scanner);
                if (truncatedEntries > 0) {
                    m_numObjects -= truncatedEntries;
                    if (segment.getNumEntries() == 0) {
                        // All entries were truncated mark the segment as quarantined
                        quarantineSegment(entry);
                    }
                }
            } catch (IOException e) {
                m_usageSpecificLog.warn("Error scanning segment: " + segment.file() + ". Quarantining segment.");
                quarantineSegment(entry);
            }
        }

        return;
    }

    @Override
    public synchronized boolean deletePBDSegment(BinaryDequeValidator<M> validator) throws IOException
    {
        boolean segmentDeleted = false;
        if (m_closed) {
            throw new IOException("Cannot deletePBDSegment(): PBD has been closed");
        }

        assertions();
        if (m_segments.isEmpty()) {
            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("PBD " + m_nonce + " has no segments to delete.");
            }
            return segmentDeleted;
        }

        /*
         * Iterator all the objects in all the segments and pass them to the scanner
         */
        Iterator<PBDSegment<M>> iter = m_segments.values().iterator();
        while (iter.hasNext()) {
            PBDSegment<M> segment = iter.next();
            try {
                int entriesToDelete = segment.validate(validator);
                if (entriesToDelete != 0) {
                    m_numObjects -= entriesToDelete;
                    iter.remove();
                    closeAndDeleteSegment(segment);
                    segmentDeleted = true;
                }
            } catch (IOException e) {
                m_usageSpecificLog.warn("Error validating segment: " + segment.file() + ". Quarantining segment.");
                quarantineSegment(segment);
            }
        }
        return segmentDeleted;
    }

    @Override
    public void setRetentionPolicy(RetentionPolicyType policyType, Object... params) {
        assert(m_retentionPolicy == null);
        m_retentionPolicy = RetentionPolicyMgr.getInstance().addRetentionPolicy(policyType, this, params);
    }

    @Override
    public void startRetentionPolicyEnforcement() {
        try {
            if (m_retentionPolicy != null) {
                m_retentionPolicy.startPolicyEnforcement();
            }
        } catch(IOException e) {
            // Unexpected error. Hence runtime error
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a new builder for constructing instances of {@link PersistentBinaryDeque}
     *
     * @param nonce  To be used by the {@link PersistentBinaryDeque}
     * @param path   to a directory where the {@link PersistentBinaryDeque} files will be stored
     * @param logger to use to log any messages generated by the {@link PersistentBinaryDeque} instance
     * @throws NullPointerException if any parameters are {@code null}
     */
    public static Builder<Void> builder(String nonce, File path, VoltLogger logger) {
        return new Builder<>(nonce, path, logger);
    }

    /**
     * Builder class for constructing an instance of a {@link PersistentBinaryDeque}
     *
     * @param <M> Type of extra header metadata used with this PBD
     */
    public static final class Builder<M> {
        final String m_nonce;
        final File m_path;
        final VoltLogger m_logger;
        boolean m_useCompression = false;
        boolean m_deleteExisting = false;
        BinaryDequeSerializer<M> m_extraHeaderSerializer;
        M m_initialExtraHeader;
        PBDSegmentFactory m_pbdSegmentFactory = PBDRegularSegment::new;

        private Builder(String nonce, File path, VoltLogger logger) {
            super();
            m_nonce = requireNonNull(nonce, "nonce");
            m_path = requireNonNull(path, "path");
            m_logger = requireNonNull(logger, "logger");
        }

        private Builder(Builder<?> builder, M extraHeader, BinaryDequeSerializer<M> serializer) {
            this(builder.m_nonce, builder.m_path, builder.m_logger);
            m_useCompression = builder.m_useCompression;
            m_initialExtraHeader = extraHeader;
            m_extraHeaderSerializer = serializer;
            m_pbdSegmentFactory = builder.m_pbdSegmentFactory;
        }

        /**
         * Set whether compression should be enabled or not.
         * <p>
         * Default: {@code false}
         *
         * @param enabled {@code true} if compression should be used.
         * @return An updated {@link Builder} instance
         */
        public Builder<M> compression(boolean enabled) {
            m_useCompression = enabled;
            return this;
        }

        /**
         * Set whether the pre-existing PBD files should be deleted.
         * <p>
         * Default: {@code false}
         *
         * @param deleteExisting {@code true} if existing PBD files should be deleted.
         * @return An updated {@link Builder} instance
         */
        public Builder<M> deleteExisting(boolean deleteExisting) {
            m_deleteExisting = deleteExisting;
            return this;
        }

        /**
         * Set the initial extra header metadata to be stored with entries as well as a {@link BinaryDequeSerializer} to
         * write and read that type of metadata.
         * <p>
         * Default: {@code null}
         *
         * @param <T>         Type of extra header metadata
         * @param extraHeader Instance of extra header metadata
         * @param serializer  {@link BinaryDequeSerializer} used write and read the extra header
         * @return An updated {@link Builder} instance
         * @throws NullPointerException if {@code serializer} is {@code null}
         */
        public <T> Builder<T> initialExtraHeader(T extraHeader, BinaryDequeSerializer<T> serializer) {
            return new Builder<>(this, extraHeader, requireNonNull(serializer, "serializer"));
        }

        /**
         * @return A new instance of {@link PersistentBinaryDeque} constructed by this builder
         * @throws IOException If there was an error constructing the instance
         */
        public PersistentBinaryDeque<M> build() throws IOException {
            return new PersistentBinaryDeque<>(this);
        }

        Builder<M> pbdSegmentFactory(PBDSegmentFactory pbdSegmentFactory) {
            m_pbdSegmentFactory = requireNonNull(pbdSegmentFactory);
            return this;
        }
    }

    /** Internal interface for a factory to create {@link PBDSegment}s */
    interface PBDSegmentFactory {
        <M> PBDSegment<M> create(long segmentIndex, long segmentId, File file, VoltLogger logger,
                BinaryDequeSerializer<M> extraHeaderSerializer);
    }

    @Override
    public synchronized int countCursors() {
        return m_readCursors.size();
    }
}
