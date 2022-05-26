/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
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
    private static RetentionPolicyMgr s_retentionPolicyMgr;

    public static synchronized void setupRetentionPolicyMgr(int numThreads, int numCompactionThreads) {
        if (s_retentionPolicyMgr == null) {
            s_retentionPolicyMgr = new RetentionPolicyMgr();
        }
        s_retentionPolicyMgr.configure(numThreads, numCompactionThreads);
    }

    public static RetentionPolicyMgr getRetentionPolicyMgr() {
        return s_retentionPolicyMgr;
    }

    class GapWriter implements BinaryDequeGapWriter<M> {
        private M m_gapHeader;
        private PBDSegment<M> m_activeSegment;
        private boolean m_closed;

        @Override
        public void updateGapHeader(M gapHeader) throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                if (m_closed) {
                    throw new IOException("updateGapHeader call on closed PBD " + m_nonce);
                }
                m_gapHeader = gapHeader;
                if (m_activeSegment!= null) {
                    finishActiveSegmentWrite();
                }
            }
        }

        private void finishActiveSegmentWrite() throws IOException {
            finishWrite(m_activeSegment);
            if (m_retentionPolicy != null) {
                m_retentionPolicy.finishedGapSegment();
            }
            m_activeSegment = null;
        }

        @Override
        public int offer(BBContainer data, long startId, long endId, long timestamp) throws IOException {
            try {
                return offer0(data, startId, endId, timestamp);
            } finally {
                data.discard();
            }
        }

        private int offer0(BBContainer data, long startId, long endId, long timestamp) throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                if (m_closed) {
                    throw new IOException("updateGapHeader call on closed PBD " + m_nonce);
                }
                assert(m_gapHeader != null);
                assert(startId != PBDSegment.INVALID_ID && endId >= startId);

                PBDSegment<M> prev = m_activeSegment;
                if (m_activeSegment == null || startId != m_activeSegment.getEndId()+1) {
                    Map.Entry<Long, PBDSegment<M>> entry = m_segments.floorEntry(startId);
                    prev = findValidSegmentFrom((entry == null ? null : entry.getValue()), false, true);
                    if (m_activeSegment!= null && m_activeSegment.m_id != m_segments.lastKey()) {
                        finishActiveSegmentWrite();
                    }
                }

                PBDSegment<M> next = null;
                if (prev == null) { // inserting at the beginning
                    next = findValidSegmentFrom(peekFirstSegment(), true, true);
                } else {
                    Map.Entry<Long, PBDSegment<M>> nextEntry = m_segments.higherEntry(prev.m_id);
                    next = findValidSegmentFrom((nextEntry == null ? null : nextEntry.getValue()), true, true);
                }

                // verify that these ids don't already exist
                if (prev!=null && startId <= prev.getEndId()) {
                    throw new IllegalArgumentException("PBD segment with range [" + prev.getStartId() + "-" + prev.getEndId() +
                            "] already contains some entries offered in the range [" + startId + "-" + endId + "]");
                }

                if (next!=null && endId >= next.getStartId()) {
                    throw new IllegalArgumentException("PBD segment with range [" + next.getStartId() + "-" + next.getEndId() +
                            "] already contains some entries offered in the range [" + startId + "-" + endId + "]");
                }

                // By now prev.endId < startId. But it may not be the next id, in which case, we need to start a new segment
                if (prev != null && startId != prev.getEndId()+1) {
                    prev = null;
                }

                Pair<Integer, PBDSegment<M>> result = offerToSegment(prev, data, startId, endId, timestamp, m_gapHeader,
                        false);
                m_activeSegment = result.getSecond();
                updateCursorsReadCount(m_activeSegment, 1);
                assertions();
                return result.getFirst();
            }
        }

        @Override
        public void close() throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                if (m_closed) {
                    return;
                }

                if (m_activeSegment != null) {
                    finishActiveSegmentWrite();
                }
                m_closed = true;
                m_gapWriter = null;
            }
        }
    }

    /**
     * Used to read entries from the PBD. Multiple readers may be active at the same time,
     * but only one read or write may happen concurrently.
     */
    class ReadCursor implements BinaryDequeReader<M> {
        private final String m_cursorId;
        private PBDSegment<M> m_segment;
        private final Set<PBDSegmentReader<M>> m_segmentReaders = new HashSet<>();
        // Number of objects out of the total
        //that were deleted at the time this cursor was created
        private final long m_numObjectsDeleted;
        private long m_numRead;
        // If a rewind occurred this is set to the segment id where this cursor was before the rewind
        private long m_rewoundFromId = -1;
        private boolean m_cursorClosed = false;
        private final boolean m_isTransient;
        /**
         * True if m_segment had its entry count modified under this cursor and m_numRead is not reliable. This is set
         * by {@link #updateReadCount(long, int)}
         */
        boolean m_hasBadCount = false;

        public ReadCursor(String cursorId, long numObjectsDeleted) {
            this(cursorId, numObjectsDeleted, false);
        }

        public ReadCursor(String cursorId, long numObjectsDeleted, boolean isTransient) {
            m_cursorId = cursorId;
            m_numObjectsDeleted = numObjectsDeleted;
            m_isTransient = isTransient;
        }

        @Override
        public Wrapper poll(OutputContainerFactory ocf) throws IOException {
            return pollCommon(ocf, Integer.MAX_VALUE);
        }

        @Override
        public BinaryDequeReader.Entry<M> pollEntry(OutputContainerFactory ocf, int maxSize) throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                Wrapper wrapper = pollCommon(ocf, maxSize);
                if (wrapper == null) {
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
                        return wrapper.b();
                    }

                    @Override
                    public void release() {
                        wrapper.discard();
                    }

                    @Override
                    public void free() {
                        wrapper.free();
                    }
                };
            }
        }

        private Wrapper pollCommon(OutputContainerFactory ocf, int maxSize) throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                if (m_cursorClosed) {
                    throw new IOException("PBD.ReadCursor.poll(): " + m_cursorId + " - Reader has been closed");
                }
                assertions();

                if (moveToValidSegment() == null) {
                    return null;
                }

                PBDSegmentReader<M> segmentReader = getOrOpenReader();
                long lastSegmentId = m_segments.lastEntry().getValue().segmentId();
                while (!segmentReader.hasMoreEntries()) {
                    if (m_segment.segmentId() == lastSegmentId) { // nothing more to read
                        return null;
                    }

                    // Save closed readers until everything in the segment is acked.
                    if (m_isTransient || segmentReader.allReadAndDiscarded()) {
                        segmentReader.close();
                    } else {
                        segmentReader.closeAndSaveReaderState();
                    }
                    m_segment = m_segments.higherEntry(m_segment.segmentId()).getValue();
                    // push to PBD will rewind cursors. So, this cursor may have already opened this segment
                    segmentReader = getOrOpenReader();
                }
                BBContainer retcont = segmentReader.poll(ocf, maxSize);
                if (retcont == null) {
                    return null;
                }

                m_numRead++;
                assertions();
                assert (retcont.b() != null);
                return wrapRetCont(m_segment, segmentReader, retcont);
            }
        }

        PBDSegment<M> getCurrentSegment() {
            return m_segment;
        }

        public void seekToFirstSegment() throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                if (m_segments.isEmpty() ||
                    (getCurrentSegment() != null && getCurrentSegment().m_id == m_segments.firstKey())) {
                    return;
                }

                seekToSegment(m_segments.firstEntry().getValue());
            }
        }

        @Override
        public void seekToSegment(long entryId, SeekErrorRule errorRule)
                throws NoSuchOffsetException, IOException {
            assert(entryId >= 0);

            synchronized (PersistentBinaryDeque.this) {
                PBDSegment<M> seekSegment = findSegmentWithEntry(entryId, errorRule);
                seekToSegment(seekSegment);
            }
        }

        private void seekToSegment(PBDSegment<M> seekSegment) throws IOException {
            if (moveToValidSegment() == null) {
                return;
            }

            if (m_segment.segmentId() == seekSegment.segmentId()) {
                //Close and open to rewind reader to the beginning and reset everything
                PBDSegmentReader<M> reader = m_segment.getReader(m_cursorId);
                if (reader != null) {
                    m_numRead -= reader.readIndex();
                    closeSegmentReader(reader);
                }
            } else { // rewind or fastforward, adjusting the numRead accordingly
                if (m_segment.segmentId() > seekSegment.segmentId()) { // rewind
                    for (PBDSegment<M> curr : m_segments
                            .subMap(seekSegment.segmentId(), true, m_segment.segmentId(), true).values()) {
                        PBDSegmentReader<M> currReader = curr.getReader(m_cursorId);
                        if (assertionsOn) {
                            if (curr.segmentId() == m_segment.segmentId()) {
                                if (currReader != null) {
                                    m_numRead -= currReader.readIndex();
                                }
                            } else {
                                m_numRead -= curr.getNumEntries();
                            }
                        }
                        if (currReader != null) {
                            closeSegmentReader(currReader);
                        }
                    }
                } else { // fastforward
                    PBDSegmentReader<M> segmentReader = m_segment.getReader(m_cursorId);
                    m_numRead += m_segment.getNumEntries();
                    if (segmentReader != null) {
                        m_numRead -= segmentReader.readIndex();
                        closeSegmentReader(segmentReader);
                    }
                    if (assertionsOn) {
                        // increment numRead
                        for (PBDSegment<M> curr : m_segments
                                .subMap(m_segment.segmentId(), false, seekSegment.segmentId(), false).values()) {
                            m_numRead += curr.getNumEntries();
                        }
                    }
                }
                m_segment = seekSegment;
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

                if (m_segments.size() == 0) {
                    return false;
                }

                long recordTime = getSegmentTimestamp();

                // If this one is invalid keep searching forward until there are no more or there is a valid one
                boolean foundInvalid = recordTime == PBDSegment.INVALID_TIMESTAMP;
                while (recordTime == PBDSegment.INVALID_TIMESTAMP) {
                    PBDSegment<M> nextValidSegment = findNextValidSegmentFrom(m_segment, true, false);
                    if (nextValidSegment == null) {
                        return false;
                    } else {
                        m_segment = nextValidSegment;
                        recordTime = getSegmentTimestamp();
                    }
                }

                if (System.currentTimeMillis() - recordTime < millis) { // can't skip yet
                    return false;
                }

                if (m_segment.isActive() || m_segment.segmentId() == m_segments.lastEntry().getValue().segmentId()) {
                    if (foundInvalid) {
                        // Current segment is old enough to be deleted but is active or latest so delete invalid ones
                        deleteSegmentsBefore(m_segment);
                    }
                    return false;
                }

                skipToNextSegment(true);
                return true;
            }
        }

        @Override
        public void skipPast(long id) throws IOException {
            synchronized(PersistentBinaryDeque.this) {
                if (moveToValidSegment() == null) {
                    return;
                }
                while (id >= m_segment.getEndId() && skipToNextSegment(false)) {}
            }
        }

        // This is used by retention cursor and regular skip forward.
        // With retention cursors, you want to delete the current segment,
        // while with regular skip forward, you want to delete the older segments only.
        // In regular skip, current segment is ready to be deleted only after at least
        // one entry from the next segment is read and discarded.
        private boolean skipToNextSegment(boolean isRetention) throws IOException {
            PBDSegmentReader<M> segmentReader = getOrOpenReader();

            m_numRead += m_segment.getNumEntries() - segmentReader.readIndex();
            segmentReader.markRestReadAndDiscarded();

            Map.Entry<Long, PBDSegment<M>> entry = m_segments.higherEntry(m_segment.segmentId());
            if (entry == null) { // on the last segment
                // We are marking this one as read. So OK to delete segments before this
                callDeleteSegmentsBefore(m_segment, 1, isRetention);
                return false;
            }

            if (!segmentReader.hasOutstandingEntries()) {
                segmentReader.close();
            }
            PBDSegment<M> oldSegment = m_segment;
            m_segment = entry.getValue();
            PBDSegment<M> segmentToDeleteBefore = isRetention ? m_segment : oldSegment;
            callDeleteSegmentsBefore(segmentToDeleteBefore, 1, isRetention);
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

                if (moveToValidSegment() == null) {
                    return maxBytes;
                }

                if (m_segment.isActive() || m_segment.segmentId() == m_segments.lastKey()) {
                    long needed = maxBytes - remainingFileSize();
                    return (needed == 0) ? 1 : needed; // To fix: 0 is a special value indicating we skipped.
                }

                long diff = remainingFileSize();
                if (diff < maxBytes) {
                    return maxBytes - diff;
                }

                skipToNextSegment(true);
                return 0;
            }
        }

        private long remainingFileSize() {
            long size = 0;
            for (PBDSegment<M> segment: m_segments.tailMap(m_segment.segmentId(), false).values()) {
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

        private PBDSegment<M> moveToValidSegment() {
            PBDSegment<M> firstSegment = peekFirstSegment();
            // It is possible that m_segment got closed and removed
            if (m_segment == null || firstSegment == null || m_segment.segmentId() < firstSegment.segmentId()) {
                m_segment = firstSegment;
            }
            return m_segment;
        }

        @Override
        public long getNumObjects() throws IOException {
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

                if (moveToValidSegment() == null) {
                    return 0;
                }

                long size = 0;
                boolean inclusive = true;
                if (m_segment.isOpenForReading(m_cursorId)) { //this reader has started reading from curr segment.
                    // Find out how much is left to read.
                    size = m_segment.getReader(m_cursorId).uncompressedBytesToRead();
                    inclusive = false;
                }
                // Get the size of all unread segments
                for (PBDSegment<M> currSegment : m_segments.tailMap(m_segment.segmentId(), inclusive).values()) {
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

                if (moveToValidSegment() == null) {
                    return true;
                }

                boolean inclusive = true;
                if (m_segment.isOpenForReading(m_cursorId)) { //this reader has started reading from curr segment.
                    // Check if there are more to read.
                    if (m_segment.getReader(m_cursorId).hasMoreEntries()) {
                        return false;
                    }
                    inclusive = false;
                }

                for (PBDSegment<M> currSegment : m_segments.tailMap(m_segment.segmentId(), inclusive).values()) {
                    if (currSegment.getNumEntries() > 0) {
                        return false;
                    }
                }

                return true;
            }
        }

        private Wrapper wrapRetCont(PBDSegment<M> segment, PBDSegmentReader<M> segmentReader,
                final BBContainer retcont) {
            if (m_isTransient) {
                return new Wrapper(retcont);
            }

            int entryNumber = segmentReader.readIndex();

            return new Wrapper(retcont) {
                @Override
                public void discard() {
                    synchronized(PersistentBinaryDeque.this) {
                        free();

                        assert m_cursorClosed || m_segments.containsKey(segment.segmentId());
                        // Cursor or reader was closed so just essentially ignore this discard
                        if (m_cursorClosed) {
                            return;
                        }

                        if (segment.getReader(m_cursorId) != segmentReader) {
                            m_usageSpecificLog.warn(segment.m_file
                                    + ": Reader removed or replaced. Ignoring discard of entry " + entryNumber);
                            return;
                        }

                        assert(m_segment != null);
                        // If the reader has moved past this and all have been acked close this segment reader.
                        if (segmentReader.allReadAndDiscarded() && segment.segmentId() < m_segment.m_id) {
                            try {
                                closeSegmentReader(segmentReader);
                            } catch(IOException e) {
                                m_usageSpecificLog.warn("Unexpected error closing PBD file " + segment.m_file, e);
                            }
                        }

                        deleteSegmentsOnAck(segment, entryNumber);
                    }
                }
            };
        }

        private void closeSegmentReader(PBDSegmentReader<M> segmentReader) throws IOException {
            m_segmentReaders.remove(segmentReader);
            segmentReader.close();
        }

        private void deleteSegmentsOnAck(PBDSegment<M> segment, int entryNumber) {
            // Only continue if open and there is another segment
            if (m_cursorClosed || m_segments.size() == 1) {
                return;
            }

            // If this segment is already marked for deletion, check if it is the first one
            // and delete it if fully acked. All the subsequent marked ones can be checked and deleted as well.
            if (segment.m_deleteOnAck == true && segment.segmentId() == m_segments.firstKey()) {
                m_deferredDeleter.execute(this::deleteMarkedSegments);
                return;
            }

            callDeleteSegmentsBefore(segment, entryNumber, false);
        }

        private void deleteMarkedSegments() {
            synchronized(PersistentBinaryDeque.this) {
                // With deferred deleters, this may be executed later, so check closed status again
                if (m_cursorClosed) {
                    return;
                }

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
            }
        }

        private void callDeleteSegmentsBefore(PBDSegment<M> segment, int entryNumber, boolean isRetention) {
            if (m_isTransient) {
                return;
            }
            // If this is the first entry of a segment, see if previous segments can be deleted or marked ready to
            // delete
            if (m_cursorClosed || m_segments.size() == 1 || (entryNumber != 1 && m_rewoundFromId != segment.m_id)
                    || !PersistentBinaryDeque.this.canDeleteSegmentsBefore(segment)) {
                return;
            }
            if (isRetention) {
                try {
                    Map.Entry<Long, PBDSegment<M>> lower = m_segments.lowerEntry(segment.segmentId());
                    m_retentionDeletePoint = (lower == null) ? m_retentionDeletePoint : lower.getValue().getEndId();
                } catch(IOException e) { // cannot happen because header is read at open time of retention reader.
                    // If it does happen, don't delete without being able to save the deletion point.
                    // Next retention will hopefully be successful and delete everything.
                    m_usageSpecificLog.error("Unexpected error getting endId of segment. " + segment.m_file +
                            ". PBD files may not be deleted.", e);
                    return;
                }
            }
            m_deferredDeleter.execute(() -> deleteReaderSegmentsBefore(segment));
        }

        private void deleteReaderSegmentsBefore(PBDSegment<M> segment) {
            synchronized(PersistentBinaryDeque.this) {
                // With deferred deleters, this may be executed later, so check closed status again
                if (m_cursorClosed) {
                    return;
                }

                if (m_rewoundFromId == segment.m_id) {
                    m_rewoundFromId = -1;
                }

                try {
                    deleteSegmentsBefore(segment);
                } catch (IOException e) {
                    m_usageSpecificLog.error("Exception closing and deleting PBD segment", e);
                }
            }
        }

        void close() {
            for (PBDSegmentReader<M> reader : m_segmentReaders) {
                try {
                    reader.close();
                } catch (IOException e) {
                    m_usageSpecificLog.warn("Failed to close reader " + reader, e);
                }
            }
            m_segmentReaders.clear();
            m_segment = null;
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
                return (moveToValidSegment() == null) ? false : m_segment.isActive();
            }
        }

        /**
         * Returns the time the last record in this segment was written.
         *
         * @return timestamp in millis of the last record in this segment.
         */
        public long getSegmentTimestamp() {
            synchronized (PersistentBinaryDeque.this) {
                try {
                    return moveToValidSegment() == null ? PBDSegment.INVALID_TIMESTAMP : m_segment.getTimestamp();
                } catch (IOException e) {
                    m_usageSpecificLog.warn("Failed to read timestamp", e);
                    return PBDSegment.INVALID_TIMESTAMP;
                }
            }
        }

        /**
         * Notify this cursor that the number of entries has changed but not at the head or the tail. This can be done
         * by gap fill and update entries
         *
         * @param segmentId   ID of segment which was changed
         * @param countChange The change in the number of entries. This can be either positive or negative
         */
        void updateReadCount(long segmentId, int countChange) {
            if (m_segment != null) {
                if (m_segment.segmentId() > segmentId) {
                    m_numRead += countChange;
                } else {
                    m_hasBadCount = true;
                }
            }
        }

        private PBDSegmentReader<M> getOrOpenReader() throws IOException {
            PBDSegmentReader<M> reader = m_segment.getReader(m_cursorId);
            if (reader == null) {
                reader = m_segment.openForRead(m_cursorId);
                m_segmentReaders.add(reader);

            }
            return reader;
        }
    }

    public static final OutputContainerFactory UNSAFE_CONTAINER_FACTORY = DBBPool::allocateUnsafeByteBuffer;

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
    // Current active segment being written to or the most recent segment if m_requiresId is false
    private PBDSegment<M> m_activeSegment;
    private volatile boolean m_closed = false;
    private final HashMap<String, ReadCursor> m_readCursors = new HashMap<>();
    private long m_numObjects;
    private long m_numDeleted;

    private M m_extraHeader;
    private Executor m_deferredDeleter = Runnable::run;
    private PBDRetentionPolicy m_retentionPolicy;
    private long m_retentionDeletePoint = PBDSegment.INVALID_ID;
    private boolean m_requiresId;
    private GapWriter m_gapWriter;
    /** The number of entries which are in closed segments since the last time {@link #updateEntries()} was called */
    private long m_entriesClosedSinceUpdate;
    // The amount of time in nanosecond a segment can be left open for write since the segment is constructed.
    private long m_segmentRollTimeLimitNs = Long.MAX_VALUE;

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
        m_requiresId = builder.m_requiresId;

        if (!m_path.exists() || !m_path.canRead() || !m_path.canWrite() || !m_path.canExecute()
                || !m_path.isDirectory()) {
            throw new IOException(
                    m_path + " is not usable ( !exists || !readable " +
                    "|| !writable || !executable || !directory)");
        }

        parseFiles(builder.m_deleteExisting);
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
     * Return a segment file name from m_nonce and current segment id
     *
     * @see parseFiles for file name structure
     * @param currentId   current segment id
     * @return  segment file name
     */
    private String getSegmentFileName(long currentId) {
        return PbdSegmentName.createName(m_nonce, currentId, false);
    }

    /**
     * Parse files for this PBD; if creating, delete any crud left by a previous homonym.
     *
     * @param deleteExisting true if should delete any existing PBD files
     *
     * @throws IOException
     */
    private void parseFiles(boolean deleteExisting) throws IOException {

        TreeMap<Long, PbdSegmentName> filesById = new TreeMap<>();
        List<String> invalidPbds = new ArrayList<>();
        try {
            for (File file : m_path.listFiles()) {
                if (file.isDirectory() || !file.isFile() || file.isHidden()) {
                    continue;
                }

                PbdSegmentName segmentName = PbdSegmentName.parseFile(m_usageSpecificLog, file);

                switch (segmentName.m_result) {
                case INVALID_NAME:
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

                filesById.put(segmentName.m_id, segmentName);
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
            Long prevId = null;
            for (Map.Entry<Long, PbdSegmentName> entry : filesById.entrySet()) {
                long currId = entry.getKey();
                if (!m_requiresId && prevId != null && currId != prevId+1) {
                    throw new IOException("Found " + entry.getValue().m_file + " with id " + currId + " after previous id " + prevId);
                }
                recoverSegment(currId, entry.getValue());
                prevId = currId;
            }

            if (!m_requiresId && prevId != null) {
                m_activeSegment = m_segments.get(prevId);
            }
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

            PBDSegment<M> quarantined = new PbdQuarantinedSegment<>(quarantinedSegment.m_file, segment.segmentId());

            if (prevEntry == null) {
                PBDSegment<M> prev = m_segments.put(segment.segmentId(), quarantined);
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

    private PBDSegment<M> findValidSegmentFrom(PBDSegment<M> segment, boolean higher) throws IOException {
        return findValidSegmentFrom(segment, higher, false);
    }

    private PBDSegment<M> findValidSegmentFrom(PBDSegment<M> segment, boolean higher, boolean deleteInvalid)
            throws IOException {
        if (segment == null || segment.getNumEntries() > 0) {
            return segment;
        }
        return findNextValidSegmentFrom(segment, higher, deleteInvalid);
    }

    private PBDSegment<M> findNextValidSegmentFrom(PBDSegment<M> segment, boolean higher, boolean deleteInvalid)
            throws IOException {
        // skip past quarantined segments
        do {
            Map.Entry<Long, PBDSegment<M>> nextEntry;
            if (higher) {
                nextEntry = m_segments.higherEntry(segment.segmentId());
            } else {
                nextEntry = m_segments.lowerEntry(segment.segmentId());
            }
            if (deleteInvalid) {
                m_segments.remove(segment.segmentId());
                segment.m_file.delete();
            }
            segment = (nextEntry == null) ? null : nextEntry.getValue();
        } while (segment != null && segment.getNumEntries() == 0);

        return segment;
    }

    private PBDSegment<M> findSegmentWithEntry(long entryId, SeekErrorRule errorRule) throws NoSuchOffsetException, IOException {
        if (!m_requiresId) {
            throw new IllegalStateException("Seek is not supported in PBDs that don't store id ranges");
        }

        PBDSegment<M> first = findValidSegmentFrom(peekFirstSegment(), true);
        if (first == null) {
            throw new NoSuchOffsetException("Offset " + entryId + "not found. Empty PBD");
        }

        Map.Entry<Long, PBDSegment<M>> floorEntry = m_segments.floorEntry(entryId);
        PBDSegment<M> candidate = findValidSegmentFrom((floorEntry == null ? null : floorEntry.getValue()), false);
        if (candidate != null) {
            if (entryId >= candidate.getStartId() && entryId <= candidate.getEndId()) {
                return candidate;
            } else { // in a gap or after the last id available in the PBD
                if (errorRule == SeekErrorRule.THROW) {
                    throw new NoSuchOffsetException("PBD does not contain offset: " + entryId);
                } else if (errorRule == SeekErrorRule.SEEK_BEFORE) {
                    return candidate;
                } else {
                    Map.Entry<Long, PBDSegment<M>> nextEntry = m_segments.higherEntry(candidate.getStartId());
                    PBDSegment<M> next = findValidSegmentFrom((nextEntry == null ? null : nextEntry.getValue()), true);
                    if (next == null) {
                        throw new NoSuchOffsetException("PBD does not contain offset: " + entryId);
                    } else {
                        return next;
                    }
                }
            }
        } else { // entryId is before the first id available in the PBD
            if (errorRule == SeekErrorRule.THROW || errorRule == SeekErrorRule.SEEK_BEFORE) {
                throw new NoSuchOffsetException("PBD does not contain offset: " + entryId);
            } else {
                return first;
            }
        }
    }

    /**
     * Recover a PBD segment and add it to m_segments
     *
     * @param segment
     * @param deleteEmpty
     * @throws IOException
     */
    private void recoverSegment(long segmentId, PbdSegmentName segmentName) throws IOException {
        PBDSegment<M> segment;
        if (segmentName.m_quarantined) {
            segment = new PbdQuarantinedSegment<>(segmentName.m_file, segmentId);
        } else {
            segment = m_pbdSegmentFactory.create(segmentId, segmentName.m_file, m_usageSpecificLog,
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

                assert(!m_requiresId || segment.getNumEntries() == 0 || segment.getEndId() >= segment.getStartId());
                assert (!m_requiresId || m_activeSegment == null || segment.getNumEntries() == 0
                        || m_activeSegment.getEndId() < segment.getStartId());

                // Any recovered segment that is not final should be checked
                // for internal consistency.
                if (m_usageSpecificLog.isDebugEnabled()) {
                    m_usageSpecificLog.debugFmt(
                            "Segment %s (final: %b) has been recovered%s",
                            segment.file(), segment.isFinal(), segment.isFinal() ? "" : ", but is not in a final state");
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

        m_entriesClosedSinceUpdate += segment.getNumEntries();
        m_segments.put(segment.segmentId(), segment);
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
        if (m_activeSegment != null) {
            m_activeSegment.close();
        }

        /*
         * Iterator all the objects in all the segments and pass them to the truncator
         * When it finds the truncation point
         */
        Long lastSegmentIndex = null;
        for (Map.Entry<Long, PBDSegment<M>> entry : m_segments.entrySet()) {
            PBDSegment<M> segment = entry.getValue();
            final long segmentIndex = segment.segmentId();

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

        if (!m_requiresId) {
            m_activeSegment = m_segments.isEmpty() ? null : m_segments.lastEntry().getValue();
        }

        assertions();
    }

    private PBDSegment<M> initializeNewSegment(long segmentId, File file, String reason,
            M extraHeader)
            throws IOException {
        PBDSegment<M> segment = m_pbdSegmentFactory.create(segmentId, file, m_usageSpecificLog,
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

    @Override
    public synchronized void updateExtraHeader(M extraHeader) throws IOException {
        m_extraHeader = extraHeader;
        if (m_activeSegment != null) {
            finishWrite(m_activeSegment);
        }
    }

    @Override
    public synchronized int offer(BBContainer object) throws IOException {
        return offer(object, PBDSegment.INVALID_ID, PBDSegment.INVALID_ID, PBDSegment.INVALID_TIMESTAMP);
    }

    @Override
    public synchronized int offer(BBContainer object, long startId, long endId, long timestamp) throws IOException {
        try {
            return commonOffer(object, startId, endId, timestamp);
        } finally {
            object.discard();
        }
    }

    @Override
    public synchronized int offer(DeferredSerialization ds) throws IOException {
        return commonOffer(ds, PBDSegment.INVALID_ID, PBDSegment.INVALID_ID, PBDSegment.INVALID_TIMESTAMP);
    }

    private int commonOffer(Object object, long startId, long endId, long timestamp) throws IOException {
        boolean isDs = (object instanceof DeferredSerialization);
        assertions();
        if (m_closed) {
            throw new IOException("Cannot offer(): PBD has been Closed");
        }
        if (isDs) {
            assert(!m_requiresId && startId == PBDSegment.INVALID_ID && endId == PBDSegment.INVALID_ID);
        } else {
            assert(!m_requiresId || (startId != PBDSegment.INVALID_ID && endId != PBDSegment.INVALID_ID && endId >= startId));
            assert (!m_requiresId || m_activeSegment == null || m_activeSegment.getEndId() < startId);
        }

        Pair<Integer, PBDSegment<M>> result;
        result = offerToSegment(m_activeSegment, object, startId, endId, timestamp, m_extraHeader, isDs);
        m_activeSegment = result.getSecond();
        assertions();
        return result.getFirst();
    }

    private Pair<Integer, PBDSegment<M>> offerToSegment(PBDSegment<M> segment, Object object, long startId, long endId,
            long timestamp, M extraHeader, boolean isDs) throws IOException {

        if (segment == null || !segment.isActive()) {
            segment = addSegment(segment, startId, extraHeader);
        } else if ((System.nanoTime() - segment.getCreationTime()) > m_segmentRollTimeLimitNs) {
            finishWrite(segment);
            segment = addSegment(segment, startId, extraHeader);
        }

        int written = (isDs) ? segment.offer((DeferredSerialization) object)
                : segment.offer((BBContainer) object, startId, endId, timestamp);
        if (written < 0) {
            finishWrite(segment);
            segment = addSegment(segment, startId, extraHeader);
            written = (isDs) ? segment.offer((DeferredSerialization) object)
                    : segment.offer((BBContainer) object, startId, endId, timestamp);
            if (written < 0) {
                throw new IOException("Failed to offer object in PBD");
            }
        }

        m_numObjects++;
        callBytesAdded(written);
        return new Pair<>(written, segment);
    }

    private void callBytesAdded(int dataBytes) {
        if (m_retentionPolicy != null) {
            m_retentionPolicy.bytesAdded(dataBytes + PBDSegment.ENTRY_HEADER_BYTES);
        }
    }

    private void finishWrite(PBDSegment<M> tail) throws IOException {
        //Check to see if the tail is completely consumed so we can close it
        tail.finalize(!tail.isBeingPolled());
        tail.saveFileSize();
        m_entriesClosedSinceUpdate += tail.getNumEntries();

        if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug(
                    "Segment " + tail.file() + " (final: " + tail.isFinal() + "), has been closed by offer to PBD");
        }
    }

    private PBDSegment<M> addSegment(PBDSegment<M> tail, long startId, M extraHeader) throws IOException {
        long curId = (startId == PBDSegment.INVALID_ID)
                ? ((tail == null) ? 1 : tail.segmentId() + 1)
                : startId;
        String fname = getSegmentFileName(curId);
        PBDSegment<M> newSegment = initializeNewSegment(curId, new File(m_path, fname), "an offer", extraHeader);
        m_segments.put(newSegment.segmentId(), newSegment);

        if (m_retentionPolicy != null) {
            m_retentionPolicy.newSegmentAdded(newSegment.m_file.length());
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
                } else if (segment.m_id >= reader.m_segment.m_id) {
                    PBDSegmentReader<M> sreader = segment.getReader(reader.m_cursorId);
                    reader.m_numRead += toDelete - (sreader == null ? 0 : sreader.readIndex());
                }
            }
        }

        segment.closeAndDelete();
        m_numDeleted += toDelete;
    }

    @Override
    public void push(BBContainer[] objects) throws IOException {
        push(objects, m_extraHeader);
    }

    @Override
    public void push(BBContainer objects[], M extraHeader) throws IOException {
        try {
            push0(objects, extraHeader);
        } finally {
            for (BBContainer obj : objects) {
                obj.discard();
            }
        }
    }

    private synchronized void push0(BBContainer objects[], M extraHeader) throws IOException {
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
        Long nextIndex = first == null ? 1L : first.segmentId() - 1;

        while (segments.peek() != null) {
            ArrayDeque<BBContainer> currentSegmentContents = segments.poll();

            String fname = getSegmentFileName(nextIndex);
            PBDSegment<M> writeSegment = initializeNewSegment(nextIndex, new File(m_path, fname), "a push",
                    extraHeader);

            // Prepare for next file
            nextIndex--;

            while (currentSegmentContents.peek() != null) {
                writeSegment.offer(currentSegmentContents.pollFirst(), PBDSegment.INVALID_ID, PBDSegment.INVALID_ID,
                        PBDSegment.INVALID_TIMESTAMP);
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

            m_segments.put(writeSegment.segmentId(), writeSegment);
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
    public synchronized BinaryDequeGapWriter<M> openGapWriter() throws IOException {
        if (m_closed) {
            throw new IOException("Cannot openGapWriter(): PBD has been Closed");
        }
        assert(m_requiresId);

        if (m_gapWriter != null) {
            throw new IOException("A gap writer is already open on this PBD");
        }

        m_gapWriter = new GapWriter();
        return m_gapWriter;
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

        if (m_retentionPolicy != null) {
            assert !purgeOnLastCursor : " retention policy and purgeOnLastCursor are mutually exclusive options";
            return;
        }

        // Purge segments
        // Check all segments from latest to oldest to see if segments before that segment can be deleted.
        // Never purge when closing transient readers.
        //
        // By default with {@code purgeOnLastCursor} == false, attempt to purge segments except when closing
        // the last cursor.
        //
        // In the one-to-many DR use case, the snapshot placeholder cursor prevents purging segments that have
        // been read by the other cursors. Therefore, DR calls this method with {@code purgeOnLastCursor} == true,
        // in order to ensure that closing the last DR cursor will purge those segments.
        if (reader.m_isTransient || (m_readCursors.isEmpty() && !purgeOnLastCursor)) {
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

            long segmentIndex = segment.segmentId();
            long cursorSegmentIndex = cursor.m_segment.segmentId();
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
                if (segment.segmentId() > cursorSegmentIndex) {
                    return false;
                }
            } else if (!segmentReader.anyReadAndDiscarded()) {
                return false;
            }
        }
        return true;
    }

    // Deletions initiated by this method should not go through deferred deleter.
    // This is only called when this volt node receives retention point update from partition leader.
    // It is problematic to switch deferred deleter on partition leader updates, so always keep the
    // same deferred deleter, but delete here without going through deferred deleter.
    @Override
    public synchronized void deleteSegmentsToEntryId(long entryId) throws IOException {
        assert(m_requiresId);

        PBDSegment<M> entrySegment = null;
        try {
            entrySegment = findSegmentWithEntry(entryId, SeekErrorRule.SEEK_BEFORE);
        } catch(NoSuchOffsetException e) {
            // This means that the entryId is before any entries available in this PBD. Nothing to delete
            return;
        }

        assert(entrySegment.getStartId() <= entryId);
        if (entrySegment.getEndId() <= entryId && entrySegment.segmentId() != m_segments.lastKey()) { // this segment can also be deleted
            entrySegment = m_segments.higherEntry(entrySegment.segmentId()).getValue();
        }

        deleteSegmentsBefore(entrySegment);
    }

    private synchronized void deleteSegmentsBefore(PBDSegment<M> segment) throws IOException {
        Iterator<PBDSegment<M>> iter = m_segments.headMap(segment.segmentId(), false).values()
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
                    (cursor.m_segment == null || cursor.m_segment.segmentId() <= segment.segmentId()) ) {
                return false;
            }

            if (segmentReader != null && !segmentReader.allReadAndDiscarded()) {
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
    public synchronized long getFirstId() throws IOException {
        return (m_segments.size() == 0) ? PBDSegment.INVALID_ID : m_segments.firstEntry().getValue().getStartId();
    }

    @Override
    public void closeAndDelete() throws IOException {
        close(true);
    }

    private synchronized void close(boolean delete) throws IOException {
        if (m_closed) {
            return;
        }

        stopRetentionPolicyEnforcement();
        if (m_gapWriter != null) {
            m_gapWriter.close();
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
        m_activeSegment = null;
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

        class SegmentWithReader {
            final boolean m_currentSegment;
            final long m_id;
            final int m_entryCount;
            final int m_readerIndex;
            final boolean m_allReadAndDiscarded;

            SegmentWithReader(ReadCursor cursor, PBDSegment<M> segment, PBDSegmentReader<M> reader) throws IOException {
                m_currentSegment = cursor.m_segment != null && cursor.m_segment.segmentId() == segment.segmentId();
                m_id = segment.segmentId();
                m_entryCount = segment.getNumEntries();
                m_readerIndex = reader.readIndex();
                m_allReadAndDiscarded = reader.allReadAndDiscarded();
            }

            @Override
            public String toString() {
                return "SegmentWithReader [currentSegment=" + m_currentSegment + ", id=" + m_id + ", entryCount="
                        + m_entryCount + ", readerIndex=" + m_readerIndex + ", allReadAndDiscarded="
                        + m_allReadAndDiscarded + "]";
            }
        }

        List<String> badCounts = new ArrayList<>();
        List<SegmentWithReader> segmentsWithReaders = new ArrayList<>();
        for (ReadCursor cursor : m_readCursors.values()) {
            if (cursor.m_hasBadCount) {
                continue;
            }
            boolean noReaderForCurrent = false;
            long numObjects = 0;
            int nullReadersBefore = 0, nullReadersAfter = 0;
            segmentsWithReaders.clear();
            try {
                for (PBDSegment<M> segment : m_segments.values()) {
                    PBDSegmentReader<M> reader = segment.getReader(cursor.m_cursorId);
                    if (reader == null) {
                        // reader will be null if the pbd reader has not reached this segment or has passed this and all were acked.
                        if (cursor.m_segment == null || cursor.m_segment.segmentId() <= segment.m_id) {
                            noReaderForCurrent |= cursor.m_segment != null
                                    && cursor.m_segment.segmentId() == segment.segmentId();
                            ++nullReadersAfter;
                            numObjects += segment.getNumEntries();
                        } else {
                            ++nullReadersBefore;
                        }
                    } else {
                        segmentsWithReaders.add(new SegmentWithReader(cursor, segment, reader));
                        numObjects += segment.getNumEntries() - reader.readIndex();
                    }
                }
                if (numObjects != cursor.getNumObjects()) {
                    badCounts.add(cursor.m_cursorId + " expects " + cursor.getNumObjects()
                            + " entries but found " + numObjects + ". noReaderForCurrent=" + noReaderForCurrent
                            + ", nullReadersBefore=" + nullReadersBefore + ", nullReadersAfter=" + nullReadersAfter
                            + ", segments with reader=" + segmentsWithReaders + ", numObjectsDeleted="
                            + cursor.m_numObjectsDeleted + ", numRead=" + cursor.m_numRead);
                }
            } catch (Exception e) {
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
        assert badCounts.isEmpty() : "numDeleted=" + m_numDeleted + ", numObjects=" + m_numObjects + ": \n"
                + badCounts.stream().map(Object::toString).collect(Collectors.joining("\n"));
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
        Iterator<Map.Entry<Long, PBDSegment<M>>> iter = m_segments.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, PBDSegment<M>> entry = iter.next();
            PBDSegment<M> segment = entry.getValue();
            try {
                int entriesToDelete = segment.validate(validator);
                if (entriesToDelete != 0) {
                    iter.remove();
                    closeAndDeleteSegment(segment);
                    segmentDeleted = true;
                }
            } catch (IOException e) {
                m_usageSpecificLog.warn("Error validating segment: " + segment.file() + ". Quarantining segment.", e);
                quarantineSegment(entry);
            }
        }
        return segmentDeleted;
    }

    @Override
    public void registerDeferredDeleter(Executor deferredDeleter) {
        m_deferredDeleter = (deferredDeleter == null) ? Runnable::run : deferredDeleter;
    }

    @Override
    public synchronized void setRetentionPolicy(RetentionPolicyType policyType, Object... params) {
        if (m_retentionPolicy != null) {
            assert !m_retentionPolicy.isPolicyEnforced()
                : "Retention policy on PBD " + m_nonce + " must be stopped before replacing it";
        }

        m_retentionPolicy = (policyType == null) ? null : s_retentionPolicyMgr.addRetentionPolicy(policyType, this, params);
    }

    @Override
    public synchronized void startRetentionPolicyEnforcement() {
        try {
            if (m_retentionPolicy != null) {
                m_retentionPolicy.startPolicyEnforcement();
            }
        } catch(IOException e) {
            // Unexpected error. Hence runtime error
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void stopRetentionPolicyEnforcement() {
        if (m_retentionPolicy != null) {
            m_retentionPolicy.stopPolicyEnforcement();
        }
    }

    @Override
    public synchronized boolean isRetentionPolicyEnforced() {
        return m_retentionPolicy != null && m_retentionPolicy.isPolicyEnforced();
    }

    @Override
    public synchronized long getRetentionDeletionPoint() {
        return m_retentionDeletePoint;
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
        boolean m_requiresId = false;

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
            m_requiresId = builder.m_requiresId;
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
         * Set whether this PBD requires start and end ids for offers.
         * <p>
         * Default: {@code false}
         *
         * @param requiresId {@code true} if this PBD requires start id and end id to be specified with offers
         * @return An updated {@link Builder} instance
         */
        public Builder<M> requiresId(boolean requiresId) {
            m_requiresId = requiresId;
            return this;
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
        <M> PBDSegment<M> create(long segmentId, File file, VoltLogger logger,
                BinaryDequeSerializer<M> extraHeaderSerializer);
    }

    @Override
    public synchronized int countCursors() {
        return m_readCursors.size();
    }

    @Override
    public synchronized long newEligibleUpdateEntries() {
        return m_entriesClosedSinceUpdate;
    }

    /*
     * In order to prevent holding a object monitor for an extended period of time the monitor will only be held to
     * process each segment and at the end to clean up trailing segments
     */
    @Override
    public void updateEntries(BinaryDeque.EntryUpdater<? super M> updater) throws IOException {
        try (BinaryDeque.EntryUpdater<?> u = updater) {
            synchronized (this) {
                m_entriesClosedSinceUpdate = 0;
            }

            Long prevSegmentId = Long.MAX_VALUE;
            Map.Entry<Long, PBDSegment<M>> entry;
            do {
                synchronized (this) {
                    entry = m_segments.lowerEntry(prevSegmentId);
                    if (entry == null) {
                        break;
                    }
                    prevSegmentId = entry.getKey();
                    PBDSegment<M> segment = entry.getValue();
                    if (segment.isActive() || segment.size() == 0) {
                        continue;
                    }
                    Pair<PBDSegment<M>, Boolean> result = segment.updateEntries(updater);
                    PBDSegment<M> updated = result.getFirst();
                    if (updated != null) {
                        int updateCount = updated.getNumEntries() - segment.getNumEntries();
                        m_numObjects += updateCount;
                        updateCursorsReadCount(segment, updateCount);
                        m_segments.put(prevSegmentId, updated);
                    }

                    if (result.getSecond()) {
                        return;
                    }
                }
                Thread.yield();
            } while (true);

            synchronized (this) {
                Iterator<PBDSegment<M>> iter = m_segments.values().iterator();
                PBDSegment<M> segment;
                while (iter.hasNext() && (segment = iter.next()).size() == 0) {
                    segment.closeAndDelete();
                    iter.remove();
                }
            }
        }
    }

    /**
     * Update the read counts of cursors when a segment entry count has changed and the segment is not the head or the
     * tail
     *
     * @param segment          whose entry count has changed
     * @param entryCountChange difference in the entry count before to now
     */
    private void updateCursorsReadCount(PBDSegment<M> segment, int entryCountChange) {
        // read count is only really used for assertions() so don't bother doing the work when assertions are disabled
        if (assertionsOn) {
            long segmentId = segment.segmentId();
            for (ReadCursor cursor : m_readCursors.values()) {
                cursor.updateReadCount(segmentId, entryCountChange);
            }
        }
    }

    /**
     * Simple wrapper around a {@link BBContainer} which exposes a {@link #free()} method to directly call the
     * underlying discard
     */
    private class Wrapper extends DBBPool.DBBDelegateContainer {
        Wrapper(BBContainer delegate) {
            super(delegate);
        }

        void free() {
            super.discard();
        }
    }

    @Override
    public synchronized void setSegmentRollTimeLimit(long limit) {
        m_segmentRollTimeLimitNs = limit;
    }
}
