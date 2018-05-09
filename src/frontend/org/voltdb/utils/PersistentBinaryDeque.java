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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.Pair;
import org.voltdb.EELibraryLoader;
import org.voltdb.utils.BinaryDeque.TruncatorResponse.Status;
import org.voltdb.utils.PBDSegment.PBDSegmentReader;

import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.base.Throwables;

/**
 * A deque that specializes in providing persistence of binary objects to disk. Any object placed
 * in the deque will be persisted to disk asynchronously. Objects placed in the deque can
 * be persisted synchronously by invoking sync. The files backing this deque all start with a nonce
 * provided at construction time followed by a segment index that is stored in the filename. Files grow to
 * a maximum size of 64 megabytes and then a new segment is created. The index starts at 0. Segments are deleted
 * once all objects from the segment have been polled and all the containers returned by poll have been discarded.
 * Push is implemented by creating new segments at the head of the deque containing the objects to be pushed.
 *
 */
public class PersistentBinaryDeque implements BinaryDeque {
    private static final VoltLogger LOG = new VoltLogger("HOST");

    public static class UnsafeOutputContainerFactory implements OutputContainerFactory {
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
    private class ReadCursor implements BinaryDequeReader {
        private final String m_cursorId;
        private PBDSegment m_segment;
        // Number of objects out of the total
        //that were deleted at the time this cursor was created
        private final int m_numObjectsDeleted;
        private int m_numRead;

        public ReadCursor(String cursorId, int numObjectsDeleted) throws IOException {
            m_cursorId = cursorId;
            m_numObjectsDeleted = numObjectsDeleted;
        }

        @Override
        public BBContainer poll(OutputContainerFactory ocf) throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                if (m_closed) {
                    throw new IOException("PBD.ReadCursor.poll(): " + m_cursorId + " - Reader has been closed");
                }
                assertions();

                moveToValidSegment();
                PBDSegmentReader segmentReader = m_segment.getReader(m_cursorId);
                if (segmentReader == null) {
                    segmentReader = m_segment.openForRead(m_cursorId);
                }
                long lastSegmentId = peekLastSegment().segmentId();
                while (!segmentReader.hasMoreEntries()) {
                    if (m_segment.segmentId() == lastSegmentId) { // nothing more to read
                        return null;
                    }

                    segmentReader.close();
                    m_segment = m_segments.higherEntry(m_segment.segmentId()).getValue();
                    // push to PBD will rewind cursors. So, this cursor may have already opened this segment
                    segmentReader = m_segment.getReader(m_cursorId);
                    if (segmentReader == null) segmentReader = m_segment.openForRead(m_cursorId);
                }
                BBContainer retcont = segmentReader.poll(ocf);

                m_numRead++;
                assertions();
                assert (retcont.b() != null);
                return wrapRetCont(m_segment, retcont);
            }
        }

        private void moveToValidSegment() {
            PBDSegment firstSegment = peekFirstSegment();
            // It is possible that m_segment got closed and removed
            if (m_segment == null || m_segment.segmentId() < firstSegment.segmentId()) {
                m_segment = firstSegment;
            }
        }

        @Override
        public int getNumObjects() throws IOException {
            synchronized(PersistentBinaryDeque.this) {
                if (m_closed) {
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
                if (m_closed) {
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
                for (PBDSegment currSegment : m_segments.tailMap(m_segment.segmentId(), inclusive).values()) {
                    size += currSegment.size();
                }
                return size;
            }
        }

        @Override
        public boolean isEmpty() throws IOException {
            synchronized(PersistentBinaryDeque.this) {
                if (m_closed) {
                    throw new IOException("Closed");
                }
                assertions();

                moveToValidSegment();
                boolean inclusive = true;
                if (m_segment.isOpenForReading(m_cursorId)) { //this reader has started reading from curr segment.
                    // Check if there are more to read.
                    if (m_segment.getReader(m_cursorId).hasMoreEntries()) return false;
                    inclusive = false;
                }

                for (PBDSegment currSegment : m_segments.tailMap(m_segment.segmentId(), inclusive).values()) {
                    if (currSegment.getNumEntries() > 0)  return false;
                }

                return true;
            }
        }

        private BBContainer wrapRetCont(PBDSegment segment, final BBContainer retcont) {
            return new BBContainer(retcont.b()) {
                @Override
                public void discard() {
                    synchronized(PersistentBinaryDeque.this) {
                        checkDoubleFree();
                        retcont.discard();
                        assert(m_closed || m_segments.containsKey(segment.segmentId()));

                        //Don't do anything else if we are closed
                        if (m_closed) {
                            return;
                        }

                        //Segment is potentially ready for deletion
                        try {
                            PBDSegmentReader segmentReader = segment.getReader(m_cursorId);
                            // Don't delete if this is the last segment.
                            // Cannot be deleted if this reader hasn't finished discarding this.
                            if (segment == peekLastSegment() || !segmentReader.allReadAndDiscarded()) {
                                return;
                            }
                            if (canDeleteSegment(segment)) {
                                m_segments.remove(segment.segmentId());
                                if (m_usageSpecificLog.isDebugEnabled()) {
                                    m_usageSpecificLog.debug("Segment " + segment.file() + " has been closed and deleted after discarding last buffer");
                                }
                                closeAndDeleteSegment(segment);
                            }
                        } catch (IOException e) {
                            LOG.error("Exception closing and deleting PBD segment", e);
                        }
                    }
                }
            };
        }
    }

    public static final OutputContainerFactory UNSAFE_CONTAINER_FACTORY = new UnsafeOutputContainerFactory();

    /**
     * Processors also log using this facility.
     */
    private final VoltLogger m_usageSpecificLog;

    private final File m_path;
    private final String m_nonce;
    private boolean m_initializedFromExistingFiles = false;
    private boolean m_awaitingTruncation = false;

    //Segments that are no longer being written to and can be polled
    //These segments are "immutable". They will not be modified until deletion
    private final TreeMap<Long, PBDSegment> m_segments = new TreeMap<>();
    private volatile boolean m_closed = false;
    private final HashMap<String, ReadCursor> m_readCursors = new HashMap<>();
    private int m_numObjects;
    private int m_numDeleted;

    /**
     * Create a persistent binary deque with the specified nonce and storage
     * back at the specified path. Existing files will
     *
     * @param nonce
     * @param path
     * @param logger
     * @throws IOException
     */
    public PersistentBinaryDeque(final String nonce, final File path, VoltLogger logger) throws IOException {
        this(nonce, path, logger, true);
    }

    /**
     * Create a persistent binary deque with the specified nonce and storage back at the specified path.
     * This is convenient method for test so that
     * poll with delete can be tested.
     *
     * @param nonce
     * @param path
     * @param deleteEmpty
     * @throws IOException
     */
    public PersistentBinaryDeque(final String nonce, final File path, VoltLogger logger, final boolean deleteEmpty) throws IOException {
        EELibraryLoader.loadExecutionEngineLibrary(true);
        m_path = path;
        m_nonce = nonce;
        m_usageSpecificLog = logger;

        if (!path.exists() || !path.canRead() || !path.canWrite() || !path.canExecute() || !path.isDirectory()) {
            throw new IOException(path + " is not usable ( !exists || !readable " +
                    "|| !writable || !executable || !directory)");
        }

        final TreeMap<Long, PBDSegment> segments = new TreeMap<Long, PBDSegment>();
        //Parse the files in the directory by name to find files
        //that are part of this deque
        try {
            path.listFiles(new FileFilter() {

                @Override
                public boolean accept(File pathname) {
                    // PBD file names have three parts: nonce.seq.pbd
                    // nonce may contain '.', seq is a sequence number.
                    String[] parts = pathname.getName().split("\\.");
                    String parsedNonce = null;
                    String seqNum = null;
                    String extension = null;

                    // If more than 3 parts, it means nonce contains '.', assemble them.
                    if (parts.length > 3) {
                        Joiner joiner = Joiner.on('.').skipNulls();
                        parsedNonce = joiner.join(Arrays.asList(parts).subList(0, parts.length - 2));
                        seqNum = parts[parts.length - 2];
                        extension = parts[parts.length - 1];
                    } else if (parts.length == 3) {
                        parsedNonce = parts[0];
                        seqNum = parts[1];
                        extension = parts[2];
                    }

                    if (nonce.equals(parsedNonce) && "pbd".equals(extension)) {
                        if (pathname.length() == 4) {
                            //Doesn't have any objects, just the object count
                            pathname.delete();
                            return false;
                        }
                        Long index = Long.valueOf(seqNum);
                        PBDSegment qs = newSegment( index, pathname );
                        try {
                            m_initializedFromExistingFiles = true;
                            if (deleteEmpty) {
                                if (qs.getNumEntries() == 0) {
                                    LOG.info("Found Empty Segment with entries: " + qs.getNumEntries() + " For: " + pathname.getName());
                                    if (m_usageSpecificLog.isDebugEnabled()) {
                                        m_usageSpecificLog.debug("Segment " + qs.file() + " has been closed and deleted during init");
                                    }
                                    qs.closeAndDelete();
                                    return false;
                                }
                            }
                            if (m_usageSpecificLog.isDebugEnabled()) {
                                m_usageSpecificLog.debug("Segment " + qs.file() + " has been recovered");
                            }
                            qs.close();
                            segments.put(index, qs);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return false;
                }

            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException) {
                throw new IOException(e);
            }
            Throwables.propagate(e);
        }

        Long lastKey = null;
        for (Map.Entry<Long, PBDSegment> e : segments.entrySet()) {
            final Long key = e.getKey();
            if (lastKey == null) {
                lastKey = key;
            } else {
                if (lastKey + 1 != key) {
                    try {
                        for (PBDSegment pbds : segments.values()) {
                            pbds.close();
                        }
                    } catch (Exception ex) {}
                    throw new IOException("Missing " + nonce +
                            " pbd segments between " + lastKey + " and " + key + " in directory " + path +
                            ". The data files found in the export overflow directory were inconsistent.");
                }
                lastKey = key;
            }
            m_segments.put(e.getKey(), e.getValue());
        }

        //Find the first and last segment for polling and writing (after)
        Long writeSegmentIndex = 0L;
        try {
            writeSegmentIndex = segments.lastKey() + 1;
        } catch (NoSuchElementException e) {}

        PBDSegment writeSegment =
            newSegment(
                    writeSegmentIndex,
                    new VoltFile(m_path, m_nonce + "." + writeSegmentIndex + ".pbd"));
        m_segments.put(writeSegmentIndex, writeSegment);
        writeSegment.openForWrite(true);

        m_numObjects = countNumObjects();
        assertions();
    }

    private int countNumObjects() throws IOException {
        int numObjects = 0;
        for (PBDSegment segment : m_segments.values()) {
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
            m_usageSpecificLog.debug("PBD " + m_nonce + " has no finished segments");
            return;
        }

        // Close the last write segment for now, will reopen after truncation
        peekLastSegment().close();

        /*
         * Iterator all the objects in all the segments and pass them to the truncator
         * When it finds the truncation point
         */
        Long lastSegmentIndex = null;
        for (PBDSegment segment : m_segments.values()) {
            final long segmentIndex = segment.segmentId();

            final int truncatedEntries = segment.parseAndTruncate(truncator);

            if (truncatedEntries == -1) {
                lastSegmentIndex = segmentIndex - 1;
                break;
            } else if (truncatedEntries > 0) {
                m_numObjects -= truncatedEntries;
                //Set last segment and break the loop over this segment
                lastSegmentIndex = segmentIndex;
                break;
            }
            // truncatedEntries == 0 means nothing is truncated in this segment,
            // should move on to the next segment.
        }

        /*
         * If it was found that no truncation is necessary, lastSegmentIndex will be null.
         * Return and the parseAndTruncate is a noop.
         */
        if (lastSegmentIndex == null)  {
            // Reopen the last segment for write
            peekLastSegment().openForWrite(true);
            return;
        }
        /*
         * Now truncate all the segments after the truncation point
         */
        Iterator<Long> iterator = m_segments.descendingKeySet().iterator();
        while (iterator.hasNext()) {
            Long segmentId = iterator.next();
            if (segmentId <= lastSegmentIndex) {
                break;
            }
            PBDSegment segment = m_segments.get(segmentId);
            m_numObjects -= segment.getNumEntries();
            iterator.remove();
            m_usageSpecificLog.debug("Segment " + segment.file() + " has been closed and deleted by truncator");
            segment.closeAndDelete();
        }

        /*
         * Reset the poll and write segments
         */
        //Find the first and last segment for polling and writing (after)
        Long newSegmentIndex = 0L;
        if (peekLastSegment() != null) newSegmentIndex = peekLastSegment().segmentId() + 1;

        PBDSegment newSegment = newSegment(newSegmentIndex, new VoltFile(m_path, m_nonce + "." + newSegmentIndex + ".pbd"));
        newSegment.openForWrite(true);
        if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug("Segment " + newSegment.file() + " has been created by PBD truncator");
        }
        m_segments.put(newSegment.segmentId(), newSegment);
        assertions();
    }

    private PBDSegment newSegment(long segmentId, File file) {
        return new PBDRegularSegment(segmentId, file);
    }

    /**
     * Close the tail segment if it's not being read from currently, then offer the new segment.
     * @throws IOException
     */
    private void closeTailAndOffer(PBDSegment newSegment) throws IOException {
        PBDSegment last = peekLastSegment();
        if (last != null && !last.isBeingPolled()) {
            last.close();
        }
        m_segments.put(newSegment.segmentId(), newSegment);
    }

    private PBDSegment peekFirstSegment() {
        Map.Entry<Long, PBDSegment> entry = m_segments.firstEntry();
        // entry may be null in ctor and while we are manipulating m_segments in addSegment, for example
        return (entry==null) ? null : entry.getValue();
    }

    private PBDSegment peekLastSegment() {
        Map.Entry<Long, PBDSegment> entry = m_segments.lastEntry();
        // entry may be null in ctor and while we are manipulating m_segments in addSegment, for example
        return (entry==null) ? null : entry.getValue();
    }

    private PBDSegment pollLastSegment() {
        Map.Entry<Long, PBDSegment> entry = m_segments.pollLastEntry();
        return (entry!=null) ? entry.getValue() : null;
    }

    private void closeAndDeleteSegmentsBefore(long segmentId, String reason) throws IOException {
        // Remove this segment and segments with a smaller sequence number if there are any
        Iterator<Map.Entry<Long, PBDSegment>> iter = m_segments.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, PBDSegment> entry = iter.next();
            if (entry.getKey() > segmentId) {
                break;
            }
            PBDSegment segmentToDelete = entry.getValue();
            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + segmentToDelete.file() + " has been closed and deleted " + reason);
            }
            closeAndDeleteSegment(segmentToDelete);
            iter.remove();
        }
    }

    @Override
    public synchronized void offer(BBContainer object) throws IOException {
        offer(object, true);
    }

    @Override
    public synchronized void offer(BBContainer object, boolean allowCompression) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Closed");
        }

        PBDSegment tail = peekLastSegment();
        final boolean compress = object.b().isDirect() && allowCompression;
        if (!tail.offer(object, compress)) {
            tail = addSegment(tail);
            final boolean success = tail.offer(object, compress);
            if (!success) {
                throw new IOException("Failed to offer object in PBD");
            }
        }
        m_numObjects++;
        assertions();
    }

    @Override
    public synchronized int offer(DeferredSerialization ds) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Cannot offer(): PBD has been Closed");
        }

        PBDSegment tail = peekLastSegment();
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
        return written;
    }

    private PBDSegment addSegment(PBDSegment tail) throws IOException {
        //Check to see if the tail is completely consumed so we can close and delete it
        if (tail.hasAllFinishedReading() && canDeleteSegment(tail)) {
            pollLastSegment();
            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + tail.file() + " has been closed and deleted because of empty queue");
            }
            closeAndDeleteSegment(tail);
        }
        Long nextIndex = tail.segmentId() + 1;
        tail = newSegment(nextIndex, new VoltFile(m_path, m_nonce + "." + nextIndex + ".pbd"));
        tail.openForWrite(true);
        if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug("Segment " + tail.file() + " has been created because of an offer");
        }
        closeTailAndOffer(tail);
        return tail;
    }

    private void closeAndDeleteSegment(PBDSegment segment) throws IOException {
        int toDelete = segment.getNumEntries();
        segment.closeAndDelete();
        m_numDeleted += toDelete;
    }

    @Override
    public synchronized void push(BBContainer objects[]) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Cannot push(): PBD has been Closed");
        }

        ArrayDeque<ArrayDeque<BBContainer>> segments = new ArrayDeque<ArrayDeque<BBContainer>>();
        ArrayDeque<BBContainer> currentSegment = new ArrayDeque<BBContainer>();

        //Take the objects that were provided and separate them into deques of objects
        //that will fit in a single write segment
        int available = PBDSegment.CHUNK_SIZE - 4;
        for (BBContainer object : objects) {
            int needed = PBDSegment.OBJECT_HEADER_BYTES + object.b().remaining();

            if (available - needed < 0) {
                if (needed > PBDSegment.CHUNK_SIZE - 4) {
                    throw new IOException("Maximum object size is " + (PBDSegment.CHUNK_SIZE - 4));
                }
                segments.offer( currentSegment );
                currentSegment = new ArrayDeque<BBContainer>();
                available = PBDSegment.CHUNK_SIZE - 4;
            }
            available -= needed;
            currentSegment.add(object);
        }

        segments.add(currentSegment);
        assert(segments.size() > 0);

        //Calculate the index for the first segment to push at the front
        //This will be the index before the first segment available for read or
        //before the write segment if there are no finished segments
        Long nextIndex = 0L;
        if (m_segments.size() > 0) {
            nextIndex = peekFirstSegment().segmentId() - 1;
        }

        while (segments.peek() != null) {
            ArrayDeque<BBContainer> currentSegmentContents = segments.poll();
            PBDSegment writeSegment =
                newSegment(
                        nextIndex,
                        new VoltFile(m_path, m_nonce + "." + nextIndex + ".pbd"));
            writeSegment.openForWrite(true);
            nextIndex--;
            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + writeSegment.file() + " has been created because of a push");
            }

            while (currentSegmentContents.peek() != null) {
                writeSegment.offer(currentSegmentContents.pollFirst(), false);
                m_numObjects++;
            }

            // Don't close the last one, it'll be used for writes
            if (!m_segments.isEmpty()) {
                writeSegment.close();
            }

            m_segments.put(writeSegment.segmentId(), writeSegment);
        }
        // Because we inserted at the beginning, cursors need to be rewound to the beginning
        rewindCursors();
        assertions();
    }

    private void rewindCursors() {
        PBDSegment firstSegment = peekFirstSegment();
        for (ReadCursor cursor : m_readCursors.values()) {
            cursor.m_segment = firstSegment;
        }
    }

    @Override
    public synchronized BinaryDequeReader openForRead(String cursorId) throws IOException {
        if (m_closed) {
            throw new IOException("Cannot openForRead(): PBD has been Closed");
        }

        ReadCursor reader = m_readCursors.get(cursorId);
        if (reader == null) {
            reader = new ReadCursor(cursorId, m_numDeleted);
            m_readCursors.put(cursorId, reader);
        }

        return reader;
    }

    @Override
    public synchronized void closeCursor(String cursorId) {
        if (m_closed) {
            return;
        }
        ReadCursor reader = m_readCursors.remove(cursorId);
        // If we never did a poll from this segment for this cursor,
        // there is no reader initialized for this cursor.
        if (reader != null && reader.m_segment != null && reader.m_segment.getReader(cursorId) != null) {
            try {
                reader.m_segment.getReader(cursorId).close();
            }
            catch (IOException e) {
                // TODO ignore this for now, it is just the segment file failed to be closed
            }
        }
        // check all segments from latest to oldest (excluding the last write segment) to see if they can be deleted
        // in a separate try catch block because these two are independent
        // We need this only in closeCursor() now, which is currently only used when removing snapshot placeholder
        // cursor in one-to-many DR, this extra check is needed because other normal cursors may have read past some
        // segments, leaving them hold only by the placeholder cursor, since we won't have triggers to check deletion
        // eligibility for these segments anymore, the check needs to take place here to prevent leaking of segments
        // file
        try {
            boolean isLastSegment = true;
            for (PBDSegment segment : m_segments.descendingMap().values()) {
                // skip the last segment
                if (isLastSegment) {
                    isLastSegment = false;
                    continue;
                }
                if (canDeleteSegment(segment)) {
                    closeAndDeleteSegmentsBefore(segment.segmentId(), "because of close of cursor");
                    break;
                }
            }
        }
        catch (IOException e) {
            LOG.error("Exception closing and deleting PBD segment", e);
        }
    }

    private boolean canDeleteSegment(PBDSegment segment) throws IOException {
        for (ReadCursor cursor : m_readCursors.values()) {
            if (cursor.m_segment != null && (cursor.m_segment.segmentId() >= segment.segmentId())) {
                PBDSegmentReader segmentReader = segment.getReader(cursor.m_cursorId);
                if (segmentReader == null) {
                    assert(cursor.m_segment.segmentId() == segment.segmentId());
                    segmentReader = segment.openForRead(cursor.m_cursorId);
                }

                if (!segmentReader.allReadAndDiscarded()) {
                    return false;
                }
            } else { // this cursor hasn't reached this segment yet
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
        for (PBDSegment segment : m_segments.values()) {
            if (!segment.isClosed()) {
                segment.sync();
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (m_closed) {
            return;
        }
        m_readCursors.clear();

        for (PBDSegment segment : m_segments.values()) {
            segment.close();
        }
        m_closed = true;
    }

    @Override
    public synchronized Pair<Integer, Long> getBufferCountAndSize() throws IOException {
        int count = 0;
        long size = 0;
        for (PBDSegment segment : m_segments.values()) {
            count += segment.getNumEntries();
            size += segment.size();
        }
        return Pair.of(count, size);
    }

    @Override
    public synchronized void closeAndDelete() throws IOException {
        if (m_closed) {
            return;
        }
        m_readCursors.clear();

        for (PBDSegment qs : m_segments.values()) {
            m_usageSpecificLog.debug("Segment " + qs.file() + " has been closed and deleted due to delete all");
            closeAndDeleteSegment(qs);
        }
        m_segments.clear();
        m_closed = true;
    }

    public static class ByteBufferTruncatorResponse extends TruncatorResponse {
        private final ByteBuffer m_retval;

        public ByteBufferTruncatorResponse(ByteBuffer retval) {
            super(Status.PARTIAL_TRUNCATE);
            assert retval.remaining() > 0;
            m_retval = retval;
        }

        @Override
        public int getTruncatedBuffSize() {
            return m_retval.remaining();
        }

        @Override
        public int writeTruncatedObject(ByteBuffer output) {
            int objectSize = m_retval.remaining();
            output.putInt(objectSize);
            output.putInt(PBDSegment.NO_FLAGS);
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
        public int writeTruncatedObject(ByteBuffer output) throws IOException {
            int bytesWritten = PBDUtils.writeDeferredSerialization(output, m_ds);
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
        if (!assertionsOn || m_closed) return;
        for (ReadCursor cursor : m_readCursors.values()) {
            int numObjects = 0;
            try {
                for (PBDSegment segment : m_segments.values()) {
                    PBDSegmentReader reader = segment.getReader(cursor.m_cursorId);
                    if (reader == null) {
                        numObjects += segment.getNumEntries();
                    } else {
                        numObjects += segment.getNumEntries() - reader.readIndex();
                    }
                }
                assert numObjects == cursor.getNumObjects() : numObjects + " != " + cursor.getNumObjects();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }
    }

    // Used by test only
    int numOpenSegments() {
        int numOpen = 0;
        for (PBDSegment segment : m_segments.values()) {
            if (!segment.isClosed()) numOpen++;
        }

        return numOpen;
    }

    public synchronized boolean isAwaitingTruncation()
    {
        return m_awaitingTruncation;
    }

    public synchronized void setAwaitingTruncation(boolean m_awaitingTruncation)
    {
        this.m_awaitingTruncation = m_awaitingTruncation;
    }
}
