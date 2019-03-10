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
import java.nio.file.NoSuchFileException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.Pair;
import org.voltdb.NativeLibraryLoader;
import org.voltdb.export.ExportSequenceNumberTracker;
import org.voltdb.utils.BinaryDeque.TruncatorResponse.Status;
import org.voltdb.utils.PairSequencer.CyclicSequenceException;

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
        public BBContainer poll(OutputContainerFactory ocf, boolean checkCRC) throws IOException {
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
                long lastSegmentId = peekLastSegment().segmentIndex();
                while (!segmentReader.hasMoreEntries()) {
                    if (m_segment.segmentIndex() == lastSegmentId) { // nothing more to read
                        return null;
                    }

                    segmentReader.close();
                    m_segment = m_segments.higherEntry(m_segment.segmentIndex()).getValue();
                    // push to PBD will rewind cursors. So, this cursor may have already opened this segment
                    segmentReader = m_segment.getReader(m_cursorId);
                    if (segmentReader == null) {
                        segmentReader = m_segment.openForRead(m_cursorId);
                    }
                }
                BBContainer retcont = segmentReader.poll(ocf, checkCRC);
                if (retcont == null) {
                    return null;
                }

                m_numRead++;
                assertions();
                assert (retcont.b() != null);
                return wrapRetCont(m_segment, retcont);
            }
        }

        @Override
        public BBContainer getSchema(long segmentIndex, boolean restoreReaderOffset, boolean checkCRC)
                throws IOException {
            synchronized (PersistentBinaryDeque.this) {
                if (m_closed) {
                    throw new IOException("PBD.ReadCursor.poll(): " + m_cursorId + " - Reader has been closed");
                }
                PBDSegmentReader segmentReader = null;
                PBDSegment segment = null;
                moveToValidSegment();
                if (segmentIndex != -1) {
                    // looking for schema from a specific segment
                    segment = m_segments.get(segmentIndex);
                    if (segment == null) {
                        return null;
                    }
                } else {
                    // looking for schema from the segment that cursor is currently reading on
                    segment = m_segment;
                }

                long originalOffset = -1;
                try {
                    segmentReader = segment.getReader(m_cursorId);
                    if (segmentReader == null) {
                        segmentReader = segment.openForRead(m_cursorId);
                    }
                    // need to restore the read offset
                    originalOffset = segmentReader.readOffset();
                    segmentReader.setReadOffset(PBDSegment.ENTRY_HEADER_BYTES);
                    return segmentReader.getSchema(checkCRC);
                } finally {
                    if (segmentReader != null && restoreReaderOffset) {
                        segmentReader.setReadOffset(originalOffset);
                    }
                }
            }
        }

        private void moveToValidSegment() {
            PBDSegment firstSegment = peekFirstSegment();
            // It is possible that m_segment got closed and removed
            if (m_segment == null || m_segment.segmentIndex() < firstSegment.segmentIndex()) {
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
                for (PBDSegment currSegment : m_segments.tailMap(m_segment.segmentIndex(), inclusive).values()) {
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
                    if (m_segment.getReader(m_cursorId).hasMoreEntries()) {
                        return false;
                    }
                    inclusive = false;
                }

                for (PBDSegment currSegment : m_segments.tailMap(m_segment.segmentIndex(), inclusive).values()) {
                    if (currSegment.getNumEntries(false) > 0) {
                        return false;
                    }
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
                        assert(m_closed || m_segments.containsKey(segment.segmentIndex()));

                        //Don't do anything else if we are closed
                        if (m_closed) {
                            return;
                        }

                        //Segment is potentially ready for deletion
                        try {
                            PBDSegmentReader segmentReader = segment.getReader(m_cursorId);
                            // Don't delete if this is the last segment.
                            // Cannot be deleted if this reader hasn't finished discarding this.
                            if (segment == peekLastSegment() || (segmentReader != null && !segmentReader.allReadAndDiscarded())) {
                                return;
                            }
                            if (canDeleteSegment(segment)) {
                                m_segments.remove(segment.segmentIndex());
                                if (m_usageSpecificLog.isDebugEnabled()) {
                                    m_usageSpecificLog.debug("Segment " + segment.file() + " has been closed and deleted after discarding last buffer");
                                }
                                // If the segment that cursor currently points to is going to be deleted,
                                // looks for next segment.
                                long lastSegmentId = peekLastSegment().segmentIndex();
                                if (m_segment == segment) {
                                    // Tail segment should always exists
                                    if (m_segment.segmentIndex() == lastSegmentId) {
                                        throw new IOException("Tail segment file " + m_segment.file().getName() + " shouldn't be deleted! ");
                                    }
                                    m_segment = m_segments.higherEntry(m_segment.segmentIndex()).getValue();
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

        @Override
        public boolean isStartOfSegment() throws IOException {
            synchronized(PersistentBinaryDeque.this) {
                if (m_closed) {
                    throw new IOException("Cannot call isReadFirstObjectOfSegment: PBD has been closed");
                }
                assertions();
                moveToValidSegment();
                PBDSegmentReader segmentReader = m_segment.getReader(m_cursorId);
                // push to PBD will rewind cursors. So, this cursor may have already opened this segment
                if (segmentReader == null) {
                    segmentReader = m_segment.openForRead(m_cursorId);
                }
                long lastSegmentId = peekLastSegment().segmentIndex();
                while (!segmentReader.hasMoreEntries()) {
                    if (m_segment.segmentIndex() == lastSegmentId) { // nothing more to read
                        return false;
                    }

                    segmentReader.close();
                    m_segment = m_segments.higherEntry(m_segment.segmentIndex()).getValue();
                    // push to PBD will rewind cursors. So, this cursor may have already opened this segment
                    segmentReader = m_segment.getReader(m_cursorId);
                    if (segmentReader == null) {
                        segmentReader = m_segment.openForRead(m_cursorId);
                    }
                }
                if (segmentReader.readIndex() == 0) {
                    return true;
                }
                return false;
            }
        }

        public long getSegmentIndex() {
            synchronized(PersistentBinaryDeque.this) {
                if (m_segment == null) {
                    return -1;
                }
                return m_segment.segmentIndex();
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
    private boolean m_initializedFromExistingFiles = false;
    private boolean m_awaitingTruncation = false;

    //Segments that are no longer being written to and can be polled
    //These segments are "immutable". They will not be modified until deletion
    private final TreeMap<Long, PBDSegment> m_segments = new TreeMap<>();
    private volatile boolean m_closed = false;
    private final HashMap<String, ReadCursor> m_readCursors = new HashMap<>();
    private int m_numObjects;
    private int m_numDeleted;

    // Monotonic segment counter: note that this counter always *increases* even when
    // used for a *previous* segment (or inserting a segment *before* the others.
    private long m_segmentCounter = 0L;

    /**
     * Create a persistent binary deque with the specified nonce and storage
     * back at the specified path.
     *
     * @param nonce
     * @param schemaDS
     * @param path
     * @param logger
     * @throws IOException
     */
    public PersistentBinaryDeque(final String nonce, DeferredSerialization schemaDS,
            final File path, VoltLogger logger) throws IOException {
        this(nonce, schemaDS, path, logger, true);
    }

    /**
     * Create a persistent binary deque with the specified nonce and storage back at the specified path.
     * This is a convenience method for testing so that poll with delete can be tested.
     *
     * @param nonce
     * @param schemaDS
     * @param path
     * @param deleteEmpty
     * @throws IOException
     */
    public PersistentBinaryDeque(final String nonce, DeferredSerialization schemaDS,
            final File path, VoltLogger logger,
            final boolean deleteEmpty) throws IOException {
        NativeLibraryLoader.loadVoltDB();
        m_path = path;
        m_nonce = nonce;
        m_usageSpecificLog = logger;

        if (!path.exists() || !path.canRead() || !path.canWrite() || !path.canExecute() || !path.isDirectory()) {
            throw new IOException(path + " is not usable ( !exists || !readable " +
                    "|| !writable || !executable || !directory)");
        }

        parseFiles(deleteEmpty);

        // Find the first and last segment for polling and writing (after); ensure the
        // writing segment is not final

        long curId = getNextSegmentId();
        Map.Entry<Long, PBDSegment> lastEntry = m_segments.lastEntry();

        // Note: the "previous" id value may be > "current" id value
        long prevId = lastEntry == null ? getNextSegmentId() : lastEntry.getValue().segmentId();
        Long writeSegmentIndex = lastEntry == null ? 1L : lastEntry.getKey() + 1;

        String fname = getSegmentFileName(curId, prevId);
        PBDSegment writeSegment = newSegment(writeSegmentIndex, curId, new VoltFile(m_path, fname));

        PBDSegment check = m_segments.put(writeSegmentIndex, writeSegment);
        if (check != null) {
            // Sanity check
            throw new IllegalStateException("Overwriting segment " + writeSegmentIndex);
        }
        writeSegment.openForWrite(true);
        writeSegment.setFinal(false);
        if (schemaDS != null) {
            writeSegment.writeExtraHeader(schemaDS);
        }

        if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug("Segment " + writeSegment.file()
                + " (final: " + writeSegment.isFinal() + "), has been opened for writing");
        }

        m_numObjects = countNumObjects();
        assertions();
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
        return PbdSegmentName.createName(m_nonce, currentId, previousId);
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
     * @param deleteEmpty true if must delete empty PBD files
     *
     * @throws IOException
     */
    private void parseFiles(boolean deleteEmpty) throws IOException {

        HashMap<Long, File> filesById = new HashMap<>();
        PairSequencer<Long> sequencer = new PairSequencer<>();
        try {
            for (File file : m_path.listFiles()) {
                if (file.isDirectory() || !file.isFile() || file.isHidden()) {
                    continue;
                }

                PbdSegmentName segmentName = PbdSegmentName.parseFile(m_usageSpecificLog, file);

                switch (segmentName.m_result) {
                case INVALID_NAME:
                    deleteStalePbdFile(file);
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
                if (file.length() == 0) {
                    deleteStalePbdFile(file);
                    continue;
                }

                long maxCnt = Math.max(segmentName.m_id, segmentName.m_prevId);
                if (m_segmentCounter < maxCnt) {
                    m_segmentCounter = maxCnt;
                }
                filesById.put(segmentName.m_id, file);
                sequencer.add(new Pair<Long, Long>(segmentName.m_prevId, segmentName.m_id));
            }

            // Handle common cases: no PBD files or just one
            if (filesById.size() == 0) {
                m_usageSpecificLog.info("No PBD segments for " + m_nonce);
                return;
            }

            m_initializedFromExistingFiles = true;
            if (filesById.size() == 1) {
                // Common case, only 1 PBD segment
                for (Map.Entry<Long, File> entry: filesById.entrySet()) {
                    PBDSegment seg = newSegment(1, entry.getKey(), entry.getValue());
                    recoverSegment(seg, deleteEmpty);
                    break;
                }
            } else {
                // Handle the uncommon case of more than 1 PBD file, get the sequence of segments
                Deque<Deque<Long>> sequences = sequencer.getSequences();
                if (sequences.size() > 1) {
                    // FIXME: reject this case for now
                    // FIXME: we could select the sequence that has the oldest entry and delete
                    // the other files
                    throw new IOException("Found " + sequences.size() + " PBD sequences for " + m_nonce);
                }
                Deque<Long> sequence = sequences.getFirst();
                Long index = 1L;
                for (Long segmentId : sequence) {
                    File file = filesById.get(segmentId);
                    if (file == null) {
                        // This is an Id in the sequence referring to a previous file that
                        // was deleted, so move on.
                        continue;
                    }
                    PBDSegment seg = newSegment(index++, segmentId, filesById.get(segmentId));
                    recoverSegment(seg, deleteEmpty);
                }
            }
        } catch (CyclicSequenceException e) {
            LOG.error("Failed to parse files: " + e);
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
     * @param file
     * @throws IOException
     */
    private void deleteStalePbdFile(File file) throws IOException {
        try {
            PBDSegment.setFinal(file, false);
            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + file.getName()
                + " (final: " + PBDSegment.isFinal(file) + "), will be closed and deleted during init");
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
     * Recover a PBD segment and add it to m_segments
     *
     * @param qs
     * @param deleteEmpty
     * @throws IOException
     */
    private void recoverSegment(PBDSegment qs, boolean deleteEmpty) throws IOException {

        if (deleteEmpty && qs.getNumEntries(false) == 0) {
            qs.setFinal(false);
            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Found Empty Segment with entries: " + qs.getNumEntries(false) + " For: " + qs.file().getName());
                m_usageSpecificLog.debug("Segment " + qs.file()
                + " (final: " + qs.isFinal() + "), will be closed and deleted during init");
            }
            qs.closeAndDelete();
            return;
        }

        // Any recovered segment that is not final should be checked
        // for internal consistency.
        if (!qs.isFinal()) {
            LOG.warn("Segment " + qs.file()
            + " (final: " + qs.isFinal() + "), has been recovered but is not in a final state");
        } else if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug("Segment " + qs.file()
                + " (final: " + qs.isFinal() + "), has been recovered");
        }
        qs.close();
        m_segments.put(qs.segmentIndex(), qs);
    }

    private int countNumObjects() throws IOException {
        int numObjects = 0;
        for (PBDSegment segment : m_segments.values()) {
            numObjects += segment.getNumEntries(false);
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
            final long segmentIndex = segment.segmentIndex();

            final int truncatedEntries = segment.parseAndTruncate(truncator);

            if (truncatedEntries == -1) {
                // This whole segment will be truncated in the truncation loop below
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
         * Return and the parseAndTruncate is a noop, except for the finalization.
         */
        if (lastSegmentIndex == null)  {
            // Reopen the last segment for write - ensure it is not final
            PBDSegment lastSegment = peekLastSegment();
            lastSegment.openForWrite(true);
            lastSegment.setFinal(false);

            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + lastSegment.file()
                    + " (final: " + lastSegment.isFinal() + "), has been opened for writing after truncation");
            }
            return;
        }
        /*
         * Now truncate all the segments after the truncation point.
         */
        Iterator<Long> iterator = m_segments.descendingKeySet().iterator();
        while (iterator.hasNext()) {
            Long segmentId = iterator.next();
            if (segmentId <= lastSegmentIndex) {
                break;
            }
            PBDSegment segment = m_segments.get(segmentId);
            m_numObjects -= segment.getNumEntries(false);
            iterator.remove();

            // Ensure the file is not final before closing and truncating
            segment.setFinal(false);
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

        PBDSegment lastSegment = peekLastSegment();
        Long newSegmentIndex = lastSegment == null ? 1L : lastSegment.segmentIndex() + 1;
        long prevId = lastSegment == null ? getNextSegmentId() : lastSegment.segmentId();

        String fname = getSegmentFileName(curId, prevId);
        PBDSegment newSegment = newSegment(newSegmentIndex, curId, new VoltFile(m_path, fname));
        newSegment.openForWrite(true);
        // Ensure the new segment is not final
        newSegment.setFinal(false);

        if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug("Segment " + newSegment.file()
            + " (final: " + newSegment.isFinal() + "), has been created by PBD truncator");
        }
        m_segments.put(newSegment.segmentIndex(), newSegment);
        assertions();
    }

    private PBDSegment newSegment(long segmentIndex, long segmentId, File file) {
        return new PBDRegularSegment(segmentIndex, segmentId, file);
    }

    /**
     * Close the tail segment if it's not being read from currently, then offer the new segment.
     * @throws IOException
     */
    private void closeTailAndOffer(PBDSegment newSegment) throws IOException {
        PBDSegment last = peekLastSegment();
        if (last != null) {
            if (!last.isBeingPolled()) {
                last.close();
            } else {
                last.sync();
            }
            last.setFinal(true);

            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + last.file()
                + " (final: " + last.isFinal() + "), has been closed by offer to PBD");
            }
        }
        m_segments.put(newSegment.segmentIndex(), newSegment);
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
        offer(object, null, true, false);
    }

    @Override
    public synchronized void offer( BBContainer object,
                                    DeferredSerialization schemaDS,
                                    boolean allowCompression,
                                    boolean createNewFile) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Closed");
        }

        PBDSegment tail = peekLastSegment();
        final boolean compress = object.b().isDirect() && allowCompression;
        if (createNewFile) {
            tail = addSegment(tail, schemaDS);
        }
        if (!tail.offer(object, compress)) {
            tail = addSegment(tail, schemaDS);
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
            tail = addSegment(tail, null);
            written = tail.offer(ds);
            if (written < 0) {
                throw new IOException("Failed to offer object in PBD");
            }
        }
        m_numObjects++;
        assertions();
        return written;
    }

    private PBDSegment addSegment(PBDSegment tail, DeferredSerialization schemaDS) throws IOException {
        //Check to see if the tail is completely consumed so we can close and delete it
        if (tail.hasAllFinishedReading() && canDeleteSegment(tail)) {
            pollLastSegment();
            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + tail.file() + " has been closed and deleted because of empty queue");
            }
            closeAndDeleteSegment(tail);
        }
        Long nextIndex = tail.segmentIndex() + 1;
        long lastId = tail.segmentId();

        long curId = getNextSegmentId();
        String fname = getSegmentFileName(curId, lastId);
        tail = newSegment(nextIndex, curId, new VoltFile(m_path, fname));
        tail.openForWrite(true);
        tail.setFinal(false);
        if (schemaDS != null) {
            tail.writeExtraHeader(schemaDS);
        }
        if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug("Segment " + tail.file()
                + " (final: " + tail.isFinal() + "), has been created because of an offer");
        }
        closeTailAndOffer(tail);
        return tail;
    }

    private void closeAndDeleteSegment(PBDSegment segment) throws IOException {
        int toDelete = segment.getNumEntries(false);
        segment.setFinal(false);
        if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug("Closing and deleting segment " + segment.file()
                + " (final: " + segment.isFinal() + ")");
        }
        segment.closeAndDelete();
        m_numDeleted += toDelete;
    }

    @Override
    public synchronized void push(BBContainer objects[], DeferredSerialization ds) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Cannot push(): PBD has been Closed");
        }

        ArrayDeque<ArrayDeque<BBContainer>> segments = new ArrayDeque<ArrayDeque<BBContainer>>();
        ArrayDeque<BBContainer> currentSegment = new ArrayDeque<BBContainer>();

        //Take the objects that were provided and separate them into deques of objects
        //that will fit in a single write segment
        int maxObjectSize = PBDSegment.CHUNK_SIZE - PBDSegment.SEGMENT_HEADER_BYTES ;
        if (ds != null) {
            maxObjectSize -= ds.getSerializedSize();
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
        PBDSegment first = peekFirstSegment();
        Long nextIndex = first == null ? 1L : first.segmentIndex() - 1;

        // The first segment id is either the "previous" of the current head
        // (this avoids having to rename the file of the current head), or new id.
        long curId = first == null ? getNextSegmentId() : getPreviousSegmentId(first.file());

        while (segments.peek() != null) {
            ArrayDeque<BBContainer> currentSegmentContents = segments.poll();

            long prevId = getNextSegmentId();
            String fname = getSegmentFileName(curId, prevId);

            PBDSegment writeSegment =
                newSegment(
                        nextIndex,
                        curId,
                        new VoltFile(m_path, fname));
            writeSegment.openForWrite(true);
            writeSegment.setFinal(false);
            if (ds != null) {
                writeSegment.writeExtraHeader(ds);
            }

            // Prepare for next file
            nextIndex--;
            curId = prevId;

            while (currentSegmentContents.peek() != null) {
                writeSegment.offer(currentSegmentContents.pollFirst(), false);
                m_numObjects++;
            }

            // If this segment is to become the writing segment, don't close and
            // finalize it.
            if (!m_segments.isEmpty()) {
                writeSegment.close();
                writeSegment.setFinal(true);
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
                    closeAndDeleteSegmentsBefore(segment.segmentIndex(), "because of close of cursor");
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
            if (cursor.m_segment != null && (cursor.m_segment.segmentIndex() >= segment.segmentIndex())) {
                PBDSegmentReader segmentReader = segment.getReader(cursor.m_cursorId);
                if (segmentReader == null) {
                    assert(cursor.m_segment.segmentIndex() == segment.segmentIndex());
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

            // When closing a PBD, all segments may be finalized because on
            // recover a new segment will be opened for writing
            segment.close();
            segment.setFinal(true);
            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Closed segment " + segment.file()
                    + " (final: " + segment.isFinal() + "), on PBD close");
            }
        }
        m_closed = true;
    }

    @Override
    public synchronized Pair<Integer, Long> getBufferCountAndSize() throws IOException {
        int count = 0;
        long size = 0;
        for (PBDSegment segment : m_segments.values()) {
            count += segment.getNumEntries(false);
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
        private final CRC32 m_crc;

        public ByteBufferTruncatorResponse(ByteBuffer retval) {
            super(Status.PARTIAL_TRUNCATE);
            assert retval.remaining() > 0;
            m_retval = retval;
            m_crc = new CRC32();
        }

        @Override
        public int getTruncatedBuffSize() {
            return m_retval.remaining();
        }

        @Override
        public int writeTruncatedObject(ByteBuffer output) {
            int objectSize = m_retval.remaining();
            // write entry header
            PBDUtils.writeEntryHeader(m_crc, output, m_retval, objectSize, PBDSegment.NO_FLAGS);
            // write entry
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
        public int writeTruncatedObject(ByteBuffer output) throws IOException {
            output.position(PBDSegment.ENTRY_HEADER_BYTES);
            int bytesWritten = MiscUtils.writeDeferredSerialization(output, m_ds);
            output.position(PBDSegment.ENTRY_HEADER_BYTES);
            ByteBuffer header = output.duplicate();
            header.position(PBDSegment.ENTRY_HEADER_CRC_OFFSET);
            PBDUtils.writeEntryHeader(m_crc32, header, output, bytesWritten, PBDSegment.NO_FLAGS);
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
                for (PBDSegment segment : m_segments.values()) {
                    PBDSegmentReader reader = segment.getReader(cursor.m_cursorId);
                    if (reader == null) {
                        numObjects += segment.getNumEntries(false);
                    } else {
                        numObjects += segment.getNumEntries(false) - reader.readIndex();
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
            if (!segment.isClosed()) {
                numOpen++;
            }
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

    @Override
    public ExportSequenceNumberTracker scanForGap(BinaryDequeScanner scaner) throws IOException
    {
        if (m_closed) {
            throw new IOException("Cannot scanForGap(): PBD has been closed");
        }

        assertions();
        if (m_segments.isEmpty()) {
            m_usageSpecificLog.debug("PBD " + m_nonce + " has no finished segments");
            return new ExportSequenceNumberTracker();
        }

        // Close the last write segment for now, will reopen after scan
        peekLastSegment().close();

        ExportSequenceNumberTracker gapTracker = new ExportSequenceNumberTracker();

        /*
         * Iterator all the objects in all the segments and pass them to the scanner
         */
        for (PBDSegment segment : m_segments.values()) {
            ExportSequenceNumberTracker tracker = segment.scan(scaner);
            gapTracker.mergeTracker(tracker);
        }
        // Reopen the last segment for write
        peekLastSegment().openForWrite(false);
        return gapTracker;
    }
}
