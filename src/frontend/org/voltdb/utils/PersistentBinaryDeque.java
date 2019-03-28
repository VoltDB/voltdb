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
    private class ReadCursor implements BinaryDequeReader {
        private final String m_cursorId;
        private PBDSegment m_segment;
        // Number of objects out of the total
        //that were deleted at the time this cursor was created
        private final int m_numObjectsDeleted;
        private int m_numRead;
        // If a rewind occurred this is set to the segment id where this cursor was before the rewind
        private long m_rewoundFromId = -1;

        public ReadCursor(String cursorId, int numObjectsDeleted) {
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
        public BBContainer getExtraHeader(long segmentIndex) throws IOException {
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

                segmentReader = segment.getReader(m_cursorId);
                if (segmentReader == null) {
                    segmentReader = segment.openForRead(m_cursorId);
                } else if (segmentReader.isClosed()) {
                    segmentReader.reopen();
                }
                // need to restore the read offset
                return segmentReader.getExtraHeader();
            }
        }

        void rewindTo(PBDSegment segment) {
            if (m_rewoundFromId == -1 && m_segment != null) {
                m_rewoundFromId = m_segment.segmentId();
            }
            m_segment = segment;
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
                    if (currSegment.getNumEntries() > 0) {
                        return false;
                    }
                }

                return true;
            }
        }

        private BBContainer wrapRetCont(PBDSegment segment, int entryNumber, final BBContainer retcont) {
            return new BBContainer(retcont.b()) {
                @Override
                public void discard() {
                    synchronized(PersistentBinaryDeque.this) {
                        checkDoubleFree();
                        retcont.discard();
                        assert(m_closed || m_segments.containsKey(segment.segmentIndex()));

                        // Only continue if open there is another segment and this is the first entry of a segment
                        if (m_closed || m_segments.size() == 1
                                || (entryNumber != 1 && m_rewoundFromId != segment.m_id)
                                || !canDeleteSegmentsBefore(segment)) {
                            return;
                        }

                        if (m_rewoundFromId == segment.m_id) {
                            m_rewoundFromId = -1;
                        }

                        try {
                            Iterator<PBDSegment> iter = m_segments.headMap(segment.segmentIndex(), false).values()
                                    .iterator();
                            while (iter.hasNext()) {
                                PBDSegment earlierSegment = iter.next();
                                iter.remove();
                                if (m_usageSpecificLog.isDebugEnabled()) {
                                    m_usageSpecificLog.debug("Segment " + earlierSegment.file()
                                            + " has been closed and deleted after discarding last buffer");
                                }
                                closeAndDeleteSegment(earlierSegment);
                            }
                        } catch (IOException e) {
                            m_usageSpecificLog.error("Exception closing and deleting PBD segment", e);
                        }
                    }
                }
            };
        }

        @Override
        public boolean isStartOfSegment() throws IOException {
            synchronized(PersistentBinaryDeque.this) {
                if (m_closed) {
                    throw new IOException("Cannot call isStartOfSegment: PBD has been closed");
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

        @Override
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
    private final boolean m_compress;
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

    private DeferredSerialization m_extraHeader;

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
        this(nonce, schemaDS, path, logger, false);
    }

    /**
     * Create a persistent binary deque with the specified nonce and storage back at the specified path.
     * This is a convenience method for testing so that poll with delete can be tested.
     *
     * @param nonce
     * @param extraHeader
     * @param path
     * @param deleteEmpty
     * @throws IOException
     */
    public PersistentBinaryDeque(final String nonce, DeferredSerialization extraHeader,
            final File path, VoltLogger logger,
            final boolean compress) throws IOException {
        NativeLibraryLoader.loadVoltDB();
        m_path = path;
        m_nonce = nonce;
        m_usageSpecificLog = logger;
        m_compress = compress;

        if (!path.exists() || !path.canRead() || !path.canWrite() || !path.canExecute() || !path.isDirectory()) {
            throw new IOException(path + " is not usable ( !exists || !readable " +
                    "|| !writable || !executable || !directory)");
        }

        parseFiles();

        // Find the first and last segment for polling and writing (after); ensure the
        // writing segment is not final

        long curId = getNextSegmentId();
        Map.Entry<Long, PBDSegment> lastEntry = m_segments.lastEntry();

        // Note: the "previous" id value may be > "current" id value
        long prevId = lastEntry == null ? getNextSegmentId() : lastEntry.getValue().segmentId();
        Long writeSegmentIndex = lastEntry == null ? 1L : lastEntry.getKey() + 1;

        String fname = getSegmentFileName(curId, prevId);
        m_extraHeader = extraHeader;
        PBDSegment writeSegment = initializeNewSegment(writeSegmentIndex, curId, new VoltFile(m_path, fname),
                "initialization");
        if (m_segments.put(writeSegmentIndex, writeSegment) != null) {
            // Sanity check
            throw new IllegalStateException("Overwriting segment " + writeSegmentIndex);
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
     * @throws IOException
     */
    private void parseFiles() throws IOException {

        HashMap<Long, PbdSegmentName> filesById = new HashMap<>();
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
                filesById.put(segmentName.m_id, segmentName);
                sequencer.add(new Pair<Long, Long>(segmentName.m_prevId, segmentName.m_id));
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
                    throw new IOException("Found " + sequences.size() + " PBD sequences for " + m_nonce);
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
     * Quarantine a segment which was already added to {@link #m_segments}
     *
     * @param prevEntry {@link Map.Entry} from {@link #m_segments} to quarantine
     * @throws IOException
     */
    private void quarantineSegment(Map.Entry<Long, PBDSegment> prevEntry) throws IOException {
        quarantineSegment(prevEntry, prevEntry.getValue(), prevEntry.getValue().getNumEntries());
    }

    /**
     * Quarantine a segment which has not yet been added to {@link #m_segments}
     *
     * @param segment
     * @throws IOException
     */
    private void quarantineSegment(PBDSegment segment) throws IOException {
        quarantineSegment(null, segment, 0);
    }

    private void quarantineSegment(Map.Entry<Long, PBDSegment> prevEntry, PBDSegment segment, int decrementEntryCount)
            throws IOException {
        try {
            PbdSegmentName quarantinedSegment = PbdSegmentName.asQuarantinedSegment(m_usageSpecificLog, segment.file());
            if (!segment.file().renameTo(quarantinedSegment.m_file)) {
                throw new IOException("Failed to quarantine segment: " + segment.file());
            }

            PBDSegment quarantined = new PbdQuarantinedSegment(quarantinedSegment.m_file, segment.segmentIndex(),
                    segment.segmentId());

            if (prevEntry == null) {
                PBDSegment prev = m_segments.put(segment.segmentIndex(), quarantined);
                assert prev == null;
            } else {
                PBDSegment prev = prevEntry.setValue(quarantined);
                assert segment == prev;
            }
            m_numObjects -= decrementEntryCount;
        } finally {
            segment.close();
        }
    }

    /**
     * Recover a PBD segment and add it to m_segments
     *
     * @param segment
     * @param deleteEmpty
     * @throws IOException
     */
    private void recoverSegment(long segmentIndex, long segmentId, PbdSegmentName segmentName) throws IOException {
        PBDSegment segment;
        if (segmentName.m_quarantined) {
            segment = new PbdQuarantinedSegment(segmentName.m_file, segmentIndex, segmentId);
        } else {
            segment = newSegment(segmentIndex, segmentId, segmentName.m_file);

            try {
                if (segment.getNumEntries() == 0) {
                    if (m_usageSpecificLog.isDebugEnabled()) {
                        m_usageSpecificLog.debug("Found Empty Segment with entries: " + segment.getNumEntries()
                                + " For: " + segment.file().getName());
                        m_usageSpecificLog.debug("Segment " + segment.file() + " (final: " + segment.isFinal()
                                + "), will be closed and deleted during init");
                    }
                    segment.closeAndDelete();
                    return;
                }

                // Any recovered segment that is not final should be checked
                // for internal consistency.
                if (!segment.isFinal()) {
                    m_usageSpecificLog.warn("Segment " + segment.file() + " (final: " + segment.isFinal()
                            + "), has been recovered but is not in a final state");
                } else if (m_usageSpecificLog.isDebugEnabled()) {
                    m_usageSpecificLog.debug(
                            "Segment " + segment.file() + " (final: " + segment.isFinal() + "), has been recovered");
                }
                m_segments.put(segment.segmentIndex(), segment);
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
        for (Map.Entry<Long, PBDSegment> entry : m_segments.entrySet()) {
            PBDSegment segment = entry.getValue();
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
            PBDSegment lastSegment = peekLastSegment();
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
        Iterator<PBDSegment> iterator = m_segments.tailMap(lastSegmentIndex, false).values().iterator();
        while (iterator.hasNext()) {
            PBDSegment segment = iterator.next();
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

        PBDSegment lastSegment = peekLastSegment();
        Long newSegmentIndex = lastSegment == null ? 1L : lastSegment.segmentIndex() + 1;
        long prevId = lastSegment == null ? getNextSegmentId() : lastSegment.segmentId();

        String fname = getSegmentFileName(curId, prevId);
        PBDSegment newSegment = initializeNewSegment(newSegmentIndex, curId, new VoltFile(m_path, fname),
                "PBD truncator");
        m_segments.put(newSegment.segmentIndex(), newSegment);
        assertions();
    }

    private PBDSegment newSegment(long segmentIndex, long segmentId, File file) {
        return new PBDRegularSegment(segmentIndex, segmentId, file);
    }

    private PBDSegment initializeNewSegment(long segmentIndex, long segmentId, File file, String reason)
            throws IOException {
        PBDSegment segment = newSegment(segmentIndex, segmentId, file);
        try {
            segment.openNewSegment(m_compress);
            if (m_extraHeader != null) {
                segment.writeExtraHeader(m_extraHeader);
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

    @Override
    public void updateExtraHeader(DeferredSerialization extraHeaderSerializer) throws IOException {
        m_extraHeader = extraHeaderSerializer;
        addSegment(peekLastSegment());
    }

    @Override
    public synchronized void offer(BBContainer object) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Closed");
        }

        PBDSegment tail = peekLastSegment();
        if (!tail.offer(object)) {
            tail = addSegment(tail);
            final boolean success = tail.offer(object);
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
        tail.finalize(!tail.isBeingPolled());

        if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug(
                    "Segment " + tail.file() + " (final: " + tail.isFinal() + "), has been closed by offer to PBD");
        }
        Long nextIndex = tail.segmentIndex() + 1;
        long lastId = tail.segmentId();

        long curId = getNextSegmentId();
        String fname = getSegmentFileName(curId, lastId);
        PBDSegment newSegment = initializeNewSegment(nextIndex, curId, new VoltFile(m_path, fname), "an offer");
        m_segments.put(newSegment.segmentIndex(), newSegment);

        return newSegment;
    }

    private void closeAndDeleteSegment(PBDSegment segment) throws IOException {
        int toDelete = segment.getNumEntries();
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

            PBDSegment writeSegment = initializeNewSegment(nextIndex, curId, new VoltFile(m_path, fname), "a push");

            // Prepare for next file
            nextIndex--;
            curId = prevId;

            while (currentSegmentContents.peek() != null) {
                writeSegment.offer(currentSegmentContents.pollFirst());
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
        PBDSegment firstSegment = peekFirstSegment();
        for (ReadCursor cursor : m_readCursors.values()) {
            cursor.rewindTo(firstSegment);
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
        // check all segments from latest to oldest to see if segments before that segment can be deleted
        // We need this only in closeCursor() now, which is currently only used when removing snapshot placeholder
        // cursor in one-to-many DR, this extra check is needed because other normal cursors may have read past some
        // segments, leaving them hold only by the placeholder cursor, since we won't have triggers to check deletion
        // eligibility for these segments anymore, the check needs to take place here to prevent leaking of segments
        // file
        try {
            boolean deleteSegment = false;
            Iterator<PBDSegment> iter = m_segments.descendingMap().values().iterator();
            while (iter.hasNext()) {
                PBDSegment segment = iter.next();
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

    private boolean canDeleteSegmentsBefore(PBDSegment segment) {
        for (ReadCursor cursor : m_readCursors.values()) {
            if (cursor.m_segment == null) {
                return false;
            }

            long segmentIndex = segment.segmentIndex();
            long cursorSegmentIndex = cursor.m_segment.segmentIndex();
            if (cursorSegmentIndex < segmentIndex) {
                return false;
            }

            PBDSegmentReader segmentReader = segment.getReader(cursor.m_cursorId);
            if (segmentReader == null) {
                return false;
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
            segment.finalize(true);
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
            if (m_usageSpecificLog.isDebugEnabled()) {
                m_usageSpecificLog.debug("Segment " + qs.file() + " has been closed and deleted due to delete all");
            }
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

    int numberOfSegments() {
        return m_segments.size();
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
    public void scanEntries(BinaryDequeScanner scanner) throws IOException
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
        for (Map.Entry<Long, PBDSegment> entry : m_segments.entrySet()) {
            PBDSegment segment = entry.getValue();
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
}
