/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DBBPool.MBBContainer;
import org.voltdb.EELibraryLoader;
import org.xerial.snappy.Snappy;

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
                      final ByteBuffer buf = checkDoubleFree();
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

    public static final OutputContainerFactory UNSAFE_CONTAINER_FACTORY = new UnsafeOutputContainerFactory();

    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    private final File m_path;
    private final String m_nonce;

    //Segments that are no longer being written to and can be polled
    //These segments are "immutable". They will not be modified until deletion
    private final Deque<PBDSegment> m_segments = new ArrayDeque<PBDSegment>();
    private int m_numObjects = 0;
    private volatile boolean m_closed = false;

    /**
     * Create a persistent binary deque with the specified nonce and storage
     * back at the specified path. Existing files will
     *
     * @param nonce
     * @param path
     * @throws IOException
     */
    public PersistentBinaryDeque(final String nonce, final File path) throws IOException {
        this(nonce, path, true);
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
    public PersistentBinaryDeque(final String nonce, final File path, final boolean deleteEmpty) throws IOException {
        EELibraryLoader.loadExecutionEngineLibrary(true);
        m_path = path;
        m_nonce = nonce;

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
                        PBDSegment qs = new PBDSegment( index, pathname );
                        try {
                            qs.open(false);
                            if (deleteEmpty) {
                                if (qs.getNumEntries() == 0) {
                                    LOG.info("Found Empty Segment with entries: " + qs.getNumEntries() + " For: " + pathname.getName());
                                    qs.closeAndDelete();
                                    return false;
                                }
                            }
                            m_numObjects += qs.getNumEntries();
                            segments.put( index, qs);
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
            m_segments.offer(e.getValue());
        }

        //Find the first and last segment for polling and writing (after)
        Long writeSegmentIndex = 0L;
        try {
            writeSegmentIndex = segments.lastKey() + 1;
        } catch (NoSuchElementException e) {}

        PBDSegment writeSegment =
            new PBDSegment(
                    writeSegmentIndex,
                    new VoltFile(m_path, m_nonce + "." + writeSegmentIndex + ".pbd"));
        m_segments.offer(writeSegment);
        writeSegment.open(true);
        assertions();
    }

    @Override
    public synchronized void offer(BBContainer object) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Closed");
        }

        PBDSegment tail = m_segments.peekLast();
        //If we are mostly empty, don't do compression, otherwise compress to reduce space and IO
        final boolean compress = object.b().isDirect() && (m_segments.size() > 1 || tail.sizeInBytes() > 1024 * 512);
        if (!tail.offer(object, compress)) {
            //Check to see if the tail is completely consumed so we can close and delete it
            if (!tail.hasMoreEntries() && tail.m_discardCount == tail.getNumEntries()) {
                m_segments.pollLast();
                tail.closeAndDelete();
            }
            Long nextIndex = tail.m_index + 1;
            tail = new PBDSegment(nextIndex, new VoltFile(m_path, m_nonce + "." + nextIndex + ".pbd"));
            tail.open(true);
            m_segments.offer(tail);
            final boolean success = tail.offer(object, compress);
            if (!success) {
                throw new IOException("Failed to offer object in PBD");
            }
        }
        incrementNumObjects();
        assertions();
    }

    @Override
    public synchronized void push(BBContainer objects[]) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Closed");
        }

        ArrayDeque<ArrayDeque<BBContainer>> segments = new ArrayDeque<ArrayDeque<BBContainer>>();
        ArrayDeque<BBContainer> currentSegment = new ArrayDeque<BBContainer>();

        //Take the objects that were provided and separate them into deques of objects
        //that will fit in a single write segment
        int available = PBDSegment.m_chunkSize - 4;
        for (BBContainer object : objects) {
            int needed = PBDSegment.m_objectHeaderBytes + object.b().remaining();

            if (available - needed < 0) {
                if (needed > PBDSegment.m_chunkSize - 4) {
                    throw new IOException("Maximum object size is " + (PBDSegment.m_chunkSize - 4));
                }
                segments.offer( currentSegment );
                currentSegment = new ArrayDeque<BBContainer>();
                available = PBDSegment.m_chunkSize - 4;
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
            nextIndex = m_segments.peek().m_index - 1;
        }

        while (segments.peek() != null) {
            ArrayDeque<BBContainer> currentSegmentContents = segments.poll();
            PBDSegment writeSegment =
                new PBDSegment(
                        nextIndex,
                        new VoltFile(m_path, m_nonce + "." + nextIndex + ".pbd"));
            writeSegment.open(true);
            nextIndex--;

            while (currentSegmentContents.peek() != null) {
                writeSegment.offer(currentSegmentContents.pollFirst(), false);
                incrementNumObjects();
            }

            m_segments.push(writeSegment);
        }
        assertions();
    }

    @Override
    public synchronized BBContainer poll(OutputContainerFactory ocf) throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Closed");
        }

        BBContainer retcont = null;
        PBDSegment segment = m_segments.peek();
        if (segment.hasMoreEntries()) {
            retcont = segment.poll(ocf);
        } else {
            for (PBDSegment s : m_segments) {
                if (s.hasMoreEntries()) {
                    segment = s;
                    retcont = segment.poll(ocf);
                    break;
                }
            }
        }
        if (retcont == null) {
            return null;
        }

        decrementNumObjects();
        assertions();
        assert (retcont.b() != null);
        return wrapRetCont(segment, retcont);
    }

    private BBContainer wrapRetCont(final PBDSegment segment, final BBContainer retcont) {
        return new BBContainer(retcont.b()) {
            private boolean m_discarded = false;
            @Override
            public void discard() {
                checkDoubleFree();
                if (m_discarded) {
                    LOG.error("PBD Container discarded more than once");
                    return;
                }
                m_discarded = true;
                retcont.discard();
                segment.m_discardCount++;
                assert(m_closed || m_segments.contains(segment));

                //Don't do anything else if we are closed
                if (m_closed) {
                    return;
                }

                //Segment is potentially ready for deletion
                try {
                    if (segment.m_discardCount == segment.getNumEntries()) {
                        if (segment != m_segments.peekLast()) {
                            m_segments.remove(segment);
                            segment.closeAndDelete();
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Exception closing and deleting PBD segment", e);
                }
            }
        };
    }

    @Override
    public synchronized void sync() throws IOException {
        if (m_closed) {
            throw new IOException("Closed");
        }
        for (PBDSegment segment : m_segments) {
            segment.sync();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (m_closed) {
            return;
        }
        m_closed = true;
        if (!m_segments.peekLast().hasMoreEntries()) {
            m_segments.pollLast().closeAndDelete();
        }
        for (PBDSegment segment : m_segments) {
            segment.close();
        }
        m_closed = true;
    }

    @Override
    public synchronized boolean isEmpty() throws IOException {
        assertions();
        if (m_closed) {
            throw new IOException("Closed");
        }

        PBDSegment segment = m_segments.peek();
        if (segment == null) {
            return true;
        }
        if (segment.hasMoreEntries()) return false;
        for (PBDSegment s : m_segments) {
            if (segment.hasMoreEntries()) return false;
        }
        return true;
    }

    /*
     * Don't use size in bytes to determine empty, could potentially
     * diverge from object count on crash or power failure
     * although incredibly unlikely
     */
    @Override
    public long sizeInBytes() {
        assertions();
        long size = 0;
        for (PBDSegment segment : m_segments) {
            size += segment.sizeInBytes();
        }
        return size;
    }

    @Override
    public synchronized void closeAndDelete() throws IOException {
        if (m_closed) return;
        m_closed = true;
        for (PBDSegment qs : m_segments) {
            qs.closeAndDelete();
        }
    }

    @Override
    public synchronized void parseAndTruncate(BinaryDequeTruncator truncator) throws IOException {
        assertions();
        if (m_segments.isEmpty()) {
            exportLog.debug("PBD " + m_nonce + " has no finished segments");
            return;
        }

        /*
         * Iterator all the objects in all the segments and pass them to the truncator
         * When it finds the truncation point
         */
        Long lastSegmentIndex = null;
        BBContainer decompressionBuffer = DBBPool.allocateDirect(1024 * 512);
        try {
            for (PBDSegment segment : m_segments) {
                long segmentIndex = segment.m_index;

                File segmentFile = segment.m_file;
                RandomAccessFile ras = new RandomAccessFile(segmentFile, "rw");
                FileChannel fc = ras.getChannel();
                MBBContainer readBufferC = DBBPool.wrapMBB(fc.map(MapMode.READ_WRITE, 0, fc.size()));
                final ByteBuffer readBuffer = readBufferC.b();
                final long buffAddr = readBufferC.address();
                try {
                    //Get the number of objects and then iterator over them
                    int numObjects = readBuffer.getInt();
                    int size = readBuffer.getInt();
                    int objectsProcessed = 0;
                    exportLog.debug("PBD " + m_nonce + " has " + numObjects + " objects to parse and truncate");
                    for (int ii = 0; ii < numObjects; ii++) {
                        final int nextObjectLength = readBuffer.getInt();
                        final int nextObjectFlags = readBuffer.getInt();
                        final boolean compressed = nextObjectFlags == PBDSegment.FLAG_COMPRESSED;
                        final int uncompressedLength = compressed ? (int)Snappy.uncompressedLength(buffAddr + readBuffer.position(), nextObjectLength) : nextObjectLength;
                        objectsProcessed++;
                        //Copy the next object into a separate heap byte buffer
                        //do the old limit stashing trick to avoid buffer overflow
                        BBContainer nextObject = null;
                        if (compressed) {
                            decompressionBuffer.b().clear();
                            if (decompressionBuffer.b().remaining() < uncompressedLength ) {
                                decompressionBuffer.discard();
                                decompressionBuffer = DBBPool.allocateDirect(uncompressedLength);
                            }
                            nextObject = DBBPool.dummyWrapBB(decompressionBuffer.b());
                            final long sourceAddr = (buffAddr + readBuffer.position());
                            final long destAddr = nextObject.address();
                            Snappy.rawUncompress(sourceAddr, nextObjectLength, destAddr);
                            readBuffer.position(readBuffer.position() + nextObjectLength);
                        } else {
                            final int oldLimit = readBuffer.limit();
                            readBuffer.limit(readBuffer.position() + nextObjectLength);
                            nextObject = DBBPool.dummyWrapBB(readBuffer.slice());
                            readBuffer.position(readBuffer.limit());
                            readBuffer.limit(oldLimit);
                        }
                        try {
                            //Handoff the object to the truncator and await a decision
                            ByteBuffer retval = truncator.parse(nextObject.b());
                            if (retval == null) {
                                //Nothing to do, leave the object alone and move to the next
                                continue;
                            } else {
                                //If the returned bytebuffer is empty, remove the object and truncate the file
                                if (retval.remaining() == 0) {
                                    if (ii == 0) {
                                        /*
                                         * If truncation is occuring at the first object
                                         * Whammo! Delete the file. Do it by setting the lastSegmentIndex
                                         * to 1 previous. We may end up with an empty finished segment
                                         * set.
                                         */
                                        lastSegmentIndex = segmentIndex - 1;
                                    } else {
                                        addToNumObjects(-(numObjects - (objectsProcessed - 1)));
                                        //Don't forget to update the number of entries in the file
                                        ByteBuffer numObjectsBuffer = ByteBuffer.allocate(4);
                                        numObjectsBuffer.putInt(0, ii);
                                        fc.position(0);
                                        while (numObjectsBuffer.hasRemaining()) {
                                            fc.write(numObjectsBuffer);
                                        }
                                        fc.truncate(readBuffer.position() - (nextObjectLength + PBDSegment.m_objectHeaderBytes));
                                    }

                                } else {
                                    addToNumObjects(-(numObjects - objectsProcessed));
                                    //Partial object truncation
                                    ByteBuffer copy = ByteBuffer.allocate(retval.remaining());
                                    copy.put(retval);
                                    copy.flip();
                                    readBuffer.position(readBuffer.position() - (nextObjectLength + PBDSegment.m_objectHeaderBytes));
                                    readBuffer.putInt(copy.remaining());
                                    readBuffer.putInt(0);
                                    readBuffer.put(copy);

                                    readBuffer.putInt(0, ii + 1);

                                    /*
                                     * SHOULD REALLY make a copy of the original and then swap them with renaming
                                     */
                                    fc.truncate(readBuffer.position());
                                }
                                //Set last segment and break the loop over this segment
                                if (lastSegmentIndex == null) {
                                    lastSegmentIndex = segmentIndex;
                                }
                                break;
                            }
                        } finally {
                            nextObject.discard();
                        }
                    }

                    //If this is set the just processed segment was the last one
                    if (lastSegmentIndex != null) {
                        break;
                    }
                } finally {
                    fc.close();
                    readBufferC.discard();
                }
            }
        } finally {
            decompressionBuffer.discard();
        }

        /*
         * If it was found that no truncation is necessary, lastSegmentIndex will be null.
         * Return and the parseAndTruncate is a noop.
         */
        if (lastSegmentIndex == null)  {
            return;
        }
        /*
         * Now truncate all the segments after the truncation point
         */
        Iterator<PBDSegment> iterator = m_segments.descendingIterator();
        while (iterator.hasNext()) {
            PBDSegment segment = iterator.next();
            if (segment.m_index <= lastSegmentIndex) {
                break;
            }
            addToNumObjects(-segment.getNumEntries());
            iterator.remove();
            segment.closeAndDelete();
        }

        /*
         * Reset the poll and write segments
         */
        //Find the first and last segment for polling and writing (after)
        Long newSegmentIndex = 0L;
        if (m_segments.peekLast() != null) newSegmentIndex = m_segments.peekLast().m_index + 1;

        PBDSegment newSegment =
            new PBDSegment(
                    newSegmentIndex,
                    new VoltFile(m_path, m_nonce + "." + newSegmentIndex + ".pbd"));
        newSegment.open(true);
        m_segments.offer(newSegment);
        assertions();
    }

    private void addToNumObjects(int num) {
        assert(m_numObjects >= 0);
        m_numObjects += num;
    }
    private void incrementNumObjects() {
        assert(m_numObjects >= 0);
         m_numObjects++;
    }

    private void decrementNumObjects() {
        m_numObjects--;
        assert(m_numObjects >= 0);
    }

    @Override
    public int getNumObjects() {
        return m_numObjects;
    }

    private static final boolean assertionsOn;
    static {
        boolean assertOn = false;
        assert(assertOn = true);
        assertionsOn = assertOn;
    }

    private void assertions() {
        if (!assertionsOn) return;
        int numObjects = 0;
        for (PBDSegment segment : m_segments) {
            try {
                numObjects += segment.getNumEntries() - segment.m_objectReadIndex;
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }
        assert(numObjects == m_numObjects);
    }
}
