/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.utils;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.Iterator;

import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.DBBPool.BBContainer;

/**
 * A deque that specializes in providing persistence of binary objects to disk. Any object placed
 * in the deque will be persisted to disk asynchronously. Objects placed in the queue can
 * be persisted synchronously by invoking sync. The files backing this deque all start with a nonce
 * provided at construction time followed by a segment index that is stored in the filename. Files grow to
 * a maximum size of 64 megabytes and then a new segment is created. The index starts at 0. Segments are deleted
 * once all objects from the segment have been polled and all the containers returned by poll have been discarded.
 * Push is implemented by creating new segments at the head of the queue containing the objects to be pushed.
 *
 */
public class PersistentBinaryDeque implements BinaryDeque {

    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    private final File m_path;
    private final String m_nonce;
    private java.util.concurrent.atomic.AtomicLong m_sizeInBytes =
        new java.util.concurrent.atomic.AtomicLong(0);

    /**
     * Objects placed in the deque are stored in file segments that are up to 64 megabytes.
     * Segments only support appending objects. A segment will throw an IOException if an attempt
     * to insert an object that exceeds the remaining space is made. A segment can be used
     * for reading and writing, but not both at the same time.
     *
     */
    private class DequeSegment {
        //Avoid unecessary sync with this flag
        private boolean m_syncedSinceLastEdit = true;
        private final File m_file;
        private RandomAccessFile m_ras;
        private FileChannel m_fc;

        //Index of the next object to read, not an offset into the file
        //The offset is maintained by the ByteBuffer. Used to determine if there is another object
        private int m_objectReadIndex = 0;

        //ID of this segment
        private final Long m_index;
        private static final int m_chunkSize = (1024 * 1024) * 64;

        //How many entries that have been polled have from this file have been discarded.
        //Once this == the number of entries the segment can close and delete itself
        private int m_discardsUntilDeletion = 0;

        public DequeSegment(Long index, File file) {
            m_index = index;
            m_file = file;
        }

        private final ByteBuffer m_bufferForNumEntries = ByteBuffer.allocateDirect(4);

        private int getNumEntries() throws IOException {
            if (m_fc == null) {
                open();
            }
            if (m_fc.size() > 0) {
                m_bufferForNumEntries.clear();
                while (m_bufferForNumEntries.hasRemaining()) {
                    int read = m_fc.read(m_bufferForNumEntries, 0);
                    if (read == -1) {
                        throw new EOFException();
                    }
                }
                m_bufferForNumEntries.flip();
                return m_bufferForNumEntries.getInt();
            } else {
                return 0;
            }
        }

        private void initNumEntries() throws IOException {
            m_bufferForNumEntries.clear();
            m_bufferForNumEntries.putInt(0).flip();
            while (m_bufferForNumEntries.hasRemaining()) {
                m_fc.write(m_bufferForNumEntries, 0);
            }
            m_syncedSinceLastEdit = false;
        }

        private void incrementNumEntries() throws IOException {
            //First read the existing amount
            m_bufferForNumEntries.clear();
            while (m_bufferForNumEntries.hasRemaining()) {
                int read = m_fc.read(m_bufferForNumEntries, 0);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            m_bufferForNumEntries.flip();

            //Then write the incremented value
            int numEntries = m_bufferForNumEntries.getInt();
            m_bufferForNumEntries.flip();
            m_bufferForNumEntries.putInt(++numEntries).flip();
            while (m_bufferForNumEntries.hasRemaining()) {
                m_fc.write(m_bufferForNumEntries, 0);
            }
            m_syncedSinceLastEdit = false;

            //For when this buffer is eventually finished and starts being polled
            //Stored on disk and in memory
            m_discardsUntilDeletion++;
        }

        /**
         * Bytes of space available for inserting more entries
         * @return
         */
        private int remaining() throws IOException {
            //Subtract 4 for the length prefix
            return (int)(m_chunkSize - m_fc.position()) - 4;
        }

        private void open() throws IOException {
            if (!m_file.exists()) {
                m_syncedSinceLastEdit = false;
            }
            if (m_ras != null) {
                throw new IOException(m_file + " was already opened");
            }
            m_ras = new RandomAccessFile( m_file, "rw");
            m_fc = m_ras.getChannel();
            m_fc.position(4);
            if (m_fc.size() >= 4) {
                m_discardsUntilDeletion = getNumEntries();
            }
        }

        private void closeAndDelete() throws IOException {
            close();
            m_sizeInBytes.addAndGet(-sizeInBytes());
            m_file.delete();
        }

        private void close() throws IOException {
            if (m_fc != null) {
                m_fc.close();
                m_ras = null;
                m_fc = null;
            }
        }

        private void sync() throws IOException {
            if (!m_syncedSinceLastEdit) {
                m_fc.force(true);
            }
            m_syncedSinceLastEdit = true;
        }

        private BBContainer poll() throws IOException {
            if (m_fc == null) {
                open();
            }

            //No more entries to read
            if (m_objectReadIndex >= getNumEntries()) {
                return null;
            }

            m_objectReadIndex++;

            //If this is the last object to read from this segment
            //increment the poll segment index so that the next poll
            //selects the correct segment
            if (m_objectReadIndex >= getNumEntries()) {
                m_currentPollSegmentIndex++;
            }

            //Get the length prefix and then read the object
            m_bufferForNumEntries.clear();
            while (m_bufferForNumEntries.hasRemaining()) {
                int read = m_fc.read(m_bufferForNumEntries);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            m_bufferForNumEntries.flip();
            int length = m_bufferForNumEntries.getInt();
            if (length < 1) {
                throw new IOException("Read an invalid length");
            }

            ByteBuffer resultBuffer = ByteBuffer.allocate(length);
            while (resultBuffer.hasRemaining()) {
                int read = m_fc.read(resultBuffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            resultBuffer.flip();

            return new BBContainer( resultBuffer, 0L) {
                private boolean discarded = false;

                private final Throwable t = new Throwable();
                @Override
                public void discard() {
                    if (!discarded) {
                        discarded = true;
                        m_discardsUntilDeletion--;
                        if (m_discardsUntilDeletion == 0) {
                            m_finishedSegments.remove(m_index);
                            try {
                                closeAndDelete();
                            } catch (IOException e) {
                                exportLog.error(e);
                            }
                        }
                    } else {
                        exportLog.error("An export buffer was discarded multiple times");
                    }
                }

                @Override
                public void finalize() {
                    if (!discarded && !m_closed) {
                        exportLog.error(m_file + " had a buffer that was finalized without being discarded");
                        StringWriter sw  = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        t.printStackTrace(pw);
                        exportLog.error(sw.toString());
                        discard();
                    }
                }
            };
        }

        private void offer(BBContainer objects[]) throws IOException {
            int length = 0;
            for (BBContainer obj : objects ) {
                length += obj.b.remaining();
            }

            if (remaining() < length) {
                throw new IOException(m_file + " has insufficient space");
            }

            m_bufferForNumEntries.clear();
            m_bufferForNumEntries.putInt(length).flip();
            while (m_bufferForNumEntries.hasRemaining()) {
                m_fc.write(m_bufferForNumEntries);
            }

            int objectIndex = 0;
            for (BBContainer obj : objects ) {
                boolean success = false;
                try {
                    while (obj.b.hasRemaining()) {
                        m_fc.write(obj.b);
                    }
                    obj.discard();
                    success = true;
                    objectIndex++;
                } finally {
                    if (!success) {
                        for (int ii = objectIndex; ii < objects.length; ii++) {
                            objects[ii].discard();
                        }
                    }
                }
            }
            m_sizeInBytes.addAndGet(4 + length);
            incrementNumEntries();
        }

        //A white lie, don't include the object count prefix
        //so that the size is 0 when there is no user data
        private long sizeInBytes() {
            return m_file.length() - 4;
        }
    }

    //Segments that are no longer being written to and can be polled
    //These segments are "immutable". They will not be modified until deletion
    private final TreeMap<Long, DequeSegment> m_finishedSegments = new TreeMap<Long, DequeSegment>();

    //The current segment being written to
    private DequeSegment m_writeSegment = null;

    //Index of the segment being polled
    private Long m_currentPollSegmentIndex = 0L;

    private volatile boolean m_closed = false;

    /**
     * Create a persistent binary deque with the specified nonce and storage back at the specified path.
     * Existing files will
     * @param nonce
     * @param path
     * @throws IOException
     */
    public PersistentBinaryDeque(final String nonce, final File path) throws IOException {
        m_path = path;
        m_nonce = nonce;

        if (!path.exists() || !path.canRead() || !path.canWrite() || !path.canExecute() || !path.isDirectory()) {
            throw new IOException(path + " is not usable ( !exists || !readable " +
                    "|| !writable || !executable || !directory)");
        }

        //Parse the files in the directory by name to find files
        //that are part of this deque
        path.listFiles(new FileFilter() {

            @Override
            public boolean accept(File pathname) {
                String name = pathname.getName();
                if (name.startsWith(nonce) && name.endsWith(".pbd")) {
                    if (pathname.length() == 4) {
                        //Doesn't have any objects, just the object count
                        pathname.delete();
                        return false;
                    }
                    Long index = Long.valueOf(name.substring( nonce.length() + 1, name.length() - 4));
                    DequeSegment ds = new DequeSegment( index, pathname);
                    m_finishedSegments.put( index, ds);
                    m_sizeInBytes.addAndGet(ds.sizeInBytes());
                }
                return false;
            }

        });

        Long lastKey = null;
        for (Long key : m_finishedSegments.keySet()) {
            if (lastKey == null) {
                lastKey = key;
            } else {
                if (lastKey + 1 != key) {
                    throw new IOException("Missing " + nonce +
                            " pbd segments between " + lastKey + " and " + key + " in directory " + path +
                            ". The data files found in the export overflow directory were inconsistent.");
                }
                lastKey = key;
            }
        }
        //Find the first and last segment for polling and writing (after)
        Long writeSegmentIndex = 0L;
        try {
            m_currentPollSegmentIndex = m_finishedSegments.firstKey();
            writeSegmentIndex = m_finishedSegments.lastKey() + 1;
        } catch (NoSuchElementException e) {}

        m_writeSegment =
            new DequeSegment(
                    writeSegmentIndex,
                    new VoltFile(m_path, m_nonce + "." + writeSegmentIndex + ".pbd"));
        m_writeSegment.open();
        m_writeSegment.initNumEntries();
    }

    @Override
    public synchronized void offer(BBContainer[] objects) throws IOException {
        if (m_writeSegment == null) {
            throw new IOException("Closed");
        }
        int needed = 0;
        for (BBContainer b : objects) {
            needed +=  b.b.remaining();
        }

        if (needed > DequeSegment.m_chunkSize - 4) {
            throw new IOException("Maxiumum object size is " + (DequeSegment.m_chunkSize - 4));
        }

        if (m_writeSegment.remaining() < needed) {
            openNewWriteSegment();
        }

        m_writeSegment.offer(objects);
    }

    @Override
    public synchronized void push(BBContainer[][] objects) throws IOException {
        if (m_writeSegment == null) {
            throw new IOException("Closed");
        }
        if (!m_finishedSegments.isEmpty()) {
            assert(m_finishedSegments.firstKey() == m_currentPollSegmentIndex);
        }
        ArrayDeque<ArrayDeque<BBContainer[]>> segments = new ArrayDeque<ArrayDeque<BBContainer[]>>();
        ArrayDeque<BBContainer[]> currentSegment = new ArrayDeque<BBContainer[]>();

        //Take the objects that were provided and separate them into deques of objects
        //that will fit in a single write segment
        int available = DequeSegment.m_chunkSize - 4;
        for (BBContainer object[] : objects) {
            int needed = 4;
            for (BBContainer obj : object) {
                needed += obj.b.remaining();
            }

            if (available - needed < 0) {
                if (needed > DequeSegment.m_chunkSize - 4) {
                    throw new IOException("Maximum object size is " + (DequeSegment.m_chunkSize - 4));
                }
                segments.offer( currentSegment );
                currentSegment = new ArrayDeque<BBContainer[]>();
                available = DequeSegment.m_chunkSize - 4;
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
        if (m_finishedSegments.size() > 0) {
            nextIndex = m_finishedSegments.firstKey() - 1;
        } else {
            nextIndex = m_writeSegment.m_index - 1;
        }

        while (segments.peek() != null) {
            ArrayDeque<BBContainer[]> currentSegmentContents = segments.poll();
            DequeSegment writeSegment =
                new DequeSegment(
                        nextIndex,
                        new VoltFile(m_path, m_nonce + "." + nextIndex + ".pbd"));
            m_currentPollSegmentIndex = nextIndex;
            writeSegment.open();
            writeSegment.initNumEntries();
            nextIndex--;

            while (currentSegmentContents.peek() != null) {
                writeSegment.offer(currentSegmentContents.pollFirst());
            }

            writeSegment.m_fc.position(4);
            m_finishedSegments.put(writeSegment.m_index, writeSegment);
        }
    }

    private void openNewWriteSegment() throws IOException {
        if (m_writeSegment == null) {
            throw new IOException("Closed");
        }
        m_writeSegment.m_fc.position(4);
        m_finishedSegments.put(m_writeSegment.m_index, m_writeSegment);
        Long nextIndex = m_writeSegment.m_index + 1;
        m_writeSegment =
            new DequeSegment(
                    nextIndex,
                    new VoltFile(m_path, m_nonce + "." + nextIndex + ".pbd"));
        m_writeSegment.open();
        m_writeSegment.initNumEntries();
    }

    @Override
    public synchronized BBContainer poll() throws IOException {
        if (m_writeSegment == null) {
            throw new IOException("Closed");
        }
        DequeSegment segment = m_finishedSegments.get(m_currentPollSegmentIndex);
        if (segment == null) {
            assert(m_writeSegment.m_index.equals(m_currentPollSegmentIndex));
            //See if we can steal the write segment, otherwise return null
            if (m_writeSegment.getNumEntries() > 0) {
                openNewWriteSegment();
                return poll();
            } else {
                return null;
            }
        }
        return segment.poll();
    }

    @Override
    public synchronized void sync() throws IOException {
        if (m_writeSegment == null) {
            throw new IOException("Closed");
        }
        m_writeSegment.sync();
        for (DequeSegment segment : m_finishedSegments.values()) {
            segment.sync();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (m_writeSegment == null) {
            throw new IOException("Closed");
        }
        if (m_writeSegment.getNumEntries() > 0) {
            m_finishedSegments.put(m_writeSegment.m_index, m_writeSegment);
        } else {
            m_writeSegment.closeAndDelete();
        }
        m_writeSegment = null;
        for (DequeSegment segment : m_finishedSegments.values()) {
            segment.close();
        }
        m_closed = true;
    }

    @Override
    public synchronized boolean isEmpty() throws IOException {
        if (m_writeSegment == null) {
            throw new IOException("Closed");
        }
        DequeSegment segment = m_finishedSegments.get(m_currentPollSegmentIndex);
        if (segment == null) {
            assert(m_writeSegment.m_index.equals(m_currentPollSegmentIndex));
            //See if we can steal the write segment, otherwise return null
            if (m_writeSegment.getNumEntries() > 0) {
                return false;
            } else {
                return true;
            }
        }
        return segment.m_objectReadIndex >= segment.getNumEntries();
    }

    @Override
    public long sizeInBytes() {
        return m_sizeInBytes.get();
    }

    @Override
    public synchronized void closeAndDelete() throws IOException {
        m_writeSegment.closeAndDelete();
        for (DequeSegment ds : m_finishedSegments.values()) {
            ds.closeAndDelete();
        }
    }

    @Override
    public void parseAndTruncate(BinaryDequeTruncator truncator) throws IOException {
        if (m_finishedSegments.isEmpty()) {
            exportLog.debug("PBD " + m_nonce + " has no finished segments");
            return;
        }
        //+16 because I am not sure if the max chunk size is enforced right
        ByteBuffer readBuffer = ByteBuffer.allocateDirect(DequeSegment.m_chunkSize + 16);

        /*
         * Iterator all the objects in all the segments and pass them to the truncator
         * When it finds the truncation point
         */
        Long lastSegmentIndex = null;
        for (Map.Entry<Long, DequeSegment> entry : m_finishedSegments.entrySet()) {
            readBuffer.clear();
            DequeSegment segment = entry.getValue();
            long segmentIndex = entry.getKey();

            File segmentFile = segment.m_file;
            RandomAccessFile ras = new RandomAccessFile(segmentFile, "rw");
            FileChannel fc = ras.getChannel();
            try {
                /*
                 * Read the entire segment into memory
                 */
                while (readBuffer.hasRemaining()) {
                    int read = fc.read(readBuffer);
                    if (read == -1) {
                        break;
                    }
                }
                readBuffer.flip();

                //Get the number of objects and then iterator over them
                int numObjects = readBuffer.getInt();
                exportLog.debug("PBD " + m_nonce + " has " + numObjects + " objects to parse and truncate");
                for (int ii = 0; ii < numObjects; ii++) {
                    final int nextObjectLength = readBuffer.getInt();
                    //Copy the next object into a separate heap byte buffer
                    //do the old limit stashing trick to avoid buffer overflow
                    ByteBuffer nextObject = ByteBuffer.allocate(nextObjectLength);
                    final int oldLimit = readBuffer.limit();
                    readBuffer.limit(readBuffer.position() + nextObjectLength);

                    nextObject.put(readBuffer).flip();

                    //Put back the original limit
                    readBuffer.limit(oldLimit);

                    //Handoff the object to the truncator and await a decision
                    ByteBuffer retval = truncator.parse(nextObject);
                    if (retval == null) {
                        //Nothing to do, leave the object alone and move to the next
                        continue;
                    } else {
                        long startSize = fc.size();
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
                                //Don't forget to update the number of entries in the file
                                ByteBuffer numObjectsBuffer = ByteBuffer.allocate(4);
                                numObjectsBuffer.putInt(0, ii);
                                fc.position(0);
                                while (numObjectsBuffer.hasRemaining()) {
                                    fc.write(numObjectsBuffer);
                                }
                                fc.truncate(readBuffer.position() - (nextObjectLength + 4));
                            }

                        } else {
                            readBuffer.position(readBuffer.position() - (nextObjectLength + 4));
                            readBuffer.putInt(retval.remaining());
                            readBuffer.put(retval);
                            readBuffer.flip();

                            readBuffer.putInt(0, ii + 1);
                            /*
                             * SHOULD REALLY make a copy of the original and then swap them with renaming
                             */
                            fc.position(0);
                            fc.truncate(0);

                            while (readBuffer.hasRemaining()) {
                                fc.write(readBuffer);
                            }
                        }
                        long endSize = fc.size();
                        m_sizeInBytes.addAndGet(endSize - startSize);
                        //Set last segment and break the loop over this segment
                        if (lastSegmentIndex == null) {
                            lastSegmentIndex = segmentIndex;
                        }
                        break;
                    }
                }

                //If this is set the just processed segment was the last one
                if (lastSegmentIndex != null) {
                    break;
                }
            } finally {
                fc.close();
            }
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
        Iterator<Map.Entry<Long, DequeSegment>> iterator = m_finishedSegments.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, DequeSegment> entry = iterator.next();
            if (entry.getKey() <= lastSegmentIndex) {
                continue;
            }
            DequeSegment ds = entry.getValue();
            iterator.remove();
            ds.closeAndDelete();
        }

        //The write segment may have the wrong index, delete it
        m_writeSegment.closeAndDelete();

        /*
         * Reset the poll and write segments
         */
        //Find the first and last segment for polling and writing (after)
        m_currentPollSegmentIndex = 0L;
        Long writeSegmentIndex = 0L;
        try {
            m_currentPollSegmentIndex = m_finishedSegments.firstKey();
            writeSegmentIndex = m_finishedSegments.lastKey() + 1;
        } catch (NoSuchElementException e) {}

        m_writeSegment =
            new DequeSegment(
                    writeSegmentIndex,
                    new VoltFile(m_path, m_nonce + "." + writeSegmentIndex + ".pbd"));
        m_writeSegment.open();
        m_writeSegment.initNumEntries();
        if (m_finishedSegments.isEmpty()) {
            assert(m_writeSegment.m_index.equals(m_currentPollSegmentIndex));
        }
    }
}
