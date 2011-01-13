/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.EOFException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.TreeMap;

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

    /**
     * Objects placed in the deque are stored in file segments that are up to 64 megabytes.
     * Segments only support appending objects. A segment will throw an IOException of an attempt
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
                    if (!discarded) {
                        exportLog.error(m_file + " had a buffer that was finalized without being discarded");
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

            incrementNumEntries();
        }
    }

    //Segments that are no longer being written to and can be polled
    //These segments are "immutable". They will not be modified until deletion
    private final TreeMap<Long, DequeSegment> m_finishedSegments = new TreeMap<Long, DequeSegment>();

    //The current segment being written to
    private DequeSegment m_writeSegment = null;

    //Index of the segment being polled
    private Long m_currentPollSegmentIndex = 0L;

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

        //Parse the files in the directory by name to find files
        //that are part of this deque
        path.listFiles(new FileFilter() {

            @Override
            public boolean accept(File pathname) {
                String name = pathname.getName();
                if (name.startsWith(nonce) && name.endsWith(".pbd")) {
                    Long index = Long.valueOf(name.substring( nonce.length() + 1, name.length() - 4));
                    m_finishedSegments.put( index, new DequeSegment( index, pathname));
                }
                return false;
            }

        });

        //Find the first and last segment for polling and writing (after)
        Long writeSegmentIndex = 0L;
        try {
            m_currentPollSegmentIndex = m_finishedSegments.firstKey();
            writeSegmentIndex = m_finishedSegments.lastKey() + 1;
        } catch (NoSuchElementException e) {}

        m_writeSegment =
            new DequeSegment(
                    writeSegmentIndex,
                    new File(m_path, m_nonce + "." + writeSegmentIndex + ".pbd"));
        m_writeSegment.open();
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

        assert(needed < DequeSegment.m_chunkSize);

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
        assert(m_finishedSegments.firstKey() == m_currentPollSegmentIndex);
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
                assert(needed < DequeSegment.m_chunkSize - 4);
                segments.offer( currentSegment );
                currentSegment = new ArrayDeque<BBContainer[]>();
                available = DequeSegment.m_chunkSize - 4;
            }
            currentSegment.add(object);
        }

        assert(segments.size() > 0);
        segments.add(currentSegment);

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
                        new File(m_path, m_nonce + "." + nextIndex + ".pbd"));
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
                    new File(m_path, m_nonce + "." + nextIndex + ".pbd"));
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
            assert(m_writeSegment.m_index == m_currentPollSegmentIndex);
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
        m_finishedSegments.put(m_writeSegment.m_index, m_writeSegment);
        m_writeSegment = null;
        for (DequeSegment segment : m_finishedSegments.values()) {
            segment.close();
        }
    }

}
