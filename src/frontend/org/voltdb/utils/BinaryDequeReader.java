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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.BinaryDeque.OutputContainerFactory;

/**
 * Reader interface used to read entries from the deque. Multiple readers may be active at the same time,
 * each of them maintaining their own read location within the deque.
 */
public interface BinaryDequeReader<M> {
    public enum SeekErrorRule {
        THROW,
        SEEK_AFTER,
        SEEK_BEFORE;
    }

    public class NoSuchOffsetException extends Exception {

        private static final long serialVersionUID = -1717317350763722360L;

        public NoSuchOffsetException(String msg) {
            super(msg);
        }

    }

    /**
     * Move the reader to the segment containing the id available after the input id.
     * This call will position the reader after the input id only if the id is the end id of a segment.
     * Otherwise, the reader will be moved to the segment containing the next id after the input id.
     * If it is in the current segment, the reader will not be repositioned at all.
     */
    public void skipPast(long id) throws IOException;

    /**
     * Read and return the object at the current read position of this reader.
     * The entry will be removed once all active readers have read the entry.
     * @param ocf
     * @return BBContainer with the bytes read. Null if there is nothing left to read.
     * @throws IOException
     */
    public BBContainer poll(OutputContainerFactory ocf) throws IOException;

    /**
     * Read and return the full entry at the current read position of this reader. The entry will be removed once all
     * active readers have read the entry.
     *
     * @param ocf     Factory used to create {@link DBBPool.BBContainer} as destinations for the entry data
     * @param maxSize maximum entry size to return. If the next entry exceeds that value {@code null} will be returned
     * @return {@link Entry} containing data entry and any extra header associated with it or {@code null} if there is
     *         no entry or next entry is larger than {@code maxSize}
     * @throws IOException
     */
    public Entry<M> pollEntry(OutputContainerFactory ocf, int maxSize) throws IOException;

    /**
     * Read and return the full entry at the current read position of this reader. The entry will be removed once all
     * active readers have read the entry.
     *
     * @param ocf Factory used to create {@link DBBPool.BBContainer} as destinations for the entry data
     * @return {@link Entry} containing data entry and any extra header associated with it or {@code null} if there is
     *         no entry
     * @throws IOException
     */
    default Entry<M> pollEntry(OutputContainerFactory ocf) throws IOException {
        return pollEntry(ocf, Integer.MAX_VALUE);
    }

    /**
     * Use this to position the reader to the beginning of the segment that contains the given entry id.
     * This method can only be used on BinaryDeque instances that are storing the start and end ids
     * in the segment headers.
     * <p>
     * After this operation, the reader should be positioned at the beginning of the segment with
     * <code>startId >= entryId >= endId</code>. However, if the given <code>entryId</code> does not exist in the BinaryDeque,
     * either because the entry is in a gap in the data, or because the entry is before the first entry currently
     * available, or because the entry is after the last entry currently available, the behavior depends on
     * {@link SeekErrorRule} specified.
     * <ul>
     * <li>{@link SeekErrorRule#THROW} - throws {@link NoSuchOffsetException} if the entry id is not found</li>
     * <li>{@link SeekErrorRule#SEEK_AFTER} - the reader will be positioned at the lowest segment such that
     * <code>startId > entryId</code>. If the highest available entry in the BinaryDeque is less than the specified entry,
     * {@link NoSuchOffsetException} will be thrown.</li>
     * <li>{@link SeekErrorRule#SEEK_BEFORE} - the reader will be positioned at the highest segment such that <code>endId < entryId</code>.
     * If the first entry available in the BinaryDeque is higher than the specified entry, {@link NoSuchOffsetException} will be thrown.</li>
     * </ul>
     * @param entryId the id of the entry we are looking for
     * @param errorRule specifies the behavior when the <code>entryId</code> cannot be found
     * @throws IllegalStateException if this BinaryDeque is not storing ids in its segment header
     * @throws NoSuchOffsetException if the entry id cannot be found and based on the {@link SeekErrorRule} specified
     * @throws IOException if an IO error occurs trying to read the data
     */
    public void seekToSegment(long entryId, SeekErrorRule errorRule) throws NoSuchOffsetException, IOException;

    /**
     * Number of bytes left to read for this reader.
     * @return number of bytes left to read for this reader.
     * @throws IOException
     */
    public long sizeInBytes() throws IOException;

    /**
     *  Number of objects left to read for this reader.
     * @return number of objects left to read for this reader
     * @throws IOException
     */
    public long getNumObjects() throws IOException;

    /**
     * Returns true if this reader still has entries to read. False otherwise
     * @return true if this reader still has entries to read. False otherwise
     * @throws IOException
     */
    public boolean isEmpty() throws IOException;

    /**
     * Returns true if the reader is open, false it has been closed.
     * @return true if the reader is open, false otherwise.
     */
    public boolean isOpen();

    /**
     * Entry class to hold all metadata and data associated with an entry in a {@link BinaryDeque}
     *
     * @param <M> Type of extra header metadata
     */
    public interface Entry<M> {
        /**
         * Wrap a basic {@link BBContainer} in the {@link Entry} interface. When using this wrapper {@link #free()} is
         * equivalent to {@link #release()}
         * <p>
         * This wrapper should only be used to wrap {@link BBContainers} whose discard only free the memory and have no
         * BinaryDeque related side effects.
         *
         * @param <M>       Type of metadata
         * @param container to wrap
         * @return {@link Entry} instance which wraps {@code container}
         */
        static <M> Entry<M> wrap(BBContainer container) {
            return new Entry<M>() {
                @Override
                public M getExtraHeader() {
                    return null;
                }

                @Override
                public ByteBuffer getData() {
                    return container.b();
                }

                @Override
                public void release() {
                    free();
                }

                @Override
                public void free() {
                    container.discard();
                }
            };
        }
        /**
         * @return any associated extra header metadata. May return {@code null} if there was none
         */
        M getExtraHeader();

        /**
         * @return A {@link ByteBuffer} holding the entry data
         */
        ByteBuffer getData();

        /**
         * Indicates that this entry has been consumed and is eligible for deletion with respect to the
         * {@link BinaryDequeReader} which returned this entry.
         */
        void release();

        /**
         * Free the memory being held by this entry but it is not eligible for deletion
         */
        void free();
    }
}
