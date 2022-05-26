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

import java.io.File;
import java.io.IOException;

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.Pair;
import org.voltdb.utils.BinaryDeque.BinaryDequeScanner;
import org.voltdb.utils.BinaryDeque.BinaryDequeTruncator;
import org.voltdb.utils.BinaryDeque.BinaryDequeValidator;
import org.voltdb.utils.BinaryDeque.EntryUpdater;
import org.voltdb.utils.BinaryDeque.OutputContainerFactory;

/**
 * Dummy PBDSegment which represents a quarantined segment. A quarantined segment cannot be read because of header
 * corruption but is kept around in case any data in the segment is needed to recover data.
 */
class PbdQuarantinedSegment<M> extends PBDSegment<M> {
    PbdQuarantinedSegment(File file, long id) {
        super(file, id);
    }

    @Override
    int getNumEntries() {
        return 0;
    }

    @Override
    boolean isBeingPolled() {
        return false;
    }

    @Override
    boolean isOpenForReading(String cursorId) {
        return true;
    }

    @Override
    PBDSegmentReader<M> openForRead(String cursorId) {
        return getReader(cursorId);
    }

    @SuppressWarnings("unchecked")
    @Override
    PBDSegmentReader<M> getReader(String cursorId) {
        return (PBDSegmentReader<M>) READER;
    }

    @Override
    void openNewSegment(boolean compress) {
        throw new UnsupportedOperationException();
    }

    @Override
    void openForTruncate() {
    }

    @Override
    void closeAndDelete() {
        m_file.delete();
    }

    @Override
    boolean isClosed() {
        return true;
    }

    @Override
    void close() {}

    @Override
    void sync() {}

    @Override
    boolean hasAllFinishedReading() {
        return true;
    }

    @Override
    int offer(BBContainer cont, long startId, long endId, long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    int offer(DeferredSerialization ds) {
        throw new UnsupportedOperationException();
    }

    @Override
    int size() {
        return 0;
    }

    @Override
    void writeExtraHeader(M extraHeader) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setReadOnly() {}

    @Override
    int parseAndTruncate(BinaryDequeTruncator truncator) {
        return 0;
    }

    @Override
    int scan(BinaryDequeScanner scanner) {
        return 0;
    }

    @Override
    int validate(BinaryDequeValidator<M> validator) throws IOException {
        return 0;
    }

    @Override
    boolean isFinal() {
        return false;
    }

    @Override
    void finalize(boolean close) {}

    @Override
    M getExtraHeader() {
        return null;
    }

    @Override
    boolean isActive() {
        return false;
    }

    private static final PBDSegmentReader<Void> READER = new PBDSegmentReader<Void>() {
        @Override
        public int uncompressedBytesToRead() {
            return 0;
        }

        @Override
        public void rewindReadOffset(int byBytes) {}

        @Override
        public void reopen() {}

        @Override
        public long readOffset() {
            return 0;
        }

        @Override
        public int readIndex() {
            return 0;
        }

        @Override
        public BBContainer poll(OutputContainerFactory factory, int maxSize) {
            return null;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public boolean hasMoreEntries() {
            return false;
        }

        @Override
        public BBContainer getExtraHeader() {
            return null;
        }

        @Override
        public boolean anyReadAndDiscarded() {
            return false;
        }

        @Override
        public boolean allReadAndDiscarded() {
            return false;
        }

        @Override
        public boolean hasOutstandingEntries() {
            return false;
        }

        @Override
        public void close() {
        }

        @Override
        public void closeAndSaveReaderState() {
        }

        @Override
        public void markRestReadAndDiscarded() {
        }
    };

    @Override
    long getStartId() throws IOException {
        return PBDSegment.INVALID_ID;
    }

    @Override
    long getEndId() throws IOException {
        return PBDSegment.INVALID_ID;
    }

    @Override
    long getTimestamp() throws IOException {
        return PBDSegment.INVALID_TIMESTAMP;
    }

    @Override
    Pair<PBDSegment<M>, Boolean> updateEntries(EntryUpdater<? super M> updater) {
        return Pair.of(null, Boolean.FALSE);
    }
}
