/* Copyright (c) 2001-2014, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.persist;

import org.hsqldb_voltpatches.lib.DoubleIntIndex;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.2
 * @since 2.3.0
 */
public class DataSpaceManagerSimple implements DataSpaceManager {

    DataFileCache     cache;
    TableSpaceManager defaultSpaceManager;
    int               fileBlockSize = DataSpaceManager.fixedBlockSizeUnit;
    long              totalFragmentSize;
    int               spaceIdSequence = tableIdFirst;
    DoubleIntIndex    lookup;

    /**
     * Used for default, readonly, Text and Session data files
     */
    DataSpaceManagerSimple(DataFileCache cache) {

        this.cache = cache;

        if (cache instanceof DataFileCacheSession) {
            defaultSpaceManager = new TableSpaceManagerSimple(cache);
        } else if (cache instanceof TextCache) {
            defaultSpaceManager = new TableSpaceManagerSimple(cache);
        } else {
            int capacity = cache.database.logger.propMaxFreeBlocks;

            defaultSpaceManager = new TableSpaceManagerBlocks(this,
                    DataSpaceManager.tableIdDefault, fileBlockSize, capacity,
                    cache.dataFileScale);

            initialiseSpaces();

            cache.spaceManagerPosition = 0;
        }

        totalFragmentSize = cache.lostSpaceSize;
    }

    public TableSpaceManager getDefaultTableSpace() {
        return defaultSpaceManager;
    }

    public TableSpaceManager getTableSpace(int spaceId) {

        if (spaceId >= spaceIdSequence) {
            spaceIdSequence = spaceId + 1;
        }

        return defaultSpaceManager;
    }

    public int getNewTableSpaceID() {
        return spaceIdSequence++;
    }

    public long getFileBlocks(int tableId, int blockCount) {

        long filePosition = cache.enlargeFileSpace((long)blockCount * fileBlockSize);

        return filePosition;
    }

    public void freeTableSpace(int spaceId) {}

    public void freeTableSpace(DoubleIntIndex spaceList, long offset,
                               long limit, boolean full) {

        totalFragmentSize += spaceList.getTotalValues();

        if (full) {
            if (cache.fileFreePosition == limit) {
                cache.writeLock.lock();

                try {
                    cache.fileFreePosition = offset;
                } finally {
                    cache.writeLock.unlock();
                }
            } else {
                totalFragmentSize += limit - offset;
            }

            if (spaceList.size() != 0) {
                lookup = new DoubleIntIndex(spaceList.size(), true);

                spaceList.copyTo(lookup);
            }
        } else {
            compactLookup(spaceList, cache.dataFileScale);
            spaceList.setValuesSearchTarget();
            spaceList.sort();

            int extra = spaceList.size() - spaceList.capacity() / 2;

            if (extra > 0) {
                spaceList.removeRange(0, extra);

                totalFragmentSize -= spaceList.getTotalValues();
            }
        }
    }

    public long getLostBlocksSize() {
        return totalFragmentSize + defaultSpaceManager.getLostBlocksSize();
    }

    public int getFileBlockSize() {
        return Integer.MAX_VALUE;
    }

    public boolean isModified() {
        return true;
    }

    public void initialiseSpaces() {

        long currentSize = cache.getFileFreePos();
        long totalBlocks = (currentSize + fileBlockSize) / fileBlockSize;
        long lastFreePosition = cache.enlargeFileSpace(totalBlocks
            * fileBlockSize - currentSize);

        defaultSpaceManager.initialiseFileBlock(lookup, lastFreePosition,
                cache.getFileFreePos());

        if (lookup != null) {
            totalFragmentSize -= lookup.getTotalValues();
            lookup            = null;
        }
    }

    public void reset() {
        defaultSpaceManager.reset();
    }

    public boolean isMultiSpace() {
        return false;
    }

    static boolean compactLookup(DoubleIntIndex lookup, int fileScale) {

        lookup.setKeysSearchTarget();
        lookup.sort();

        if (lookup.size() == 0) {
            return false;
        }

        int[] keys   = lookup.getKeys();
        int[] values = lookup.getValues();
        int   base   = 0;

        for (int i = 1; i < lookup.size(); i++) {
            int key   = keys[base];
            int value = values[base];

            if (key + value / fileScale == keys[i]) {
                values[base] += values[i];    // base updated
            } else {
                base++;

                if (base != i) {
                    keys[base]   = keys[i];
                    values[base] = values[i];
                }
            }
        }

        for (int i = base + 1; i < lookup.size(); i++) {
            keys[i]   = 0;
            values[i] = 0;
        }

        if (lookup.size() != base + 1) {
            lookup.setSize(base + 1);

            return true;
        }

        return false;
    }
}
