/* Copyright (c) 2001-2009, The HSQL Development Group
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
 * Maintains a list of free file blocks with fixed capacity.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.8.0
 * @since 1.8.0
 */
public class DataFileBlockManager {

    private DoubleIntIndex lookup;
    private final int      capacity;
    private int            midSize;
    private final int      scale;
    private long           releaseCount;
    private long           requestCount;
    private long           requestSize;

    // reporting vars
    long    lostFreeBlockSize;
    boolean isModified;

    /**
     *
     */
    public DataFileBlockManager(int capacity, int scale, long lostSize) {

        lookup = new DoubleIntIndex(capacity, true);

        lookup.setValuesSearchTarget();

        this.capacity          = capacity;
        this.scale             = scale;
        this.lostFreeBlockSize = lostSize;
        this.midSize           = 128;    // arbitrary initial value
    }

    /**
     */
    void add(int pos, int rowSize) {

        isModified = true;

        if (capacity == 0) {
            lostFreeBlockSize += rowSize;

            return;
        }

        releaseCount++;

        //
        if (lookup.size() == capacity) {
            resetList();
        }

        lookup.add(pos, rowSize);
    }

    /**
     * Returns the position of a free block or 0.
     */
    int get(int rowSize) {

        if (lookup.size() == 0) {
            return -1;
        }

        int index = lookup.findFirstGreaterEqualKeyIndex(rowSize);

        if (index == -1) {
            return -1;
        }

        // statistics for successful requests only - to be used later for midSize
        requestCount++;

        requestSize += rowSize;

        int length     = lookup.getValue(index);
        int difference = length - rowSize;
        int key        = lookup.getKey(index);

        lookup.remove(index);

        if (difference >= midSize) {
            int pos = key + (rowSize / scale);

            lookup.add(pos, difference);
        } else {
            lostFreeBlockSize += difference;
        }

        return key;
    }

    int size() {
        return lookup.size();
    }

    long getLostBlocksSize() {
        return lostFreeBlockSize;
    }

    boolean isModified() {
        return isModified;
    }

    void clear() {
        removeBlocks(lookup.size());
    }

    private void resetList() {

        if (requestCount != 0) {
            midSize = (int) (requestSize / requestCount);
        }

        int first = lookup.findFirstGreaterEqualSlotIndex(midSize);

        if (first < lookup.size() / 4) {
            first = lookup.size() / 4;
        }

        removeBlocks(first);
    }

    private void removeBlocks(int blocks) {

        for (int i = 0; i < blocks; i++) {
            lostFreeBlockSize += lookup.getValue(i);
        }

        lookup.removeRange(0, blocks);
    }

    private void checkIntegrity() throws NullPointerException {}
}
