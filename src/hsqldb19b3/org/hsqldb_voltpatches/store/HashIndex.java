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


package org.hsqldb_voltpatches.store;

/**
 * A chained bucket hash index implementation.
 *
 * hashTable and linkTable are arrays of signed integral types. This
 * implementation uses int as the type but short or byte can be used for
 * smaller index sizes (cardinality).
 *
 * hashTable[index] contains the pointer to the first node with
 * (index == hash modulo hashTable.length) or -1 if there is no corresponding
 * node. linkTable[{0,newNodePointer}] (the range between 0 and newNodePointer)
 * contains either the pointer to the next node or -1 if there is no
 * such node. reclaimedNodeIndex contains a pointer to an element
 * of linkTable which is the first element in the list of reclaimed nodes
 * (nodes no longer in index) or -1 if there is no such node.
 *
 * elemenet at and above linkTable[newNodePointer] have never been used
 * as a node and their contents is not significant.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
class HashIndex {

    int[]   hashTable;
    int[]   linkTable;
    int     newNodePointer;
    int     elementCount;
    int     reclaimedNodePointer = -1;
    boolean fixedSize;
    boolean modified;
    // A VoltDB extension to diagnose ArrayOutOfBounds.
    int voltDBresetCount = 0;
    int voltDBresetCapacity = -1;
    int voltDBlastResetEvent = 0;
    int voltDBclearCount = 0;
    int voltDBlastClearEvent = 0;
    int voltDBclearCapacity = -1;
    int voltDBhistoryDepth = 0;
    final int voltDBhistoryMinCapacity = 75;
    final int voltDBhistoryMaxCapacity = voltDBhistoryMinCapacity*10;
    int voltDBhistoryCapacity = voltDBhistoryMinCapacity;
    int[] voltDBhistory = new int[voltDBhistoryMinCapacity];
    // End of VoltDB extension

    HashIndex(int hashTableSize, int capacity, boolean fixedSize) {

        // CHERRY PICK to prevent a flaky crash case
        if (capacity < hashTableSize) {
            capacity = hashTableSize;
        }
        // End of CHERRY PICK
        reset(hashTableSize, capacity);

        this.fixedSize = fixedSize;
    }

    /**
     * Reset the structure with a new size as empty.
     *
     * @param hashTableSize
     * @param capacity
     */
    void reset(int hashTableSize, int capacity) {
        // A VoltDB extension to diagnose ArrayOutOfBounds.
        if (linkTable != null) {
            voltDBresetCapacity = linkTable.length;
        }
        ++voltDBresetCount;
        voltDBlastResetEvent = voltDBhistoryDepth;
        voltDBhistoryCapacity = Math.min(voltDBhistoryMaxCapacity, 
                                         Math.max(voltDBhistoryMinCapacity, voltDBhistoryDepth));
        voltDBhistory = new int[voltDBhistoryCapacity];
        // End of VoltDB extension

        int[] newHT = new int[hashTableSize];
        int[] newLT = new int[capacity];

        // allocate memory before assigning
        hashTable = newHT;
        linkTable = newLT;

        resetTables();
    }

    void resetTables() {

        int   to       = hashTable.length;
        int[] intArray = hashTable;

        while (--to >= 0) {
            intArray[to] = -1;
        }

        newNodePointer       = 0;
        elementCount         = 0;
        reclaimedNodePointer = -1;
        modified             = false;
    }

    /**
     * Reset the index as empty.
     */
    void clear() {
        // A VoltDB extension to diagnose ArrayOutOfBounds.
        if (linkTable != null) {
            voltDBclearCapacity = linkTable.length;
        }
        ++voltDBclearCount;
        voltDBlastClearEvent = voltDBhistoryDepth;
        // End of VoltDB extension

        int   to       = linkTable.length;
        int[] intArray = linkTable;

        while (--to >= 0) {
            intArray[to] = 0;
        }

        resetTables();
    }

    /**
     * @param hash
     */
    int getHashIndex(int hash) {
        return (hash & 0x7fffffff) % hashTable.length;
    }

    /**
     * Return the array index for a hash.
     *
     * @param hash the hash value used for indexing
     * @return either -1 or the first node for this hash value
     */
    int getLookup(int hash) {

        if (elementCount == 0) {
            return -1;
        }

        int index = (hash & 0x7fffffff) % hashTable.length;

        return hashTable[index];
    }

    /**
     * This looks from a given node, so the parameter is always > -1.
     *
     * @param lookup A valid node to look from
     * @return either -1 or the next node from this node
     */
    int getNextLookup(int lookup) {
        return linkTable[lookup];
    }

    /**
     * Link a new node to the end of the linked for a hash index.
     *
     * @param index an index into hashTable
     * @param lastLookup either -1 or the node to which the new node will be linked
     * @return the new node
     */
    int linkNode(int index, int lastLookup) {

        // get the first reclaimed slot
        int lookup = reclaimedNodePointer;

        // A VoltDB extension to diagnose ArrayOutOfBounds.
        boolean voltDBreclaimed = (reclaimedNodePointer != -1);
        // Keep a history of events, wrapping to the start of the buffer when capacity runs out
        // so the most recent events are never lost.
        voltDBhistory[voltDBhistoryDepth++ % voltDBhistoryCapacity] = index;
        // End of VoltDB extension
        if (lookup == -1) {
            lookup = newNodePointer++;
        } else {

            // reset the first reclaimed slot
            reclaimedNodePointer = linkTable[lookup];
        }

        // link the node
        if (lastLookup == -1) {
            hashTable[index] = lookup;
        } else {
            linkTable[lastLookup] = lookup;
        }

        // A VoltDB extension to diagnose ArrayOutOfBounds.
        if (lookup >= linkTable.length) {
            StringBuilder report = new StringBuilder();
            report.append("linkTable size is ").append(linkTable.length);
            report.append(voltDBreclaimed ? " reclaimed" : " new").append(" index is ").append(lookup);
            report.append(" linkTable content is [");
            for (int link : linkTable) {
                report.append(link).append(", ");
            }
            report.append("]\n");
            report.append(" hashTable content is [");
            for (int look : hashTable) {
                report.append(look).append(", ");
            }
            report.append("]\n");
            report.append(" history is [");
            int depth = 0;
            for (int look : voltDBhistory) {
                ++depth;
                if (depth == (voltDBhistoryDepth % voltDBhistoryCapacity)) {
                    report.append(" /* <- history ends here and/or starts here -> */ ");
                }
                if (look < 0) {
                    report.append(-(look+1)).append(" unlnkd, ");
                } else if (look > 1000000) {
                    report.append(look-1000000).append(" rmdlkp, ");
                } else {
                    report.append(look).append(" linked, ");
                }
            }
            report.append("]\n");
            report.append(" lost history length is ").append(voltDBhistoryDepth / voltDBhistoryCapacity * voltDBhistoryCapacity);
            report.append("next reclaimedPointer is ").append(reclaimedNodePointer);
            report.append(" next newNodePointer is ").append(newNodePointer);
            report.append(" last reset was #").append(voltDBresetCount).append(" after event ").append(voltDBlastResetEvent);
            report.append(" from ").append(voltDBresetCapacity);
            report.append(" last clear was #").append(voltDBclearCount).append(" after event ").append(voltDBlastClearEvent);
            report.append(" from ").append(voltDBclearCapacity);
            throw new ArrayIndexOutOfBoundsException(report.toString());
        }
        // End of VoltDB extension
        linkTable[lookup] = -1;

        elementCount++;

        modified = true;

        return lookup;
    }

    /**
     * Unlink a node from a linked list and link into the reclaimed list.
     *
     * @param index an index into hashTable
     * @param lastLookup either -1 or the node to which the target node is linked
     * @param lookup the node to remove
     */
    void unlinkNode(int index, int lastLookup, int lookup) {

        // A VoltDB extension to diagnose ArrayOutOfBounds.
        voltDBhistory[voltDBhistoryDepth++ % voltDBhistoryCapacity] = -index-1;
        // End of VoltDB extension
        // unlink the node
        if (lastLookup == -1) {
            hashTable[index] = linkTable[lookup];
        } else {
            linkTable[lastLookup] = linkTable[lookup];
        }

        // add to reclaimed list
        linkTable[lookup]    = reclaimedNodePointer;
        reclaimedNodePointer = lookup;

        elementCount--;
    }

    /**
     * Remove a node that has already been unlinked. This is not required
     * for index operations. It is used only when the row needs to be removed
     * from the data structures that store the actual indexed data and the
     * nodes need to be contiguous.
     *
     * @param lookup the node to remove
     * @return true if node found in unlinked state
     */
    boolean removeEmptyNode(int lookup) {

        // A VoltDB extension to diagnose ArrayOutOfBounds.
        voltDBhistory[voltDBhistoryDepth++ % voltDBhistoryCapacity] = 1000000 + lookup;
        // End of VoltDB extension
        boolean found      = false;
        int     lastLookup = -1;

        for (int i = reclaimedNodePointer; i >= 0;
                lastLookup = i, i = linkTable[i]) {
            if (i == lookup) {
                if (lastLookup == -1) {
                    reclaimedNodePointer = linkTable[lookup];
                } else {
                    linkTable[lastLookup] = linkTable[lookup];
                }

                found = true;

                break;
            }
        }

        if (!found) {
            return false;
        }

        for (int i = 0; i < newNodePointer; i++) {
            if (linkTable[i] > lookup) {
                linkTable[i]--;
            }
        }

        System.arraycopy(linkTable, lookup + 1, linkTable, lookup,
                         newNodePointer - lookup - 1);

        linkTable[newNodePointer - 1] = 0;

        newNodePointer--;

        for (int i = 0; i < hashTable.length; i++) {
            if (hashTable[i] > lookup) {
                hashTable[i]--;
            }
        }

        return true;
    }
}
