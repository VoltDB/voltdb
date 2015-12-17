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

#ifndef VOLTDB_TUPLEBLOCK_H_
#define VOLTDB_TUPLEBLOCK_H_

#include "common/ThreadLocalPool.h"
#include "common/tabletuple.h"
#include "structures/CompactingMap.h"
#include "structures/CompactingSet.h"

#include <vector>
#include <stdint.h>
#include <string.h>
#include <cassert>

#include "boost/scoped_array.hpp"
#include "boost/shared_ptr.hpp"
#include "boost/unordered_set.hpp"
#include <math.h>
#include <iostream>

namespace voltdb {
class TupleBlock;
}

void intrusive_ptr_add_ref(voltdb::TupleBlock * p);
void intrusive_ptr_release(voltdb::TupleBlock * p);

// LLVM wants to see intrusive_ptr_add_ref before this include.
#include "boost/intrusive_ptr.hpp"

namespace voltdb {
class Table;

//typedef boost::shared_ptr<TupleBlock> TBPtr;
typedef boost::intrusive_ptr<TupleBlock> TBPtr;
//typedef TupleBlock* TBPtr;
typedef CompactingMap<NormalKeyValuePair<char*, TBPtr>, comp<char*>, false> TBMap;
typedef TBMap::iterator TBMapI;
typedef CompactingSet<TBPtr> TBBucket;
typedef TBBucket::iterator TBBucketI;
typedef boost::shared_ptr<TBBucket> TBBucketPtr;
typedef std::vector<TBBucketPtr> TBBucketPtrVector;
const int TUPLE_BLOCK_NUM_BUCKETS = 20;

const int NO_NEW_BUCKET_INDEX = -1;

class TupleBlock {
    friend void ::intrusive_ptr_add_ref(voltdb::TupleBlock * p);
    friend void ::intrusive_ptr_release(voltdb::TupleBlock * p);
public:
    TupleBlock(Table *table, TBBucketPtr bucket);

    void* operator new(std::size_t sz)
    {
        assert(sz == sizeof(TupleBlock));
        return ThreadLocalPool::allocateExactSizedObject(sizeof(TupleBlock));
    }

    void operator delete(void* object)
    { return ThreadLocalPool::freeExactSizedObject(sizeof(TupleBlock), object); }

    double loadFactor() {
        return static_cast <double> (m_activeTuples) / m_tuplesPerBlock;
    }

    inline bool hasFreeTuples() {
        return m_activeTuples < m_tuplesPerBlock;
    }

    inline bool isEmpty() {
        if (m_activeTuples == 0) {
            return true;
        }
        return false;
    }

    /**
     * If the tuple block with its current fullness is not able to be in the current bucket,
     * return the new bucket index, otherwise return NO_NEW_BUCKET_INDEX index.
     */
    inline int calculateBucketIndex(uint32_t tuplesPendingDeleteOnUndoRelease = 0) {
        if (!hasFreeTuples() || tuplesPendingDeleteOnUndoRelease == m_activeTuples) {
            //(1)
            //Completely full, don't need be considered for merging
            //Remove itself from current bucket and null out the bucket

            //(2)
            //Someone was kind enough to scan the whole block, move all tuples
            //not pending delete on undo release to another block as part
            //of compaction. Now this block doesn't need to be considered
            //for compaction anymore. The block will be completely discard along with the undo
            //information. Any tuples pending delete due to a snapshot will moved and picked up
            //by the snapshot scan from the other block

            if (m_bucket.get() != NULL) {
                m_bucket->erase(TBPtr(this));
                m_bucket = TBBucketPtr();
            }
            return NO_NEW_BUCKET_INDEX;
        }

        int index = TUPLE_BLOCK_NUM_BUCKETS * m_activeTuples / m_tuplesPerBlock;
        assert(index < TUPLE_BLOCK_NUM_BUCKETS);
        return index;
    }

    inline int getBucketIndex() {
        return m_bucketIndex;
    }

    /**
     * Find next free tuple storage address.
     */
    char* nextFreeTuple() {
        char *retval = NULL;
        if (m_freedTuple < m_boundaryTuple) {
            m_lastCompactionOffset = 0;
            retval = popFreedTuple();
        }
        else {
            retval = tupleAtIndex(m_boundaryTuple);
            ++m_boundaryTuple;
            m_freedTuple = m_boundaryTuple;
        }
        m_activeTuples++;
        return retval;
    }

    /**
     * Detect a changed bucket index after allocating or freeing tuples.
     */
    int nextBucketIndex() {
        int newBucketIndex = calculateBucketIndex();
        if (newBucketIndex == m_bucketIndex) {
            // tuple block is not too full for its current bucket
            return NO_NEW_BUCKET_INDEX;
        }
        // needs a new bucket and update bucket index
        m_bucketIndex = newBucketIndex;
        return newBucketIndex;
    }

    void swapToBucket(TBBucketPtr newBucket) {
        if (m_bucket != NULL) {
            m_bucket->erase(TBPtr(this));
        }
        m_bucket = newBucket;
        if (m_bucket != NULL) {
            m_bucket->insert(TBPtr(this));
        }
    }

    void freeTuple(char *tupleStorage) {
        m_lastCompactionOffset = 0;
        m_activeTuples--;
        //Find the "row index" of the tupleStorage relative to the start of the block.
        uint32_t offset = static_cast<uint32_t>(tupleStorage - m_storage) / m_tupleLength;
        if (offset == m_boundaryTuple - 1) {
            // Pull in the boundary to absorb the neighboring tuple
            --m_boundaryTuple;
        }
        else {
            // Add the "hole" to the free chain.
            pushFreedTuple(tupleStorage, offset);
        }
    }

    char* address() { return m_storage; }

    char* tupleAtIndex(uint32_t index) { return m_storage + (m_tupleLength * index); }

    void reset() {
        m_activeTuples = 0;
        m_boundaryTuple = 0;
        m_freedTuple = 0;
    }

    bool outsideUsedTupleBoundary(uint32_t offset) const {
        return offset >= m_boundaryTuple;
    }

    ~TupleBlock();

    inline uint32_t lastCompactionOffset() {
        return m_lastCompactionOffset;
    }

    inline void lastCompactionOffset(uint32_t offset) {
        m_lastCompactionOffset = offset;
    }

    inline uint32_t activeTuples() {
        return m_activeTuples;
    }

    inline TBBucketPtr currentBucket() {
        return m_bucket;
    }
private:
    void pushFreedTuple(char* tupleStorage, uint32_t offset) {
        uint32_t* tupleHeader = reinterpret_cast<uint32_t*>(tupleStorage);
        *tupleHeader = m_freedTuple << 8; // shift to keep the tuple flag byte clear
        m_freedTuple = offset;
    }

    char* popFreedTuple() {
        char* retval = tupleAtIndex(m_freedTuple);
        m_freedTuple = *reinterpret_cast<uint32_t*>(retval) >> 8;
        return retval;
    }

    char* m_storage;
    uint32_t m_references;
    const uint32_t m_tupleLength;
    const uint32_t m_tuplesPerBlock;
    uint32_t m_activeTuples;
    uint32_t m_boundaryTuple;
    uint32_t m_freedTuple;
    uint32_t m_lastCompactionOffset;

    TBBucketPtr m_bucket;
    int m_bucketIndex;
};

} // namespace voltdb

inline void intrusive_ptr_add_ref(voltdb::TupleBlock * p)
{
    ++(p->m_references);
}

inline void intrusive_ptr_release(voltdb::TupleBlock * p)
{
    if (--(p->m_references) == 0) {
        delete p;
    }
}

#endif /* VOLTDB_TUPLEBLOCK_H_ */
