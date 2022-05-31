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

#ifndef VOLTDB_TUPLEBLOCK_H_
#define VOLTDB_TUPLEBLOCK_H_
#include <vector>
#include <stdint.h>
#include <string.h>
#include <common/debuglog.h>

#include "boost/scoped_array.hpp"
#include "boost/shared_ptr.hpp"
#include "boost/unordered_set.hpp"
#include "stx/btree_map.h"
#include "stx/btree_set.h"
#include <math.h>
#include <iostream>
#include "boost_ext/FastAllocator.hpp"
#include "common/ThreadLocalPool.h"
#include "common/tabletuple.h"
#include <deque>
#include <stdlib.h>
#if __cplusplus >= 201103L
#include <atomic>
#else
#include <cstdatomic>
#endif

namespace voltdb {
const int NO_NEW_BUCKET_INDEX = -1;
class TupleBlock;
}

void intrusive_ptr_add_ref(voltdb::TupleBlock * p);
void intrusive_ptr_release(voltdb::TupleBlock * p);

// LLVM wants to see intrusive_ptr_add_ref before this include.
#include "boost/intrusive_ptr.hpp"

namespace voltdb {
class Table;
class TupleMovementListener;

class TruncatedInt {
public:
    TruncatedInt(uint32_t value) {
        ::memcpy(m_data, reinterpret_cast<char*>(&value), 3);
    }

    TruncatedInt(const TruncatedInt &other) {
        ::memcpy(m_data, other.m_data, 3);
    }

    TruncatedInt& operator=(const TruncatedInt&rhs) {
        ::memcpy(m_data, rhs.m_data, 3);
        return *this;
    }

    uint32_t unpack() {
        char valueBytes[4];
        ::memcpy(valueBytes, m_data, 3);
        valueBytes[3] = 0;
        return *reinterpret_cast<int32_t*>(valueBytes);
    }
private:
    char m_data[3];
};

typedef boost::intrusive_ptr<TupleBlock> TBPtr;
typedef stx::btree_map< char*, TBPtr > TBMap;
typedef TBMap::iterator TBMapI;
typedef stx::btree_set<TBPtr> TBBucket;
typedef TBBucket::iterator TBBucketI;
typedef boost::shared_ptr<TBBucket> TBBucketPtr;
typedef std::vector<TBBucketPtr> TBBucketPtrVector;
const int TUPLE_BLOCK_NUM_BUCKETS = 20;

/**
 * This class represents a fixed-size container of tuples.  The tuples
 * it contains are also fixed-size, with pointers to non-inlined data
 * (VARCHAR, VARBINARY, GEOGRAPHY) mangaged elsewhere.
 */
class TupleBlock {
    friend void ::intrusive_ptr_add_ref(voltdb::TupleBlock * p);
    friend void ::intrusive_ptr_release(voltdb::TupleBlock * p);
public:
    /** Constructor. The bucket argument is optional and may be null.
        It's used for compacting persistent tables---a block's bucket
        is determined by how full it is, so that very full blocks can
        be merged with almost empty ones.*/
    TupleBlock(Table *table, TBBucketPtr bucket);

    /** Overloaded operator new so that we can allocate quickly. This
        uses boost::pool under the hood. */
    void* operator new(std::size_t sz)
    {
        vassert(sz == sizeof(TupleBlock));
        return ThreadLocalPool::allocateExactSizedObject(sizeof(TupleBlock));
    }

    /** Overloaded operator delete to pair with new. */
    void operator delete(void* object)
    { return ThreadLocalPool::freeExactSizedObject(sizeof(TupleBlock), object); }

    /** This method will be 1.0 for a full block, and 0.0 for an empty one. */
    double loadFactor() {
        return static_cast <double> (m_activeTuples) / m_tuplesPerBlock;
    }

    /** Returns true if more tuples can be inserted into this block. */
    inline bool hasFreeTuples() {
        return m_activeTuples < m_tuplesPerBlock;
    }

    /** Returns true if block is empty */
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
        vassert(index < TUPLE_BLOCK_NUM_BUCKETS);
        return index;
    }

    inline int getBucketIndex() {
        return m_bucketIndex;
    }

    /** Merge this block with the given block. Returns the new bucket
        index for this and the other block. */
    std::pair<int, int> merge(Table *table, TBPtr source, TupleMovementListener *listener = NULL);

    /**
     * Find next free tuple storage address and its tupleblock's bucket index,
     * return them as a pair.
     */
    inline std::pair<char*, int> nextFreeTuple() {
        char *retval = NULL;
        if (!m_freeList.empty()) {
            m_lastCompactionOffset = 0;
            retval = m_storage;
            TruncatedInt offset = m_freeList.back();
            m_freeList.pop_back();
            retval += offset.unpack();
        } else {
            retval = &(m_storage[m_tupleLength * m_nextFreeTuple]);
            if(m_nextFreeTuple >= m_tuplesPerBlock)
            {
                return std::pair<char*, int>(NULL, -1);
            }
            m_nextFreeTuple++;
        }
        m_activeTuples++;
        int newBucketIndex = calculateBucketIndex();
        if (newBucketIndex == m_bucketIndex) {
            // tuple block is not too full for its current bucket
            newBucketIndex = NO_NEW_BUCKET_INDEX;
        } else {
            // needs a new bucket and update bucket index
            m_bucketIndex = newBucketIndex;
        }
        return std::pair<char*, int>(retval, newBucketIndex);
    }

    /** Assigns this block to a new bucket. */
    void swapToBucket(TBBucketPtr newBucket) {
        if (m_bucket != NULL) {
            m_bucket->erase(TBPtr(this));
        }
        m_bucket = newBucket;
        if (m_bucket != NULL) {
            m_bucket->insert(TBPtr(this));
        }
    }

    /** Deletes this tuple from the block, and adds the offset to the
        free tuple list.  If the bucket index changes as a result, the
        new buck index is returned. */
    inline int freeTuple(char *tupleStorage) {
        m_lastCompactionOffset = 0;
        m_activeTuples--;
        //Find the offset
        uint32_t offset = static_cast<uint32_t>(tupleStorage - m_storage);
        m_freeList.push_back(offset);
        int newBucketIndex = calculateBucketIndex();
        if (newBucketIndex == m_bucketIndex) {
            return NO_NEW_BUCKET_INDEX;
        }

        m_bucketIndex = newBucketIndex;
        return newBucketIndex;
    }

    /** Return the address of the storage for tuples. */
    inline char * address() {
        return m_storage;
    }

    /** Make this tuple block empty.  (Note: seems to do nothing with
        this block's bucket.) */
    inline void reset() {
        m_activeTuples = 0;
        m_nextFreeTuple = 0;
        m_freeList.clear();
    }

    /** Return the ordinal position of the first empty slot in the
        tuple block.  This will return 0 when the block is empty, 1
        when the block contains 1 tuple, and so on.  Note that this
        isn't just a count of the tuples in the block since there may
        be inactive, empty tuples on the free list. */
    inline uint32_t unusedTupleBoundary() {
        return m_nextFreeTuple;
    }

    /** Frees the storage for this block. */
    ~TupleBlock();

    /** If tuples in this block were merged to another block, this is
        the ordinal position of the last tuple moved. */
    inline uint32_t lastCompactionOffset() {
        return m_lastCompactionOffset;
    }

    /** Sets the last compaction offset for this block. */
    inline void lastCompactionOffset(uint32_t offset) {
        m_lastCompactionOffset = offset;
    }

    /** A count of active tuples in this block. */
    inline uint32_t activeTuples() {
        return m_activeTuples;
    }

    /** Returns the current bucket for this block, to aid in
        compaction. */
    inline TBBucketPtr currentBucket() {
        return m_bucket;
    }

    /**
     * Return the maximum number of bytes in this tuple block which
     * may actually be used for tuples, i.e., the size of the chunk of
     * memory pointed to by m_storage.
     */
    inline int64_t getAllocatedMemory() {
        return m_tupleLength * m_tuplesPerBlock;
    }
private:
    char*   m_storage;
    std::atomic<uint32_t> m_references;
    uint32_t m_tupleLength;
    uint32_t m_tuplesPerBlock;
    uint32_t m_activeTuples;
    uint32_t m_nextFreeTuple;
    uint32_t m_lastCompactionOffset;

    /*
     * queue of offsets to <b>once used and then deleted</b> tuples.
     * Tuples after m_nextFreeTuple are also free, this queue
     * is used to find "hole" tuples which were once used (before used_tuples index)
     * and also deleted.
     * NOTE THAT THESE ARE NOT THE ONLY FREE TUPLES.
     **/
    std::deque<TruncatedInt, FastAllocator<TruncatedInt> > m_freeList;

    TBBucketPtr m_bucket;
    int m_bucketIndex;
};

/**
 * Interface for tuple movement notification.
 */
class TupleMovementListener {
public:
    virtual ~TupleMovementListener() {}

    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple) = 0;
};

}


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
