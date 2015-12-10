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
#include <vector>
#include <stdint.h>
#include <string.h>
#include <cassert>

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

namespace voltdb {
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

//typedef boost::shared_ptr<TupleBlock> TBPtr;
typedef boost::intrusive_ptr<TupleBlock> TBPtr;
//typedef TupleBlock* TBPtr;
typedef stx::btree_map< char*, TBPtr > TBMap;
typedef TBMap::iterator TBMapI;
typedef stx::btree_set<TBPtr> TBBucket;
typedef TBBucket::iterator TBBucketI;
typedef boost::shared_ptr<TBBucket> TBBucketPtr;
typedef std::vector<TBBucketPtr> TBBucketMap;
const int TUPLE_BLOCK_NUM_BUCKETS = 20;

class TupleBlock {
    friend void ::intrusive_ptr_add_ref(voltdb::TupleBlock * p);
    friend void ::intrusive_ptr_release(voltdb::TupleBlock * p);
public:
    TupleBlock(Table *table, TBBucketPtr bucket);

    double loadFactor() {
        return m_activeTuples / m_tuplesPerBlock;
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

    inline int calculateBucketIndex(uint32_t tuplesPendingDeleteOnUndoRelease = 0) {
        if (!hasFreeTuples()) {
            //Completely full, don't need be considered for merging
            //Remove self from current bucket and null out the bucket
            if (m_bucket.get() != NULL) {
                //std::cout << static_cast<void*>(this) << " is full, erasing from bucket" << std::endl;
                m_bucket->erase(TBPtr(this));
                m_bucket = TBBucketPtr();
            }
            return -1;
        } else if (tuplesPendingDeleteOnUndoRelease == m_activeTuples) {
            //Someone was kind enough to scan the whole block, move all tuples
            //not pending delete on undo release to another block as part
            //of compaction. Now this block doesn't need to be considered
            //for compaction anymore. The block will be completely discard along with the undo
            //information. Any tuples pending delete due to a snapshot will moved and picked up
            //by the snapshot scan from the other block
            if (m_bucket.get() != NULL) {
                //std::cout << static_cast<void*>(this) << " has only deleted tuples that are pending undo release "
                //        << tuplesPendingDeleteOnUndoRelease << std::endl;
                m_bucket->erase(TBPtr(this));
                m_bucket = TBBucketPtr();
            }
            return -1;
        }
        else {
            int index = static_cast<int>(::floor(m_activeTuples / m_tuplesPerBlockDivNumBuckets));
            assert(index < TUPLE_BLOCK_NUM_BUCKETS);
            assert(index >= 0);
            return index;
        }
    }

    inline int getBucketIndex() {
        return m_bucketIndex;
    }

    std::pair<int, int> merge(Table *table, TBPtr source, TupleMovementListener *listener = NULL);

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
            m_nextFreeTuple++;
        }
        m_activeTuples++;
        int newBucketIndex = calculateBucketIndex();
        if (newBucketIndex != m_bucketIndex) {
            m_bucketIndex = newBucketIndex;
            return std::pair<char*, int>(retval, newBucketIndex);
        } else {
            return std::pair<char*, int>(retval, -1);
        }
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

    inline int freeTuple(char *tupleStorage) {
        m_lastCompactionOffset = 0;
        m_activeTuples--;
        //Find the offset
        uint32_t offset = static_cast<uint32_t>(tupleStorage - m_storage);
        m_freeList.push_back(offset);
        int newBucketIndex = calculateBucketIndex();
        if (newBucketIndex != m_bucketIndex) {
            m_bucketIndex = newBucketIndex;
            return newBucketIndex;
        } else {
            return -1;
        }
    }

    inline char * address() {
        return m_storage;
    }

    inline void reset() {
        m_activeTuples = 0;
        m_nextFreeTuple = 0;
        m_freeList.clear();
    }

    inline uint32_t unusedTupleBoundry() {
        return m_nextFreeTuple;
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
    char*   m_storage;
    uint32_t m_references;
    uint32_t m_tupleLength;
    uint32_t m_tuplesPerBlock;
    uint32_t m_activeTuples;
    uint32_t m_nextFreeTuple;
    uint32_t m_lastCompactionOffset;
    const double m_tuplesPerBlockDivNumBuckets;

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
        p->~TupleBlock();
        voltdb::ThreadLocalPool::getExact(sizeof(voltdb::TupleBlock))->free(p);
    }
}

#endif /* VOLTDB_TUPLEBLOCK_H_ */
