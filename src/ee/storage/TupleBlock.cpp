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
#include "storage/TupleBlock.h"
#include "storage/table.h"
#include <sys/mman.h>
#include <errno.h>
#include "common/ThreadLocalPool.h"

namespace voltdb {

volatile int tupleBlocksAllocated = 0;

TupleBlock::TupleBlock(Table *table, TBBucketPtr bucket) :
        m_storage(NULL),
        m_references(0),
        m_tupleLength(table->m_tupleLength),
        m_tuplesPerBlock(table->m_tuplesPerBlock),
        m_activeTuples(0),
        m_nextFreeTuple(0),
        m_lastCompactionOffset(0),
        m_tuplesPerBlockDivNumBuckets(m_tuplesPerBlock / static_cast<double>(TUPLE_BLOCK_NUM_BUCKETS)),
        m_bucket(bucket),
        m_bucketIndex(0)
{
#ifdef USE_MMAP
    size_t tableAllocationSize = static_cast<size_t> (m_tupleLength * m_tuplesPerBlock);
    m_storage = static_cast<char*>(::mmap( 0, tableAllocationSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0 ));
    if (m_storage == MAP_FAILED) {
        std::cout << strerror( errno ) << std::endl;
        throwFatalException("Failed mmap");
    }
#else
    m_storage = new char[table->m_tableAllocationSize];
#endif
    tupleBlocksAllocated++;
}

TupleBlock::~TupleBlock() {
#ifdef USE_MMAP
    size_t tableAllocationSize = static_cast<size_t> (m_tupleLength * m_tuplesPerBlock);
    if (::munmap( m_storage, tableAllocationSize) != 0) {
        std::cout << strerror( errno ) << std::endl;
        throwFatalException("Failed munmap");
    }
#else
    delete []m_storage;
#endif
}

std::pair<int, int> TupleBlock::merge(Table *table, TBPtr source, TupleMovementListener *listener) {
    assert(source != this);
    /*
      std::cout << "Attempting to merge " << static_cast<void*> (this)
                << "(" << m_activeTuples << ") with " << static_cast<void*>(source.get())
                << "(" << source->m_activeTuples << ")";
      std::cout << " source last compaction offset is " << source->lastCompactionOffset()
                << " and active tuple count is " << source->m_activeTuples << std::endl;
    */

    uint32_t m_nextTupleInSourceOffset = source->lastCompactionOffset();
    int sourceTuplesPendingDeleteOnUndoRelease = 0;
    while (hasFreeTuples() && !source->isEmpty()) {
        TableTuple sourceTupleWithNewValues(table->schema());
        TableTuple destinationTuple(table->schema());

        bool foundSourceTuple = false;
        //Iterate further into the block looking for active tuples
        //Stop when running into the unused tuple boundry
        while (m_nextTupleInSourceOffset < source->unusedTupleBoundry()) {
            sourceTupleWithNewValues.move(&source->address()[m_tupleLength * m_nextTupleInSourceOffset]);
            m_nextTupleInSourceOffset++;
            if (sourceTupleWithNewValues.isActive()) {
                foundSourceTuple = true;
                break;
            }
        }

        if (!foundSourceTuple) {
           //The block isn't empty, but there are no more active tuples.
           //Some of the tuples that make it register as not empty must have been
           //pending delete and those aren't mergable
            assert(sourceTuplesPendingDeleteOnUndoRelease);
            break;
        }

        //Can't move a tuple with a pending undo action, it would invalidate the pointer
        //Keep a count so that the block can be notified of the number
        //of tuples pending delete on undo release when calculating the correct bucket
        //index. If all the active tuples are pending delete on undo release the block
        //is effectively empty and shouldn't be considered for merge ops.
        //It will be completely discarded once the undo log releases the block.
        if (sourceTupleWithNewValues.isPendingDeleteOnUndoRelease()) {
            sourceTuplesPendingDeleteOnUndoRelease++;
            continue;
        }

        destinationTuple.move(nextFreeTuple().first);
        table->swapTuples(sourceTupleWithNewValues, destinationTuple);

        // Notify the listener if provided.
        if (listener != NULL) {
            listener->notifyTupleMovement(source, this, sourceTupleWithNewValues, destinationTuple);
        }

        source->freeTuple(sourceTupleWithNewValues.address());
    }
    source->lastCompactionOffset(m_nextTupleInSourceOffset);

    int newBucketIndex = calculateBucketIndex();
    if (newBucketIndex != m_bucketIndex) {
        m_bucketIndex = newBucketIndex;
        //std::cout << "Merged " << static_cast<void*> (this) << "(" << m_activeTuples << ") with " << static_cast<void*>(source.get())  << "(" << source->m_activeTuples << ")";
        //std::cout << " found " << sourceTuplesPendingDeleteOnUndoRelease << " tuples pending delete on undo release "<< std::endl;
        return std::pair<int, int>(newBucketIndex, source->calculateBucketIndex(sourceTuplesPendingDeleteOnUndoRelease));
    } else {
        //std::cout << "Merged " << static_cast<void*> (this) << "(" << m_activeTuples << ") with " << static_cast<void*>(source.get()) << "(" << source->m_activeTuples << ")";
        //std::cout << " found " << sourceTuplesPendingDeleteOnUndoRelease << " tuples pending delete on undo release "<< std::endl;
        return std::pair<int, int>( -1, source->calculateBucketIndex(sourceTuplesPendingDeleteOnUndoRelease));
    }
}
}

