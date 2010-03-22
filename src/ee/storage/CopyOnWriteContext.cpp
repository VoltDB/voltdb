/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
#include "storage/CopyOnWriteContext.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/CopyOnWriteIterator.h"
#include "storage/tableiterator.h"
#include <algorithm>
#include <cassert>
#include <boost/crc.hpp>

/**
 * Matches the allocations size in persistenttable.cpp. It's a terrible idea to cut and paste
 * it, but if they become inconsistent the memcheck build will catch it.
 */
#ifndef MEMCHECK
#define TABLE_BLOCKSIZE 2097152
#endif
namespace voltdb {

// Created this simple comparitor to compare addresses of BlockPairs for sorting
bool simplePairAddressToPairAddressComparator(const BlockPair a, const BlockPair b) {
        return a.first < b.first;
}
// These next two methods here do some Ariel-foo that probably merits a comment.
#ifdef MEMCHECK
bool pairAddressToPairAddressComparator(const BlockPair a, const BlockPair b) {
    return a.pair.first + a.tupleLength < b.pair.first;
}
#else
bool pairAddressToPairAddressComparator(const BlockPair a, const BlockPair b) {
    return a.first + TABLE_BLOCKSIZE < b.first;
}
#endif

CopyOnWriteContext::CopyOnWriteContext(Table *table, TupleSerializer *serializer, int32_t partitionId) :
             m_table(table),
             m_backedUpTuples(TableFactory::getCopiedTempTable(table->databaseId(), "COW of " + table->name(), table, NULL)),
             m_serializer(serializer), m_pool(2097152, 320), m_blocks(m_table->m_data.size()),
             m_iterator(new CopyOnWriteIterator(table)),
             m_maxTupleLength(serializer->getMaxSerializedTupleSize(table->schema())),
             m_tuple(table->schema()), m_finishedTableScan(false), m_partitionId(partitionId),
             m_tuplesSerialized(0) {
    for (int ii = 0; ii < table->m_data.size(); ii++) {
#ifdef MEMCHECK
        BlockPair p;
        p.pair =  std::pair<char*, int>(table->m_data[ii], ii);
        p.tupleLength = table->tempTuple().tupleLength();
#else
        const BlockPair p(table->m_data[ii], ii);
#endif
        m_blocks[ii] = p;
    }
    std::sort( m_blocks.begin(), m_blocks.end(), simplePairAddressToPairAddressComparator);
#ifdef DEBUG
#ifndef MEMCHECK
    for (int ii = 0; ii < m_blocks.size() - 1; ii++) {
        assert(m_blocks[ii].first < m_blocks[ii + 1].first);
    }
#endif
#endif
#ifdef MEMCHECK
    for (int ii = 0; ii < m_blocks.size() - 1; ii++) {
        assert(m_blocks[ii].pair.first < m_blocks[ii + 1].pair.first);
    }
#endif
}

bool CopyOnWriteContext::serializeMore(ReferenceSerializeOutput *out) {
    boost::crc_32_type crc;
    const std::size_t crcPosition = out->reserveBytes(4);//For CRC
    out->writeInt(m_partitionId);
    crc.process_bytes(out->data() + crcPosition + 4, 4);//CRC the partition ID
    int rowsSerialized = 0;

    TableTuple tuple(m_table->schema());
    if (out->remaining() < (m_maxTupleLength + sizeof(int32_t))) {
        throw std::runtime_error("Serialize more should never be called "
                "a 2nd time after return indicating there is no more data");
//        out->writeInt(0);
//        assert(false);
//        return false;
    }

    while (out->remaining() >= (m_maxTupleLength + sizeof(int32_t))) {
        const bool hadMore = m_iterator->next(tuple);

        /**
         * After this finishes scanning the persistent table switch to scanning
         * the temp table with the tuples that were backed up
         */
        if (!hadMore) {
            if (m_finishedTableScan) {
                out->writeInt(rowsSerialized);
                crc.process_bytes(out->data() + out->position() - 4, 4);
                out->writeIntAt(crcPosition, crc.checksum());
                return false;
            } else {
                m_finishedTableScan = true;
                m_iterator.reset(new TableIterator(m_backedUpTuples.get()));
                continue;
            }
        }

        const std::size_t tupleStartPosition = out->position();
        m_serializer->serializeTo( tuple, out);
        const std::size_t tupleEndPosition = out->position();
        crc.process_block(out->data() + tupleStartPosition, out->data() + tupleEndPosition);
        m_tuplesSerialized++;
        rowsSerialized++;
    }
    out->writeInt(rowsSerialized);
    crc.process_bytes(out->data() + out->position() - 4, 4);
    out->writeIntAt(crcPosition, crc.checksum());
    return true;
}

void CopyOnWriteContext::markTupleDirty(TableTuple tuple, bool newTuple) {
    /**
     * If this an update or a delete of a tuple that is already dirty then no further action is
     * required.
     */
    if (!newTuple && tuple.isDirty()) {
        return;
    }

    /**
     * If the table has been scanned already there is no need to continue marking tuples dirty
     * If the tuple is dirty then it has already been backed up.
     */
    if (m_finishedTableScan) {
        tuple.setDirtyFalse();
        return;
    }

    /**
     * Find out which block the address is contained in.
     */
    char *address = tuple.address();
#ifdef MEMCHECK
        BlockPair compP;
        compP.pair =  std::pair<char*, int>(address, 0);
        compP.tupleLength = tuple.tupleLength();
#else
    const BlockPair compP(address, 0);
#endif
    BlockPairVectorI i =
            std::lower_bound(m_blocks.begin(), m_blocks.end(), compP, pairAddressToPairAddressComparator);
    if (i == m_blocks.end()) {
        tuple.setDirtyFalse();
        return;
    }
#ifdef MEMCHECK
    const char *blockStartAddress = (*i).pair.first;
    const int blockIndex = (*i).pair.second;
    const char *blockEndAddress = blockStartAddress + tuple.tupleLength();
#else
    const char *blockStartAddress = (*i).first;
    const int blockIndex = (*i).second;
    const char *blockEndAddress = blockStartAddress + TABLE_BLOCKSIZE;
#endif

    if (address >= blockEndAddress || address < blockStartAddress) {
        /**
         * Tuple is in a block allocated after the start of COW
         */
        tuple.setDirtyFalse();
        return;
    }

    /**
     * Now check where this is relative to the COWIterator.
     */
    CopyOnWriteIterator *iter = reinterpret_cast<CopyOnWriteIterator*>(m_iterator.get());
    if (iter->needToDirtyTuple(blockIndex, address, newTuple)) {
        tuple.setDirtyTrue();
        /**
         * Don't back up a newly introduced tuple, just mark it as dirty.
         */
        if (!newTuple) {
            m_backedUpTuples->insertTupleNonVirtualWithDeepCopy(tuple, &m_pool);
        }
    } else {
        tuple.setDirtyFalse();
        return;
    }
}

CopyOnWriteContext::~CopyOnWriteContext() {}

}
