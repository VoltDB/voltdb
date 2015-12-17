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
        m_boundaryTuple(0),
        m_freedTuple(0),
        m_lastCompactionOffset(0),
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

} // namespace voltdb
