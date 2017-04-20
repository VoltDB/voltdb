/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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

#ifndef _VOLTDB_LARGETABLEITERATOR_H
#define _VOLTDB_LARGETABLEITERATOR_H

#include "common/executorcontext.hpp"
#include "common/tabletuple.h"
#include "common/LargeTempTableBlockCache.h"

namespace voltdb {

class LargeTableIterator {

    friend class LargeTempTable;

public:

    LargeTableIterator(const LargeTableIterator& that)
        : m_schema(that.m_schema)
        , m_currBlock(nullptr)
        , m_blockIds(that.m_blockIds)
        , m_blockIterator(that.m_blockIterator)
        , m_currPosition(that.m_currPosition)
        , m_storage(that.m_schema)
    {
        // xxx this is too expensive because the tuple storage gets
        // realloced... can use move ctor?
    }

    inline bool next(TableTuple& out);
    bool hasNext() const;

protected:
    LargeTableIterator(const TupleSchema *schema,
                       const std::vector<int64_t> *blockIds)
        : m_schema(schema)
        , m_currBlock(nullptr)
        , m_blockIds(blockIds)
        , m_blockIterator(std::begin(*blockIds))
        , m_currPosition(0)
        , m_storage(schema)
    {
        LargeTempTableBlockCache* lttCache = ExecutorContext::getExecutorContext()->lttBlockCache();
        m_currBlock = lttCache->fetchBlock(*m_blockIterator);
    }

private:

    const TupleSchema* m_schema;
    LargeTempTableBlock* m_currBlock;
    const std::vector<int64_t> *m_blockIds;
    std::vector<int64_t>::const_iterator m_blockIterator;
    size_t m_currPosition;
    StandAloneTupleStorage m_storage;

};


bool LargeTableIterator::next(TableTuple& out) {
    if (m_blockIterator == std::end(*m_blockIds)) {
        return false;
    }

    /* char* data = m_currBlock->getData(); */
    /* ReferenceSerializeInputBE input(data + m_currPosition, */
    /*                                 LargeTempTableBlock::getBlocksize() - m_currPosition); */
    /* out = m_storage.tuple(); */

    /* out.deserializeFrom(input, ExecutorContext::getTempStringPool()); */

    /* // xxx hack! */
    /* m_currPosition = input.getRawPointer() - data; */

    /* assert(m_currPosition <= m_currBlock->getUsedBytes()); */
    /* if (m_currPosition == m_currBlock->getUsedBytes()) { */
    /*     LargeTempTableBlockCache* lttCache = ExecutorContext::getExecutorContext()->lttBlockCache(); */

    /*     // unpin the current block */
    /*     lttCache->unpinBlock(*m_blockIterator); */

    /*     // Get the next block if one exists */
    /*     ++m_blockIterator; */
    /*     if (m_blockIterator != std::end(*m_blockIds)) { */
    /*         m_currBlock = lttCache->fetchBlock(*m_blockIterator); */
    /*         m_currPosition = 0; */
    /*     } */
    /* } */

    return true;
}


} // namespace voltdb

#endif // _VOLTDB_LARGETABLEITERATOR_H
