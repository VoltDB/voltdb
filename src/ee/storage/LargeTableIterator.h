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

#include "common/tabletuple.h"
#include "storage/LargeTempTable.h"

namespace voltdb {

class LargeTableIterator {

    friend class LargeTempTable;

public:

    LargeTableIterator(const LargeTableIterator& that)
        : m_table(that.m_table)
        , m_data(that.m_data)
        , m_dataSize(that.m_dataSize)
        , m_currPosition(that.m_currPosition)
        , m_numTuplesReturned(that.m_numTuplesReturned)
        , m_storage(that.m_table->schema())
    {
        // xxx this is too expensive because the tuple storage gets
        // realloced... can use move ctor?
    }

    inline bool next(TableTuple& out);
    bool hasNext() const;

protected:
    LargeTableIterator(const LargeTempTable* table, char* data)
        : m_table(table)
        , m_data(data)
        , m_dataSize(LargeTempTable::BLOCKSIZE)
        , m_currPosition(0)
        , m_numTuplesReturned(0)
        , m_storage(table->schema())
    {
    }

private:

    const LargeTempTable* m_table;
    const char* m_data;
    const size_t m_dataSize;
    size_t m_currPosition;
    int64_t m_numTuplesReturned;
    StandAloneTupleStorage m_storage;

};


bool LargeTableIterator::next(TableTuple& out) {
    if (m_numTuplesReturned >= m_table->numTuples()) {
        return false;
    }

    ReferenceSerializeInputBE input(m_data + m_currPosition,
                                    m_dataSize - m_currPosition);
    out = m_storage.tuple();

    out.deserializeFrom(input, ExecutorContext::getTempStringPool());

    ++m_numTuplesReturned;

    // xxx hack!
    m_currPosition = input.getRawPointer() - m_data;

    return true;
}


} // namespace voltdb

#endif // _VOLTDB_LARGETABLEITERATOR_H
