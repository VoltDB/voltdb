/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "temptable.h"
#include "tableiterator.h"
#include "common/tabletuple.h"
#include "common/serializeio.h"
#include "common/debuglog.h"
#include "storage/TableStats.h"

#define TABLE_BLOCKSIZE 131072

namespace voltdb {

TempTable::TempTable() : Table(TABLE_BLOCKSIZE) {
}
TempTable::~TempTable() {}

// ------------------------------------------------------------------
// OPERATIONS
// ------------------------------------------------------------------
void TempTable::deleteAllTuples(bool freeAllocatedStrings) { deleteAllTuplesNonVirtual(freeAllocatedStrings); }
bool TempTable::insertTuple(TableTuple &source) {
    insertTupleNonVirtual(source);
    return true;
}

bool TempTable::updateTuple(TableTuple &source, TableTuple &target, bool updatesIndexes) {
    updateTupleNonVirtual(source, target);
    return true;
}

bool TempTable::deleteTuple(TableTuple &target, bool deleteAllocatedStrings) {
    VOLT_ERROR("TempTable does not support deleting individual tuples");
    return false;
}

std::string TempTable::tableType() const {
    return "TempTable";
}
void TempTable::onSetColumns() {
    m_tmpTarget1 = TableTuple(m_schema);
    m_tmpTarget2 = TableTuple(m_schema);
}

void TempTable::loadTuplesFrom(bool, SerializeInput &serialize_io,
                               Pool *stringPool) {

    /*
     * directly receives a VoltTable buffer.
     * [00 01]   [02 03]   [04 .. 0x]
     * rowstart  colcount  colcount * 1 byte (column types)
     *
     * [0x+1 .. 0y]
     * colcount * strings (column names)
     *
     * [0y+1 0y+2 0y+3 0y+4]
     * rowcount
     *
     * [0y+5 .. end]
     * rowdata
     */

    // todo: just skip ahead to this position
    serialize_io.readInt(); // rowstart

    int16_t colcount = serialize_io.readShort();
    assert(colcount >= 0);

    // skip the column types
    for (int i = 0; i < colcount; ++i) {
        serialize_io.readEnumInSingleByte();
    }

    // skip the column names
    for (int i = 0; i < colcount; ++i) {
        serialize_io.readTextString();
    }

    int tupleCount = serialize_io.readInt();
    assert(tupleCount >= 0);

    // allocate required data blocks first to make them alligned well
    while (tupleCount + m_usedTuples > m_allocatedTuples) {
        allocateNextBlock();
    }

    for (int i = 0; i < tupleCount; ++i) {
        m_tmpTarget1.move(dataPtrForTuple((int) m_usedTuples + i));
        m_tmpTarget1.setDeletedFalse();
        m_tmpTarget1.deserializeFrom(serialize_io, stringPool);
    }

    m_tupleCount += tupleCount;
    m_usedTuples += tupleCount;
}

voltdb::TableStats* TempTable::getTableStats() { return NULL; }
}


