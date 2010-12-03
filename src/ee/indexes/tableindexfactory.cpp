/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

#include <cassert>
#include <iostream>
#include "indexes/tableindexfactory.h"
#include "common/types.h"
#include "catalog/index.h"
#include "indexes/tableindex.h"
#include "indexes/indexkey.h"
#include "indexes/arrayuniqueindex.h"
#include "indexes/CompactingTreeUniqueIndex.h"
#include "indexes/BinaryTreeMultiMapIndex.h"

namespace voltdb {

TableIndex *TableIndexFactory::getInstance(const TableIndexScheme &scheme) {
    int colCount = (int)scheme.columnIndices.size();
    bool unique = scheme.unique;
    bool ints_only = scheme.intsOnly;
    TableIndexType type = scheme.type;
    std::vector<int32_t> columnIndices = scheme.columnIndices;
    voltdb::TupleSchema *tupleSchema = scheme.tupleSchema;
    std::vector<voltdb::ValueType> keyColumnTypes;
    std::vector<int32_t> keyColumnLengths;
    std::vector<bool> keyColumnAllowNull(colCount, true);
    for (int i = 0; i < colCount; ++i) {
        keyColumnTypes.push_back(tupleSchema->columnType(columnIndices[i]));
        keyColumnLengths.push_back(tupleSchema->columnLength(columnIndices[i]));
    }
    voltdb::TupleSchema *keySchema = voltdb::TupleSchema::createTupleSchema(keyColumnTypes, keyColumnLengths, keyColumnAllowNull, true);
    TableIndexScheme schemeCopy(scheme);
    schemeCopy.keySchema = keySchema;
    VOLT_TRACE("Creating index for %s.\n%s", scheme.name.c_str(), keySchema->debug().c_str());
    const int keySize = keySchema->tupleLength();

    // no int specialization beyond this point
    if (keySize > sizeof(int64_t) * 4) {
        ints_only = false;
    }

    // a bit of a hack, this should be improved later
    if ((ints_only) && (unique) && (type == ARRAY_INDEX)) {
        return new ArrayUniqueIndex(schemeCopy);
    }

    if ((ints_only) && (unique)) {
        if (type == HASH_TABLE_INDEX) {
            VOLT_INFO("Producing a tree index for %s: "
                      "hash index not currently supported for this index key.\n",
                      scheme.name.c_str());
        }

        if (keySize <= sizeof(uint64_t)) {
            return new CompactingTreeUniqueIndex<IntsKey<1>, IntsComparator<1>, IntsEqualityChecker<1> >(schemeCopy);
        } else if (keySize <= sizeof(int64_t) * 2) {
            return new CompactingTreeUniqueIndex<IntsKey<2>, IntsComparator<2>, IntsEqualityChecker<2> >(schemeCopy);
        } else if (keySize <= sizeof(int64_t) * 3) {
            return new CompactingTreeUniqueIndex<IntsKey<3>, IntsComparator<3>, IntsEqualityChecker<3> >(schemeCopy);
        } else if (keySize <= sizeof(int64_t) * 4) {
            return new CompactingTreeUniqueIndex<IntsKey<4>, IntsComparator<4>, IntsEqualityChecker<4> >(schemeCopy);
        } else {
            throwFatalException("We currently only support tree index on unique integer keys of size 32 bytes or smaller...");
        }
    }

    if ((ints_only) && (!unique)) {
        if (type == HASH_TABLE_INDEX) {
            VOLT_INFO("Producing a tree index for %s: "
                      "hash index not currently supported for this index key.\n",
                      scheme.name.c_str());
        }

        if (keySize <= sizeof(uint64_t)) {
            return new BinaryTreeMultiMapIndex<IntsKey<1>, IntsComparator<1>, IntsEqualityChecker<1> >(schemeCopy);
        } else if (keySize <= sizeof(int64_t) * 2) {
            return new BinaryTreeMultiMapIndex<IntsKey<2>, IntsComparator<2>, IntsEqualityChecker<2> >(schemeCopy);
        } else if (keySize <= sizeof(int64_t) * 3) {
            return new BinaryTreeMultiMapIndex<IntsKey<3>, IntsComparator<3>, IntsEqualityChecker<3> >(schemeCopy);
        } else if (keySize <= sizeof(int64_t) * 4) {
            return new BinaryTreeMultiMapIndex<IntsKey<4>, IntsComparator<4>, IntsEqualityChecker<4> >(schemeCopy);
        } else {
            throwFatalException( "We currently only support tree index on non-unique integer keys of size 32 bytes or smaller..." );
        }
    }

    if (unique) {
        if (type == HASH_TABLE_INDEX) {
            VOLT_INFO("Producing a tree index for %s: "
                      "hash index not currently supported for this index key.\n",
                      scheme.name.c_str());
        }

        if (keySize <= 4) {
            return new CompactingTreeUniqueIndex<GenericKey<4>, GenericComparator<4>, GenericEqualityChecker<4> >(schemeCopy);
        } else if (keySize <= 8) {
            return new CompactingTreeUniqueIndex<GenericKey<8>, GenericComparator<8>, GenericEqualityChecker<8> >(schemeCopy);
        } else if (keySize <= 12) {
            return new CompactingTreeUniqueIndex<GenericKey<12>, GenericComparator<12>, GenericEqualityChecker<12> >(schemeCopy);
        } else if (keySize <= 16) {
            return new CompactingTreeUniqueIndex<GenericKey<16>, GenericComparator<16>, GenericEqualityChecker<16> >(schemeCopy);
        } else if (keySize <= 24) {
            return new CompactingTreeUniqueIndex<GenericKey<24>, GenericComparator<24>, GenericEqualityChecker<24> >(schemeCopy);
        } else if (keySize <= 32) {
            return new CompactingTreeUniqueIndex<GenericKey<32>, GenericComparator<32>, GenericEqualityChecker<32> >(schemeCopy);
        } else if (keySize <= 48) {
            return new CompactingTreeUniqueIndex<GenericKey<48>, GenericComparator<48>, GenericEqualityChecker<48> >(schemeCopy);
        } else if (keySize <= 64) {
            return new CompactingTreeUniqueIndex<GenericKey<64>, GenericComparator<64>, GenericEqualityChecker<64> >(schemeCopy);
        } else if (keySize <= 96) {
            return new CompactingTreeUniqueIndex<GenericKey<96>, GenericComparator<96>, GenericEqualityChecker<96> >(schemeCopy);
        } else if (keySize <= 128) {
            return new CompactingTreeUniqueIndex<GenericKey<128>, GenericComparator<128>, GenericEqualityChecker<128> >(schemeCopy);
        } else if (keySize <= 256) {
            return new CompactingTreeUniqueIndex<GenericKey<256>, GenericComparator<256>, GenericEqualityChecker<256> >(schemeCopy);
        } else {
            return new CompactingTreeUniqueIndex<TupleKey, TupleKeyComparator, TupleKeyEqualityChecker>(schemeCopy);
        }
    }

    if (!unique) {
        if (type == HASH_TABLE_INDEX) {
            VOLT_INFO("Producing a tree index for %s: "
                      "hash index not currently supported for this index key.\n",
                      scheme.name.c_str());
        }

        if (keySize <= 4) {
            return new BinaryTreeMultiMapIndex<GenericKey<4>, GenericComparator<4>, GenericEqualityChecker<4> >(schemeCopy);
        } else if (keySize <= 8) {
            return new BinaryTreeMultiMapIndex<GenericKey<8>, GenericComparator<8>, GenericEqualityChecker<8> >(schemeCopy);
        } else if (keySize <= 12) {
            return new BinaryTreeMultiMapIndex<GenericKey<12>, GenericComparator<12>, GenericEqualityChecker<12> >(schemeCopy);
        } else if (keySize <= 16) {
            return new BinaryTreeMultiMapIndex<GenericKey<16>, GenericComparator<16>, GenericEqualityChecker<16> >(schemeCopy);
        } else if (keySize <= 24) {
            return new BinaryTreeMultiMapIndex<GenericKey<24>, GenericComparator<24>, GenericEqualityChecker<24> >(schemeCopy);
        } else if (keySize <= 32) {
            return new BinaryTreeMultiMapIndex<GenericKey<32>, GenericComparator<32>, GenericEqualityChecker<32> >(schemeCopy);
        } else if (keySize <= 48) {
            return new BinaryTreeMultiMapIndex<GenericKey<48>, GenericComparator<48>, GenericEqualityChecker<48> >(schemeCopy);
        } else if (keySize <= 64) {
            return new BinaryTreeMultiMapIndex<GenericKey<64>, GenericComparator<64>, GenericEqualityChecker<64> >(schemeCopy);
        } else if (keySize <= 96) {
            return new BinaryTreeMultiMapIndex<GenericKey<96>, GenericComparator<96>, GenericEqualityChecker<96> >(schemeCopy);
        } else if (keySize <= 128) {
            return new BinaryTreeMultiMapIndex<GenericKey<128>, GenericComparator<128>, GenericEqualityChecker<128> >(schemeCopy);
        } else if (keySize <= 256) {
            return new BinaryTreeMultiMapIndex<GenericKey<256>, GenericComparator<256>, GenericEqualityChecker<256> >(schemeCopy);
        } else {
            return new BinaryTreeMultiMapIndex<TupleKey, TupleKeyComparator, TupleKeyEqualityChecker>(schemeCopy);
        }
    }

    throwFatalException("Unsupported index scheme..." );
    return NULL;
}

}
