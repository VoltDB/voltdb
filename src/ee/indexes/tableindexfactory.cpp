/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include "common/SerializableEEException.h"
#include "common/types.h"
#include "catalog/index.h"
#include "expressions/tuplevalueexpression.h"
#include "indexes/tableindex.h"
#include "indexes/indexkey.h"
#include "indexes/CompactingTreeUniqueIndex.h"
#include "indexes/CompactingTreeMultiMapIndex.h"
#include "indexes/CompactingHashUniqueIndex.h"
#include "indexes/CompactingHashMultiMapIndex.h"

namespace voltdb {

class TableIndexPicker
{
    template <class TKeyType>
    TableIndex *getInstanceForKeyType() const
    {
        if (m_scheme.unique) {
            if (m_type != BALANCED_TREE_INDEX) {
                return new CompactingHashUniqueIndex<TKeyType >(m_keySchema, m_scheme);
            } else if (m_scheme.countable) {
                return new CompactingTreeUniqueIndex<NormalKeyValuePair<TKeyType>, true>(m_keySchema, m_scheme);
            } else {
                return new CompactingTreeUniqueIndex<NormalKeyValuePair<TKeyType>, false>(m_keySchema, m_scheme);
            }
        } else {
            if (m_type != BALANCED_TREE_INDEX) {
                return new CompactingHashMultiMapIndex<TKeyType >(m_keySchema, m_scheme);
            } else if (m_scheme.countable) {
                return new CompactingTreeMultiMapIndex<PointerKeyValuePair<TKeyType>, true>(m_keySchema, m_scheme);
            } else {
                return new CompactingTreeMultiMapIndex<PointerKeyValuePair<TKeyType>, false>(m_keySchema, m_scheme);
            }
        }
    }

    template <std::size_t KeySize>
    TableIndex *getInstanceIfKeyFits()
    {
        if (m_keySize > KeySize) {
            return NULL;
        }
        if (m_intsOnly) {
            // The IntsKey size parameter ((KeySize-1)/8 + 1) is calculated to be
            // the number of 8-byte uint64's required to store KeySize packed bytes.
            return getInstanceForKeyType<IntsKey<(KeySize-1)/8 + 1> >();
        }
        // Generic Key
        if (m_type == HASH_TABLE_INDEX) {
            VOLT_INFO("Producing a tree index for %s: "
                      "hash index not currently supported for this index key.\n",
                      m_scheme.name.c_str());
            m_type = BALANCED_TREE_INDEX;
        }
        // If any indexed expression value can not either be stored "inline" within a (GenericKey) key tuple
        // or specifically in a non-inlined object shared with the base table (because it is a simple column value),
        // then the GenericKey will have to reference and maintain its own persistent non-inline storage.
        // That's exactly what the GenericPersistentKey subtype of GenericKey does. This incurs extra overhead
        // for object copying and freeing, so is only enabled as needed.
        if (m_inlinesOrColumnsOnly) {
            return getInstanceForKeyType<GenericKey<KeySize> >();
        }
        return getInstanceForKeyType<GenericPersistentKey<KeySize> >();
    }

    template <int ColCount>
    TableIndex *getInstanceForHashedGenericColumns() const
    {
        if (m_scheme.unique) {
            return new CompactingHashUniqueIndex<GenericKey<ColCount> >(m_keySchema, m_scheme);
        } else {
            return new CompactingHashMultiMapIndex<GenericKey<ColCount> >(m_keySchema, m_scheme);
        }
    }


public:

    TableIndex *getInstance()
    {
        TableIndex *result;
/*
        if ((!m_intsOnly) && (m_type == HASH_TABLE_INDEX)) {
            switch (colCount) {
                case 1: return getInstanceForHashedGenericColumns<1>();
                case 2: return getInstanceForHashedGenericColumns<2>();
                case 3: return getInstanceForHashedGenericColumns<3>();
                case 4: return getInstanceForHashedGenericColumns<4>();
                case 5: return getInstanceForHashedGenericColumns<5>();
                case 6: return getInstanceForHashedGenericColumns<6>();
                default: throwFatalException( "We currently only support up to 6 column generic hash indexes..." );
            }
        }
*/

        if ((result = getInstanceIfKeyFits<4>())) {
            return result;
        }
        if ((result = getInstanceIfKeyFits<8>())) {
            return result;
        }
        if ((result = getInstanceIfKeyFits<12>())) {
            return result;
        }
        if ((result = getInstanceIfKeyFits<16>())) {
            return result;
        }
        if ((result = getInstanceIfKeyFits<24>())) {
            return result;
        }
        if ((result = getInstanceIfKeyFits<32>())) {
            return result;
        }

        // no int specialization beyond this size (32 bytes == 4 'uint64_t's)
        m_intsOnly = false;
        //TODO: short term, throw if scheme uses generalized indexed expressions (m_scheme.indexedExpressions.size() > 0)

        if ((result = getInstanceIfKeyFits<48>())) {
            return result;
        }
        if ((result = getInstanceIfKeyFits<64>())) {
            return result;
        }
        if ((result = getInstanceIfKeyFits<96>())) {
            return result;
        }
        if ((result = getInstanceIfKeyFits<128>())) {
            return result;
        }
        if ((result = getInstanceIfKeyFits<256>())) {
            return result;
        }

        if (m_scheme.unique) {
            if (m_scheme.countable) {
                return new CompactingTreeUniqueIndex<NormalKeyValuePair<TupleKey>, true >(m_keySchema, m_scheme);
            } else {
                return new CompactingTreeUniqueIndex<NormalKeyValuePair<TupleKey>, false>(m_keySchema, m_scheme);
            }
        }
        if (m_scheme.countable) {
            return new CompactingTreeMultiMapIndex<PointerKeyValuePair<TupleKey>, true >(m_keySchema, m_scheme);
        } else {
            return new CompactingTreeMultiMapIndex<PointerKeyValuePair<TupleKey>, false>(m_keySchema, m_scheme);
        }
    }

    TableIndexPicker(const TupleSchema *keySchema, bool intsOnly, bool inlinesOrColumnsOnly,
                     const TableIndexScheme &scheme) :
        m_scheme(scheme),
        m_keySchema(keySchema),
        m_keySize(keySchema->tupleLength()),
        m_intsOnly(intsOnly),
        m_inlinesOrColumnsOnly(inlinesOrColumnsOnly),
        m_type(scheme.type)
    {}

private:
    const TableIndexScheme &m_scheme;
    const TupleSchema *m_keySchema;
    const int m_keySize;
    bool m_intsOnly;
    bool m_inlinesOrColumnsOnly;
    TableIndexType m_type;
};

TableIndex *TableIndexFactory::getInstance(const TableIndexScheme &scheme) {
    const TupleSchema *tupleSchema = scheme.tupleSchema;
    assert(tupleSchema);
    bool isIntsOnly = true;
    bool isInlinesOrColumnsOnly = true;
    std::vector<ValueType> keyColumnTypes;
    std::vector<int32_t> keyColumnLengths;
    std::vector<bool> keyColumnInBytes;
    size_t valueCount = 0;
    size_t exprCount = scheme.indexedExpressions.size();
    if (exprCount != 0) {
        valueCount = exprCount;
        // TODO: This is where we could gain some extra runtime and space efficiency by
        // somehow marking which indexed expressions happen to be non-inlined column expressions.
        // This case is significant because it presents an opportunity for the GenericPersistentKey
        // index keys to avoid a persistent allocation and copy of an already persistent value.
        // This could be implemented as a bool attribute of TupleSchema::ColumnInfo that is only
        // set to true in this special case. It would universally disable deep copying of that
        // particular "tuple column"'s referenced object.
        for (size_t ii = 0; ii < valueCount; ++ii) {
            ValueType exprType = scheme.indexedExpressions[ii]->getValueType();
            if ( ! isIntegralType(exprType)) {
                isIntsOnly = false;
            }
            bool inBytes = false;
            uint32_t declaredLength;
            if (exprType == VALUE_TYPE_VARCHAR || exprType == VALUE_TYPE_VARBINARY) {
                // Setting the column length to TUPLE_SCHEMA_COLUMN_MAX_VALUE_LENGTH constrains the
                // maximum length of expression values that can be indexed with the same limit
                // that gets applied to column values.
                // In theory, indexed expression values could have an independent limit
                // up to any length that can be allocated via ThreadLocalPool.
                // Currently, all of these cases are constrained with the same limit,
                // which is also the default/maximum size for variable columns defined in schema,
                // as controlled in java by VoltType.MAX_VALUE_LENGTH.
                // It's not clear whether scheme.indexedExpressions[ii]->getValueSize()
                // can or should be called for a more useful answer.
                // There's probably little to gain since expressions usually do not contain enough information
                // to reliably determine that the result value is always small enough to "inline".
                declaredLength = TupleSchema::COLUMN_MAX_VALUE_LENGTH;
                isInlinesOrColumnsOnly = false;
                if (exprType == VALUE_TYPE_VARCHAR) {
                    inBytes = true;
                }
            } else {
                declaredLength = NValue::getTupleStorageSize(exprType);
            }
            keyColumnTypes.push_back(exprType);
            keyColumnLengths.push_back(declaredLength);
            // Use MAX VARCHAR IN BYTES
            keyColumnInBytes.push_back(inBytes);
        }
    } else {
        valueCount = scheme.columnIndices.size();
        for (size_t ii = 0; ii < valueCount; ++ii) {
            const TupleSchema::ColumnInfo *columnInfo = tupleSchema->getColumnInfo(scheme.columnIndices[ii]);
            ValueType exprType = columnInfo->getVoltType();
            if ( ! isIntegralType(exprType)) {
                isIntsOnly = false;
            }
            keyColumnTypes.push_back(exprType);
            keyColumnLengths.push_back(columnInfo->length);
            keyColumnInBytes.push_back(columnInfo->inBytes);
        }
    }
    std::vector<bool> keyColumnAllowNull(valueCount, true);
    TupleSchema *keySchema = TupleSchema::createTupleSchema(keyColumnTypes, keyColumnLengths,
            keyColumnAllowNull, keyColumnInBytes);
    assert(keySchema);
    VOLT_TRACE("Creating index for '%s' with key schema '%s'", scheme.name.c_str(), keySchema->debug().c_str());
    TableIndexPicker picker(keySchema, isIntsOnly, isInlinesOrColumnsOnly, scheme);
    TableIndex *retval = picker.getInstance();
    return retval;
}

TableIndex *TableIndexFactory::cloneEmptyTreeIndex(const TableIndex& pkey_index)
{
    return pkey_index.cloneEmptyNonCountingTreeIndex();
}

}
