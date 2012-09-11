/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

#ifndef INDEXKEY_H
#define INDEXKEY_H

#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"

#include "boost/array.hpp"
#include "boost/unordered_map.hpp"

#include <cassert>
#include <iostream>
#include <sstream>

namespace voltdb {

/*
 * Convert from a uint64_t that has had a signed number packed into it to
 * the specified signed type. The max signed value for that type is supplied as
 * the typeMaxValue template parameter.
 */
template<typename signedType, int64_t typeMaxValue>
inline static signedType convertUnsignedValueToSignedValue(uint64_t value) {
    int64_t retval = static_cast<int64_t>(value);
    retval -= typeMaxValue + 1;
    return static_cast<signedType>(retval);
}

/*
 * Specialization of convertUnsignedValueToSignedValue for int64_t. int64_t requires a comparison
 * to prevent overflow.
 */
template<>
inline int64_t convertUnsignedValueToSignedValue< int64_t, INT64_MAX>(uint64_t value) {
    if (value > static_cast<uint64_t>(INT64_MAX) + 1) {
        value -= INT64_MAX;
        value--;
        return static_cast<int64_t>(value);
    } else {
        int64_t retval = static_cast<int64_t>(value);
        retval -= INT64_MAX;
        retval--;
        return retval;
    }
}

/*
 * Convert from a signed value to an unsigned value. The max value for the type is supplied as a template
 * parameter. int64_t is used for all types to prevent overflow.
 */
template<int64_t typeMaxValue, typename signedValueType, typename unsignedValueType>
inline static unsignedValueType convertSignedValueToUnsignedValue(signedValueType value) {

    return static_cast<unsignedValueType>(value + typeMaxValue + 1);
}

/*
 * Specialization for int64_ts necessary to prevent overflow.
 */
template<>
inline uint64_t convertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(int64_t value) {
    uint64_t retval = 0;
    if (value < 0) {
        value += INT64_MAX;
        value++;
        retval = static_cast<uint64_t>(value);
    } else {
        retval = static_cast<uint64_t>(value);
        retval += INT64_MAX;
        retval++;
    }
    return retval;
}

template <std::size_t keySize> struct IntsEqualityChecker;
template <std::size_t keySize> struct IntsComparator;
template <std::size_t keySize> struct IntsHasher;

/**
 *  Integer key that will pack all key data into keySize number of uint64_t.
 *  The minimum number of uint64_ts necessary to pack all the integers is used.
 */
template <std::size_t keySize>
struct IntsKey
{
    typedef IntsEqualityChecker<keySize> KeyEqualityChecker;
    typedef IntsComparator<keySize> KeyComparator;
    typedef IntsHasher<keySize> KeyHasher;

    static inline bool keyDependsOnTupleAddress() { return false; }
    static inline bool keyUsesNonInlinedMemory() { return false; }

    /*
     * Take a value that is part of the key (already converted to a uint64_t) and inserts it into the
     * most significant bytes available in the key. Templated on the size of the type of key being inserted.
     * This allows the compiler to unroll the loop.
     *
     *
     * Algorithm is:
     * Start with the most significant byte of the keyValue we are inserting and put into the most significant byte
     * of the uint64_t portion of the key that is indexed by keyOffset. The most significant available byte within
     * the uint64_t portion of the key is indexed by intraKeyOffset. Both keyOffset and intraKeyOffset are passed
     * by reference so they can be updated.
     *
     *
     * Rinse and repeat with the less significant bytes.
     *
     */
    template <typename keyValueType>
    inline void insertKeyValue(int &keyOffset, int &intraKeyOffset, uint64_t keyValue) {
        for (int ii = static_cast<int>(sizeof(keyValueType)) - 1; ii >= 0; ii--) {

            /*
             * Extract the most significant byte from keyValue by shifting it all the way to the right.
             * Mask off the rest. Then shift it to the left to the most significant byte location available
             * in the key and OR it in.
             */
            data[keyOffset] |= (0xFF & (keyValue >> (ii * 8))) << (intraKeyOffset * 8); //
            intraKeyOffset--;//Move the offset inside the uint64 key back one.
            /*
             * If there are no more bytes available in the uint64_t indexed by keyOffset then increment keyOffset
             * to point to the next uint64_t and set intraKeyOffset to index to the most significant byte
             * in this next uint64_t.
             */
            if (intraKeyOffset < 0) {
                intraKeyOffset = sizeof(uint64_t) - 1;
                keyOffset++;
            }
        }
    }

    /*
     * Inverse of insertKeyValue.
     */
    template <typename keyValueType>
    inline uint64_t extractKeyValue(int &keyOffset, int &intraKeyOffset) const {
        uint64_t retval = 0;
        for (int ii = static_cast<int>(sizeof(keyValueType)) - 1; ii >= 0; ii--) {
            retval |= (0xFF & (data[keyOffset] >> (intraKeyOffset * 8))) << (ii * 8);
            intraKeyOffset--;
            if (intraKeyOffset < 0) {
                intraKeyOffset = sizeof(uint64_t) - 1;
                keyOffset++;
            }
        }
        return retval;
    }

    std::string debug( const voltdb::TupleSchema *keySchema) const {
        std::ostringstream buffer;
        int keyOffset = 0;
        int intraKeyOffset = sizeof(uint64_t) - 1;
        const int columnCount = keySchema->columnCount();
        for (int ii = 0; ii < columnCount; ii++) {
            switch(keySchema->columnType(ii)) {
            case voltdb::VALUE_TYPE_BIGINT: {
                const uint64_t keyValue = extractKeyValue<uint64_t>(keyOffset, intraKeyOffset);
                buffer << convertUnsignedValueToSignedValue< int64_t, INT64_MAX>(keyValue) << ",";
                break;
            }
            case voltdb::VALUE_TYPE_INTEGER: {
                const uint64_t keyValue = extractKeyValue<uint32_t>(keyOffset, intraKeyOffset);
                buffer << convertUnsignedValueToSignedValue< int32_t, INT32_MAX>(keyValue) << ",";
                break;
            }
            case voltdb::VALUE_TYPE_SMALLINT: {
                const uint64_t keyValue = extractKeyValue<uint16_t>(keyOffset, intraKeyOffset);
                buffer << convertUnsignedValueToSignedValue< int16_t, INT16_MAX>(keyValue) << ",";
                break;
            }
            case voltdb::VALUE_TYPE_TINYINT: {
                const uint64_t keyValue = extractKeyValue<uint8_t>(keyOffset, intraKeyOffset);
                buffer << static_cast<int64_t>(convertUnsignedValueToSignedValue< int8_t, INT8_MAX>(keyValue)) << ",";
                break;
            }
            default:
                throwFatalException("We currently only support a specific set of column index types/sizes for IntsKeys [%s]",
                                    getTypeName(keySchema->columnType(ii)).c_str());
                break;
            }
        }
        return std::string(buffer.str());
    }

    IntsKey() {}

    IntsKey(const TableTuple *tuple) {
        ::memset(data, 0, keySize * sizeof(uint64_t));
        assert(tuple);
        const TupleSchema *keySchema = tuple->getSchema();
        const int columnCount = keySchema->columnCount();
        int keyOffset = 0;
        int intraKeyOffset = sizeof(uint64_t) - 1;
        for (int ii = 0; ii < columnCount; ii++) {
            switch(keySchema->columnType(ii)) {
            case voltdb::VALUE_TYPE_BIGINT: {
                const int64_t value = ValuePeeker::peekBigInt(tuple->getNValue(ii));
                const uint64_t keyValue = convertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(value);
                insertKeyValue<uint64_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
            case voltdb::VALUE_TYPE_INTEGER: {
                const int32_t value = ValuePeeker::peekInteger(tuple->getNValue(ii));
                const uint32_t keyValue = convertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(value);
                insertKeyValue<uint32_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
            case voltdb::VALUE_TYPE_SMALLINT: {
                const int16_t value = ValuePeeker::peekSmallInt(tuple->getNValue(ii));
                const uint16_t keyValue = convertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(value);
                insertKeyValue<uint16_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
            case voltdb::VALUE_TYPE_TINYINT: {
                const int8_t value = ValuePeeker::peekTinyInt(tuple->getNValue(ii));
                const uint8_t keyValue = convertSignedValueToUnsignedValue<INT8_MAX, int8_t, uint8_t>(value);
                insertKeyValue<uint8_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
            default:
                throwFatalException("We currently only support a specific set of column index types/sizes for IntsKeys (%s)", getTypeName(keySchema->columnType(ii)).c_str());
                break;
            }
        }
    }

    IntsKey(const TableTuple *tuple, const std::vector<int> &indices, const TupleSchema *keySchema) {
        ::memset(data, 0, keySize * sizeof(uint64_t));
        const int columnCount = keySchema->columnCount();
        int keyOffset = 0;
        int intraKeyOffset = sizeof(uint64_t) - 1;
        for (int ii = 0; ii < columnCount; ii++) {
            switch(keySchema->columnType(ii)) {
            case voltdb::VALUE_TYPE_BIGINT: {
                const int64_t value = ValuePeeker::peekBigInt(tuple->getNValue(indices[ii]));
                const uint64_t keyValue = convertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(value);
                insertKeyValue<uint64_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
            case voltdb::VALUE_TYPE_INTEGER: {
                const int32_t value = ValuePeeker::peekInteger(tuple->getNValue(indices[ii]));
                const uint32_t keyValue = convertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(value);
                insertKeyValue<uint32_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
            case voltdb::VALUE_TYPE_SMALLINT: {
                const int16_t value = ValuePeeker::peekSmallInt(tuple->getNValue(indices[ii]));
                const uint16_t keyValue = convertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(value);
                insertKeyValue<uint16_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
            case voltdb::VALUE_TYPE_TINYINT: {
                const int8_t value = ValuePeeker::peekTinyInt(tuple->getNValue(indices[ii]));
                const uint8_t keyValue = convertSignedValueToUnsignedValue<INT8_MAX, int8_t, uint8_t>(value);
                insertKeyValue<uint8_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
            default:
                throwFatalException("We currently only support a specific set of column index types/sizes for IntsKeys {%s} at column (%d) (%d of %d)",
                                    getTypeName(keySchema->columnType(ii)).c_str(), indices[ii], ii+1, columnCount);
                break;
            }
        }
    }

    // actual location of data
    uint64_t data[keySize];
};

/** comparator for Int specialized indexes.
 * Required by CompactingMap keyed by IntsKey<>
 */
template <std::size_t keySize>
struct IntsComparator
{
    IntsComparator(const TupleSchema *unused_keySchema) {}

    inline int operator()(const IntsKey<keySize> &lhs, const IntsKey<keySize> &rhs) const {
        // lexographical compare could be faster for fixed N
        /*
         * Hopefully the compiler can unroll this loop
         */
        for (unsigned int ii = 0; ii < keySize; ii++) {
            const uint64_t *lvalue = &lhs.data[ii];
            const uint64_t *rvalue = &rhs.data[ii];
            if (*lvalue < *rvalue)  return -1;
            else if (*lvalue > *rvalue) return 1;
        }
        return 0;
    }
};

/**
 * Required by CompactingHashTable keyed by IntsKey<>
 */
template <std::size_t keySize>
struct IntsEqualityChecker
{
    IntsEqualityChecker(const TupleSchema *keySchema) : m_keySchema(keySchema) {}

    inline bool operator()(const IntsKey<keySize> &lhs, const IntsKey<keySize> &rhs) const {
        for (unsigned int ii = 0; ii < keySize; ii++) {
            const uint64_t *lvalue = &lhs.data[ii];
            const uint64_t *rvalue = &rhs.data[ii];

            if (*lvalue != *rvalue) {
                return false;
            }
        }
        return true;
    }
private:
    const TupleSchema *m_keySchema;
};

/**
 * Required by CompactingHashTable keyed by IntsKey<>
 */
template <std::size_t keySize>
struct IntsHasher
{
    IntsHasher(const TupleSchema *unused_keySchema) {}

    inline size_t operator()(IntsKey<keySize> const& p) const
    {
        size_t seed = 0;
        for (int ii = 0; ii < keySize; ii++) {
            boost::hash_combine(seed, p.data[ii]);
        }
        return seed;
    }
};

template <std::size_t keySize> struct GenericEqualityChecker;
template <std::size_t keySize> struct GenericComparator;
template <std::size_t keySize> struct GenericHasher;

/**
 * Key object for indexes of mixed types.
 * Using TableTuple to store columns.
 */
template <std::size_t keySize>
struct GenericKey
{
    typedef GenericEqualityChecker<keySize> KeyEqualityChecker;
    typedef GenericComparator<keySize> KeyComparator;
    typedef GenericHasher<keySize> KeyHasher;

    static inline bool keyDependsOnTupleAddress() { return false; }
    static inline bool keyUsesNonInlinedMemory() { return true; } // maybe

    GenericKey() {}

    GenericKey(const TableTuple *tuple) {
        assert(tuple);
        ::memcpy(data, tuple->m_data + TUPLE_HEADER_SIZE, tuple->getSchema()->tupleLength());
    }

    GenericKey(const TableTuple *tuple, const std::vector<int> &indices, const TupleSchema *keySchema) {
        assert(tuple);
        TableTuple keyTuple(keySchema);
        keyTuple.moveNoHeader(reinterpret_cast<void*>(data));
        for (int i = 0; i < keySchema->columnCount(); i++) {
            keyTuple.setNValue(i, tuple->getNValue(indices[i]));
        }
    }

    // actual location of data, extends past the end.
    char data[keySize];
};

/**
 * Function object returns -1/0/1 if lhs </==/> rhs.
 * Required by CompactingMap keyed by GenericKey<>
 */
template <std::size_t keySize>
struct GenericComparator
{
    /** Type information passed to the constuctor as it's not in the key itself */
    GenericComparator(const TupleSchema *keySchema) : m_keySchema(keySchema) {}

    inline int operator()(const GenericKey<keySize> &lhs, const GenericKey<keySize> &rhs) const {
        TableTuple lhTuple(m_keySchema); lhTuple.moveToReadOnlyTuple(reinterpret_cast<const void*>(&lhs));
        TableTuple rhTuple(m_keySchema); rhTuple.moveToReadOnlyTuple(reinterpret_cast<const void*>(&rhs));
        // lexographical compare could be faster for fixed N
        return lhTuple.compare(rhTuple);
    }
private:
    const TupleSchema *m_keySchema;
};

/**
 * Equality-checking function object
 * Required by CompactingHashTable keyed by GenericKey<>
 */
template <std::size_t keySize>
struct GenericEqualityChecker
{
    /** Type information passed to the constuctor as it's not in the key itself */
    GenericEqualityChecker(const TupleSchema *keySchema) : m_keySchema(keySchema) {}

    inline bool operator()(const GenericKey<keySize> &lhs, const GenericKey<keySize> &rhs) const {
        TableTuple lhTuple(m_keySchema); lhTuple.moveToReadOnlyTuple(reinterpret_cast<const void*>(&lhs));
        TableTuple rhTuple(m_keySchema); rhTuple.moveToReadOnlyTuple(reinterpret_cast<const void*>(&rhs));
        return lhTuple.equalsNoSchemaCheck(rhTuple);
    }
private:
    const TupleSchema *m_keySchema;
};

/**
 * Hash function object for Generic Keys in tuple data format.
 * Required by CompactingHashTable keyed by GenericKey<>
 */
template <std::size_t keySize>
struct GenericHasher
{
    /** Type information passed to the constuctor as it's not in the key itself */
    GenericHasher(const TupleSchema *keySchema) : m_keySchema(keySchema) {}

    /** Generate a 64-bit number for the key value */
    inline size_t operator()(GenericKey<keySize> const &p) const
    {
        TableTuple pTuple(m_keySchema); pTuple.moveToReadOnlyTuple(reinterpret_cast<const void*>(&p));
        return pTuple.hashCode();
    }
private:
    const TupleSchema *m_keySchema;
};


struct TupleKeyEqualityChecker;
struct TupleKeyComparator;

/*
 * TupleKey is the all-purpose fallback key for indexes that can't be
 * better specialized. Each TupleKey wraps a pointer to a *persistent
 * table tuple*. TableIndex knows the column indices from the
 * persistent table that form the index key. TupleKey uses this data
 * to evaluate and compare keys by extracting and comparing
 * the appropriate columns' values.
 *
 * Note that the index code will create keys in the schema of the
 * the index key. While all TupleKeys resident in the index itself
 * will point to persistent tuples, there are ephemeral TupleKey
 * instances that point to tuples in the index key schema.
 *
 * Pros: supports any combination of columns in a key. Each index
 * key is 24 bytes (a pointer to a tuple and a pointer to the column
 * indices (which map index columns to table columns).
 *
 * Cons: requires an indirection to evaluate a key (must follow the
 * the pointer to read the underlying tabletuple). Compares what are
 * probably very wide keys one column at a time by initializing and
 * comparing nvalues.
 */
struct TupleKey
{
    typedef TupleKeyEqualityChecker KeyEqualityChecker;
    typedef TupleKeyComparator KeyComparator;
    // typedef TupleKeyHasher KeyHasher; // Future?

    inline TupleKey() {
        m_columnIndices = NULL;
        m_keyTuple = NULL;
        m_keyTupleSchema = NULL;
    }

    static inline bool keyDependsOnTupleAddress() { return true; }
    static inline bool keyUsesNonInlinedMemory() { return true; } // maybe

    // Set a key from a key-schema tuple.
    TupleKey(const TableTuple *tuple) {
        assert(tuple);
        m_columnIndices = NULL;
        m_keyTuple = tuple->address();
        m_keyTupleSchema = tuple->getSchema();
    }

    // Set a key from a table-schema tuple.
    TupleKey(const TableTuple *tuple, const std::vector<int> &indices, const TupleSchema *unused_keySchema) {
        assert(tuple);
        assert(indices.size() > 0);
        m_columnIndices = &indices;
        m_keyTuple = tuple->address();
        m_keyTupleSchema = tuple->getSchema();
    }

    // Return true if the TupleKey references an ephemeral index key.
    bool isKeySchema() const {
        return m_columnIndices == NULL;
    }

    // Return a table tuple that is valid for comparison
    TableTuple getTupleForComparison() const {
        return TableTuple(m_keyTuple, m_keyTupleSchema);
    }

    // Return the indexColumn'th key-schema column.
    int columnForIndexColumn(int indexColumn) const {
        if (isKeySchema())
            return indexColumn;
        else
            return (*m_columnIndices)[indexColumn];
    }

private:
    // TableIndex owns this array - NULL if an ephemeral key
    const std::vector<int> *m_columnIndices;

    // Pointer a persistent tuple in non-ephemeral case.
    char *m_keyTuple;
    const TupleSchema *m_keyTupleSchema;
};

/**
 * Required by CompactingMap keyed by TupleKey
 */
struct TupleKeyComparator
{
    TupleKeyComparator(const TupleSchema *keySchema) : m_keySchema(keySchema) {}

    // return -1/0/1 if lhs </==/> rhs
    inline int operator()(const TupleKey &lhs, const TupleKey &rhs) const {
        TableTuple lhTuple = lhs.getTupleForComparison();
        TableTuple rhTuple = rhs.getTupleForComparison();
        NValue lhValue, rhValue;

        for (int ii=0; ii < m_keySchema->columnCount(); ++ii) {
            lhValue = lhTuple.getNValue(lhs.columnForIndexColumn(ii));
            rhValue = rhTuple.getNValue(rhs.columnForIndexColumn(ii));

            int comparison = lhValue.compare(rhValue);

            if (comparison == VALUE_COMPARE_LESSTHAN) return -1;
            else if (comparison == VALUE_COMPARE_GREATERTHAN) return 1;
        }
        return 0;
    }
private:
    const TupleSchema *m_keySchema;
};

}
#endif // INDEXKEY_H
