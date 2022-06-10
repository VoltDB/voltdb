/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#ifndef INDEXKEY_H
#define INDEXKEY_H

#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"

#include "expressions/abstractexpression.h"

#include <common/debuglog.h>
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
                intraKeyOffset = static_cast<int>(sizeof(uint64_t) - 1);
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
                intraKeyOffset = static_cast<int>(sizeof(uint64_t) - 1);
                keyOffset++;
            }
        }
        return retval;
    }

    std::string debug( const voltdb::TupleSchema *keySchema) const {
        std::ostringstream buffer;
        int keyOffset = 0;
        int intraKeyOffset = static_cast<int>(sizeof(uint64_t) - 1);
        const int columnCount = keySchema->columnCount();
        for (int ii = 0; ii < columnCount; ii++) {
            switch(keySchema->columnType(ii)) {
                case voltdb::ValueType::tBIGINT: {
                const uint64_t keyValue = extractKeyValue<uint64_t>(keyOffset, intraKeyOffset);
                buffer << convertUnsignedValueToSignedValue< int64_t, INT64_MAX>(keyValue) << ",";
                break;
            }
                case voltdb::ValueType::tINTEGER: {
                const uint64_t keyValue = extractKeyValue<uint32_t>(keyOffset, intraKeyOffset);
                buffer << convertUnsignedValueToSignedValue< int32_t, INT32_MAX>(keyValue) << ",";
                break;
            }
                case voltdb::ValueType::tSMALLINT: {
                const uint64_t keyValue = extractKeyValue<uint16_t>(keyOffset, intraKeyOffset);
                buffer << convertUnsignedValueToSignedValue< int16_t, INT16_MAX>(keyValue) << ",";
                break;
            }
                case voltdb::ValueType::tTINYINT: {
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

    IntsKey() {
        ::memset(data, 0, keySize * sizeof(uint64_t));
    }

    IntsKey(const TableTuple *tuple) {
        ::memset(data, 0, keySize * sizeof(uint64_t));
        vassert(tuple);
        const TupleSchema *keySchema = tuple->getSchema();
        const int columnCount = keySchema->columnCount();
        int keyOffset = 0;
        int intraKeyOffset = static_cast<int>(sizeof(uint64_t) - 1);
        for (int ii = 0; ii < columnCount; ii++) {
            switch(keySchema->columnType(ii)) {
                case voltdb::ValueType::tBIGINT: {
                const int64_t value = ValuePeeker::peekBigInt(tuple->getNValue(ii));
                const uint64_t keyValue = convertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(value);
                insertKeyValue<uint64_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
                case voltdb::ValueType::tINTEGER: {
                const int32_t value = ValuePeeker::peekInteger(tuple->getNValue(ii));
                const uint32_t keyValue = convertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(value);
                insertKeyValue<uint32_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
                case voltdb::ValueType::tSMALLINT: {
                const int16_t value = ValuePeeker::peekSmallInt(tuple->getNValue(ii));
                const uint16_t keyValue = convertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(value);
                insertKeyValue<uint16_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
                case voltdb::ValueType::tTINYINT: {
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

    IntsKey(const TableTuple *tuple,
            const std::vector<int> &indices,
            const std::vector<AbstractExpression*> &indexed_expressions,
            const TupleSchema *keySchema) {
        ::memset(data, 0, keySize * sizeof(uint64_t));
        const int columnCount = keySchema->columnCount();
        int keyOffset = 0;
        int intraKeyOffset = static_cast<int>(sizeof(uint64_t) - 1);
        if (indexed_expressions.size() != 0) {
            for (int ii = 0; ii < columnCount; ii++) {
                AbstractExpression* ae = indexed_expressions[ii];
                switch(ae->getValueType()) {
                    case voltdb::ValueType::tBIGINT: {
                    const int64_t value = ValuePeeker::peekBigInt(ae->eval(tuple, NULL));
                    const uint64_t keyValue = convertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(value);
                    insertKeyValue<uint64_t>( keyOffset, intraKeyOffset, keyValue);
                    break;
                }
                    case voltdb::ValueType::tINTEGER: {
                    const int32_t value = ValuePeeker::peekInteger(ae->eval(tuple, NULL));
                    const uint32_t keyValue = convertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(value);
                    insertKeyValue<uint32_t>( keyOffset, intraKeyOffset, keyValue);
                    break;
                }
                    case voltdb::ValueType::tSMALLINT: {
                    const int16_t value = ValuePeeker::peekSmallInt(ae->eval(tuple, NULL));
                    const uint16_t keyValue = convertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(value);
                    insertKeyValue<uint16_t>( keyOffset, intraKeyOffset, keyValue);
                    break;
                }
                    case voltdb::ValueType::tTINYINT: {
                    const int8_t value = ValuePeeker::peekTinyInt(ae->eval(tuple, NULL));
                    const uint8_t keyValue = convertSignedValueToUnsignedValue<INT8_MAX, int8_t, uint8_t>(value);
                    insertKeyValue<uint8_t>( keyOffset, intraKeyOffset, keyValue);
                    break;
                }
                default:
                    throwFatalException( "We currently only support a specific set of column index types/sizes for IntsKeys {%s}", getTypeName(keySchema->columnType(ii)).c_str());
                    break;
                }
            }
            return;
        }
        for (int ii = 0; ii < columnCount; ii++) {
            switch(keySchema->columnType(ii)) {
                case voltdb::ValueType::tBIGINT: {
                const int64_t value = ValuePeeker::peekBigInt(tuple->getNValue(indices[ii]));
                const uint64_t keyValue = convertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(value);
                insertKeyValue<uint64_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
                case voltdb::ValueType::tINTEGER: {
                const int32_t value = ValuePeeker::peekInteger(tuple->getNValue(indices[ii]));
                const uint32_t keyValue = convertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(value);
                insertKeyValue<uint32_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
                case voltdb::ValueType::tSMALLINT: {
                const int16_t value = ValuePeeker::peekSmallInt(tuple->getNValue(indices[ii]));
                const uint16_t keyValue = convertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(value);
                insertKeyValue<uint16_t>( keyOffset, intraKeyOffset, keyValue);
                break;
            }
                case voltdb::ValueType::tTINYINT: {
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
    IntsComparator(const TupleSchema *unused_keySchema) : m_keySchema(unused_keySchema) {}

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

    // This method is provided to be compatible with ComparatorWithPointer.
    int compareWithoutPointer(const IntsKey<keySize> &lhs, const IntsKey<keySize> &rhs) const {
        return operator()(lhs, rhs);
    }

    // IntsComparator not really has a NullAsMaxComparator, we can pre-process the data and
    // convert the NULLs into maximums.
    const IntsComparator getNullAsMaxComparator() const {
        return *this;
    }

protected:
    const TupleSchema *m_keySchema;
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

    GenericKey() {
        ::memset(data, 0, keySize * sizeof(char));
    }

    GenericKey(const TableTuple *tuple) {
        vassert(tuple);
        ::memcpy(data, tuple->address() + TUPLE_HEADER_SIZE, tuple->getSchema()->tupleLength());
    }

    GenericKey(const TableTuple *tuple, const std::vector<int> &indices,
               const std::vector<AbstractExpression*> &indexed_expressions, const TupleSchema *keySchema) {
        vassert(tuple);
        TableTuple keyTuple(keySchema);
        keyTuple.moveNoHeader(reinterpret_cast<void*>(data));
        const int columnCount = keySchema->columnCount();
        if (indexed_expressions.size() > 0) {
            for (int ii = 0; ii < columnCount; ++ii) {
                AbstractExpression* ae = indexed_expressions[ii];
                keyTuple.setNValue(ii, ae->eval(tuple, NULL));
            }
            return;
        } // else take advantage of columns-only optimization
        for (int ii = 0; ii < columnCount; ++ii) {
            keyTuple.setNValue(ii, tuple->getNValue(indices[ii]));
        }
    }

    // actual location of data, extends past the end.
    char data[keySize];
};

/**
 * Key object for indexes of mixed types that need to persist their own backing storage for general string expressions.
 * Plain vanilla GenericKey is the better (simpler) choice for indexes of:
 *   numeric (inlined) values (columns or expressions)
 *   string-valued COLUMNS (also varbinary or any other non-inline types we may eventually support)
 *
 * It's only the edge case of non-inline-typed non-column expressions that need this class for
 * its ability to persist the indexed expression values using pooled storage.
 */
template <std::size_t keySize>
struct GenericPersistentKey : public GenericKey<keySize>
{
    // These keys Compare and Hash as GenericKeys
    GenericPersistentKey() : m_keySchema(NULL) {}

    GenericPersistentKey(const TableTuple *tuple) : GenericKey<keySize>(tuple)
        , m_keySchema(NULL) // This is just an ephemeral search key -- it doesn't need to persist.
    { }

    GenericPersistentKey(const TableTuple *tuple, const std::vector<int> &notUsedIndices,
                         const std::vector<AbstractExpression*> &indexed_expressions, const TupleSchema *keySchema)
        // Not bothering to delegate to the full-blown GenericKey constructor,
        // since in some ways the special case processing here is simpler.
        : GenericKey<keySize>()
        , m_keySchema(keySchema)
    {
        vassert(tuple);
        // Assume that there are indexed expressions.
        // Columns-only indexes don't use GenericPersistentKey
        vassert(indexed_expressions.size() > 0);
        TableTuple keyTuple(keySchema);
        keyTuple.moveNoHeader(reinterpret_cast<void*>(this->data));
        const int columnCount = keySchema->columnCount();
        for (int ii = 0; ii < columnCount; ++ii) {
            AbstractExpression* ae = indexed_expressions[ii];
            NValue indexedValue;
            try {
                indexedValue = ae->eval(tuple, NULL);
            } catch (const SQLException& ignoredDuringIndexing) {
                // For the sake of keeping non-unique index upkeep as a non-failable operation,
                // all exception-throwing expression evaluations get treated as NULL for index
                // purposes.
                indexedValue = NValue::getNullValue(ae->getValueType());
            }
            // The NULL argument means use the persistent memory pool for the varchar
            // allocation rather than any particular COW context's pool.
            // XXX: Could this ever somehow interact badly with a COW context?
            keyTuple.setNValueAllocateForObjectCopies(ii, indexedValue);
        }
    }

    // Both copy constructor and assignment operator are apparently required by CompactingMap.
    // It is now VERY IMPORTANT to only extract keys OUT of a map by const reference AND avoid
    // using these constructor/assignment functions in scenarios like that
    // -- with a local variable as the lhs and an in-map key as the rhs.
    // That would effectively corrupt the map entry when the local variable goes out of scope
    // and/or prevent the in-map key from properly freeing its referenced objects when it got
    // deleted from the map.
    GenericPersistentKey( const GenericPersistentKey& other )
        : GenericKey<keySize>() // Copying the inherited member explicitly.
        , m_keySchema(other.m_keySchema)
    {
        ::memcpy(this->data, other.data, keySize);
        // Only one key, this, can own the tuple and its objects.
        const_cast<GenericPersistentKey&>(other).m_keySchema = NULL;
    }

    const GenericPersistentKey& operator=( const GenericPersistentKey& other )
    {
        // To avoid leaking data, "*this" and "other" -- typically a temp -- actually SWAP state
        // so that data memory management becomes a simple matter of running the expected
        // destructors.  If overwriting a full-blown actively-in-use persistent key,
        // its previous value (presumably obsolete) will go out of scope with other!
        // This data memory management is only a concern for "full-blown" keys
        // that have m_keySchema -- not a problem for ephemeral search keys that just "borrow" memory.
        const TupleSchema *keptKeySchema = this->m_keySchema;
        this->m_keySchema = other.m_keySchema;

        // Exactly one full-blown key must own each tuple and its objects.
        // So either use other as a lifeboat for the prior value of *this.
        // It's destructor will deal with the old value of "this", probably VERY SOON.
        // Or mark it as ephemeral if that's what *this was.
        GenericPersistentKey& writableOther = const_cast<GenericPersistentKey&>(other);

        if (keptKeySchema) {
            writableOther.m_keySchema = keptKeySchema;
            // Other needs to lifeboat the original value of *this.
            char keptData[keySize];
            ::memcpy(keptData, this->data, keySize);

            ::memcpy(this->data, other.data, keySize);

            ::memcpy(writableOther.data, keptData, keySize);
        } else {
            // *this was ephemeral, so other must become ephemeral.
            // The state of its data on exit does not matter.
            writableOther.m_keySchema = NULL;

            ::memcpy(this->data, other.data, keySize);
        }
        return *this;
    }

    ~GenericPersistentKey()
    {
        if (m_keySchema == NULL) {
            return;
        }
        TableTuple keyTuple(m_keySchema);
        keyTuple.moveNoHeader(reinterpret_cast<void*>(this->data));
#ifdef VOLT_POOL_CHECKING
        if (!m_shutdown) {
#endif
        keyTuple.freeObjectColumns();
#ifdef VOLT_POOL_CHECKING
        }
#endif
    }

#ifdef VOLT_POOL_CHECKING
    void shutdown(bool sd) { m_shutdown = sd;}
#endif

private:
    // The keySchema is only retained for object memory reclaim purposes.
    // If NULL, this was either constructed as an ephemeral search key,
    // or it has been "demoted" in the process of shuffling keys around
    // in the map, passing its memory management responsibilities
    // to another key, so no reclaim is required.
    const TupleSchema *m_keySchema;
#ifdef VOLT_POOL_CHECKING
    bool m_shutdown = false;
#endif
};

template <std::size_t keySize>
struct GenericNullAsMaxComparator
{
    /** Type information passed to the constuctor as it's not in the key itself */
    GenericNullAsMaxComparator(const TupleSchema *keySchema) : m_keySchema(keySchema) {}

    inline int operator()(const GenericKey<keySize> &lhs, const GenericKey<keySize> &rhs) const {
        TableTuple lhTuple(m_keySchema); lhTuple.moveToReadOnlyTuple(reinterpret_cast<const void*>(&lhs));
        TableTuple rhTuple(m_keySchema); rhTuple.moveToReadOnlyTuple(reinterpret_cast<const void*>(&rhs));
        // lexographical compare could be faster for fixed N
        return lhTuple.compareNullAsMax(rhTuple);
    }

    // This method is provided to be compatible with ComparatorWithPointer.
    int compareWithoutPointer(const GenericKey<keySize> &lhs, const GenericKey<keySize> &rhs) const {
        return operator()(lhs, rhs);
    }

private:
    const TupleSchema *m_keySchema;
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

    // This method is provided to be compatible with ComparatorWithPointer.
    int compareWithoutPointer(const GenericKey<keySize> &lhs, const GenericKey<keySize> &rhs) const {
        return operator()(lhs, rhs);
    }

    const GenericNullAsMaxComparator<keySize> getNullAsMaxComparator() const {
        return GenericNullAsMaxComparator<keySize>(m_keySchema);
    }

protected:
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
    // typedef TupleKeyEqualityChecker KeyEqualityChecker; // Required by (future?) support for CompactingHash...
    typedef TupleKeyComparator KeyComparator;
    // typedef TupleKeyHasher KeyHasher; // Required by (future?) support for CompactingHash...

    inline TupleKey() {
        m_columnIndices = NULL;
        m_indexedExprs = NULL;
        m_keyTuple = NULL;
        m_keyTupleSchema = NULL;
    }

    static inline bool keyDependsOnTupleAddress() { return true; }
    static inline bool keyUsesNonInlinedMemory() { return true; } // maybe

    // Set a key from a key-schema tuple.
    TupleKey(const TableTuple *tuple) {
        vassert(tuple);
        m_columnIndices = NULL;
        m_indexedExprs = NULL;
        m_keyTuple = tuple->address();
        m_keyTupleSchema = tuple->getSchema();
    }

    // Set a key from a table-schema tuple.
    TupleKey(const TableTuple *tuple, const std::vector<int> &indices,
             const std::vector<AbstractExpression*> &indexed_expressions, const TupleSchema *unused_keySchema) {
        vassert(tuple);
        vassert(indices.size() > 0);
        m_columnIndices = &indices;
        if (indexed_expressions.size() != 0) {
            m_indexedExprs = &indexed_expressions;
        } else {
            m_indexedExprs = NULL;
        }
        m_keyTuple = tuple->address();
        m_keyTupleSchema = tuple->getSchema();
    }

    // Return a table tuple that is valid for comparison
    TableTuple getTupleForComparison() const {
        return TableTuple(static_cast<char*>(const_cast<void*>(m_keyTuple)), m_keyTupleSchema);
    }

    // Return the indexColumn'th key-schema column.
    NValue indexedValue(const TableTuple &tuple, int indexColumn) const {
        if (m_columnIndices == NULL) {
            // Pass through the values from an ephemeral index key "tuple".
            return tuple.getNValue(indexColumn);
        }
        if (m_indexedExprs == NULL) {
            // Project key column values from a column index's persistent tuple
            return tuple.getNValue((*m_columnIndices)[indexColumn]);
        }
        // Evaluate more complicated key expressions on a persistent tuple.
        return (*m_indexedExprs)[indexColumn]->eval(&tuple, NULL);
    }

protected:
    // TableIndex owns these vectors which are used to extract key values from a persistent tuple
    // - both are NULL for an ephemeral key
    const std::vector<int> *m_columnIndices;
    const std::vector<AbstractExpression*> *m_indexedExprs;

    // Pointer to a persistent tuple in the non-ephemeral case.
    const void *m_keyTuple;
    const TupleSchema *m_keyTupleSchema;
};

struct TupleKeyNullAsMaxComparator
{
    TupleKeyNullAsMaxComparator(const TupleSchema *keySchema) : m_keySchema(keySchema) {}

    // return -1/0/1 if lhs </==/> rhs
    inline int operator()(const TupleKey &lhs, const TupleKey &rhs) const {
        TableTuple lhTuple = lhs.getTupleForComparison();
        TableTuple rhTuple = rhs.getTupleForComparison();
        NValue lhValue, rhValue;

        const int columnCount = m_keySchema->columnCount();
        for (int ii=0; ii < columnCount; ++ii) {
            lhValue = lhs.indexedValue(lhTuple, ii);
            rhValue = rhs.indexedValue(rhTuple, ii);

            int comparison = lhValue.compareNullAsMax(rhValue);

            if (comparison != VALUE_COMPARE_EQUAL) return comparison;
        }
        return VALUE_COMPARE_EQUAL;
    }

    // This method is provided to be compatible with ComparatorWithPointer.
    int compareWithoutPointer(const TupleKey &lhs, const TupleKey &rhs) const {
        return operator()(lhs, rhs);
    }

private:
    const TupleSchema *m_keySchema;
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

        const int columnCount = m_keySchema->columnCount();
        for (int ii=0; ii < columnCount; ++ii) {
            lhValue = lhs.indexedValue(lhTuple, ii);
            rhValue = rhs.indexedValue(rhTuple, ii);

            int comparison = lhValue.compare(rhValue);

            if (comparison != VALUE_COMPARE_EQUAL) return comparison;
        }
        return VALUE_COMPARE_EQUAL;
    }

    // This method is provided to be compatible with ComparatorWithPointer.
    int compareWithoutPointer(const TupleKey &lhs, const TupleKey &rhs) const {
        return operator()(lhs, rhs);
    }

    const TupleKeyNullAsMaxComparator getNullAsMaxComparator() const {
        return TupleKeyNullAsMaxComparator(m_keySchema);
    }

protected:
    const TupleSchema *m_keySchema;
};

static inline int comparePointer(const void *lhs, const void *rhs) {
    const uintptr_t l = reinterpret_cast<const uintptr_t>(lhs);
    const uintptr_t r = reinterpret_cast<const uintptr_t>(rhs);

    if (l == r) return 0;
    else if (l < r) return -1;
    else return 1;
}

template <typename KeyType> struct ComparatorWithPointer;
template <typename KeyType> struct NullAsMaxComparatorWithPointer;

template <typename KeyType>
struct KeyWithPointer : public KeyType {
    typedef ComparatorWithPointer<KeyType> KeyComparator;
    friend struct ComparatorWithPointer<KeyType>;
    friend struct NullAsMaxComparatorWithPointer<KeyType>;

    KeyWithPointer() : KeyType(), m_keyTuple(NULL) {}

    KeyWithPointer(const TableTuple *tuple) : KeyType(tuple), m_keyTuple(NULL) {}

    KeyWithPointer(const TableTuple *tuple, const std::vector<int> &indices,
                   const std::vector<AbstractExpression*> &indexed_expressions,
                   const TupleSchema *keySchema)
        : KeyType(tuple, indices, indexed_expressions, keySchema) {
        m_keyTuple = tuple->address();
    }

    static inline bool keyDependsOnTupleAddress() { return true; }

    const void * const& getValue() const { return m_keyTuple;}
    void setValue(const void * const &value) { m_keyTuple = value; }
    const void *setPointerValue(const void *value) {
        const void *rv = m_keyTuple;
        m_keyTuple = value;
        return rv;
    }

private:
    const void* m_keyTuple;
};

template <>
struct KeyWithPointer<TupleKey> : public TupleKey {
    typedef ComparatorWithPointer<TupleKey> KeyComparator;
    friend struct ComparatorWithPointer<TupleKey>;
    friend struct NullAsMaxComparatorWithPointer<TupleKey>;

    KeyWithPointer() : TupleKey() {}

    KeyWithPointer(const TableTuple *tuple) : TupleKey(tuple) {}

    KeyWithPointer(const TableTuple *tuple, const std::vector<int> &indices,
                   const std::vector<AbstractExpression*> &indexed_expressions,
                   const TupleSchema *unused_keySchema)
        : TupleKey(tuple, indices, indexed_expressions, unused_keySchema) {}

    const void * const& getValue() const { return m_keyTuple; }
    void setValue(const void * const &value) { m_keyTuple = value; }
    const void *setPointerValue(const void * &value) {
        const void *rv = m_keyTuple;
        m_keyTuple = value;
        return rv;
    }
};

template <typename KeyType>
struct NullAsMaxComparatorWithPointer : public KeyType::KeyComparator {
    NullAsMaxComparatorWithPointer(const TupleSchema *keySchema)
            : KeyType::KeyComparator(keySchema) {}

    int operator()(const KeyWithPointer<KeyType> &lhs, const KeyWithPointer<KeyType> &rhs) const {
        int rv = KeyType::KeyComparator::getNullAsMaxComparator()(lhs, rhs);
        return rv == 0 ? comparePointer(lhs.m_keyTuple, rhs.m_keyTuple) : rv;
    }

    // Do a comparison, but don't compare pointers to tuple storage.
    int compareWithoutPointer(const KeyWithPointer<KeyType> &lhs, const KeyWithPointer<KeyType> &rhs) const {
        return KeyType::KeyComparator::getNullAsMaxComparator()(lhs, rhs);
    }
};

template <typename KeyType>
struct ComparatorWithPointer : public KeyType::KeyComparator {
    ComparatorWithPointer(const TupleSchema *keySchema)
        : KeyType::KeyComparator(keySchema) {}

    int operator()(const KeyWithPointer<KeyType> &lhs, const KeyWithPointer<KeyType> &rhs) const {
        int rv = KeyType::KeyComparator::operator()(lhs, rhs);
        return rv == 0 ? comparePointer(lhs.m_keyTuple, rhs.m_keyTuple) : rv;
    }

    // Do a comparison, but don't compare pointers to tuple storage.
    int compareWithoutPointer(const KeyWithPointer<KeyType> &lhs, const KeyWithPointer<KeyType> &rhs) const {
        return KeyType::KeyComparator::operator()(lhs, rhs);
    }

    const NullAsMaxComparatorWithPointer<KeyType> getNullAsMaxComparator() const {
        return NullAsMaxComparatorWithPointer<KeyType>(this->m_keySchema);
    }

};

// overload template
template <typename KeyType>
inline void setPointerValue(KeyWithPointer<KeyType>& k, const void * v) { k.setValue(v); }

// PointerKeyValuePair is the entry type for multimaps that implement
// non-unique indexes, to speed up deletion of entries.  When rows are
// deleted, they are deleted by a pointer to the tuple.  In order to
// find all the rows that need to be deleted quickly, the pointer to
// the tuple is the last component of the key.
template < typename KeyType, typename DataType = const void*>
class PointerKeyValuePair {
public:
    // in order to be consistent with std::pair
    typedef KeyWithPointer<KeyType> first_type;
    typedef DataType second_type;

    // the caller has to make sure the key has already contained the value
    // the signatures are to be consist with the general template
    PointerKeyValuePair() {}
    // TODO: For safety, post-assert that (value == k.getValue()).
    PointerKeyValuePair(const first_type &key, const second_type &value) : k(key) {}

    const first_type& getKey() const { return k; }
    const second_type& getValue() const { return k.getValue(); }
    void setKey(const first_type &key) { k = key; }
    void setValue(const second_type &value) { k.setValue(value); }
    // TODO: Optimize to take advantage of how k/key contain both
    //       the "key proper" AND the value, so k.setValue should be redundant?
    //       For safety, post-assert that (value == k.getValue()).
    void setKeyValuePair(const first_type &key, const second_type &value)
    { k = key; k.setValue(value); }

    // set the tuple pointer to the new value, and return the old value
    const void *setPointerValue(const void *value) {
        return k.setPointerValue(value);
    }

#ifdef VOLT_POOL_CHECKING
        void shutdown(bool sd) { m_shutdown = sd;}
#endif
private:
    first_type k;
#ifdef VOLT_POOL_CHECKING
    bool m_shutdown = false;
#endif
};

}
#endif // INDEXKEY_H
