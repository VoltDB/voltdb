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

#ifndef NVALUE_HPP_
#define NVALUE_HPP_

#include "common/Pool.hpp"
#include "common/SQLException.h"
#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/types.h"
#include "common/value_defs.h"
#include "common/FatalException.hpp"

#include <cassert>
#include <cfloat>
#include <climits>
#include <cmath>
#include <exception>
#include <limits>
#include <stdint.h>
#include <string>

#include "boost/unordered_map.hpp"
#include "ttmath/ttmathint.h"

#define CHECK_FPE( x ) ( std::isinf(x) || std::isnan(x) )
namespace voltdb {


//The int used for storage and return values
typedef ttmath::Int<2> TTInt;
//Long integer with space for multiplication and division without carry/overflow
typedef ttmath::Int<4> TTLInt;

template<typename T>
void throwCastSQLValueOutOfRangeException(
        const T value,
        const ValueType origType,
        const ValueType newType);

template<>
void throwCastSQLValueOutOfRangeException<double>(
        const double value,
        const ValueType origType,
        const ValueType newType);

template<>
void throwCastSQLValueOutOfRangeException<int64_t>(
                                  const int64_t value,
                                  const ValueType origType,
                                  const ValueType newType);

/**
 * A class to wrap all scalar values regardless of type and
 * storage. An NValue is not the representation used in the
 * serialization of VoltTables nor is it the representation of how
 * scalar values are stored in tables. NValue does have serialization
 * and deserialization mechanisms for both those storage formats.
 * NValues are designed to be immutable and for the most part not
 * constructable from raw data types. Access to the raw data is
 * restricted so that all operations have to go through the member
 * functions that can perform the correct casting and error
 * checking. ValueFactory can be used to construct new NValues, but
 * that should be avoided if possible.
 */
class NValue {
    friend class ValuePeeker;
    friend class ValueFactory;

  public:
    /* Create a default NValue */
    NValue();

        // todo: free() should not really be const

    /* Release memory associated to object type NValues */
    void free() const;

    /* Set value to the correct SQL NULL representation. */
    void setNull();

    /* Reveal the contained pointer for type values  */
    void* castAsAddress() const;

    /* Create a boolean true NValue */
    static NValue getTrue();

    /* Create a boolean false NValue */
    static NValue getFalse();

    /* Create an NValue with the minimum value for type */
    static NValue getMinValue(ValueType);

    /* Create an NValue with the maximum value for type */
    static NValue getMaxValue(ValueType);

    /* Create an NValue promoted/demoted to type */
    NValue castAs(ValueType type) const;

        // todo: Why doesn't this return size_t? Also, this is a
        // quality of ValueType, not NValue.

    /* Calculate the tuple storage size for an NValue type. VARCHARs
       assume out-of-band tuple storage */
    static uint16_t getTupleStorageSize(const ValueType type);

       // todo: Could the isInlined argument be removed by have the
       // caller dereference the pointer?

    /* Deserialize a scalar of the specified type from the tuple
       storage area provided. If this is an Object type then the third
       argument indicates whether the object is stored in the tuple
       inline */
    static const NValue deserializeFromTupleStorage(
        const void *storage, const ValueType type, const bool isInlined);

    /* Serialize the scalar this NValue represents to the provided
       storage area. If the scalar is an Object type that is not
       inlined then the provided data pool or the heap will be used to
       allocated storage for a copy of the object. */
    void serializeToTupleStorageAllocateForObjects(
        void *storage, const bool isInlined, const int16_t maxLength,
        Pool *dataPool) const;

    /* Serialize the scalar this NValue represents to the storage area
       provided. If the scalar is an Object type then the object will
       be copy if it can be inlined into the tuple. Otherwise a
       pointer to the object will be copied into the storage area. No
       allocations are performed. */
    void serializeToTupleStorage(
        void *storage, const bool isInlined, const int16_t maxLength) const;

    /* Deserialize a scalar value of the specified type from the
       SerializeInput directly into the tuple storage area
       provided. This function will perform memory allocations for
       Object types as necessary using the provided data pool or the
       heap. This is used to deserialize tables. */
    static void deserializeFrom(
        SerializeInput &input, const ValueType type, char *storage,
        bool isInlined, Pool *dataPool);

        // TODO: no callers use the first form; Should combine these
        // eliminate the potential NValue copy.

    /* Read a ValueType from the SerializeInput stream and deserialize
       a scalar value of the specified type from the provided
       SerializeInput and perform allocations as necessary. */
    static const NValue deserializeFromAllocateForStorage(
        SerializeInput &input, Pool *dataPool);

    /* Serialize this NValue to a SerializeOutput */
    void serializeTo(SerializeOutput &output) const;

    /* Serialize this NValue to an ELT stream */
    size_t serializeToELT(char *) const;

    /* Check if the value represents SQL NULL */
    bool isNull() const;

    /* For boolean NValues, convert to bool */
    bool isTrue() const;
    bool isFalse() const;

    /* For number values, check the number line. */
    bool isNegative() const;
    bool isZero() const;

    /* For boolean NValues only, logical operators */
    NValue op_negate() const;
    NValue op_and(const NValue rhs) const;
    NValue op_or(const NValue rhs) const;

    /* Evaluate the ordering relation against two NValues. Promotes
       exact types to allow disparate type comparison. See also the
       op_ functions which return boolean NValues. */
    int compare(const NValue rhs) const;

    /* Return a boolean NValue with the comparison result */
    NValue op_equals(const NValue rhs) const;
    NValue op_notEquals(const NValue rhs) const;
    NValue op_lessThan(const NValue rhs) const;
    NValue op_lessThanOrEqual(const NValue rhs) const;
    NValue op_greaterThan(const NValue rhs) const;
    NValue op_greaterThanOrEqual(const NValue rhs) const;

    /* Return a copy of MAX(this, rhs) */
    NValue op_max(const NValue rhs) const;

    /* Return a copy of MIN(this, rhs) */
    NValue op_min(const NValue rhs) const;

    /* For number NValues, compute new NValues for arthimetic operators */
    NValue op_increment() const;
    NValue op_decrement() const;
    NValue op_subtract(const NValue rhs) const;
    NValue op_add(const NValue rhs) const;
    NValue op_multiply(const NValue rhs) const;
    NValue op_divide(const NValue rhs) const;

    /* For boost hashing */
    void hashCombine(std::size_t &seed) const;

    /* Functor comparator for use with std::set */
    struct ltNValue {
        bool operator()(const NValue v1, const NValue v2) const {
            return v1.compare(v2) < 0;
        }
    };

    /* Functor equality predicate for use with boost unordered */
    struct equal_to : std::binary_function<NValue, NValue, bool>
    {
        bool operator()(NValue const& x,
            NValue const& y) const
        {
            return x.compare(y) == 0;
        }
    };

    /* Functor hash predicate for use with boost unordered */
    struct hash : std::unary_function<NValue, std::size_t>
    {
        std::size_t operator()(NValue const& x) const
        {
            std::size_t seed = 0;
            x.hashCombine(seed);
            return seed;
        }
    };

    /* Return a string full of arcana and wonder. */
    std::string debug() const;

    // Constants for Decimal type
    // Precision and scale (inherent in the schema)
    static const uint16_t kMaxDecPrec = 38;
    static const uint16_t kMaxDecScale = 12;
    static const int64_t kMaxScaleFactor = 1000000000000;

  private:
    /*
     * Private methods are private for a reason. Don't expose the raw
     * data so that it can be operated on directly.
     */

    // Function declarations for NValue.cpp definitions.
    static std::string getTypeName(ValueType type);
    void createDecimalFromString(const std::string &txt);
    std::string createStringFromDecimal() const;
    NValue opDivideDecimals(const NValue lhs, const NValue rhs) const;
    NValue opMultiplyDecimals(const NValue &lhs, const NValue &rhs) const;

    // Promotion Rules. Initialized in NValue.cpp
    static ValueType s_intPromotionTable[];
    static ValueType s_decimalPromotionTable[];
    static ValueType s_doublePromotionTable[];
    static TTInt s_maxDecimal;
    static TTInt s_minDecimal;

    static ValueType promoteForOp(ValueType vta, ValueType vtb) {
        ValueType rt;
        switch (vta) {
          case VALUE_TYPE_TINYINT:
          case VALUE_TYPE_SMALLINT:
          case VALUE_TYPE_INTEGER:
          case VALUE_TYPE_BIGINT:
          case VALUE_TYPE_TIMESTAMP:
            rt = s_intPromotionTable[vtb];
            break;

          case VALUE_TYPE_DECIMAL:
            rt = s_decimalPromotionTable[vtb];
            break;

          case VALUE_TYPE_DOUBLE:
            rt = s_doublePromotionTable[vtb];
            break;

          // no valid promotion (currently) for these types
          case VALUE_TYPE_ADDRESS:
          case VALUE_TYPE_VARCHAR:
          case VALUE_TYPE_BOOLEAN:
          case VALUE_TYPE_INVALID:
          case VALUE_TYPE_NULL:
          default:
            rt = VALUE_TYPE_INVALID;
            break;
        }
        assert(rt != VALUE_TYPE_INVALID);
        return rt;
    }

    /**
     * 16 bytes of storage for NValue data.
     * The 16th byte stores the ValueType!
     */
    char m_data[17];

    /**
     * Private constructor that initializes storage and the specifies the type of value
     * that will be stored in this instance
     */
    NValue(const ValueType type) {
        ::memset( m_data, 0, 16);
        setValueType(type);
    }

    /**
     * Set the type of the value that will be stored in this instance.
     * The last of the 16 bytes of storage allocated in an NValue
     * is used to store the type
     */
    void setValueType(ValueType type) {
        reinterpret_cast<uint8_t*>(m_data)[16] = static_cast<uint8_t>(type);
    }

    /**
     * Get the type of the value. This information is private
     * to prevent code outside of NValue from branching based on the type of a value.
     */
    ValueType getValueType() const {
        return static_cast<ValueType>(m_data[16]);
    }

    /**
     * An Object is something like a String that has a variable length
     * (thus it is length preceded) and can potentially have indirect
     * storage (will always be indirect when referenced via an NValue)
     *
     * Leverage private access and enforce strict requirements on
     * calling correctness.
     */
    int16_t getObjectLength() const {
        if (isNull()) {
            // Conceptually, I think a NULL object should just have
            // length 0. In practice, this code path is often a defect
            // in code not correctly handling null. May favor a more
            // defensive "return 0" in the future? (rtb)
            throwFatalException("Must not ask  for object length on sql null object.");
        }
        if (getValueType() != VALUE_TYPE_VARCHAR) {
            // probably want getTupleStorageSize() for non-object types.
            // at the moment, only varchars are using getObjectLength().
            throwFatalException("Must not ask for object length for non-object types");
        }
        // now safe to read and return the length preceding value.
        return **(reinterpret_cast<int16_t * const*>(m_data));
    }

    /**
     * Get a pointer to the value of an Object that lies beyond the storage of the length information
     */
    void* getObjectValue() const {
        if (*reinterpret_cast<void* const*>(m_data) == NULL) {
            return NULL;
        } else if(**reinterpret_cast<int16_t* const*>(m_data) == OBJECTLENGTH_NULL) {
            return NULL;
        } else {
            void *value = *reinterpret_cast<char* const*>(m_data) + sizeof(int16_t);
            return value;
        }
    }

    // getters
    const int8_t& getTinyInt() const {
        assert(getValueType() == VALUE_TYPE_TINYINT);
        return *reinterpret_cast<const int8_t*>(m_data);
    }

    int8_t& getTinyInt() {
        assert(getValueType() == VALUE_TYPE_TINYINT);
        return *reinterpret_cast<int8_t*>(m_data);
    }

    const int16_t& getSmallInt() const {
        assert(getValueType() == VALUE_TYPE_SMALLINT);
        return *reinterpret_cast<const int16_t*>(m_data);
    }

    int16_t& getSmallInt() {
        assert(getValueType() == VALUE_TYPE_SMALLINT);
        return *reinterpret_cast<int16_t*>(m_data);
    }

    const int32_t& getInteger() const {
        assert(getValueType() == VALUE_TYPE_INTEGER);
        return *reinterpret_cast<const int32_t*>(m_data);
    }

    int32_t& getInteger() {
        assert(getValueType() == VALUE_TYPE_INTEGER);
        return *reinterpret_cast<int32_t*>(m_data);
    }

    const int64_t& getBigInt() const {
        assert((getValueType() == VALUE_TYPE_BIGINT) ||
               (getValueType() == VALUE_TYPE_TIMESTAMP) ||
               (getValueType() == VALUE_TYPE_ADDRESS));
        return *reinterpret_cast<const int64_t*>(m_data);
    }

    int64_t& getBigInt() {
        assert((getValueType() == VALUE_TYPE_BIGINT) ||
               (getValueType() == VALUE_TYPE_TIMESTAMP) ||
               (getValueType() == VALUE_TYPE_ADDRESS));
        return *reinterpret_cast<int64_t*>(m_data);
    }

    const int64_t& getTimestamp() const {
        assert(getValueType() == VALUE_TYPE_TIMESTAMP);
        return *reinterpret_cast<const int64_t*>(m_data);
    }

    int64_t& getTimestamp() {
        assert(getValueType() == VALUE_TYPE_TIMESTAMP);
        return *reinterpret_cast<int64_t*>(m_data);
    }

    const double& getDouble() const {
        assert(getValueType() == VALUE_TYPE_DOUBLE);
        return *reinterpret_cast<const double*>(m_data);
    }

    double& getDouble() {
        assert(getValueType() == VALUE_TYPE_DOUBLE);
        return *reinterpret_cast<double*>(m_data);
    }

    const bool& getBoolean() const {
        assert(getValueType() == VALUE_TYPE_BOOLEAN);
        return *reinterpret_cast<const bool*>(m_data);
    }

    TTInt& getDecimal() {
        assert(getValueType() == VALUE_TYPE_DECIMAL);
        void* retval = reinterpret_cast<void*>(m_data);
        return *reinterpret_cast<TTInt*>(retval);
    }

    const TTInt& getDecimal() const {
        assert(getValueType() == VALUE_TYPE_DECIMAL);
        const void* retval = reinterpret_cast<const void*>(m_data);
        return *reinterpret_cast<const TTInt*>(retval);
    }

    bool& getBoolean() {
        assert(getValueType() == VALUE_TYPE_BOOLEAN);
        return *reinterpret_cast<bool*>(m_data);
    }

    static void throwCastSQLException(const ValueType origType,
                                      const ValueType newType)
    {
        char msg[1024];
        sprintf(msg, "Type %s can't be cast as %s",
                valueToString(origType).c_str(),
                valueToString(newType).c_str());
        throw SQLException(SQLException::
                           volt_unsupported_type_conversion_error,
                           msg);
    }

    int64_t castAsBigIntAndGetValue() const {
        const ValueType type = getValueType();
        if (isNull()) {
            return INT64_NULL;
        }

        switch (type) {
        case VALUE_TYPE_NULL:
            return INT64_NULL;
        case VALUE_TYPE_TINYINT:
            return static_cast<int64_t>(getTinyInt());
        case VALUE_TYPE_SMALLINT:
            return static_cast<int64_t>(getSmallInt());
        case VALUE_TYPE_INTEGER:
            return static_cast<int64_t>(getInteger());
        case VALUE_TYPE_ADDRESS:
            return getBigInt();
        case VALUE_TYPE_BIGINT:
            return getBigInt();
        case VALUE_TYPE_TIMESTAMP:
            return getTimestamp();
        case VALUE_TYPE_DOUBLE:
            if (getDouble() > (double)INT64_MAX || getDouble() < (double)VOLT_INT64_MIN) {
                throwCastSQLValueOutOfRangeException<double>(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
            }
            return static_cast<int64_t>(getDouble());
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_DECIMAL:
        default:
            throwCastSQLException(type, VALUE_TYPE_BIGINT);
            return 0; // NOT REACHED
        }
    }

    double castAsDoubleAndGetValue() const {
        const ValueType type = getValueType();
        if (isNull()) {
            return DOUBLE_MIN;
        }

        switch (type) {
          case VALUE_TYPE_NULL:
              return DOUBLE_MIN;
          case VALUE_TYPE_TINYINT:
            return static_cast<double>(getTinyInt());
          case VALUE_TYPE_SMALLINT:
            return static_cast<double>(getSmallInt());
          case VALUE_TYPE_INTEGER:
            return static_cast<double>(getInteger());
          case VALUE_TYPE_ADDRESS:
            return static_cast<double>(getBigInt());
          case VALUE_TYPE_BIGINT:
            return static_cast<double>(getBigInt());
          case VALUE_TYPE_TIMESTAMP:
            return static_cast<double>(getTimestamp());
          case VALUE_TYPE_DOUBLE:
            return getDouble();
          case VALUE_TYPE_VARCHAR:
          case VALUE_TYPE_DECIMAL:
          default:
            throwCastSQLException(type, VALUE_TYPE_DOUBLE);
            return 0; // NOT REACHED
        }
    }

    TTInt castAsDecimalAndGetValue() const {
        const ValueType type = getValueType();
        if (isNull()) {
            TTInt retval;
            retval.SetMin();
            return retval;
        }

        switch (type) {
          case VALUE_TYPE_TINYINT:
          case VALUE_TYPE_SMALLINT:
          case VALUE_TYPE_INTEGER:
          case VALUE_TYPE_BIGINT:
          case VALUE_TYPE_TIMESTAMP: {
            int64_t value = castAsBigIntAndGetValue();
            TTInt retval(value);
            retval *= NValue::kMaxScaleFactor;
            return retval;
          }
          case VALUE_TYPE_DECIMAL:
              return getDecimal();
          case VALUE_TYPE_VARCHAR:
          default:
            throwCastSQLException(type, VALUE_TYPE_DOUBLE);
            return 0; // NOT REACHED
        }
    }

    NValue castAsBigInt() const {
        NValue retval(VALUE_TYPE_BIGINT);
        const ValueType type = getValueType();
        if (isNull()) {
            retval.setNull();
            return retval;
        }
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getBigInt() = static_cast<int64_t>(getTinyInt()); break;
        case VALUE_TYPE_SMALLINT:
            retval.getBigInt() = static_cast<int64_t>(getSmallInt()); break;
        case VALUE_TYPE_INTEGER:
            retval.getBigInt() = static_cast<int64_t>(getInteger()); break;
        case VALUE_TYPE_ADDRESS:
            retval.getBigInt() = getBigInt(); break;
        case VALUE_TYPE_BIGINT:
            return *this;
        case VALUE_TYPE_TIMESTAMP:
            retval.getBigInt() = getTimestamp(); break;
        case VALUE_TYPE_DOUBLE:
            if (getDouble() > (double)INT64_MAX || getDouble() < (double)VOLT_INT64_MIN) {
                throwCastSQLValueOutOfRangeException<double>(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
            }
            retval.getBigInt() = static_cast<int64_t>(getDouble()); break;
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_DECIMAL:
        default:
            throwCastSQLException(type, VALUE_TYPE_BIGINT);
        }
        return retval;
    }

    NValue castAsTimestamp() const {
        NValue retval(VALUE_TYPE_TIMESTAMP);
        const ValueType type = getValueType();
        if (isNull()) {
            retval.setNull();
            return retval;
        }
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getTimestamp() = static_cast<int64_t>(getTinyInt()); break;
        case VALUE_TYPE_SMALLINT:
            retval.getTimestamp() = static_cast<int64_t>(getSmallInt()); break;
        case VALUE_TYPE_INTEGER:
            retval.getTimestamp() = static_cast<int64_t>(getInteger()); break;
        case VALUE_TYPE_BIGINT:
            retval.getTimestamp() = getBigInt(); break;
        case VALUE_TYPE_TIMESTAMP:
            retval.getTimestamp() = getTimestamp(); break;
        case VALUE_TYPE_DOUBLE:
            if (getDouble() > (double)INT64_MAX || getDouble() < (double)VOLT_INT64_MIN) {
                throwCastSQLValueOutOfRangeException<double>(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
            }
            retval.getTimestamp() = static_cast<int64_t>(getDouble()); break;
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_DECIMAL:
        default:
            throwCastSQLException(type, VALUE_TYPE_TIMESTAMP);
        }
        return retval;
    }

    NValue castAsInteger() const {
        NValue retval(VALUE_TYPE_INTEGER);
        const ValueType type = getValueType();
        if (isNull()) {
            retval.setNull();
            return retval;
        }
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getInteger() = static_cast<int32_t>(getTinyInt()); break;
        case VALUE_TYPE_SMALLINT:
            retval.getInteger() = static_cast<int32_t>(getSmallInt()); break;
        case VALUE_TYPE_INTEGER:
            return *this;
        case VALUE_TYPE_BIGINT:
            if (getBigInt() > INT32_MAX || getBigInt() < VOLT_INT32_MIN) {
                throwCastSQLValueOutOfRangeException(getBigInt(), VALUE_TYPE_BIGINT, VALUE_TYPE_INTEGER);
            }
            retval.getInteger() = static_cast<int32_t>(getBigInt()); break;
        case VALUE_TYPE_TIMESTAMP:
            if (getTimestamp() > INT32_MAX || getTimestamp() < VOLT_INT32_MIN) {
                throwCastSQLValueOutOfRangeException(getTimestamp(), VALUE_TYPE_TIMESTAMP, VALUE_TYPE_INTEGER);
            }
            retval.getInteger() = static_cast<int32_t>(getTimestamp()); break;
        case VALUE_TYPE_DOUBLE:
            if (getDouble() > (double)INT32_MAX || getDouble() < (double)VOLT_INT32_MIN) {
                throwCastSQLValueOutOfRangeException(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_INTEGER);
            }
            retval.getInteger() = static_cast<int32_t>(getDouble()); break;
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_DECIMAL:
        default:
            throwCastSQLException(type, VALUE_TYPE_INTEGER);
        }
        return retval;
    }

    NValue castAsSmallInt() const {
        NValue retval(VALUE_TYPE_SMALLINT);
        const ValueType type = getValueType();
        if (isNull()) {
            retval.setNull();
            return retval;
        }
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getSmallInt() = static_cast<int16_t>(getTinyInt()); break;
        case VALUE_TYPE_SMALLINT:
            retval.getSmallInt() = getSmallInt(); break;
        case VALUE_TYPE_INTEGER:
            if (getInteger() > INT16_MAX || getInteger() < VOLT_INT16_MIN) {
                throwCastSQLValueOutOfRangeException((int64_t)getInteger(), VALUE_TYPE_INTEGER, VALUE_TYPE_SMALLINT);
            }
            retval.getSmallInt() = static_cast<int16_t>(getInteger()); break;
        case VALUE_TYPE_BIGINT:
            if (getBigInt() > INT16_MAX || getBigInt() < VOLT_INT16_MIN) {
                throwCastSQLValueOutOfRangeException(getBigInt(), VALUE_TYPE_BIGINT, VALUE_TYPE_SMALLINT);
            }
            retval.getSmallInt() = static_cast<int16_t>(getBigInt()); break;
        case VALUE_TYPE_TIMESTAMP:
            if (getTimestamp() > INT16_MAX || getTimestamp() < VOLT_INT16_MIN) {
                throwCastSQLValueOutOfRangeException(getTimestamp(), VALUE_TYPE_BIGINT, VALUE_TYPE_SMALLINT);
            }
            retval.getSmallInt() = static_cast<int16_t>(getTimestamp()); break;
        case VALUE_TYPE_DOUBLE:
            if (getDouble() > (double)INT16_MAX || getDouble() < (double)VOLT_INT16_MIN) {
                throwCastSQLValueOutOfRangeException(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_SMALLINT);
            }
            retval.getSmallInt() = static_cast<int16_t>(getDouble()); break;
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_DECIMAL:
        default:
            throwCastSQLException(type, VALUE_TYPE_SMALLINT);
        }
        return retval;
    }

    NValue castAsTinyInt() const {
        NValue retval(VALUE_TYPE_TINYINT);
        const ValueType type = getValueType();
        if (isNull()) {
            retval.setNull();
            return retval;
        }
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getTinyInt() = getTinyInt(); break;
        case VALUE_TYPE_SMALLINT:
            if (getSmallInt() > INT8_MAX || getSmallInt() < VOLT_INT8_MIN) {
                throwCastSQLValueOutOfRangeException((int64_t)getSmallInt(), VALUE_TYPE_SMALLINT, VALUE_TYPE_TINYINT);
            }
            retval.getTinyInt() = static_cast<int8_t>(getSmallInt()); break;
        case VALUE_TYPE_INTEGER:
            if (getInteger() > INT8_MAX || getInteger() < VOLT_INT8_MIN) {
                throwCastSQLValueOutOfRangeException((int64_t)getInteger(), VALUE_TYPE_INTEGER, VALUE_TYPE_TINYINT);
            }
            retval.getTinyInt() = static_cast<int8_t>(getInteger()); break;
        case VALUE_TYPE_BIGINT:
            if (getBigInt() > INT8_MAX || getBigInt() < VOLT_INT8_MIN) {
                throwCastSQLValueOutOfRangeException(getBigInt(), VALUE_TYPE_BIGINT, VALUE_TYPE_TINYINT);
            }
            retval.getTinyInt() = static_cast<int8_t>(getBigInt()); break;
        case VALUE_TYPE_TIMESTAMP:
            if (getTimestamp() > INT8_MAX || getTimestamp() < VOLT_INT8_MIN) {
                throwCastSQLValueOutOfRangeException(getTimestamp(), VALUE_TYPE_TIMESTAMP, VALUE_TYPE_TINYINT);
            }
            retval.getTinyInt() = static_cast<int8_t>(getTimestamp()); break;
        case VALUE_TYPE_DOUBLE:
            if (getDouble() > (double)INT8_MAX || getDouble() < (double)VOLT_INT8_MIN) {
                throwCastSQLValueOutOfRangeException(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_TINYINT);
            }
            retval.getTinyInt() = static_cast<int8_t>(getDouble()); break;
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_DECIMAL:
        default:
            throwCastSQLException(type, VALUE_TYPE_TINYINT);
        }
        return retval;
    }

    NValue castAsDouble() const {
        NValue retval(VALUE_TYPE_DOUBLE);
        const ValueType type = getValueType();
        if (isNull()) {
            retval.setNull();
            return retval;
        }
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getDouble() = static_cast<double>(getTinyInt()); break;
        case VALUE_TYPE_SMALLINT:
            retval.getDouble() = static_cast<double>(getSmallInt()); break;
        case VALUE_TYPE_INTEGER:
            retval.getDouble() = static_cast<double>(getInteger()); break;
        case VALUE_TYPE_BIGINT:
            retval.getDouble() = static_cast<double>(getBigInt()); break;
        case VALUE_TYPE_TIMESTAMP:
            retval.getDouble() = static_cast<double>(getTimestamp()); break;
        case VALUE_TYPE_DOUBLE:
            retval.getDouble() = getDouble(); break;
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_DECIMAL:
        default:
            throwCastSQLException(type, VALUE_TYPE_DOUBLE);
        }
        return retval;
    }

    NValue castAsString() const {
        NValue retval(VALUE_TYPE_VARCHAR);
        const ValueType type = getValueType();
        if (isNull()) {
            retval.setNull();
            return retval;
        }
        switch (type) {
        case VALUE_TYPE_VARCHAR:
            *reinterpret_cast<char**>(retval.m_data) = *reinterpret_cast<char* const*>(m_data);
            break;
        default:
            throwCastSQLException(type, VALUE_TYPE_VARCHAR);
        }
        return retval;
    }

    NValue castAsDecimal() const {
        NValue retval(VALUE_TYPE_DECIMAL);
        const ValueType type = getValueType();
        if (isNull()) {
            retval.setNull();
            return retval;
        }
        switch (type) {
          case VALUE_TYPE_TINYINT:
          case VALUE_TYPE_SMALLINT:
          case VALUE_TYPE_INTEGER:
          case VALUE_TYPE_BIGINT:
          {
              int64_t rhsint = castAsBigIntAndGetValue();
              TTInt retval(rhsint);
              retval *= NValue::kMaxScaleFactor;
              return getDecimalValue(retval);
         }
        case VALUE_TYPE_DECIMAL:
            ::memcpy(retval.m_data, m_data, sizeof(TTInt));
            break;
        default:
            throwCastSQLException(type, VALUE_TYPE_DECIMAL);
        }
        return retval;
    }

    /**
     * Copy the arbitrary size object that this value points to as an
     * inline object in the provided storage area
     */
    void inlineCopyObject(void *storage, int16_t maxLength) const {
        if (isNull()) {
            *reinterpret_cast<int16_t*>(storage) = OBJECTLENGTH_NULL;
        }
        else {
            const int16_t objectLength = getObjectLength();
            if (objectLength > maxLength) {
                char msg[1024];
                sprintf(msg, "Object exceeds specified size. Size is %hd and max is %hd", objectLength, maxLength);
                throw SQLException(SQLException::data_exception_string_data_length_mismatch,
                                   msg);
            }
            ::memcpy( storage, *reinterpret_cast<char *const *>(m_data), sizeof(int16_t) + objectLength);
            reinterpret_cast<char *>(storage)[sizeof(int16_t) + objectLength] = '\0';
        }

    }

    int compareAnyIntegerValue (const NValue rhs) const {
        int64_t lhsValue, rhsValue;

        // get the right hand side as a bigint
        if (rhs.getValueType() != VALUE_TYPE_BIGINT) rhsValue = rhs.castAsBigIntAndGetValue();
        else rhsValue = rhs.getBigInt();

        // convert the left hand side
        switch(getValueType()) {
        case VALUE_TYPE_TINYINT:
        case VALUE_TYPE_SMALLINT:
        case VALUE_TYPE_INTEGER:
        case VALUE_TYPE_TIMESTAMP:
            lhsValue = castAsBigIntAndGetValue(); break;
        case VALUE_TYPE_BIGINT:
            lhsValue = getBigInt(); break;
        default: {
            throwFatalException("non comparable types lhs '%d' rhs '%d'", getValueType(), rhs.getValueType());
        }
        }

        // do the comparison
        if (lhsValue == rhsValue) {
            return VALUE_COMPARE_EQUAL;
        } else if (lhsValue > rhsValue) {
            return VALUE_COMPARE_GREATERTHAN;
        } else {
            return VALUE_COMPARE_LESSTHAN;
        }
    }

    int compareDoubleValue (const NValue rhs) const {
        switch (rhs.getValueType()) {
          case VALUE_TYPE_DOUBLE: {
              const double lhsValue = getDouble();
              const double rhsValue = rhs.getDouble();
              if (lhsValue == rhsValue) {
                  return VALUE_COMPARE_EQUAL;
              } else if (lhsValue > rhsValue) {
                  return VALUE_COMPARE_GREATERTHAN;
              } else {
                  return VALUE_COMPARE_LESSTHAN;
              }
          }
          case VALUE_TYPE_TINYINT:
          case VALUE_TYPE_SMALLINT:
          case VALUE_TYPE_INTEGER:
          case VALUE_TYPE_BIGINT:
          case VALUE_TYPE_TIMESTAMP: {
              const double lhsValue = getDouble();
              const double rhsValue = rhs.castAsDouble().getDouble();
              if (lhsValue == rhsValue) {
                  return VALUE_COMPARE_EQUAL;
              } else if (lhsValue > rhsValue) {
                  return VALUE_COMPARE_GREATERTHAN;
              } else {
                  return VALUE_COMPARE_LESSTHAN;
              }
          }
          case VALUE_TYPE_DECIMAL:
          {
              double val = rhs.castAsDoubleAndGetValue();
              if (rhs.isNegative()) {
                  val *= -1;
              }
              return ((getDouble() > val) - (getDouble() < val));
          }

          default:
              throwFatalException("non comparable types lhs '%d' rhs '%d'", getValueType(), rhs.getValueType());
        }
    }

    int compareStringValue (const NValue rhs) const {
        if (rhs.getValueType() != VALUE_TYPE_VARCHAR) {
            throwFatalException( "non comparable types lhs '%d' rhs '%d'", getValueType(), rhs.getValueType());
        }
        const char* left = *reinterpret_cast<char* const*>(m_data);
        const char* right = *reinterpret_cast<char* const*>(rhs.m_data);
        if (isNull()) {
            if (rhs.isNull()) {
                return VALUE_COMPARE_EQUAL;
            } else {
                return VALUE_COMPARE_LESSTHAN;
            }
        } else if (rhs.isNull()) {
            return VALUE_COMPARE_GREATERTHAN;
        }
        const int result = ::strcmp(left + sizeof(int16_t), right + sizeof(int16_t));
        return result;
    }

    int compareDecimalValue(const NValue rhs) const {
        switch (rhs.getValueType()) {
          // create the equivalent decimal value
          case VALUE_TYPE_TINYINT:
          case VALUE_TYPE_SMALLINT:
          case VALUE_TYPE_INTEGER:
          case VALUE_TYPE_BIGINT:
          {
              const TTInt lhsValue = getDecimal();
              const TTInt rhsValue = rhs.castAsDecimalAndGetValue();

              if (lhsValue == rhsValue) {
                  return VALUE_COMPARE_EQUAL;
              } else if (lhsValue > rhsValue) {
                  return VALUE_COMPARE_GREATERTHAN;
              } else {
                  return VALUE_COMPARE_LESSTHAN;
              }
          }
          case VALUE_TYPE_DECIMAL:
          {
              const TTInt lhsValue = getDecimal();
              const TTInt rhsValue = rhs.getDecimal();

              if (lhsValue == rhsValue) {
                  return VALUE_COMPARE_EQUAL;
              } else if (lhsValue > rhsValue) {
                  return VALUE_COMPARE_GREATERTHAN;
              } else {
                  return VALUE_COMPARE_LESSTHAN;
              }
          }
          case VALUE_TYPE_DOUBLE:
          {
              const double lhsValue = castAsDoubleAndGetValue();
              const double rhsValue = rhs.getDouble();

              if (lhsValue == rhsValue) {
                  return VALUE_COMPARE_EQUAL;
              } else if (lhsValue > rhsValue) {
                  return VALUE_COMPARE_GREATERTHAN;
              } else {
                  return VALUE_COMPARE_LESSTHAN;
              }
          }
          default:
          {
              throwFatalException( "non comparable types lhs '%d' rhs '%d'", getValueType(), rhs.getValueType());
          }
        }
    }

    NValue opAddBigInts(const int64_t lhs, const int64_t rhs) const {
        if (lhs == INT64_NULL || rhs == INT64_NULL)
            return getBigIntValue(INT64_NULL);
        //Scary overflow check from https://www.securecoding.cert.org/confluence/display/cplusplus/INT32-CPP.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow
        if ( ((lhs^rhs)
                | (((lhs^(~(lhs^rhs)
                  & (1L << (sizeof(int64_t)*CHAR_BIT-1))))+rhs)^rhs)) >= 0) {
            char message[4096];
            sprintf( message, "Adding %jd and %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
            throw SQLException( SQLException::data_exception_numeric_value_out_of_range, message);
        }
        return getBigIntValue(lhs + rhs);
    }

    NValue opSubtractBigInts(const int64_t lhs, const int64_t rhs) const {
        if (lhs == INT64_NULL || rhs == INT64_NULL)
            return getBigIntValue(INT64_NULL);
        //Scary overflow check from https://www.securecoding.cert.org/confluence/display/cplusplus/INT32-CPP.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow
        if ( ((lhs^rhs)
                & (((lhs ^ ((lhs^rhs)
                  & (1L << (sizeof(int64_t)*CHAR_BIT-1))))-rhs)^rhs)) < 0) {
            char message[4096];
            sprintf( message, "Subtracting %jd from %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
            throw SQLException( SQLException::data_exception_numeric_value_out_of_range, message);
        }
        return getBigIntValue(lhs - rhs);
    }

    NValue opMultiplyBigInts(const int64_t lhs, const int64_t rhs) const {
        if (lhs == INT64_NULL || rhs == INT64_NULL)
            return getBigIntValue(INT64_NULL);
        bool overflow = false;
        //Scary overflow check from https://www.securecoding.cert.org/confluence/display/cplusplus/INT32-CPP.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow
        if (lhs > 0){  /* lhs is positive */
            if (rhs > 0) {  /* lhs and rhs are positive */
                if (lhs > (INT64_MAX / rhs)) {
                    overflow= true;
                }
            } /* end if lhs and rhs are positive */
            else { /* lhs positive, rhs non-positive */
                if (rhs < (INT64_MIN / lhs)) {
                    overflow = true;
                }
            } /* lhs positive, rhs non-positive */
        } /* end if lhs is positive */
        else { /* lhs is non-positive */
            if (rhs > 0) { /* lhs is non-positive, rhs is positive */
                if (lhs < (INT64_MIN / rhs)) {
                    overflow = true;
                }
            } /* end if lhs is non-positive, rhs is positive */
            else { /* lhs and rhs are non-positive */
                if ( (lhs != 0) && (rhs < (INT64_MAX / lhs))) {
                    overflow = true;
                }
            } /* end if lhs and rhs non-positive */
        } /* end if lhs is non-positive */

        const int64_t result = lhs * rhs;

        if (result == INT64_NULL) {
            overflow = true;
        }

        if (overflow) {
            char message[4096];
            sprintf( message, "Multiplying %jd with %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
            throw SQLException( SQLException::data_exception_numeric_value_out_of_range, message);
        }

        return getBigIntValue(result);
    }

    NValue opDivideBigInts(const int64_t lhs, const int64_t rhs) const {
        if (lhs == INT64_NULL || rhs == INT64_NULL)
            return getBigIntValue(INT64_NULL);

        if (rhs == 0) {
            char message[4096];
            sprintf( message, "Attempted to divide %jd by 0", (intmax_t)lhs);
            throw SQLException(SQLException::data_exception_division_by_zero,
                               message);
        }

        /**
         * Because the smallest int64 value is used to represent null (and this is checked for an handled above)
         * it isn't necessary to check for any kind of overflow since none is possible.
         */
        return getBigIntValue(int64_t(lhs / rhs));
    }

    NValue opAddDoubles(const double lhs, const double rhs) const {
        if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
            return getDoubleValue(DOUBLE_MIN);

        const double result = lhs + rhs;

        if (CHECK_FPE(result)) {
            char message[4096];
            sprintf( message, "Attempted to add %f with %f caused overflow/underflow or some other error. Result was %f",
                    lhs, rhs, result);
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                               message);
        }
        return getDoubleValue(result);
    }

    NValue opSubtractDoubles(const double lhs, const double rhs) const {
        if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
            return getDoubleValue(DOUBLE_MIN);

        const double result = lhs - rhs;

        if (CHECK_FPE(result)) {
            char message[4096];
            sprintf( message, "Attempted to subtract %f by %f caused overflow/underflow or some other error. Result was %f",
                    lhs, rhs, result);
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                               message);
        }
        return getDoubleValue(result);
    }

    NValue opMultiplyDoubles(const double lhs, const double rhs) const {
        if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
            return getDoubleValue(DOUBLE_MIN);

        const double result = lhs * rhs;

        if (CHECK_FPE(result)) {
            char message[4096];
            sprintf( message, "Attempted to multiply %f by %f caused overflow/underflow or some other error. Result was %f",
                    lhs, rhs, result);
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                               message);
        }
        return getDoubleValue(result);
    }

    NValue opDivideDoubles(const double lhs, const double rhs) const {
        if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
            return getDoubleValue(DOUBLE_MIN);


        const double result = lhs / rhs;

        if (CHECK_FPE(result)) {
            char message[4096];
            sprintf( message, "Attempted to divide %f by %f caused overflow/underflow or some other error. Result was %f",
                    lhs, rhs, result);
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                               message);
        }
        return getDoubleValue(result);
    }

    NValue opAddDecimals(const NValue lhs, const NValue rhs) const {
        if ((lhs.getValueType() != VALUE_TYPE_DECIMAL) ||
            (rhs.getValueType() != VALUE_TYPE_DECIMAL))
        {
            throwFatalException("Non-decimal NValue in decimal adder.");
        }

        if (lhs.isNull() || rhs.isNull()) {
            TTInt retval;
            retval.SetMin();
            return getDecimalValue(retval);
        }

        TTInt retval(lhs.getDecimal());
        if (retval.Add(rhs.getDecimal()) || retval > NValue::s_maxDecimal || retval < s_minDecimal) {
            char message[4096];
            sprintf( message, "Attempted to add %s with %s causing overflow/underflow",
                    lhs.createStringFromDecimal().c_str(), rhs.createStringFromDecimal().c_str());
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                               message);
        }

        return getDecimalValue(retval);
    }

    NValue opSubtractDecimals(const NValue lhs, const NValue rhs) const {
        if ((lhs.getValueType() != VALUE_TYPE_DECIMAL) ||
            (rhs.getValueType() != VALUE_TYPE_DECIMAL))
        {
            throwFatalException("Non-decimal NValue in decimal subtract.");
        }

        if (lhs.isNull() || rhs.isNull()) {
            TTInt retval;
            retval.SetMin();
            return getDecimalValue(retval);
        }

        TTInt retval(lhs.getDecimal());
        if (retval.Sub(rhs.getDecimal()) || retval > NValue::s_maxDecimal || retval < NValue::s_minDecimal) {
            char message[4096];
            sprintf( message, "Attempted to subtract %s from %s causing overflow/underflow",
                    rhs.createStringFromDecimal().c_str(), lhs.createStringFromDecimal().c_str());
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                               message);
        }

        return getDecimalValue(retval);
    }

    static NValue getTinyIntValue(int8_t value) {
        NValue retval(VALUE_TYPE_TINYINT);
        retval.getTinyInt() = value;
        return retval;
    }

    static NValue getSmallIntValue(int16_t value) {
        NValue retval(VALUE_TYPE_SMALLINT);
        retval.getSmallInt() = value;
        return retval;
    }

    static NValue getIntegerValue(int32_t value) {
        NValue retval(VALUE_TYPE_INTEGER);
        retval.getInteger() = value;
        return retval;
    }

    static NValue getBigIntValue(int64_t value) {
        NValue retval(VALUE_TYPE_BIGINT);
        retval.getBigInt() = value;
        return retval;
    }

    static NValue getTimestampValue(int64_t value) {
        NValue retval(VALUE_TYPE_TIMESTAMP);
        retval.getTimestamp() = value;
        return retval;
    }

    static NValue getDoubleValue(double value) {
        NValue retval(VALUE_TYPE_DOUBLE);
        retval.getDouble() = value;
        return retval;
    }

    static NValue getDecimalValueFromString(const std::string &value) {
        NValue retval(VALUE_TYPE_DECIMAL);
        retval.createDecimalFromString(value);
        return retval;
    }

    static NValue getStringValue(std::string value) {
        NValue retval(VALUE_TYPE_VARCHAR);
        const int16_t length = static_cast<int16_t>(value.length());
        const std::size_t minLength = value.length() + sizeof(int16_t) + 1;
        char *storage = new char[minLength];
        *reinterpret_cast<int16_t*>(storage) = length;
        ::memcpy( storage + sizeof(int16_t), value.c_str(), length);
        storage[sizeof(int16_t) + length] = '\0';
        *reinterpret_cast<char**>(retval.m_data) = storage;
        return retval;
    }

    static NValue getNullStringValue() {
        NValue retval(VALUE_TYPE_VARCHAR);
        *reinterpret_cast<char**>(retval.m_data) = NULL;
        return retval;
    }

    static NValue getNullValue() {
        NValue retval(VALUE_TYPE_NULL);
        return retval;
    }

    static NValue getDecimalValue(TTInt value) {
        NValue retval(VALUE_TYPE_DECIMAL);
        retval.getDecimal() = value;
        return retval;
    }

    static NValue getAddressValue(void *address) {
        NValue retval(VALUE_TYPE_ADDRESS);
        *reinterpret_cast<void**>(retval.m_data) = address;
        return retval;
    }
};

/**
 * Public constructor that initializes to an NValue that is unusable
 * with other NValues.  Useful for declaring storage for an NValue.
 */
inline NValue::NValue() {
    ::memset( m_data, 0, 16);
    setValueType(VALUE_TYPE_INVALID);
}

/**
 * Retrieve a boolean NValue that is true
 */
inline NValue NValue::getTrue() {
    NValue retval(VALUE_TYPE_BOOLEAN);
    retval.getBoolean() = true;
    return retval;
}

/**
 * Retrieve a boolean NValue that is false
 */
inline NValue NValue::getFalse() {
    NValue retval(VALUE_TYPE_BOOLEAN);
    retval.getBoolean() = false;
    return retval;
}

/**
 * Return a new NValue that is the opposite of this one. Only works on
 * booleans
 */
inline NValue NValue::op_negate() const {
    assert(getValueType() == VALUE_TYPE_BOOLEAN);
    NValue retval(VALUE_TYPE_BOOLEAN);
    retval.getBoolean() = !getBoolean();
    return retval;
}

/**
 * Returns C++ true if this NValue is a boolean and is true
 */
inline bool NValue::isTrue() const {
    assert(getValueType() == VALUE_TYPE_BOOLEAN);
    return getBoolean();
}

/**
 * Returns C++ false if this NValue is a boolean and is true
 */
inline bool NValue::isFalse() const {
    assert(getValueType() == VALUE_TYPE_BOOLEAN);
    return !getBoolean();
}

inline bool NValue::isNegative() const {
        const ValueType type = getValueType();
        switch (type) {
        case VALUE_TYPE_TINYINT:
            return getTinyInt() < 0;
        case VALUE_TYPE_SMALLINT:
            return getSmallInt() < 0;
        case VALUE_TYPE_INTEGER:
            return getInteger() < 0;
        case VALUE_TYPE_BIGINT:
            return getBigInt() < 0;
        case VALUE_TYPE_TIMESTAMP:
            return getTimestamp() < 0;
        case VALUE_TYPE_DOUBLE:
            return getDouble() < 0;
        case VALUE_TYPE_DECIMAL:
            return getDecimal().IsSign();
        default: {
            throwFatalException( "Invalid value type '%s' for checking negativity", getTypeName(type).c_str());
        }
        }
    }

/**
 * Logical and operation for NValues
 */
inline NValue NValue::op_and(const NValue rhs) const {
    if (getBoolean() && rhs.getBoolean()) {
        return getTrue();
    }
    return getFalse();
}

/*
 * Logical or operation for NValues
 */
inline NValue NValue::op_or(const NValue rhs) const {
    if(getBoolean() || rhs.getBoolean()) {
        return getTrue();
    }
    return getFalse();
}

/**
 * Objects may have storage allocated for them. Calling free causes the NValue to return the storage allocated for
 * the object to the heap
 */
inline void NValue::free() const {
    switch (getValueType()) {
      case VALUE_TYPE_VARCHAR:
        delete[] *reinterpret_cast<const char* const*>(m_data);
        break;
      default:
        return;
    }
}

/**
 * Get the amount of storage necessary to store a value of the specified type
 * in a tuple
 */
inline uint16_t NValue::getTupleStorageSize(const ValueType type) {
    switch (type) {
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        return sizeof(int64_t);
      case VALUE_TYPE_TINYINT:
        return sizeof(int8_t);
      case VALUE_TYPE_SMALLINT:
        return sizeof(int16_t);
      case VALUE_TYPE_INTEGER:
        return sizeof(int32_t);
      case VALUE_TYPE_DOUBLE:
        return sizeof(double);
      case VALUE_TYPE_VARCHAR:
        return sizeof(char*);
      case VALUE_TYPE_DECIMAL:
        return sizeof(TTInt);
      default:
          char message[128];
          sprintf(message, "NValue::getTupleStorageSize() unrecognized type"
                  " '%d'", type);
          throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                        message);
    }
}

/**
 * Compare any two NValues. Comparison is not guaranteed to
 * succeed if the values are incompatible.  Avoid use of
 * comparison in favor of op_*.
 */
inline int NValue::compare(const NValue rhs) const {
    switch (getValueType()) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
        if (rhs.getValueType() == VALUE_TYPE_DOUBLE) {
            return castAsDouble().compareDoubleValue(rhs);
        } else if (rhs.getValueType() == VALUE_TYPE_DECIMAL) {
            return -1 * rhs.compareDecimalValue(*this);
        } else {
            return compareAnyIntegerValue(rhs);
        }
      case VALUE_TYPE_TIMESTAMP:
        if (rhs.getValueType() == VALUE_TYPE_DOUBLE) {
            return castAsDouble().compareDoubleValue(rhs);
        } else {
            return compareAnyIntegerValue(rhs);
        }
      case VALUE_TYPE_DOUBLE:
        return compareDoubleValue(rhs);
      case VALUE_TYPE_VARCHAR:
        return compareStringValue(rhs);
      case VALUE_TYPE_DECIMAL:
        return compareDecimalValue(rhs);
      default: {
          throwFatalException( "non comparable type '%d'", rhs.getValueType());
      }
    }
}

/**
 * Set this NValue to null.
 */
inline void NValue::setNull() {
    switch (getValueType()) {
      case VALUE_TYPE_NULL:
      case VALUE_TYPE_INVALID:
        return;
      case VALUE_TYPE_TINYINT:
        getTinyInt() = INT8_NULL;
        break;
      case VALUE_TYPE_SMALLINT:
        getSmallInt() = INT16_NULL;
        break;
      case VALUE_TYPE_INTEGER:
        getInteger() = INT32_NULL;
        break;
      case VALUE_TYPE_TIMESTAMP:
        getTimestamp() = INT64_NULL;
        break;
      case VALUE_TYPE_BIGINT:
        getBigInt() = INT64_NULL;
        break;
      case VALUE_TYPE_DOUBLE:
        getDouble() = DOUBLE_MIN;
        break;
      case VALUE_TYPE_VARCHAR:
        *reinterpret_cast<void**>(m_data) = NULL;
        break;
      case VALUE_TYPE_DECIMAL:
        getDecimal().SetMin();
        break;
      default: {
          throwFatalException( "NValue::setNull() called with ValueType '%d'", getValueType());
      }
    }
}

/**
 * Deserialize a scalar of the specified type from the tuple
 * storage area provided. If this is an Object type then the third
 * argument indicates whether the object is stored in the tuple
 * inline TODO: Could the isInlined argument be removed by have
 * the caller dereference the pointer?
 */
inline const NValue NValue::deserializeFromTupleStorage(const void *storage,
                                                 const ValueType type,
                                                 const bool isInlined)
{
        NValue retval(type);
        switch (type) {
          case VALUE_TYPE_TIMESTAMP:
            retval.getTimestamp() = *reinterpret_cast<const int64_t*>(storage);
            break;
          case VALUE_TYPE_TINYINT:
            retval.getTinyInt() = *reinterpret_cast<const int8_t*>(storage);
            break;
          case VALUE_TYPE_SMALLINT:
            retval.getSmallInt() = *reinterpret_cast<const int16_t*>(storage);
            break;
          case VALUE_TYPE_INTEGER:
            retval.getInteger() = *reinterpret_cast<const int32_t*>(storage);
            break;
          case VALUE_TYPE_BIGINT:
            retval.getBigInt() = *reinterpret_cast<const int64_t*>(storage);
            break;
          case VALUE_TYPE_DOUBLE:
            retval.getDouble() = *reinterpret_cast<const double*>(storage);
            break;
          case VALUE_TYPE_DECIMAL:
            ::memcpy( retval.m_data, storage, NValue::getTupleStorageSize(type));
            break;
          case VALUE_TYPE_VARCHAR:
            //Potentially non-inlined type requires special handling
            if (isInlined) {
                //If it is inlined the storage area contains the actual data so copy a reference
                //to the storage area
                *reinterpret_cast<void**>(retval.m_data) = const_cast<void*>(storage);
            } else {
                //If it isn't inlined the storage area contains a pointer to the memory location
                //containing the string.
                memcpy( retval.m_data, storage, sizeof(void*));
            }
            break;
          default:
              throwFatalException( "NValue::getLength() unrecognized type '%d'", type);
        }
        return retval;
}

/**
 * Serialize the scalar this NValue represents to the provided
 * storage area. If the scalar is an Object type that is not
 * inlined then the provided data pool or the heap will be used to
 * allocated storage for a copy of the object.
 */
inline void NValue::serializeToTupleStorageAllocateForObjects(void *storage, const bool isInlined,
                                                       const int16_t maxLength, Pool *dataPool) const
{
    const ValueType type = getValueType();
    int16_t length = 0;

    switch (type) {
      case VALUE_TYPE_TIMESTAMP:
        *reinterpret_cast<int64_t*>(storage) = getTimestamp();
        break;
      case VALUE_TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(storage) = getTinyInt();
        break;
      case VALUE_TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(storage) = getSmallInt();
        break;
      case VALUE_TYPE_INTEGER:
        *reinterpret_cast<int32_t*>(storage) = getInteger();
        break;
      case VALUE_TYPE_BIGINT:
        *reinterpret_cast<int64_t*>(storage) = getBigInt();
        break;
      case VALUE_TYPE_DOUBLE:
        *reinterpret_cast<double*>(storage) = getDouble();
        break;
      case VALUE_TYPE_DECIMAL:
        ::memcpy( storage, m_data, NValue::getTupleStorageSize(type));
        break;
      case VALUE_TYPE_VARCHAR:
        //Potentially non-inlined type requires special handling
        if (isInlined) {
            inlineCopyObject(storage, maxLength);
        }
        else {
            if (isNull()) {
                *reinterpret_cast<void**>(storage) = NULL;
            }
            else {
                length = getObjectLength();
                const size_t minlength = sizeof(int16_t) + length + 1;
                char *copy = NULL;
                if (length > maxLength) {
                    char msg[1024];
                    sprintf(msg, "Object exceeds specified size. Size is %hd"
                            " and max is %hd", length, maxLength);
                    throw SQLException(
                        SQLException::data_exception_string_data_length_mismatch,
                        msg);

                }
                if (dataPool != NULL) {
                    copy = reinterpret_cast<char*>(dataPool->allocate(length + sizeof(int16_t) + 1));
                } else {
                    copy = new char[minlength];
                }
                *(reinterpret_cast<int16_t*>(copy)) = static_cast<int16_t>(length);
                ::memcpy(copy + sizeof(int16_t),getObjectValue(), length);
                copy[length + sizeof(int16_t)] = '\0';
                *reinterpret_cast<char**>(storage) = copy;
            }
        }
        break;
      default: {
          throwFatalException("NValue::getLength() unrecognized type '%d'", type);
      }
    }
}

/**
 * Serialize the scalar this NValue represents to the storage area
 * provided. If the scalar is an Object type then the object will
 * be copy if it can be inlined into the tuple. Otherwise a
 * pointer to the object will be copied into the storage area. No
 * allocations are performed.
 */
inline void NValue::serializeToTupleStorage(void *storage, const bool isInlined, const int16_t maxLength) const
{
    const ValueType type = getValueType();
    switch (type) {
      case VALUE_TYPE_TIMESTAMP:
        *reinterpret_cast<int64_t*>(storage) = getTimestamp();
        break;
      case VALUE_TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(storage) = getTinyInt();
        break;
      case VALUE_TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(storage) = getSmallInt();
        break;
      case VALUE_TYPE_INTEGER:
        *reinterpret_cast<int32_t*>(storage) = getInteger();
        break;
      case VALUE_TYPE_BIGINT:
        *reinterpret_cast<int64_t*>(storage) = getBigInt();
        break;
      case VALUE_TYPE_DOUBLE:
        *reinterpret_cast<double*>(storage) = getDouble();
        break;
      case VALUE_TYPE_DECIMAL:
        ::memcpy( storage, m_data, NValue::getTupleStorageSize(type));
        break;
      case VALUE_TYPE_VARCHAR:
        //Potentially non-inlined type requires special handling
        if (isInlined) {
            inlineCopyObject(storage, maxLength);
        }
        else {
            if (isNull() || getObjectLength() <= maxLength) {
                // copy the pointers
                *reinterpret_cast<char**>(storage) =
                  *reinterpret_cast<char* const*>(m_data);
            }
            else {
                const int16_t length = getObjectLength();
                char msg[1024];
                sprintf(msg, "Object exceeds specified size. Size is %hd and max is %hd", length, maxLength);
                throw SQLException(
                    SQLException::data_exception_string_data_length_mismatch,
                    msg);

            }
        }
        break;
      default:
          char message[128];
          sprintf(message, "NValue::getLength() unrecognized type '%d'", type);
          throw SQLException(SQLException::volt_unsupported_type_conversion_error,
                             message);
    }
}


/**
 * Deserialize a scalar value of the specified type from the
 * SerializeInput directly into the tuple storage area
 * provided. This function will perform memory allocations for
 * Object types as necessary using the provided data pool or the
 * heap. This is used to deserialize tables.
 */
inline void NValue::deserializeFrom(SerializeInput &input, const ValueType type,
                             char *storage, bool isInlined, Pool *dataPool) {
    switch (type) {
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        *reinterpret_cast<int64_t*>(storage) = input.readLong();
        break;
      case VALUE_TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(storage) = input.readByte();
        break;
      case VALUE_TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(storage) = input.readShort();
        break;
      case VALUE_TYPE_INTEGER:
        *reinterpret_cast<int32_t*>(storage) = input.readInt();
        break;
      case VALUE_TYPE_DOUBLE:
        *reinterpret_cast<double* >(storage) = input.readDouble();
        break;
      case VALUE_TYPE_VARCHAR: {
          const int32_t length = input.readInt();
          // the NULL SQL string is a NULL C pointer
          if (isInlined) {
              *reinterpret_cast<int16_t*>(storage) = static_cast<int16_t>(length);
              if (length == OBJECTLENGTH_NULL) {
                  break;
              }
              const char *data = reinterpret_cast<const char*>(input.getRawPointer(length));
              ::memcpy( storage + sizeof(int16_t), data, length);
              storage[sizeof(int16_t) + length] = '\0';
          } else {
              if (length == OBJECTLENGTH_NULL) {
                  *reinterpret_cast<void**>(storage) = NULL;
                  return;
              }
              const char *data = reinterpret_cast<const char*>(input.getRawPointer(length));
              const size_t minlength = sizeof(int16_t) + length + 1;
              char *copy = NULL;
              if (dataPool != NULL) {
                  copy = reinterpret_cast<char*>(dataPool->allocate(length + sizeof(int16_t) + 1));
              } else {
                  copy = new char[minlength];
              }
              *(reinterpret_cast<int16_t*>(copy)) = static_cast<int16_t>(length);
              ::memcpy(copy + sizeof(int16_t), data, length);
              copy[length + sizeof(int16_t)] = '\0';
              *reinterpret_cast<char**>(storage) = copy;
          }
          break;
      }
      case VALUE_TYPE_DECIMAL: {
          int64_t *longStorage = reinterpret_cast<int64_t*>(storage);
          //Reverse order for Java BigDecimal BigEndian
          longStorage[1] = input.readLong();
          longStorage[0] = input.readLong();
          break;
      }
      default: {
          throwFatalException( "NValue::deserializeFrom() unrecognized type '%d'", type);
      }
    }
    return;
}

/**
 * Deserialize a scalar value of the specified type from the
 * provided SerializeInput and perform allocations as necessary.
 * This is used to deserialize parameter sets.
 */
inline const NValue NValue::deserializeFromAllocateForStorage(SerializeInput &input, Pool *dataPool) {
    const ValueType type = static_cast<ValueType>(input.readByte());
    NValue retval(type);
    switch (type) {
      case VALUE_TYPE_BIGINT:
        retval.getBigInt() = input.readLong();
        break;
      case VALUE_TYPE_TIMESTAMP:
        retval.getTimestamp() = input.readLong();
        break;
      case VALUE_TYPE_TINYINT:
        retval.getTinyInt() = input.readByte();
        break;
      case VALUE_TYPE_SMALLINT:
        retval.getSmallInt() = input.readShort();
        break;
      case VALUE_TYPE_INTEGER:
        retval.getInteger() = input.readInt();
        break;
      case VALUE_TYPE_DOUBLE:
        retval.getDouble() = input.readDouble();
        break;
      case VALUE_TYPE_VARCHAR: {
          const int32_t length = input.readInt();
          // the NULL SQL string is a NULL C pointer
          if (length == OBJECTLENGTH_NULL) {
              retval.setNull();
              break;
          }
          const void *str = input.getRawPointer(length);
          const size_t minlength = sizeof(int16_t) + length + 1;
          char *copy = NULL;
          if (dataPool != NULL) {
              copy = reinterpret_cast<char*>(dataPool->allocate(length + sizeof(int16_t) + 1));
          } else {
              copy = new char[minlength];
          }
          *(reinterpret_cast<int16_t*>(copy)) = static_cast<int16_t>(length);
          ::memcpy(copy + sizeof(int16_t), str, length);
          copy[length + sizeof(int16_t)] = '\0';
          *reinterpret_cast<char**>(retval.m_data) = copy;
          break;
      }
      case VALUE_TYPE_DECIMAL: {
          retval.getDecimal().table[1] = input.readLong();
          retval.getDecimal().table[0] = input.readLong();
          break;

      }
      default:
          throwFatalException("NValue::getTupleStorageSize() unrecognized type '%d'", type);
    }
    return retval;
}

/**
 * Serialize this NValue to the provided SerializeOutput
 */
inline void NValue::serializeTo(SerializeOutput &output) const {
    const ValueType type = getValueType();
    switch (type) {
      case VALUE_TYPE_VARCHAR: {
          if (isNull()) {
              output.writeInt(OBJECTLENGTH_NULL);
              break;
          }
          const int32_t length = getObjectLength();
          if (length < OBJECTLENGTH_NULL) {
              throwFatalException("Attempted to serialize an NValue with a negaitve length");
          }
          output.writeInt(static_cast<int32_t>(length));
          if (length != OBJECTLENGTH_NULL) {
              // Not a null string: write it out
              const char * str = reinterpret_cast<const char*>(getObjectValue());
              if (str == NULL) {}
              output.writeBytes(getObjectValue(), length);
          } else {
              assert(getObjectValue() == NULL || length == OBJECTLENGTH_NULL);
          }

          break;
      }
      case VALUE_TYPE_TINYINT: {
          output.writeByte(getTinyInt());
          break;
      }
      case VALUE_TYPE_SMALLINT: {
          output.writeShort(getSmallInt());
          break;
      }
      case VALUE_TYPE_INTEGER: {
          output.writeInt(getInteger());
          break;
      }
      case VALUE_TYPE_TIMESTAMP: {
          output.writeLong(getTimestamp());
          break;
      }
      case VALUE_TYPE_BIGINT: {
          output.writeLong(getBigInt());
          break;
      }
      case VALUE_TYPE_DOUBLE: {
          output.writeDouble(getDouble());
          break;
      }
      case VALUE_TYPE_DECIMAL: {
          output.writeLong(getDecimal().table[1]);
          output.writeLong(getDecimal().table[0]);
          break;
      }
      default:
          throwFatalException("NValue::serializeTo() found a column "
                   "with ValueType '%d' that is not handled", type);
    }
}

inline size_t NValue::serializeToELT(char *dataPtr) const
{
    switch (getValueType()) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
      {
          int64_t val = castAsBigIntAndGetValue();
          *(reinterpret_cast<int64_t*>(dataPtr)) = htonll(val);
          return sizeof(int64_t);
      }
      case VALUE_TYPE_DOUBLE:
      {
          int64_t data;
          double value = getDouble();
          ::memcpy(&data, &value, sizeof(data));
          *(reinterpret_cast<int64_t*>(dataPtr)) = htonll(data);
          return sizeof(int64_t);
      }
      case VALUE_TYPE_VARCHAR:
      {
          int32_t objectLength = getObjectLength();
          *(reinterpret_cast<int32_t*>(dataPtr)) = htonl(objectLength);
          ::memcpy((dataPtr+4), getObjectValue(), objectLength);
          return sizeof(int32_t) + objectLength;
      }
      case VALUE_TYPE_DECIMAL:
      {
          std::string decstr = createStringFromDecimal();
          int32_t objectLength = (int32_t)decstr.length();
          *(reinterpret_cast<int32_t*>(dataPtr)) = htonl(objectLength);
          ::memcpy((dataPtr+4), decstr.c_str(), objectLength);
          return sizeof(int32_t) + objectLength;
      }
      case VALUE_TYPE_INVALID:
      case VALUE_TYPE_NULL:
      case VALUE_TYPE_BOOLEAN:
      case VALUE_TYPE_ADDRESS:
          throwFatalException("Invalid type in serializeToELT: %d", getValueType());
        break;
    }
    throwFatalException("Invalid type in serializeToELT");
    return 0;
}

inline bool NValue::isNull() const {
    switch (getValueType()) {
      case VALUE_TYPE_NULL:
      case VALUE_TYPE_INVALID:
        return true;
      case VALUE_TYPE_TINYINT:
        return getTinyInt() == INT8_NULL;
      case VALUE_TYPE_SMALLINT:
        return getSmallInt() == INT16_NULL;
      case VALUE_TYPE_INTEGER:
        return getInteger() == INT32_NULL;
      case VALUE_TYPE_TIMESTAMP:
      case VALUE_TYPE_BIGINT:
        return getBigInt() == INT64_NULL;
      case VALUE_TYPE_ADDRESS:
        return *reinterpret_cast<void* const*>(m_data) == NULL;
      case VALUE_TYPE_DOUBLE:
        return getDouble() <= DOUBLE_NULL;
      case VALUE_TYPE_VARCHAR:
        return *reinterpret_cast<void* const*>(m_data) == NULL ||
          **reinterpret_cast<int16_t* const*>(m_data) == OBJECTLENGTH_NULL;
      case VALUE_TYPE_DECIMAL: {
          TTInt min;
          min.SetMin();
          return getDecimal() == min;
      }
      default:
          throwFatalException("NValue::isNull() called with ValueType '%d'", getValueType());
    }
    return false;
}

inline NValue NValue::op_equals(const NValue rhs) const {
    return compare(rhs) == 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_notEquals(const NValue rhs) const {
    return compare(rhs) != 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_lessThan(const NValue rhs) const {
    return compare(rhs) < 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_lessThanOrEqual(const NValue rhs) const {
    return compare(rhs) <= 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_greaterThan(const NValue rhs) const {
    return compare(rhs) > 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_greaterThanOrEqual(const NValue rhs) const {
    return compare(rhs) >= 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_max(const NValue rhs) const {
    const int value = compare(rhs);
    if (value > 0) {
        return *this;
    } else {
        return rhs;
        }
}

inline NValue NValue::op_min(const NValue rhs) const {
    const int value = compare(rhs);
    if (value < 0) {
        return *this;
    } else {
        return rhs;
        }
}

inline NValue NValue::getMinValue(ValueType type) {
    NValue retval(type);
    switch(type) {
      case VALUE_TYPE_TINYINT:
        retval.getTinyInt() = std::numeric_limits<int8_t>::min(); break;
      case VALUE_TYPE_SMALLINT:
        retval.getSmallInt() = std::numeric_limits<int16_t>::min(); break;
      case VALUE_TYPE_INTEGER:
        retval.getInteger() = std::numeric_limits<int32_t>::min(); break;
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        retval.getBigInt() = std::numeric_limits<int64_t>::min(); break;
      case VALUE_TYPE_DOUBLE:
        retval.getDouble() = std::numeric_limits<double>::min(); break;
      default:
          throwFatalException ("No configured min value for type %d", (int) type);
    }
    return retval;
}

inline NValue NValue::getMaxValue(ValueType type) {
    NValue retval(type);
    switch(type) {
      case VALUE_TYPE_TINYINT:
        retval.getTinyInt() = std::numeric_limits<int8_t>::max(); break;
      case VALUE_TYPE_SMALLINT:
        retval.getSmallInt() = std::numeric_limits<int16_t>::max(); break;
      case VALUE_TYPE_INTEGER:
        retval.getInteger() = std::numeric_limits<int32_t>::max(); break;
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        retval.getBigInt() = std::numeric_limits<int64_t>::max(); break;
      case VALUE_TYPE_DOUBLE:
        retval.getDouble() = std::numeric_limits<double>::max(); break;
      default:
          throwFatalException ("No configured max value for type %d", (int) type);
    }
    return retval;
}

inline void NValue::hashCombine(std::size_t &seed) const {
    const ValueType type = getValueType();
    switch (type) {
      case VALUE_TYPE_TINYINT:
        boost::hash_combine( seed, getTinyInt()); break;
      case VALUE_TYPE_SMALLINT:
        boost::hash_combine( seed, getSmallInt()); break;
      case VALUE_TYPE_INTEGER:
        boost::hash_combine( seed, getInteger()); break;
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        boost::hash_combine( seed, getBigInt()); break;
      case VALUE_TYPE_DOUBLE:
        boost::hash_combine( seed, getDouble()); break;
      case VALUE_TYPE_VARCHAR: {
        int length = getObjectLength();
        if ((getObjectValue() == NULL) || (length == OBJECTLENGTH_NULL))
            boost::hash_combine( seed, std::string(""));
        else
            boost::hash_combine( seed, std::string( reinterpret_cast<const char*>(getObjectValue()) ));
        break;
      }
      case VALUE_TYPE_DECIMAL:
        getDecimal().hash(seed); break;
      default:
          throwFatalException ("unknown type %d", (int) type);
    }
}


inline NValue NValue::castAs(ValueType type) const {
    VOLT_TRACE("Converting from %s to %s",
            voltdb::getTypeName(getValueType()).c_str(),
            voltdb::getTypeName(type).c_str());
    if (getValueType() == type) {
        return *this;
    }

    switch (type) {
      case VALUE_TYPE_TINYINT:
        return castAsTinyInt();
      case VALUE_TYPE_SMALLINT:
        return castAsSmallInt();
      case VALUE_TYPE_INTEGER:
        return castAsInteger();
      case VALUE_TYPE_BIGINT:
        return castAsBigInt();
      case VALUE_TYPE_TIMESTAMP:
        return castAsTimestamp();
      case VALUE_TYPE_DOUBLE:
        return castAsDouble();
      case VALUE_TYPE_VARCHAR:
        return castAsString();
      case VALUE_TYPE_DECIMAL:
        return castAsDecimal();
      default:
          char message[128];
          sprintf(message, "Type %d not a recognized type for casting",
                  (int) type);
          throw SQLException(SQLException::volt_unsupported_type_conversion_error,
                             message);
    }
}

inline void* NValue::castAsAddress() const {
    const ValueType type = getValueType();
    switch (type) {
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_ADDRESS:
        return *reinterpret_cast<void* const*>(m_data);
      default:
          throwFatalException ("Type %d not a recognized type for casting as an address", (int) type);
    }
}

inline NValue NValue::op_increment() const {
        const ValueType type = getValueType();
        NValue retval(type);
        switch(type) {
        case VALUE_TYPE_TINYINT:
            if (getTinyInt() == INT8_MAX) {
                throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                        "Incrementing this TinyInt results in a value out of range");
            }
            retval.getTinyInt() = static_cast<int8_t>(getTinyInt() + 1); break;
        case VALUE_TYPE_SMALLINT:
            if (getSmallInt() == INT16_MAX) {
                throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                        "Incrementing this SmallInt results in a value out of range");
            }
            retval.getSmallInt() = static_cast<int16_t>(getSmallInt() + 1); break;
        case VALUE_TYPE_INTEGER:
            if (getInteger() == INT32_MAX) {
                throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                        "Incrementing this Integer results in a value out of range");
            }
            retval.getInteger() = getInteger() + 1; break;
        case VALUE_TYPE_BIGINT:
        case VALUE_TYPE_TIMESTAMP:
            if (getBigInt() == INT64_MAX) {
                throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                        "Incrementing this BigInt/Timestamp results in a value out of range");
            }
            retval.getBigInt() = getBigInt() + 1; break;
        case VALUE_TYPE_DOUBLE:
            retval.getDouble() = getDouble() + 1; break;
        default:
            throwFatalException ("type %d is not incrementable", (int) type);
            break;
        }
        return retval;
    }

inline NValue NValue::op_decrement() const {
        const ValueType type = getValueType();
        NValue retval(type);
        switch(type) {
        case VALUE_TYPE_TINYINT:
            if (getTinyInt() == VOLT_INT8_MIN) {
                throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                        "Decrementing this TinyInt results in a value out of range");
            }
            retval.getTinyInt() = static_cast<int8_t>(getTinyInt() - 1); break;
        case VALUE_TYPE_SMALLINT:
            if (getSmallInt() == VOLT_INT16_MIN) {
                throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                        "Decrementing this SmallInt results in a value out of range");
            }
            retval.getSmallInt() = static_cast<int16_t>(getSmallInt() - 1); break;
        case VALUE_TYPE_INTEGER:
            if (getInteger() == VOLT_INT32_MIN) {
                throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                        "Decrementing this Integer results in a value out of range");
            }
            retval.getInteger() = getInteger() - 1; break;
        case VALUE_TYPE_BIGINT:
        case VALUE_TYPE_TIMESTAMP:
            if (getBigInt() == VOLT_INT64_MIN) {
                throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                        "Decrementing this BigInt/Timestamp results in a value out of range");
            }
            retval.getBigInt() = getBigInt() - 1; break;
        case VALUE_TYPE_DOUBLE:
            retval.getDouble() = getDouble() - 1; break;
        default:
            throwFatalException ("type %d is not decrementable", (int) type);
            break;
        }
        return retval;
    }

inline bool NValue::isZero() const {
    const ValueType type = getValueType();
    switch(type) {
      case VALUE_TYPE_TINYINT:
            return getTinyInt() == 0;
      case VALUE_TYPE_SMALLINT:
        return getSmallInt() == 0;
      case VALUE_TYPE_INTEGER:
        return getInteger() == 0;
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        return getBigInt() == 0;
      case VALUE_TYPE_DECIMAL:
        return getDecimal().IsZero();
      default:
          throwFatalException ("type %d is not a numeric type that implements isZero()", (int) type);
    }
}

inline NValue NValue::op_subtract(const NValue rhs) const {
    ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
    switch (vt) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        return opSubtractBigInts(castAsBigIntAndGetValue(),
                                 rhs.castAsBigIntAndGetValue());

      case VALUE_TYPE_DOUBLE:
        return opSubtractDoubles(castAsDoubleAndGetValue(),
                                 rhs.castAsDoubleAndGetValue());

      case VALUE_TYPE_DECIMAL:
        return opSubtractDecimals(castAsDecimal(),
                                  rhs.castAsDecimal());

      default:
        break;
    }
    throwFatalException("Promotion of %s and %s failed in op_subtract.",
               getTypeName(getValueType()).c_str(),
               getTypeName(rhs.getValueType()).c_str());
}

inline NValue NValue::op_add(const NValue rhs) const {
    ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
    switch (vt) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        return opAddBigInts(castAsBigIntAndGetValue(),
                            rhs.castAsBigIntAndGetValue());

      case VALUE_TYPE_DOUBLE:
        return opAddDoubles(castAsDoubleAndGetValue(),
                            rhs.castAsDoubleAndGetValue());

      case VALUE_TYPE_DECIMAL:
        return opAddDecimals(castAsDecimal(),
                             rhs.castAsDecimal());

      default:
        break;
    }
    throwFatalException("Promotion of %s and %s failed in op_add.",
               getTypeName(getValueType()).c_str(),
               getTypeName(rhs.getValueType()).c_str());
}

inline NValue NValue::op_multiply(const NValue rhs) const {
    ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
    switch (vt) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        return opMultiplyBigInts(castAsBigIntAndGetValue(),
                                 rhs.castAsBigIntAndGetValue());

      case VALUE_TYPE_DOUBLE:
        return opMultiplyDoubles(castAsDoubleAndGetValue(),
                                 rhs.castAsDoubleAndGetValue());

      case VALUE_TYPE_DECIMAL:
        return opMultiplyDecimals(*this, rhs);

      default:
        break;
    }
    throwFatalException("Promotion of %s and %s failed in op_multiply.",
               getTypeName(getValueType()).c_str(),
               getTypeName(rhs.getValueType()).c_str());
}

inline NValue NValue::op_divide(const NValue rhs) const {
    ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
    switch (vt) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        return opDivideBigInts(castAsBigIntAndGetValue(),
                               rhs.castAsBigIntAndGetValue());

      case VALUE_TYPE_DOUBLE:
        return opDivideDoubles(castAsDoubleAndGetValue(),
                               rhs.castAsDoubleAndGetValue());

      case VALUE_TYPE_DECIMAL:
        return opDivideDecimals(castAsDecimal(),
                                rhs.castAsDecimal());

      default:
        break;
    }
    throwFatalException("Promotion of %s and %s failed in op_divide.",
               getTypeName(getValueType()).c_str(),
               getTypeName(rhs.getValueType()).c_str());
}

}

#endif /* NVALUE_HPP_ */
