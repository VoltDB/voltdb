/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#ifndef NVALUE_HPP_
#define NVALUE_HPP_

#include <cassert>
#include <cfloat>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <exception>
#include <limits>
#include <stdint.h>
#include <string>
#include <algorithm>
#include <vector>

#include "boost/scoped_ptr.hpp"
#include "boost/functional/hash.hpp"
#include "ttmath/ttmathint.h"

#include "common/ExportSerializeIo.h"
#include "common/FatalException.hpp"
#include "common/Pool.hpp"
#include "common/SQLException.h"
#include "common/StringRef.h"
#include "common/ThreadLocalPool.h"
#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/types.h"
#include "common/value_defs.h"
#include "utf8.h"
#include "murmur3/MurmurHash3.h"

namespace voltdb {

/*
 * Objects are length preceded with a short length value or a long length value
 * depending on how many bytes are needed to represent the length. These
 * define how many bytes are used for the short value vs. the long value.
 */
#define SHORT_OBJECT_LENGTHLENGTH static_cast<char>(1)
#define LONG_OBJECT_LENGTHLENGTH static_cast<char>(4)
#define OBJECT_NULL_BIT static_cast<char>(1 << 6)
#define OBJECT_CONTINUATION_BIT static_cast<char>(1 << 7)
#define OBJECT_MAX_LENGTH_SHORT_LENGTH 63

//The int used for storage and return values
typedef ttmath::Int<2> TTInt;
//Long integer with space for multiplication and division without carry/overflow
typedef ttmath::Int<4> TTLInt;

template<typename T>
void throwCastSQLValueOutOfRangeException(
        const T value,
        const ValueType origType,
        const ValueType newType)
{
    throwCastSQLValueOutOfRangeException((const int64_t)value, origType, newType);
}

template<>
inline void throwCastSQLValueOutOfRangeException<double>(
        const double value,
        const ValueType origType,
        const ValueType newType)
{
    char msg[1024];
    snprintf(msg, 1024, "Type %s with value %f can't be cast as %s because the value is "
            "out of range for the destination type",
            valueToString(origType).c_str(),
            value,
            valueToString(newType).c_str());
    throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                       msg);
}

template<>
inline void throwCastSQLValueOutOfRangeException<int64_t>(
                                  const int64_t value,
                                  const ValueType origType,
                                  const ValueType newType)
{
    char msg[1024];
    snprintf(msg, 1024, "Type %s with value %jd can't be cast as %s because the value is "
            "out of range for the destination type",
            valueToString(origType).c_str(),
            (intmax_t)value,
            valueToString(newType).c_str());

    // record underflow or overflow for executors that catch this (indexes, mostly)
    int internalFlags = 0;
    if (value > 0) internalFlags |= SQLException::TYPE_OVERFLOW;
    if (value < 0) internalFlags |= SQLException::TYPE_UNDERFLOW;

    throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                       msg, internalFlags);
}

int warn_if(int condition, const char* message);

// This has been demonstrated to be more reliable than std::isinf
// -- less sensitive on LINUX to the "g++ -ffast-math" option.
inline int non_std_isinf( double x ) { return (x > DBL_MAX) || (x < -DBL_MAX); }

inline void throwDataExceptionIfInfiniteOrNaN(double value, const char* function)
{
    static int warned_once_no_nan = warn_if( ! std::isnan(sqrt(-1.0)),
                                            "The C++ configuration (e.g. \"g++ --fast-math\") "
                                            "does not support SQL standard handling of NaN errors.");
    static int warned_once_no_inf = warn_if( ! non_std_isinf(std::pow(0.0, -1.0)),
                                            "The C++ configuration (e.g. \"g++ --fast-math\") "
                                            "does not support SQL standard handling of numeric infinity errors.");
    // This uses a standard test for NaN, even though that fails in some configurations like LINUX "g++ -ffast-math".
    // If it is known to fail in the current config, a warning has been sent to the log,
    // so at this point, just relax the check.
    if ((warned_once_no_nan || ! std::isnan(value)) && (warned_once_no_inf || ! non_std_isinf(value))) {
        return;
    }
    char msg[1024];
    snprintf(msg, 1024, "Invalid result value (%f) from floating point %s", value, function);
    throw SQLException(SQLException::data_exception_numeric_value_out_of_range, msg);
}


/// Stream out a double value in SQL standard format, a specific variation of E-notation.
/// TODO: it has been suggested that helper routines like this that are not specifically tied to
/// the NValue representation should be defined in some other header to help reduce clutter, here.
inline void streamSQLFloatFormat(std::stringstream& streamOut, double floatValue)
{
    // Standard SQL wants capital E scientific notation.
    // Yet it differs in some detail from C/C++ E notation, even with all of its customization options.

    // For starters, for 0, the standard explicitly calls for '0E0'.
    // For across-the-board compatibility, the HSQL backend had to be patched it was using '0.0E0'.
    // C++ uses 0.000000E+00 by default. So override that explicitly.
    if (0.0 == floatValue) {
        streamOut << "0E0";
        return;
    }
    // For other values, C++ generally adds too much garnish to be standard
    // -- trailing zeros in the mantissa, an explicit '+' on the exponent, and a
    // leading 0 before single-digit exponents.  Trim it down to the minimalist sql standard.
    std::stringstream fancy;
    fancy << std::setiosflags(std::ios::scientific | std::ios::uppercase) << floatValue;
    // match any format with the regular expression:
    std::string fancyText = fancy.str();
    size_t ePos = fancyText.find('E', 3); // find E after "[-]n.n".
    assert(ePos != std::string::npos);
    size_t endSignifMantissa;
    // Never truncate mantissa down to the bare '.' EVEN for the case of "n.0".
    for (endSignifMantissa = ePos; fancyText[endSignifMantissa-2] != '.'; --endSignifMantissa) {
        // Only truncate trailing '0's.
        if (fancyText[endSignifMantissa-1] != '0') {
            break; // from loop
        }
    }
    const char* optionalSign = (fancyText[ePos+1] == '-') ? "-" : "";
    size_t startSignifExponent;
    // Always keep at least 1 exponent digit.
    size_t endExponent = fancyText.length()-1;
    for (startSignifExponent = ePos+1; startSignifExponent < endExponent; ++startSignifExponent) {
        const char& exponentLeadChar = fancyText[startSignifExponent];
        // Only skip leading '-'s, '+'s and '0's.
        if (exponentLeadChar != '-' && exponentLeadChar != '+' && exponentLeadChar != '0') {
            break; // from loop
        }
    }
    // Bring the truncated pieces together.
    streamOut << fancyText.substr(0, endSignifMantissa)
              << 'E' << optionalSign << fancyText.substr(startSignifExponent);
}

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

    /* Release memory associated to object type tuple columns */
    static void freeObjectsFromTupleStorage(std::vector<char*> const &oldObjects);

    /* Set value to the correct SQL NULL representation. */
    void setNull();

    /* Reveal the contained pointer for type values  */
    void* castAsAddress() const;

    /* Create a boolean true NValue */
    static NValue getTrue();

    /* Create a boolean false NValue */
    static NValue getFalse();

    /* Create an NValue with the null representation for valueType */
    static NValue getNullValue(ValueType);

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
        void *storage, const bool isInlined, const int32_t maxLength,
        Pool *dataPool) const;

    /* Serialize the scalar this NValue represents to the storage area
       provided. If the scalar is an Object type then the object will
       be copy if it can be inlined into the tuple. Otherwise a
       pointer to the object will be copied into the storage area. No
       allocations are performed. */
    void serializeToTupleStorage(
        void *storage, const bool isInlined, const int32_t maxLength) const;

    /* Deserialize a scalar value of the specified type from the
       SerializeInput directly into the tuple storage area
       provided. This function will perform memory allocations for
       Object types as necessary using the provided data pool or the
       heap. This is used to deserialize tables. */
    static void deserializeFrom(
        SerializeInput &input, const ValueType type, char *storage,
        bool isInlined, const int32_t maxLength, Pool *dataPool);

        // TODO: no callers use the first form; Should combine these
        // eliminate the potential NValue copy.

    /* Read a ValueType from the SerializeInput stream and deserialize
       a scalar value of the specified type into this NValue from the provided
       SerializeInput and perform allocations as necessary. */
    void deserializeFromAllocateForStorage(SerializeInput &input, Pool *dataPool);
    void deserializeFromAllocateForStorage(ValueType vt, SerializeInput &input, Pool *dataPool);

    /* Serialize this NValue to a SerializeOutput */
    void serializeTo(SerializeOutput &output) const;

    /* Serialize this NValue to an Export stream */
    void serializeToExport(ExportSerializeOutput&) const;

    // See comment with inlined body, below.
    void allocatePersistentObjectFromInlineValue();

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

    /* For number NValues, compute new NValues for arithmetic operators */
    NValue op_increment() const;
    NValue op_decrement() const;
    NValue op_subtract(const NValue rhs) const;
    NValue op_add(const NValue rhs) const;
    NValue op_multiply(const NValue rhs) const;
    NValue op_divide(const NValue rhs) const;
    /*
     * This NValue must be VARCHAR and the rhs must be VARCHAR.
     * This NValue is the value and the rhs is the pattern
     */
    NValue like(const NValue rhs) const;

    //TODO: passing NValue arguments by const reference SHOULD be standard practice
    // for the dozens of NValue "operator" functions. It saves on needless NValue copies.
    //TODO: returning bool (vs. NValue getTrue()/getFalse()) SHOULD be standard practice
    // for NValue "logical operator" functions.
    // It saves on needless NValue copies and makes unit tests more readable.
    // Cases that need the NValue -- for some actual purpose other than an immediate call to
    // "isTrue()" -- are rare and getting rarer as optimizations like short-cut eval are introduced.
    /**
     * Return true if this NValue is listed as a member of the IN LIST
     * represented as an NValueList* value cached in rhsList.
     */
    bool inList(NValue const& rhsList) const;

    /**
     * If this NValue is an array value, get it's length.
     * Undefined behavior if not an array (cassert fail in debug).
     */
    int arrayLength() const;

    /**
     * If this NValue is an array value, get a value.
     * Undefined behavior if not an array or if oob (cassert fail in debug).
     */
    NValue itemAtIndex(int index) const;

    /**
     * Used for SQL-IN-LIST to cast all array values to a specific type,
     * then sort an dedup them. Returns in a parameter vector, mostly for memory
     * management reasons. Dedup is important for index-accelerated plans, as
     * they might return duplicate rows from the inner join.
     * See MaterializedScanPlanNode & MaterializedScanExecutor
     *
     * Undefined behavior if not an array (cassert fail in debug).
     */
    void castAndSortAndDedupArrayForInList(const ValueType outputType, std::vector<NValue> &outList) const;

    /*
     * Out must have space for 16 bytes
     */
    void murmurHash3(void *out) const;

    /*
     * callConstant, callUnary, and call are templates for arbitrary NValue member functions that implement
     * SQL "column functions". They differ in how many arguments they accept:
     * 0 for callConstant, 1 ("this") for callUnary, and any number (packaged in a vector) for call.
     * The main benefit of these functions being (always explicit) template instantiations for each
     * "FUNC_*" int value instead of a more normal named member function
     * of NValue is that it allows them to be invoked from the default eval method of the
     * correspondingly templated expression subclass
     * (ConstantFunctionExpression, UnaryFunctionExpression, or GeneralFunctionExpression).
     * The alternative would be to name each function (abs, substring, etc.)
     * and explicitly implement the eval method for every expression subclass template instantiation
     * (UnaryFunctionExpression<FUNC_ABS>::eval,
     * GeneralFunctionExpression<FUNC_VOLT_SUBSTRING_FROM>::eval, etc.
     * to call the corresponding NValue member function.
     * So, these member function templates save a bit of boilerplate for each SQL function and allow
     * the function expression subclass templates to be implemented completely generically.
     */
    template <int F> // template for SQL functions returning constants (like pi)
    static NValue callConstant();

    template <int F> // template for SQL functions of one NValue ("this")
    NValue callUnary() const;

    template <int F> // template for SQL functions of multiple NValues
    static NValue call(const std::vector<NValue>& arguments);

    /// Iterates over UTF8 strings one character "code point" at a time, being careful not to walk off the end.
    class UTF8Iterator {
    public:
        UTF8Iterator(const char *start, const char *end) :
            m_cursor(start),
            m_end(end)
            // TODO: We could validate up front that the string is well-formed UTF8,
            // at least to the extent that multi-byte characters have a valid
            // prefix byte and continuation bytes that will not cause a read
            // off the end of the buffer.
            // That done, extractCodePoint could be considerably simpler/faster.
            { assert(m_cursor <= m_end); }

        //Construct a one-off with an alternative current cursor position
        UTF8Iterator(const UTF8Iterator& other, const char *start) :
            m_cursor(start),
            m_end(other.m_end)
            { assert(m_cursor <= m_end); }

        const char * getCursor() { return m_cursor; }

        bool atEnd() { return m_cursor >= m_end; }

        const char * skipCodePoints(int64_t skips) {
            while (skips-- > 0 && ! atEnd()) {
                // TODO: since the returned code point is ignored, it might be better
                // to call a faster, simpler, skipCodePoint method -- maybe once that
                // becomes trivial due to up-front validation.
                extractCodePoint();
            }
            if (atEnd()) {
                return m_end;
            }
            return m_cursor;
        }

        /*
         * Go through a lot of trouble to make sure that corrupt
         * utf8 data doesn't result in touching uninitialized memory
         * by copying the character data onto the stack.
         * That wouldn't be needed if we pre-validated the buffer.
         */
        uint32_t extractCodePoint() {
            assert(m_cursor < m_end); // Caller should have tested and handled atEnd() condition
            /*
             * Copy the next 6 bytes to a temp buffer and retrieve.
             * We should only get 4 byte code points, and the library
             * should only accept 4 byte code points, but once upon a time there
             * were 6 byte code points in UTF-8 so be careful here.
             */
            char nextPotentialCodePoint[] = { 0, 0, 0, 0, 0, 0 };
            char *nextPotentialCodePointIter = nextPotentialCodePoint;
            //Copy 6 bytes or until the end
            ::memcpy( nextPotentialCodePoint, m_cursor, std::min( 6L, m_end - m_cursor));

            /*
             * Extract the code point, find out how many bytes it was
             */
            uint32_t codePoint = utf8::unchecked::next(nextPotentialCodePointIter);
            long int delta = nextPotentialCodePointIter - nextPotentialCodePoint;

            /*
             * Increment the iterator that was passed in by ref, by the delta
             */
            m_cursor += delta;
            return codePoint;
        }

        const char * m_cursor;
        const char * const m_end;
    };


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

    // setArrayElements is a const method since it doesn't actually mutate any NValue state, just
    // the state of the contained NValues which are referenced via the allocated object storage.
    // For example, it is not intended to ever "grow the array" which would require the NValue's
    // object reference (in m_data) to be mutable.
    // The array size is predetermined in allocateANewNValueList.
    void setArrayElements(std::vector<NValue> &args) const;

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
          case VALUE_TYPE_VARBINARY:
          case VALUE_TYPE_BOOLEAN:
          case VALUE_TYPE_INVALID:
          case VALUE_TYPE_NULL:
          default:
            rt = VALUE_TYPE_INVALID;
            break;
        }
        // There ARE rare but legitimate runtime type check exceptions in SQL, so
        // unless/until those legitimate cases get re-routed to some other code path,
        // it is not safe here to ...
        // assert(rt != VALUE_TYPE_INVALID);
        return rt;
    }

  private:
    /*
     * Private methods are private for a reason. Don't expose the raw
     * data so that it can be operated on directly.
     */

    // Function declarations for NValue.cpp definitions.
    void createDecimalFromString(const std::string &txt);
    std::string createStringFromDecimal() const;
    NValue opDivideDecimals(const NValue lhs, const NValue rhs) const;
    NValue opMultiplyDecimals(const NValue &lhs, const NValue &rhs) const;

    // Helpers for inList.
    // These are purposely not inlines to avoid exposure of NValueList details.
    void deserializeIntoANewNValueList(SerializeInput &input, Pool *dataPool);
    void allocateANewNValueList(size_t elementCount, ValueType elementType);

    // Promotion Rules. Initialized in NValue.cpp
    static ValueType s_intPromotionTable[];
    static ValueType s_decimalPromotionTable[];
    static ValueType s_doublePromotionTable[];
    static TTInt s_maxDecimalValue;
    static TTInt s_minDecimalValue;
    // These initializers give the unique double values that are
    // closest but not equal to +/-1E26 within the accuracy of a double.
    static const double s_gtMaxDecimalAsDouble;
    static const double s_ltMinDecimalAsDouble;

    /**
     * 16 bytes of storage for NValue data.
     */
    char m_data[16];
    ValueType m_valueType;
    bool m_sourceInlined;

    /**
     * Private constructor that initializes storage and the specifies the type of value
     * that will be stored in this instance
     */
    NValue(const ValueType type) {
        ::memset( m_data, 0, 16);
        setValueType(type);
        m_sourceInlined = false;
    }

    /**
     * Set the type of the value that will be stored in this instance.
     * The last of the 16 bytes of storage allocated in an NValue
     * is used to store the type
     */
    void setValueType(ValueType type) {
        m_valueType = type;
    }

    /**
     * Get the type of the value. This information is private
     * to prevent code outside of NValue from branching based on the type of a value.
     */
    ValueType getValueType() const {
        return m_valueType;
    }

    /**
     * Get the type of the value. This information is private
     * to prevent code outside of NValue from branching based on the type of a value.
     */
    std::string getValueTypeString() const {
        return getTypeName(m_valueType);
    }

    void setSourceInlined(bool sourceInlined)
    {
        m_sourceInlined = sourceInlined;
    }

    /**
     * An Object is something like a String that has a variable length
     * (thus it is length preceded) and can potentially have indirect
     * storage (will always be indirect when referenced via an NValue).
     * NValues cache a decoded version of the length preceding value
     * in their data area after the pointer to the object storage area.
     *
     * Leverage private access and enforce strict requirements on
     * calling correctness.
     */
    int32_t getObjectLength() const {
        if (isNull()) {
            // Conceptually, I think a NULL object should just have
            // length 0. In practice, this code path is often a defect
            // in code not correctly handling null. May favor a more
            // defensive "return 0" in the future? (rtb)
            throw SQLException(SQLException::dynamic_sql_error,
                    "Must not ask  for object length on sql null object.");
        }
        if ((getValueType() != VALUE_TYPE_VARCHAR) && (getValueType() != VALUE_TYPE_VARBINARY)) {
            // probably want getTupleStorageSize() for non-object types.
            // at the moment, only varchars are using getObjectLength().
            throw SQLException(SQLException::dynamic_sql_error,
                    "Must not ask for object length for non-object types");
        }

        // now safe to read and return the length preceding value.
        return *reinterpret_cast<const int32_t *>(&m_data[8]);
    }

    void setObjectLength(int32_t length) {
        *reinterpret_cast<int32_t *>(&m_data[8]) = length;
    }

    /**
     * -1 is returned for the length of a NULL object. If a string
     * is inlined in its storage location there will be no pointer to
     * check for NULL. The length preceding value must be used instead.
     *
     * The format for a length preceding value is a 1-byte short representation
     * with the the 7th bit used to indicate a null value and the 8th bit used
     * to indicate that this is part of a long representation and that 3 bytes
     * follow. 6 bits are available to represent length for a maximum length
     * of 63 bytes representable with a single byte length. 30 bits are available
     * when the continuation bit is set and 3 bytes follow.
     *
     * The value is converted to network byte order so that the code
     * will always know which byte contains the most signficant digits.
     */
    static int32_t getObjectLengthFromLocation(const char *location) {
        /*
         * Location will be NULL if the NValue is operating on storage that is not
         * inlined and thus can contain a NULL pointer
         */
        if (location == NULL) {
            return -1;
        }
        char firstByte = location[0];
        const char premask = static_cast<char>(OBJECT_NULL_BIT | OBJECT_CONTINUATION_BIT);

        /*
         * Generated mask that removes the null and continuation bits
         * from a single byte length value
         */
        const char mask = ~premask;
        int32_t decodedNumber = 0;
        if ((firstByte & OBJECT_NULL_BIT) != 0) {
            return -1;
        } else if ((firstByte & OBJECT_CONTINUATION_BIT) != 0) {
            char numberBytes[4];
            numberBytes[0] = static_cast<char>(location[0] & mask);
            numberBytes[1] = location[1];
            numberBytes[2] = location[2];
            numberBytes[3] = location[3];
            decodedNumber = ntohl(*reinterpret_cast<int32_t*>(numberBytes));
        } else {
            decodedNumber = location[0] & mask;
        }
        return decodedNumber;
    }

    /*
     * Retrieve the number of bytes used by the length preceding value
     * in the object's storage area. This value
     * is cached in the NValue's 13th byte.
     */
    int8_t getObjectLengthLength() const {
        return m_data[12];
    }

    /*
     * Set the objects length preceding values length to
     * the specified value
     */
    void setObjectLengthLength(int8_t length) {
        m_data[12] = length;
    }

    /*
     * Based on the objects actual length value get the length of the
     * length preceding value to the appropriate length
     */
    static int8_t getAppropriateObjectLengthLength(int32_t length) {
        if (length <= OBJECT_MAX_LENGTH_SHORT_LENGTH) {
            return SHORT_OBJECT_LENGTHLENGTH;
        } else {
            return LONG_OBJECT_LENGTHLENGTH;
        }
    }

    /*
     * Set the length preceding value using the short or long representation depending
     * on what is necessary to represent the length.
     */
    static void setObjectLengthToLocation(int32_t length, char *location) {
        int32_t beNumber = htonl(length);
        if (length < -1) {
            throw SQLException(SQLException::dynamic_sql_error, "Object length cannot be < -1");
        } else if (length == -1) {
            location[0] = OBJECT_NULL_BIT;
        } if (length <= OBJECT_MAX_LENGTH_SHORT_LENGTH) {
            location[0] = reinterpret_cast<char*>(&beNumber)[3];
        } else {
            char *pointer = reinterpret_cast<char*>(&beNumber);
            location[0] = pointer[0];
            location[0] |= OBJECT_CONTINUATION_BIT;
            location[1] = pointer[1];
            location[2] = pointer[2];
            location[3] = pointer[3];
        }
    }

    /*
     * Not truly symmetrical with getObjectValue which returns the actual object past
     * the length preceding value
     */
    void setObjectValue(void* object) {
        *reinterpret_cast<void**>(m_data) = object;
    }

    /**
     * Get a pointer to the value of an Object that lies beyond the storage of the length information
     */
    void* getObjectValue() const {
        if (*reinterpret_cast<void* const*>(m_data) == NULL) {
            return NULL;
        } else if(*reinterpret_cast<const int32_t*>(&m_data[8]) == OBJECTLENGTH_NULL) {
            return NULL;
        } else {
            void* value;
            if (m_sourceInlined)
            {
                value = *reinterpret_cast<char* const*>(m_data) + getObjectLengthLength();
            }
            else
            {
                StringRef* sref = *reinterpret_cast<StringRef* const*>(m_data);
                value = sref->get() + getObjectLengthLength();
            }
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

    std::size_t getAllocationSizeForObject() const;
    static std::size_t getAllocationSizeForObject(int32_t length);

    static void throwCastSQLException(const ValueType origType,
                                      const ValueType newType)
    {
        char msg[1024];
        snprintf(msg, 1024, "Type %s can't be cast as %s",
                 valueToString(origType).c_str(),
                 valueToString(newType).c_str());
        throw SQLException(SQLException::
                           data_exception_most_specific_type_mismatch,
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
        default:
            throwCastSQLException(type, VALUE_TYPE_BIGINT);
            return 0; // NOT REACHED
        }
    }

    int64_t castAsRawInt64AndGetValue() const {
        const ValueType type = getValueType();

        switch (type) {
        case VALUE_TYPE_TINYINT:
            return static_cast<int64_t>(getTinyInt());
        case VALUE_TYPE_SMALLINT:
            return static_cast<int64_t>(getSmallInt());
        case VALUE_TYPE_INTEGER:
            return static_cast<int64_t>(getInteger());
        case VALUE_TYPE_BIGINT:
            return getBigInt();
        case VALUE_TYPE_TIMESTAMP:
            return getTimestamp();
        default:
            throwCastSQLException(type, VALUE_TYPE_BIGINT);
            return 0; // NOT REACHED
        }
    }

    int32_t castAsIntegerAndGetValue() const {
        const ValueType type = getValueType();
        if (isNull()) {
            return INT32_NULL;
        }

        switch (type) {
        case VALUE_TYPE_NULL:
            return INT32_NULL;
        case VALUE_TYPE_TINYINT:
            return static_cast<int32_t>(getTinyInt());
        case VALUE_TYPE_SMALLINT:
            return static_cast<int32_t>(getSmallInt());
        case VALUE_TYPE_INTEGER:
            return getInteger();
        case VALUE_TYPE_BIGINT:
        {
            const int64_t value = getBigInt();
            if (value > (int64_t)INT32_MAX || value < (int64_t)VOLT_INT32_MIN) {
                throwCastSQLValueOutOfRangeException<int64_t>(value, VALUE_TYPE_BIGINT, VALUE_TYPE_INTEGER);
            }
            return static_cast<int32_t>(value);
        }
        case VALUE_TYPE_DOUBLE:
        {
            const double value = getDouble();
            if (value > (double)INT32_MAX || value < (double)VOLT_INT32_MIN) {
                throwCastSQLValueOutOfRangeException(value, VALUE_TYPE_DOUBLE, VALUE_TYPE_INTEGER);
            }
            return static_cast<int32_t>(value);
        }
        default:
            throwCastSQLException(type, VALUE_TYPE_INTEGER);
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
          case VALUE_TYPE_DECIMAL:
          {
            double retval;
            TTInt scaledValue = getDecimal();
            TTInt whole(scaledValue);
            TTInt fractional(scaledValue);
            whole /= kMaxScaleFactor;
            fractional %= kMaxScaleFactor;
            retval = static_cast<double>(whole.ToInt()) +
                    (static_cast<double>(fractional.ToInt())/static_cast<double>(kMaxScaleFactor));
            return retval;
          }
          case VALUE_TYPE_VARCHAR:
          case VALUE_TYPE_VARBINARY:
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
            int64_t value = castAsRawInt64AndGetValue();
            TTInt retval(value);
            retval *= NValue::kMaxScaleFactor;
            return retval;
          }
          case VALUE_TYPE_DECIMAL:
              return getDecimal();
          case VALUE_TYPE_DOUBLE: {
            int64_t intValue = castAsBigIntAndGetValue();
            TTInt retval(intValue);
            retval *= NValue::kMaxScaleFactor;

            double value = getDouble();
            value -= (double)intValue; // isolate decimal part
            value *= NValue::kMaxScaleFactor; // scale up to integer.
            TTInt fracval((int64_t)value);
            retval += fracval;
            return retval;
          }
          case VALUE_TYPE_VARCHAR:
          case VALUE_TYPE_VARBINARY:
          default:
            throwCastSQLException(type, VALUE_TYPE_DECIMAL);
            return 0; // NOT REACHED
        }
    }

    double getNumberFromString() const
    {
        const int32_t strLength = getObjectLength();
        // Guarantee termination at end of object -- or strtod might not stop there.
        char safeBuffer[strLength+1];
        memcpy(safeBuffer, getObjectValue(), strLength);
        safeBuffer[strLength] = '\0';
        char * bufferEnd = safeBuffer;
        double result = strtod(safeBuffer, &bufferEnd);
        // Needs to have consumed SOMETHING.
        if (bufferEnd > safeBuffer) {
            // Unconsumed trailing chars are OK if they are whitespace.
            while (bufferEnd < safeBuffer+strLength && isspace(*bufferEnd)) {
                ++bufferEnd;
            }
            if (bufferEnd == safeBuffer+strLength) {
                return result;
            }
        }
        char errorDetail[strLength+10]; // Allocate enough extra for the text in the snprintf format.
        snprintf(errorDetail, sizeof(errorDetail), "value: '%s'", safeBuffer);
        throw SQLException(SQLException::data_exception_invalid_character_value_for_cast, errorDetail);
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
        case VALUE_TYPE_DECIMAL: {
            TTInt scaledValue = getDecimal();
            TTInt whole(scaledValue);
            whole /= NValue::kMaxScaleFactor;
            retval.getBigInt() = whole.ToInt(); break;
        }
        case VALUE_TYPE_VARCHAR:
            retval.getBigInt() = static_cast<int64_t>(getNumberFromString()); break;
        case VALUE_TYPE_VARBINARY:
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
            // TODO: Consider just eliminating this switch case to throw a cast exception,
            // or explicitly throwing some other exception here.
            // Direct cast of double to timestamp (implemented via intermediate cast to integer, here)
            // is not a SQL standard requirement, may not even make it past the planner's type-checks,
            // or may just be too far a stretch.
            // OR it might be a convenience for some obscure system-generated edge case?

            if (getDouble() > (double)INT64_MAX || getDouble() < (double)VOLT_INT64_MIN) {
                throwCastSQLValueOutOfRangeException<double>(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
            }
            retval.getTimestamp() = static_cast<int64_t>(getDouble()); break;
        case VALUE_TYPE_DECIMAL: {
            // TODO: Consider just eliminating this switch case to throw a cast exception,
            // or explicitly throwing some other exception here.
            // Direct cast of decimal to timestamp (implemented via intermediate cast to integer, here)
            // is not a SQL standard requirement, may not even make it past the planner's type-checks,
            // or may just be too far a stretch.
            // OR it might be a convenience for some obscure system-generated edge case?

            TTInt scaledValue = getDecimal();
            TTInt whole(scaledValue);
            whole /= NValue::kMaxScaleFactor;
            retval.getTimestamp() = whole.ToInt(); break;
        }
        case VALUE_TYPE_VARCHAR:
            //TODO: Seems like we want to also allow actual date formatting OR? a numeric value, here? See ENG-4284.
            retval.getTimestamp() = static_cast<int64_t>(getNumberFromString()); break;
        case VALUE_TYPE_VARBINARY:
        default:
            throwCastSQLException(type, VALUE_TYPE_TIMESTAMP);
        }
        return retval;
    }

    template <typename T>
    void narrowToInteger(const T value, ValueType sourceType)
    {
        if (value > (T)INT32_MAX || value < (T)VOLT_INT32_MIN) {
            throwCastSQLValueOutOfRangeException(value, sourceType, VALUE_TYPE_INTEGER);
        }
        getInteger() = static_cast<int32_t>(value);
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
            retval.narrowToInteger(getBigInt(), type); break;
        case VALUE_TYPE_TIMESTAMP:
            retval.narrowToInteger(getTimestamp(), type); break;
        case VALUE_TYPE_DOUBLE:
            retval.narrowToInteger(getDouble(), type); break;
        case VALUE_TYPE_DECIMAL: {
            TTInt scaledValue = getDecimal();
            TTInt whole(scaledValue);
            whole /= NValue::kMaxScaleFactor;
            int64_t value = whole.ToInt();
            retval.narrowToInteger(value, type); break;
        }
        case VALUE_TYPE_VARCHAR:
            retval.narrowToInteger(getNumberFromString(), type); break;
        case VALUE_TYPE_VARBINARY:
        default:
            throwCastSQLException(type, VALUE_TYPE_INTEGER);
        }
        return retval;
    }

    template <typename T>
    void narrowToSmallInt(const T value, ValueType sourceType)
    {
        if (value > (T)INT16_MAX || value < (T)VOLT_INT16_MIN) {
            throwCastSQLValueOutOfRangeException(value, sourceType, VALUE_TYPE_SMALLINT);
        }
        getSmallInt() = static_cast<int16_t>(value);
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
            retval.narrowToSmallInt(getInteger(), type); break;
        case VALUE_TYPE_BIGINT:
            retval.narrowToSmallInt(getBigInt(), type); break;
        case VALUE_TYPE_TIMESTAMP:
            retval.narrowToSmallInt(getTimestamp(), type); break;
        case VALUE_TYPE_DOUBLE:
            retval.narrowToSmallInt(getDouble(), type); break;
        case VALUE_TYPE_DECIMAL: {
            TTInt scaledValue = getDecimal();
            TTInt whole(scaledValue);
            whole /= NValue::kMaxScaleFactor;
            int64_t value = whole.ToInt();
            retval.narrowToSmallInt(value, type); break;
        }
        case VALUE_TYPE_VARCHAR:
            retval.narrowToSmallInt(getNumberFromString(), type); break;
        case VALUE_TYPE_VARBINARY:
        default:
            throwCastSQLException(type, VALUE_TYPE_SMALLINT);
        }
        return retval;
    }

    template <typename T>
    void narrowToTinyInt(const T value, ValueType sourceType)
    {
        if (value > (T)INT8_MAX || value < (T)VOLT_INT8_MIN) {
            throwCastSQLValueOutOfRangeException(value, sourceType, VALUE_TYPE_TINYINT);
        }
        getTinyInt() = static_cast<int8_t>(value);
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
            retval.narrowToTinyInt(getSmallInt(), type); break;
        case VALUE_TYPE_INTEGER:
            retval.narrowToTinyInt(getInteger(), type); break;
        case VALUE_TYPE_BIGINT:
            retval.narrowToTinyInt(getBigInt(), type); break;
        case VALUE_TYPE_TIMESTAMP:
            retval.narrowToTinyInt(getTimestamp(), type); break;
        case VALUE_TYPE_DOUBLE:
            retval.narrowToTinyInt(getDouble(), type); break;
        case VALUE_TYPE_DECIMAL: {
            TTInt scaledValue = getDecimal();
            TTInt whole(scaledValue);
            whole /= NValue::kMaxScaleFactor;
            int64_t value = whole.ToInt();
            retval.narrowToTinyInt(value, type); break;
        }
        case VALUE_TYPE_VARCHAR:
            retval.narrowToTinyInt(getNumberFromString(), type); break;
        case VALUE_TYPE_VARBINARY:
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
        case VALUE_TYPE_DECIMAL:
            retval.getDouble() = castAsDoubleAndGetValue(); break;
        case VALUE_TYPE_VARCHAR:
            retval.getDouble() = getNumberFromString(); break;
        case VALUE_TYPE_VARBINARY:
        default:
            throwCastSQLException(type, VALUE_TYPE_DOUBLE);
        }
        return retval;
    }

    NValue castAsString() const {
        if (isNull()) {
            NValue retval(VALUE_TYPE_VARCHAR);
            retval.setNull();
            return retval;
        }
        std::stringstream value;
        const ValueType type = getValueType();
        switch (type) {
        case VALUE_TYPE_TINYINT:
            // This cast keeps the tiny int from being confused for a char.
            value << static_cast<int>(getTinyInt()); break;
        case VALUE_TYPE_SMALLINT:
            value << getSmallInt(); break;
        case VALUE_TYPE_INTEGER:
            value << getInteger(); break;
        case VALUE_TYPE_BIGINT:
            value << getBigInt(); break;
        //case VALUE_TYPE_TIMESTAMP:
            //TODO: The SQL standard wants an actual date literal rather than a numeric value, here. See ENG-4284.
            //value << static_cast<double>(getTimestamp()); break;
        case VALUE_TYPE_DOUBLE:
            // Use the specific standard SQL formatting for float values,
            // which the C/C++ format options don't quite support.
            streamSQLFloatFormat(value, getDouble());
            break;
        case VALUE_TYPE_DECIMAL:
            value << createStringFromDecimal(); break;
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_VARBINARY: {
        // note: we allow binary conversion to strings to support
        // byte[] as string parameters...
        // In the future, it would be nice to check this is a decent string here...
            NValue retval(VALUE_TYPE_VARCHAR);
            memcpy(retval.m_data, m_data, sizeof(m_data));
            return retval;
        }
        default:
            throwCastSQLException(type, VALUE_TYPE_VARCHAR);
        }
        return getTempStringValue(value.str().c_str(), value.str().length());
    }

    NValue castAsBinary() const {
        NValue retval(VALUE_TYPE_VARBINARY);
        const ValueType type = getValueType();
        if (isNull()) {
            retval.setNull();
            return retval;
        }
        switch (type) {
        case VALUE_TYPE_VARBINARY:
            memcpy(retval.m_data, m_data, sizeof(m_data));
            break;
        default:
            throwCastSQLException(type, VALUE_TYPE_VARBINARY);
        }
        return retval;
    }

    void createDecimalFromInt(int64_t rhsint)
    {
        TTInt scaled(rhsint);
        scaled *= NValue::kMaxScaleFactor;
        getDecimal() = scaled;
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
            int64_t rhsint = castAsRawInt64AndGetValue();
            retval.createDecimalFromInt(rhsint);
            break;
        }
        case VALUE_TYPE_DECIMAL:
            ::memcpy(retval.m_data, m_data, sizeof(TTInt));
            break;
        case VALUE_TYPE_DOUBLE:
        {
            const double& value = getDouble();
            if (value >= s_gtMaxDecimalAsDouble || value <= s_ltMinDecimalAsDouble) {
                char message[4096];
                snprintf(message, 4096, "Attempted to cast value %f causing overflow/underflow", value);
                throw SQLException(SQLException::data_exception_numeric_value_out_of_range, message);
            }
            // Resort to string as the intermediary since even int64_t does not cover the full range.
            char decimalAsString[41]; // Large enough to account for digits, sign, decimal, and terminating null.
            snprintf(decimalAsString, sizeof(decimalAsString), "%.12f", value);
            // Shift the entire integer part 1 digit to the right, overwriting the decimal point.
            // This effectively creates a potentially very large integer value
            //  equal to the original double scaled up by 10^12.
            for (char* intDigit = strchr(decimalAsString, '.'); intDigit > decimalAsString; --intDigit) {
                *intDigit = *(intDigit-1);
            }
            TTInt result(decimalAsString+1);
            retval.getDecimal() = result;
            break;
        }
        case VALUE_TYPE_VARCHAR:
        {
            const int32_t length = getObjectLength();
            const char* bytes = reinterpret_cast<const char*>(getObjectValue());
            const std::string value(bytes, length);
            retval.createDecimalFromString(value);
            break;
        }
        default:
            throwCastSQLException(type, VALUE_TYPE_DECIMAL);
        }
        return retval;
    }

    /**
     * Copy the arbitrary size object that this value points to as an
     * inline object in the provided storage area
     */
    void inlineCopyObject(void *storage, int32_t maxLength) const {
        if (isNull()) {
            /*
             * The 7th bit of the length preceding value
             * is used to indicate that the object is null.
             */
            *reinterpret_cast<char*>(storage) = OBJECT_NULL_BIT;
        }
        else {
            const int32_t objectLength = getObjectLength();
            if (objectLength > maxLength) {
                if (maxLength == 0) {
                    throwFatalLogicErrorStreamed("Zero maxLength for object type " << valueToString(getValueType()));
                }
                char msg[1024];
                snprintf(msg, 1024,
                         "In NValue::inlineCopyObject, Object exceeds specified size. Size is %d and max is %d",
                                    objectLength, maxLength);
                throw SQLException(SQLException::data_exception_string_data_length_mismatch,
                                   msg);
            }
            if (m_sourceInlined)
            {
                ::memcpy( storage, *reinterpret_cast<char *const *>(m_data), getObjectLengthLength() + objectLength);
            }
            else
            {
                const StringRef* sref =
                    *reinterpret_cast<StringRef* const*>(m_data);
                ::memcpy(storage, sref->get(),
                         getObjectLengthLength() + objectLength);
            }
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
            throwDynamicSQLException(
                    "non comparable types lhs '%s' rhs '%s'",
                    getValueTypeString().c_str(),
                    rhs.getValueTypeString().c_str());
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
          {
              char message[128];
              snprintf(message, 128,
                       "Type %s cannot be cast for comparison to type %s",
                       valueToString(rhs.getValueType()).c_str(),
                       valueToString(getValueType()).c_str());
              throw SQLException(SQLException::
                                 data_exception_most_specific_type_mismatch,
                                 message);
              // Not reached
              return 0;
          }
        }
    }

    int compareStringValue (const NValue rhs) const {
        if ((rhs.getValueType() != VALUE_TYPE_VARCHAR) && (rhs.getValueType() != VALUE_TYPE_VARBINARY)) {
            char message[128];
            snprintf(message, 128,
                     "Type %s cannot be cast for comparison to type %s",
                     valueToString(rhs.getValueType()).c_str(),
                     valueToString(getValueType()).c_str());
            throw SQLException(SQLException::
                               data_exception_most_specific_type_mismatch,
                               message);
        }
        const char* left = reinterpret_cast<const char*>(getObjectValue());
        const char* right = reinterpret_cast<const char*>(rhs.getObjectValue());
        if (isNull()) {
            if (rhs.isNull()) {
                return VALUE_COMPARE_EQUAL;
            } else {
                return VALUE_COMPARE_LESSTHAN;
            }
        } else if (rhs.isNull()) {
            return VALUE_COMPARE_GREATERTHAN;
        }
        const int32_t leftLength = getObjectLength();
        const int32_t rightLength = rhs.getObjectLength();
        const int result = ::strncmp(left, right, std::min(leftLength, rightLength));
        if (result == 0 && leftLength != rightLength) {
            if (leftLength > rightLength) {
                return  VALUE_COMPARE_GREATERTHAN;
            } else {
                return VALUE_COMPARE_LESSTHAN;
            }
        }
        else if (result > 0) {
            return VALUE_COMPARE_GREATERTHAN;
        }
        else if (result < 0) {
            return VALUE_COMPARE_LESSTHAN;
        }

        return VALUE_COMPARE_EQUAL;
    }

    int compareBinaryValue (const NValue rhs) const {
        if (rhs.getValueType() != VALUE_TYPE_VARBINARY) {
            char message[128];
            snprintf(message, 128,
                     "Type %s cannot be cast for comparison to type %s",
                     valueToString(rhs.getValueType()).c_str(),
                     valueToString(getValueType()).c_str());
            throw SQLException(SQLException::
                               data_exception_most_specific_type_mismatch,
                               message);
        }
        const char* left = reinterpret_cast<const char*>(getObjectValue());
        const char* right = reinterpret_cast<const char*>(rhs.getObjectValue());
        if (isNull()) {
            if (rhs.isNull()) {
                return VALUE_COMPARE_EQUAL;
            } else {
                return VALUE_COMPARE_LESSTHAN;
            }
        } else if (rhs.isNull()) {
            return VALUE_COMPARE_GREATERTHAN;
        }
        const int32_t leftLength = getObjectLength();
        const int32_t rightLength = rhs.getObjectLength();
        const int result = ::memcmp(left, right, std::min(leftLength, rightLength));
        if (result == 0 && leftLength != rightLength) {
            if (leftLength > rightLength) {
                return  VALUE_COMPARE_GREATERTHAN;
            } else {
                return VALUE_COMPARE_LESSTHAN;
            }
        }
        else if (result > 0) {
            return VALUE_COMPARE_GREATERTHAN;
        }
        else if (result < 0) {
            return VALUE_COMPARE_LESSTHAN;
        }

        return VALUE_COMPARE_EQUAL;
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
              char message[128];
              snprintf(message, 128,
                       "Type %s cannot be cast for comparison to type %s",
                       valueToString(rhs.getValueType()).c_str(),
                       valueToString(getValueType()).c_str());
              throw SQLException(SQLException::
                                 data_exception_most_specific_type_mismatch,
                                 message);
              // Not reached
              return 0;
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
            snprintf(message, 4096, "Adding %jd and %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
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
            snprintf(message, 4096, "Subtracting %jd from %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
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
            snprintf(message, 4096, "Multiplying %jd with %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
            throw SQLException( SQLException::data_exception_numeric_value_out_of_range, message);
        }

        return getBigIntValue(result);
    }

    NValue opDivideBigInts(const int64_t lhs, const int64_t rhs) const {
        if (lhs == INT64_NULL || rhs == INT64_NULL)
            return getBigIntValue(INT64_NULL);

        if (rhs == 0) {
            char message[4096];
            snprintf(message, 4096, "Attempted to divide %jd by 0", (intmax_t)lhs);
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
        throwDataExceptionIfInfiniteOrNaN(result, "'+' operator");
        return getDoubleValue(result);
    }

    NValue opSubtractDoubles(const double lhs, const double rhs) const {
        if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
            return getDoubleValue(DOUBLE_MIN);

        const double result = lhs - rhs;
        throwDataExceptionIfInfiniteOrNaN(result, "'-' operator");
        return getDoubleValue(result);
    }

    NValue opMultiplyDoubles(const double lhs, const double rhs) const {
        if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
            return getDoubleValue(DOUBLE_MIN);

        const double result = lhs * rhs;
        throwDataExceptionIfInfiniteOrNaN(result, "'*' operator");
        return getDoubleValue(result);
    }

    NValue opDivideDoubles(const double lhs, const double rhs) const {
        if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
            return getDoubleValue(DOUBLE_MIN);

        const double result = lhs / rhs;
        throwDataExceptionIfInfiniteOrNaN(result, "'/' operator");
        return getDoubleValue(result);
    }

    NValue opAddDecimals(const NValue lhs, const NValue rhs) const {
        if ((lhs.getValueType() != VALUE_TYPE_DECIMAL) ||
            (rhs.getValueType() != VALUE_TYPE_DECIMAL))
        {
            throw SQLException(SQLException::dynamic_sql_error, "Non-decimal NValue in decimal adder.");
        }

        if (lhs.isNull() || rhs.isNull()) {
            TTInt retval;
            retval.SetMin();
            return getDecimalValue(retval);
        }

        TTInt retval(lhs.getDecimal());
        if (retval.Add(rhs.getDecimal()) || retval > s_maxDecimalValue || retval < s_minDecimalValue) {
            char message[4096];
            snprintf(message, 4096, "Attempted to add %s with %s causing overflow/underflow",
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
            throw SQLException(SQLException::dynamic_sql_error, "Non-decimal NValue in decimal subtract.");
        }

        if (lhs.isNull() || rhs.isNull()) {
            TTInt retval;
            retval.SetMin();
            return getDecimalValue(retval);
        }

        TTInt retval(lhs.getDecimal());
        if (retval.Sub(rhs.getDecimal()) || retval > s_maxDecimalValue || retval < s_minDecimalValue) {
            char message[4096];
            snprintf(message, 4096, "Attempted to subtract %s from %s causing overflow/underflow",
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

    static NValue getAllocatedArrayValueFromSizeAndType(size_t elementCount, ValueType elementType)
    {
        NValue retval(VALUE_TYPE_ARRAY);
        retval.allocateANewNValueList(elementCount, elementType);
        return retval;
    }

    static Pool* getTempStringPool();

    static NValue getTempStringValue(const char* value, size_t size) {
        return getAllocatedValue(VALUE_TYPE_VARCHAR, value, size, getTempStringPool());
    }

    static NValue getTempBinaryValue(const unsigned char* value, size_t size) {
        return getAllocatedValue(VALUE_TYPE_VARBINARY, reinterpret_cast<const char*>(value), size, getTempStringPool());
    }

    /// Assumes hex-encoded input
    static inline NValue getTempBinaryValueFromHex(const std::string& value) {
        size_t rawLength = value.length() / 2;
        unsigned char rawBuf[rawLength];
        hexDecodeToBinary(rawBuf, value.c_str());
        return getTempBinaryValue(rawBuf, rawLength);
    }

    static NValue getAllocatedValue(ValueType type, const char* value, size_t size, Pool* stringPool) {
        NValue retval(type);
        char* storage = retval.allocateValueStorage((int32_t)size, stringPool);
        ::memcpy(storage, value, (int32_t)size);
        return retval;
    }

    char* allocateValueStorage(int32_t length, Pool* stringPool)
    {
        const int8_t lengthLength = getAppropriateObjectLengthLength(length);
        const int32_t minLength = length + lengthLength;
        StringRef* sref = StringRef::create(minLength, stringPool);
        char* storage = sref->get();
        setObjectLengthToLocation(length, storage);
        storage += lengthLength;
        setObjectValue(sref);
        setObjectLength(length);
        setObjectLengthLength(lengthLength);
        return storage;
    }

    static NValue getNullStringValue() {
        NValue retval(VALUE_TYPE_VARCHAR);
        *reinterpret_cast<char**>(retval.m_data) = NULL;
        return retval;
    }

    static NValue getNullBinaryValue() {
        NValue retval(VALUE_TYPE_VARBINARY);
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
    m_sourceInlined = false;
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
            throwDynamicSQLException( "Invalid value type '%s' for checking negativity", getValueTypeString().c_str());
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
    switch (getValueType())
    {
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_ARRAY:
        {
            assert(!m_sourceInlined);
            StringRef* sref = *reinterpret_cast<StringRef* const*>(m_data);
            if (sref != NULL)
            {
                StringRef::destroy(sref);
            }
        }
        break;
    default:
        return;
    }
}

inline void NValue::freeObjectsFromTupleStorage(std::vector<char*> const &oldObjects)
{

    for (std::vector<char*>::const_iterator it = oldObjects.begin(); it != oldObjects.end(); ++it) {
        StringRef* sref = reinterpret_cast<StringRef*>(*it);
        if (sref != NULL) {
            StringRef::destroy(sref);
        }
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
      case VALUE_TYPE_VARBINARY:
        return sizeof(char*);
      case VALUE_TYPE_DECIMAL:
        return sizeof(TTInt);
      default:
          char message[128];
          snprintf(message, 128, "NValue::getTupleStorageSize() unsupported type '%s'",
                   getTypeName(type).c_str());
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
      case VALUE_TYPE_VARBINARY:
        return compareBinaryValue(rhs);
      case VALUE_TYPE_DECIMAL:
        return compareDecimalValue(rhs);
      default: {
          throwDynamicSQLException(
                  "non comparable types lhs '%s' rhs '%s'",
                  getValueTypeString().c_str(),
                  rhs.getValueTypeString().c_str());
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
      case VALUE_TYPE_VARBINARY:
        *reinterpret_cast<void**>(m_data) = NULL;
        break;
      case VALUE_TYPE_DECIMAL:
        getDecimal().SetMin();
        break;
      default: {
          throwDynamicSQLException("NValue::setNull() called with unsupported ValueType '%d'", getValueType());
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
    switch (type)
    {
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
    case VALUE_TYPE_VARBINARY:
    {
        //Potentially non-inlined type requires special handling
        char* data = NULL;
        if (isInlined) {
            //If it is inlined the storage area contains the actual data so copy a reference
            //to the storage area
            *reinterpret_cast<void**>(retval.m_data) = const_cast<void*>(storage);
            data = *reinterpret_cast<char**>(retval.m_data);
            retval.setSourceInlined(true);
        } else {
            //If it isn't inlined the storage area contains a pointer to the
            // StringRef object containing the string's memory
            memcpy( retval.m_data, storage, sizeof(void*));
            StringRef* sref = *reinterpret_cast<StringRef**>(retval.m_data);
            // If the StringRef pointer is null, that's because this
            // was a null value; leave the data pointer as NULL so
            // that getObjectLengthFromLocation will figure this out
            // correctly, otherwise get the right char* from the StringRef
            if (sref != NULL)
            {
                data = sref->get();
            }
        }
        const int32_t length = getObjectLengthFromLocation(data);
        //std::cout << "NValue::deserializeFromTupleStorage: length: " << length << std::endl;
        retval.setObjectLength(length);
        retval.setObjectLengthLength(getAppropriateObjectLengthLength(length));
        break;
    }
    default:
        throwDynamicSQLException(
                "NValue::getLength() unrecognized type '%s'",
                getTypeName(type).c_str());
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
                                                       const int32_t maxLength, Pool *dataPool) const
{
    const ValueType type = getValueType();
    int32_t length = 0;

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
      case VALUE_TYPE_VARBINARY:
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
                const int8_t lengthLength = getObjectLengthLength();
                const int32_t minlength = lengthLength + length;
                if (length > maxLength) {
                    char msg[1024];
                    snprintf(msg, 1024, "In NValue::serializeToTupleStorageAllocateForObjects, Object exceeds specified size. Size is %d"
                            " and max is %d", length, maxLength);
                    throw SQLException(
                        SQLException::data_exception_string_data_length_mismatch,
                        msg);

                }
                StringRef* sref = StringRef::create(minlength, dataPool);
                char *copy = sref->get();
                setObjectLengthToLocation(length, copy);
                ::memcpy(copy + lengthLength, getObjectValue(), length);
                *reinterpret_cast<StringRef**>(storage) = sref;
            }
        }
        break;
      default: {
          throwDynamicSQLException(
                  "NValue::serializeToTupleStorageAllocateForObjects() unrecognized type '%s'",
                  getTypeName(type).c_str());
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
inline void NValue::serializeToTupleStorage(void *storage, const bool isInlined, const int32_t maxLength) const
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
      case VALUE_TYPE_VARBINARY:
        //Potentially non-inlined type requires special handling
        if (isInlined) {
            inlineCopyObject(storage, maxLength);
        }
        else {
            if (isNull() || getObjectLength() <= maxLength) {
                if (m_sourceInlined && !isInlined)
                {
                    throwDynamicSQLException(
                            "Cannot serialize an inlined string to non-inlined tuple storage in serializeToTupleStorage()");
                }
                // copy the StringRef pointers
                *reinterpret_cast<StringRef**>(storage) =
                  *reinterpret_cast<StringRef* const*>(m_data);
            }
            else {
                const int32_t length = getObjectLength();
                throwDynamicSQLException(
                        "In NValue::serializeToTupleStorage(), Object exceeds specified size. Size is %d and max is %d", length, maxLength);
            }
        }
        break;
      default:
          char message[128];
          snprintf(message, 128, "NValue::serializeToTupleStorage() unrecognized type '%s'",
                   getTypeName(type).c_str());
          throw SQLException(SQLException::data_exception_most_specific_type_mismatch,
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
                             char *storage, bool isInlined, const int32_t maxLength, Pool *dataPool) {
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
      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_VARBINARY:
      {
          const int32_t length = input.readInt();
          if (length > maxLength) {
              char msg[1024];
              snprintf(msg, 1024, "In NValue::deserializeFrom, Object exceeds specified size. Size is %d and max is %d", length, maxLength);
              throw SQLException(
                  SQLException::data_exception_string_data_length_mismatch,
                  msg);
          }

          const int8_t lengthLength = getAppropriateObjectLengthLength(length);
          // the NULL SQL string is a NULL C pointer
          if (isInlined) {
              setObjectLengthToLocation(length, storage);
              if (length == OBJECTLENGTH_NULL) {
                  break;
              }
              const char *data = reinterpret_cast<const char*>(input.getRawPointer(length));
              ::memcpy( storage + lengthLength, data, length);
          } else {
              if (length == OBJECTLENGTH_NULL) {
                  *reinterpret_cast<void**>(storage) = NULL;
                  return;
              }
              const char *data = reinterpret_cast<const char*>(input.getRawPointer(length));
              const int32_t minlength = lengthLength + length;
              StringRef* sref = StringRef::create(minlength, dataPool);
              char* copy = sref->get();
              setObjectLengthToLocation( length, copy);
              ::memcpy(copy + lengthLength, data, length);
              *reinterpret_cast<StringRef**>(storage) = sref;
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
      default:
          char message[128];
          snprintf(message, 128, "NValue::deserializeFrom() unrecognized type '%s'",
                   getTypeName(type).c_str());
          throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                        message);
    }
}

/**
 * Deserialize a scalar value of the specified type from the
 * provided SerializeInput and perform allocations as necessary.
 * This is used to deserialize parameter sets.
 */
inline void NValue::deserializeFromAllocateForStorage(SerializeInput &input, Pool *dataPool)
{
    const ValueType type = static_cast<ValueType>(input.readByte());
    deserializeFromAllocateForStorage(type, input, dataPool);
}

inline void NValue::deserializeFromAllocateForStorage(ValueType type, SerializeInput &input, Pool *dataPool)
{
    setValueType(type);
    switch (type) {
      case VALUE_TYPE_BIGINT:
        getBigInt() = input.readLong();
        break;
      case VALUE_TYPE_TIMESTAMP:
        getTimestamp() = input.readLong();
        break;
      case VALUE_TYPE_TINYINT:
        getTinyInt() = input.readByte();
        break;
      case VALUE_TYPE_SMALLINT:
        getSmallInt() = input.readShort();
        break;
      case VALUE_TYPE_INTEGER:
        getInteger() = input.readInt();
        break;
      case VALUE_TYPE_DOUBLE:
        getDouble() = input.readDouble();
        break;
      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_VARBINARY:
      {
          const int32_t length = input.readInt();
          // the NULL SQL string is a NULL C pointer
          if (length == OBJECTLENGTH_NULL) {
              setNull();
              break;
          }
          char* storage = allocateValueStorage(length, dataPool);
          const char *str = (const char*) input.getRawPointer(length);
          ::memcpy(storage, str, length);
          break;
      }
      case VALUE_TYPE_DECIMAL: {
          getDecimal().table[1] = input.readLong();
          getDecimal().table[0] = input.readLong();
          break;

      }
      case VALUE_TYPE_NULL: {
          setNull();
          break;
      }
      case VALUE_TYPE_ARRAY: {
          deserializeIntoANewNValueList(input, dataPool);
          break;
      }
      default:
          throwDynamicSQLException(
                  "NValue::deserializeFromAllocateForStorage() unrecognized type '%s'",
                  getTypeName(type).c_str());
    }
}

/**
 * Serialize this NValue to the provided SerializeOutput
 */
inline void NValue::serializeTo(SerializeOutput &output) const {
    const ValueType type = getValueType();
    switch (type) {
      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_VARBINARY:
      {
          if (isNull()) {
              output.writeInt(OBJECTLENGTH_NULL);
              break;
          }
          const int32_t length = getObjectLength();
          if (length < OBJECTLENGTH_NULL) {
              throwDynamicSQLException("Attempted to serialize an NValue with a negative length");
          }
          output.writeInt(static_cast<int32_t>(length));
          if (length != OBJECTLENGTH_NULL) {
              // Not a null string: write it out
              const char * str = reinterpret_cast<const char*>(getObjectValue());
              if (str == NULL) {}
              output.writeBytes(getObjectValue(), length);
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
          throwDynamicSQLException( "NValue::serializeTo() found a column "
                   "with ValueType '%s' that is not handled", getValueTypeString().c_str());
    }
}

inline void NValue::serializeToExport(ExportSerializeOutput &io) const
{
    switch (getValueType()) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
      {
          int64_t val = castAsBigIntAndGetValue();
          io.writeLong(val);
          return;
      }
      case VALUE_TYPE_DOUBLE:
      {
          double value = getDouble();
          io.writeDouble(value);
          return;
      }
      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_VARBINARY:
      {
          // requires (and uses) bytecount not character count
          io.writeBinaryString(getObjectValue(), getObjectLength());
          return;
      }
      case VALUE_TYPE_DECIMAL:
      {
          std::string decstr = createStringFromDecimal();
          int32_t objectLength = (int32_t)decstr.length();
          io.writeBinaryString(decstr.data(), objectLength);
          return;
      }
      case VALUE_TYPE_INVALID:
      case VALUE_TYPE_NULL:
      case VALUE_TYPE_BOOLEAN:
      case VALUE_TYPE_ADDRESS:
      case VALUE_TYPE_ARRAY:
      case VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC:
          char message[128];
          snprintf(message, sizeof(message), "Invalid type in serializeToExport: %s", getTypeName(getValueType()).c_str());
          throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                        message);
    }

    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "Invalid type in serializeToExport");
}

/** Reformat an object-typed value from its inlined form to its allocated out-of-line form,
 *  for use with a widened tuple column. **/
inline void NValue::allocatePersistentObjectFromInlineValue()
{
    if (m_valueType == VALUE_TYPE_NULL || m_valueType == VALUE_TYPE_INVALID) {
        return;
    }

    assert(m_valueType == VALUE_TYPE_VARCHAR || m_valueType == VALUE_TYPE_VARBINARY);
    assert(m_sourceInlined);

    // Not sure why there need to be two ways to signify NULL,
    // maybe it simplifies value transfer from inline tuple storage
    // to be able to use the second object-length-based form,
    // but since this is how isNull works, do likewise.
    if (*reinterpret_cast<void* const*>(m_data) == NULL ||
        *reinterpret_cast<const int32_t*>(&m_data[8]) == OBJECTLENGTH_NULL) {
        // Standardize on the more direct NULL representation.
        // The object-length-based form of NULL appears to only be expected for inlined values.
        *reinterpret_cast<void**>(m_data) = NULL;
        // serializeToTupleStorage fusses about this inline flag being set, even for NULLs
        setSourceInlined(false);
        return;
    }

    // A future version/variant of this function may take an optional Pool argument,
    // so that it could be used for temp values/tables.
    // The default persistent string pool specified by NULL works fine here and now.
    Pool* stringPool = NULL;

    // When an object is inlined, m_data is a direct pointer into a tuple's inline storage area.
    char* source = *reinterpret_cast<char**>(m_data);

    // When it isn't inlined, m_data must contain a pointer to a StringRef object
    // that contains that same data in that same format.

    int32_t length = getObjectLength();
    // inlined objects always have a minimal (1-byte) length field.
    StringRef* sref = StringRef::create(length + SHORT_OBJECT_LENGTHLENGTH, stringPool);
    char* storage = sref->get();
    // Copy length and value into the allocated out-of-line storage
    ::memcpy(storage, source, length + SHORT_OBJECT_LENGTHLENGTH);
    setObjectValue(sref);
    setSourceInlined(false);
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
      case VALUE_TYPE_VARBINARY:
        return *reinterpret_cast<void* const*>(m_data) == NULL ||
          *reinterpret_cast<const int32_t*>(&m_data[8]) == OBJECTLENGTH_NULL;
      case VALUE_TYPE_DECIMAL: {
          TTInt min;
          min.SetMin();
          return getDecimal() == min;
      }
      default:
          throwDynamicSQLException(
                  "NValue::isNull() called with unknown ValueType '%s'",
                  getValueTypeString().c_str());
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

inline NValue NValue::getNullValue(ValueType type) {
    NValue retval(type);
    retval.setNull();
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
        // This method was observed to fail on Centos 5 / GCC 4.1.2, returning different hashes
        // for identical inputs, so the conditional was added,
        // mutated from the one in boost/type_traits/intrinsics.hpp,
        // and the broken overload for "double" was by-passed in favor of the more reliable
        // one for int64 -- even if this may give sub-optimal hashes for typical collections of double.
        // This conditional can be dropped when Centos 5 support is dropped.
#if defined(__GNUC__) && ((__GNUC__ > 4) || ((__GNUC__ == 4) && (__GNUC_MINOR__ >= 2) && !defined(__GCCXML__))) && !defined(BOOST_CLANG)
        boost::hash_combine( seed, getDouble()); break;
#else
        {
        const int64_t proxyForDouble =  *reinterpret_cast<const int64_t*>(m_data);
        boost::hash_combine( seed, proxyForDouble); break;
        }
#endif
      case VALUE_TYPE_VARCHAR: {
        if (getObjectValue() == NULL) {
            boost::hash_combine( seed, std::string(""));
        } else {
            const int32_t length = getObjectLength();
            boost::hash_combine( seed, std::string( reinterpret_cast<const char*>(getObjectValue()), length ));
        }
        break;
      }
      case VALUE_TYPE_VARBINARY: {
        if (getObjectValue() == NULL) {
            boost::hash_combine( seed, std::string(""));
        } else {
            const int32_t length = getObjectLength();
            char* data = reinterpret_cast<char*>(getObjectValue());
            for (int32_t i = 0; i < length; i++)
                boost::hash_combine(seed, data[i]);
        }
        break;
      }
      case VALUE_TYPE_DECIMAL:
        getDecimal().hash(seed); break;
      default:
          throwDynamicSQLException( "NValue::hashCombine unknown type %s", getValueTypeString().c_str());
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
      case VALUE_TYPE_VARBINARY:
        return castAsBinary();
      case VALUE_TYPE_DECIMAL:
        return castAsDecimal();
      default:
          DEBUG_IGNORE_OR_THROW_OR_CRASH("Fallout from planner error."
                                         " The invalid target value type for a cast is " <<
                                         getTypeName(type))

          char message[128];
          snprintf(message, 128, "Type %d not a recognized type for casting",
                  (int) type);
          throw SQLException(SQLException::
                             data_exception_most_specific_type_mismatch,
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
          throwDynamicSQLException(
                  "Type %s not a recognized type for casting as an address",
                  getValueTypeString().c_str());
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
            throwDynamicSQLException( "type %s is not incrementable", getValueTypeString().c_str());
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
            throwDynamicSQLException( "type %s is not decrementable", getValueTypeString().c_str());
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
          throwDynamicSQLException(
                  "type %s is not a numeric type that implements isZero()",
                  getValueTypeString().c_str());
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
    throwDynamicSQLException("Promotion of %s and %s failed in op_subtract.",
               getValueTypeString().c_str(),
               rhs.getValueTypeString().c_str());
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
    throwDynamicSQLException("Promotion of %s and %s failed in op_add.",
               getValueTypeString().c_str(),
               rhs.getValueTypeString().c_str());
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
    throwDynamicSQLException("Promotion of %s and %s failed in op_multiply.",
               getValueTypeString().c_str(),
               rhs.getValueTypeString().c_str());
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
    throwDynamicSQLException("Promotion of %s and %s failed in op_divide.",
               getValueTypeString().c_str(),
               rhs.getValueTypeString().c_str());
}

/*
 * Out must have storage for 16 bytes
 */
inline void NValue::murmurHash3(void * out) const {
    const ValueType type = getValueType();
    switch(type) {
    case VALUE_TYPE_TIMESTAMP:
    case VALUE_TYPE_DOUBLE:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_TINYINT:
        MurmurHash3_x64_128( m_data, 8, 0, out);
        break;
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_VARCHAR:
        MurmurHash3_x64_128( getObjectValue(), getObjectLength(), 0, out);
        break;
    default:
        throwFatalException("Unknown type for murmur hashing %d", type);
        break;
    }
}

/*
 * The LHS (this) should always be the string being compared
 * and the RHS should always be the LIKE expression. The planner or EE
 * needs to enforce this.
 */
inline NValue NValue::like(const NValue rhs) const {
    const bool lhsIsNull = isNull();
    const bool rhsIsNull = rhs.isNull();
    if (lhsIsNull || rhsIsNull) {
        return getFalse();
    }

    /*
     * Validate that all params are VARCHAR
     */
    const ValueType mType = getValueType();
    if (mType != VALUE_TYPE_VARCHAR) {
        throwDynamicSQLException(
                "lhs of LIKE expression is %s not %s",
                getValueTypeString().c_str(),
                getTypeName(VALUE_TYPE_VARCHAR).c_str());
    }

    const ValueType rhsType = rhs.getValueType();
    if (rhsType != VALUE_TYPE_VARCHAR) {
        throwDynamicSQLException(
                "rhs of LIKE expression is %s not %s",
                rhs.getValueTypeString().c_str(),
                getTypeName(VALUE_TYPE_VARCHAR).c_str());
    }

    const int32_t valueUTF8Length = getObjectLength();
    const int32_t patternUTF8Length = rhs.getObjectLength();

    if (0 == patternUTF8Length) {
        if (0 == valueUTF8Length) {
            return getTrue();
        } else {
            return getFalse();
        }
    }

    char *valueChars = reinterpret_cast<char*>(getObjectValue());
    char *patternChars = reinterpret_cast<char*>(rhs.getObjectValue());
    assert(valueChars);
    assert(patternChars);

    /*
     * Because lambdas are for poseurs.
     */
    class Liker {

    private:
        // Constructor used internally for temporary recursion contexts.
        Liker( const Liker& original, const char* valueChars, const char* patternChars) :
            m_value(original.m_value, valueChars),
            m_pattern(original.m_pattern, patternChars)
             {}

    public:
        Liker(char *valueChars, char* patternChars, int32_t valueUTF8Length, int32_t patternUTF8Length) :
            m_value(valueChars, valueChars + valueUTF8Length),
            m_pattern(patternChars, patternChars + patternUTF8Length)
             {}

        bool like() {
            while ( ! m_pattern.atEnd()) {
                const uint32_t nextPatternCodePoint = m_pattern.extractCodePoint();
                switch (nextPatternCodePoint) {
                case '%': {
                    if (m_pattern.atEnd()) {
                        return true;
                    }

                    const char *postPercentPatternIterator = m_pattern.getCursor();
                    const uint32_t nextPatternCodePointAfterPercent = m_pattern.extractCodePoint();
                    const bool nextPatternCodePointAfterPercentIsSpecial =
                            (nextPatternCodePointAfterPercent == '_') ||
                            (nextPatternCodePointAfterPercent == '%');

                    /*
                     * This loop tries to skip as many characters as possible with the % by checking
                     * if the next value character matches the pattern character after the %.
                     *
                     * If the next pattern character is special then we always have to recurse to
                     * match that character. For stacked %s this just skips to the last one.
                     * For stacked _ it will recurse and demand the correct number of characters.
                     *
                     * For a regular character it will recurse if the value character matches the pattern character.
                     * This saves doing a function call per character and allows us to skip if there is no match.
                     */
                    while ( ! m_value.atEnd()) {

                        const char *preExtractionValueIterator = m_value.getCursor();
                        const uint32_t nextValueCodePoint = m_value.extractCodePoint();

                        const bool nextPatternCodePointIsSpecialOrItEqualsNextValueCodePoint =
                                (nextPatternCodePointAfterPercentIsSpecial ||
                                        (nextPatternCodePointAfterPercent == nextValueCodePoint));

                        if ( nextPatternCodePointIsSpecialOrItEqualsNextValueCodePoint) {
                            Liker recursionContext( *this, preExtractionValueIterator, postPercentPatternIterator);
                            if (recursionContext.like()) {
                                return true;
                            }
                        }
                    }
                    return false;
                }
                case '_': {
                    if ( m_value.atEnd()) {
                        return false;
                    }
                    //Extract a code point to consume a character
                    m_value.extractCodePoint();
                    break;
                }
                default: {
                    if ( m_value.atEnd()) {
                        return false;
                    }
                    const int nextValueCodePoint = m_value.extractCodePoint();
                    if (nextPatternCodePoint != nextValueCodePoint) {
                        return false;
                    }
                    break;
                }
                }
            }
            //A matching value ends exactly where the pattern ends (having already accounted for '%')
            return m_value.atEnd();
        }

        UTF8Iterator m_value;
        UTF8Iterator m_pattern;
    };

    Liker liker(valueChars, patternChars, valueUTF8Length, patternUTF8Length);

    return liker.like() ? getTrue() : getFalse();
}

} // namespace voltdb

#endif /* NVALUE_HPP_ */
