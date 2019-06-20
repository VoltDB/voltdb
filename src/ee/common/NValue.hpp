/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

#pragma once

#include <cassert>
#include <cfloat>
#include <climits>
#include <cmath>
#include <cstddef>
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

#include "catalog/catalog.h"
#include "common/ExportSerializeIo.h"
#include "common/FatalException.hpp"
#include "common/MiscUtil.h"
#include "common/Pool.hpp"
#include "common/SQLException.h"
#include "common/StringRef.h"
#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/types.h"
#include "common/value_defs.h"
#include "GeographyPointValue.hpp"
#include "GeographyValue.hpp"
#include "utf8.h"
#include "murmur3/MurmurHash3.h"

namespace voltdb {

/*
 * Objects are length preceded with a short length value or a long length value
 * depending on how many bytes are needed to represent the length. These
 * define how many bytes are used for the short value vs. the long value.
 */
#define SHORT_OBJECT_LENGTHLENGTH static_cast<int>(1)
#define OBJECT_NULL_BIT static_cast<char>(1 << 6)
#define OBJECT_CONTINUATION_BIT static_cast<char>(1 << 7)
#define OBJECT_MAX_LENGTH_SHORT_LENGTH 63

#define FULL_STRING_IN_MESSAGE_THRESHOLD 100

//The int used for storage and return values
typedef ttmath::Int<2> TTInt;
//Long integer with space for multiplication and division without carry/overflow
typedef ttmath::Int<4> TTLInt;

template<typename T>
void throwCastSQLValueOutOfRangeException(
        const T value, const ValueType origType, const ValueType newType) {
    throwCastSQLValueOutOfRangeException((const int64_t)value, origType, newType);
}

template<>
inline void throwCastSQLValueOutOfRangeException<double>(
        const double value, const ValueType origType, const ValueType newType) {
    char msg[1024];
    snprintf(msg, 1024, "Type %s with value %f can't be cast as %s because the value is "
            "out of range for the destination type",
            valueToString(origType).c_str(), value, valueToString(newType).c_str());
    throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                       msg);
}

template<>
inline void throwCastSQLValueOutOfRangeException<int64_t>(
      const int64_t value, const ValueType origType, const ValueType newType) {
    char msg[1024];
    snprintf(msg, 1024, "Type %s with value %jd can't be cast as %s because the value is "
            "out of range for the destination type",
            valueToString(origType).c_str(), (intmax_t)value, valueToString(newType).c_str());

    // record underflow or overflow for executors that catch this (indexes, mostly)
    int internalFlags = 0;
    if (value > 0) internalFlags |= SQLException::TYPE_OVERFLOW;
    else if (value < 0) internalFlags |= SQLException::TYPE_UNDERFLOW;

    throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                       msg, internalFlags);
}

template<>
inline void throwCastSQLValueOutOfRangeException<TTInt>(
      const TTInt value, const ValueType origType, const ValueType newType) {
    char msg[1024];
    snprintf(msg, 1024,
          "Type %s with value %s can't be cast as %s because the value is out of range for the destination type",
          valueToString(origType).c_str(), value.ToString().c_str(), valueToString(newType).c_str());

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

inline void throwDataExceptionIfInfiniteOrNaN(double value, const char* function) {
   static int warned_once_no_nan = warn_if(! std::isnan(sqrt(-1.0)),
         "The C++ configuration (e.g. \"g++ --fast-math\") does not support SQL standard handling of NaN errors.");
   static int warned_once_no_inf = warn_if(! non_std_isinf(std::pow(0.0, -1.0)),
         "The C++ configuration (e.g. \"g++ --fast-math\") does not support SQL standard handling of numeric infinity errors.");
   // This uses a standard test for NaN, even though that fails in some configurations like LINUX "g++ -ffast-math".
   // If it is known to fail in the current config, a warning has been sent to the log,
   // so at this point, just relax the check.
   if ((warned_once_no_nan || ! std::isnan(value)) && (warned_once_no_inf || ! non_std_isinf(value))) {
      return;
   } else {
      char msg[1024];
      snprintf(msg, sizeof(msg), "Invalid result value (%f) from floating point %s", value, function);
      throw SQLException(SQLException::data_exception_numeric_value_out_of_range, msg);
   }
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
    vassert(ePos != std::string::npos);
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

    /* Get the serialized size of this NValue. */
    int32_t serializedSize() const;

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

    /** Deserialize a scalar of the specified type from the tuple
        storage area provided. If this is an Object type, the "isInlined"
        argument indicates whether the value is stored directly inline
        in the tupleStorage. **/
    static NValue initFromTupleStorage(const void *storage, ValueType type, bool isInlined, bool isVolatile);

    /** Serialize this NValue's value into the storage area provided.
        This will require an object allocation in two cases.
        In both cases, "isInlined = false" indicates that the tuple storage
        requires an object pointer rather than an inlined value.
        In the first case, "allocateObjects = true" indicates that the tuple
        is persistent and so requires its OWN persistent copy of the object.
        This can happen regardless of whether the original value is stored in
        a persistent object, in a temporary object, or in the inlined storage
        of a tuple.
        In the second case, "getSourceInlined() = true" indicates that
        there is no pre-existing persistent or temp object to share with
        the temp target tuple. If "isInlined = false" indicates that the
        temp tuple requires an object, one must be allocated from the temp
        data Pool provided.
        Note that the POOL argument may either be an instance of Pool
        or an instance of LargeTempTableBlock (these blocks store
        tuples and non-inlined objects in the same buffer).
    */
    template<class POOL>
    void serializeToTupleStorage(void *storage, bool isInlined, int32_t maxLength,
          bool isInBytes, bool allocateObjects, POOL* tempPool) const;

    /** This method is similar to the one above, but accepts no pool
        argument.  If allocation is requested (allocateObjects ==
        true), objects will be copied into persistent relocatable
        storage. */
    void serializeToTupleStorage(void *storage, bool isInlined, int32_t maxLength, bool isInBytes, bool allocateObjects) const {
        serializeToTupleStorage(storage, isInlined, maxLength, isInBytes, allocateObjects, static_cast<Pool*>(NULL));
    }

    /* Deserialize a scalar value of the specified type from the
       SerializeInput directly into the tuple storage area
       provided. This function will perform memory allocations for
       Object types as necessary using the provided data pool or the
       heap. This is used to deserialize tables. */
    template <TupleSerializationFormat F, Endianess E>
    static void deserializeFrom(SerializeInput<E>& input, Pool* tempPool, char* storage,
        const ValueType type, bool isInlined, int32_t maxLength, bool isInBytes);
    static void deserializeFrom(SerializeInputBE& input, Pool* tempPool, char* storage,
        const ValueType type, bool isInlined, int32_t maxLength, bool isInBytes);

        // TODO: no callers use the first form; Should combine these
        // eliminate the potential NValue copy.

    /* Read a ValueType from the SerializeInput stream and deserialize
       a scalar value of the specified type into this NValue from the provided
       SerializeInput and perform allocations as necessary. */
    void deserializeFromAllocateForStorage(SerializeInputBE& input, Pool* tempPool);
    void deserializeFromAllocateForStorage(ValueType vt, SerializeInputBE& input, Pool* tempPool);

    /* Serialize this NValue to a SerializeOutput */
    void serializeTo(SerializeOutput &output) const;

    /* Serialize this NValue to an Export stream */
    size_t serializeToExport_withoutNull(ExportSerializeOutput&) const;

    /* See comment with inlined body, below.  If NULL is supplied for
       the pool, use the temp string pool. */
    void allocateObjectFromPool(Pool* pool = NULL);

    /* Check if the value represents SQL NULL */
    bool isNull() const;

    /* Check if the value represents IEEE 754 NaN */
    bool isNaN() const;

    /* For boolean NValues, convert to bool */
    bool isTrue() const;
    bool isFalse() const;

    /* Tell caller if this NValue's value references memory that may
       be changed or deallcoated. E.g., an inlined string in a
       stand-alone tuple is volatile. */
    bool getVolatile() const {
        return getAttribute(VOLATILE);
    }

    /* For number values, check the number line. */
    bool isZero() const;

    /* For boolean NValues only, logical operators */
    NValue op_negate() const;
    NValue op_and(const NValue& rhs) const;
    NValue op_or(const NValue& rhs) const;

    /* Evaluate the ordering relation against two NValues. Promotes
       exact types to allow disparate type comparison. See also the
       op_ functions which return boolean NValues.
     */
    int compareNull(const NValue& rhs) const;
    int compare(const NValue& rhs) const;
    int compare_withoutNull(const NValue& rhs) const;

    /* Return a boolean NValue with the comparison result */
    NValue op_equals(const NValue& rhs) const;
    NValue op_notEquals(const NValue& rhs) const;
    NValue op_lessThan(const NValue& rhs) const;
    NValue op_lessThanOrEqual(const NValue& rhs) const;
    NValue op_greaterThan(const NValue& rhs) const;
    NValue op_greaterThanOrEqual(const NValue& rhs) const;

    NValue op_equals_withoutNull(const NValue& rhs) const;
    NValue op_notEquals_withoutNull(const NValue& rhs) const;
    NValue op_lessThan_withoutNull(const NValue& rhs) const;
    NValue op_lessThanOrEqual_withoutNull(const NValue& rhs) const;
    NValue op_greaterThan_withoutNull(const NValue& rhs) const;
    NValue op_greaterThanOrEqual_withoutNull(const NValue& rhs) const;


    /* Return a copy of MAX(this, rhs) */
    NValue op_max(const NValue& rhs) const;

    /* Return a copy of MIN(this, rhs) */
    NValue op_min(const NValue& rhs) const;

    /* For number NValues, compute new NValues for arithmetic operators */
    NValue op_increment() const;
    NValue op_decrement() const;
    NValue op_subtract(const NValue& rhs) const;
    NValue op_add(const NValue& rhs) const;
    NValue op_multiply(const NValue& rhs) const;
    NValue op_divide(const NValue& rhs) const;
    NValue op_unary_minus() const;
    /*
     * This NValue must be VARCHAR and the rhs must be VARCHAR.
     * This NValue is the value and the rhs is the pattern
     */
    NValue like(const NValue& rhs) const;
    NValue startsWith(const NValue& rhs) const;

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
    const NValue& itemAtIndex(int index) const;

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

    int32_t murmurHash3() const;

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
            m_end(end) {
            // TODO: We could validate up front that the string is well-formed UTF8,
            // at least to the extent that multi-byte characters have a valid
            // prefix byte and continuation bytes that will not cause a read
            // off the end of the buffer.
            // That done, extractCodePoint could be considerably simpler/faster.
            vassert(m_cursor <= m_end);
            }

        //Construct a one-off with an alternative current cursor position
        UTF8Iterator(const UTF8Iterator& other, const char *start) :
            m_cursor(start), m_end(other.m_end) {
               vassert(m_cursor <= m_end);
            }

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
            } else {
               return m_cursor;
            }
        }

        /*
         * Go through a lot of trouble to make sure that corrupt
         * utf8 data doesn't result in touching uninitialized memory
         * by copying the character data onto the stack.
         * That wouldn't be needed if we pre-validated the buffer.
         */
        uint32_t extractCodePoint() {
            vassert(m_cursor < m_end); // Caller should have tested and handled atEnd() condition
            /*
             * Copy the next 6 bytes to a temp buffer and retrieve.
             * We should only get 4 byte code points, and the library
             * should only accept 4 byte code points, but once upon a time there
             * were 6 byte code points in UTF-8 so be careful here.
             */
            char nextPotentialCodePoint[] = { 0, 0, 0, 0, 0, 0 };
            char *nextPotentialCodePointIter = nextPotentialCodePoint;
            //Copy 6 bytes or until the end
            ::memcpy( nextPotentialCodePoint, m_cursor, std::min(6L, m_end - m_cursor));

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
        bool operator()(const NValue& v1, const NValue& v2) const {
            return v1.compare(v2) < 0;
        }
    };

    /* Functor equality predicate for use with boost unordered */
    struct equal_to : std::binary_function<NValue, NValue, bool> {
        bool operator()(NValue const& x,
            NValue const& y) const {
            return x.compare(y) == 0;
        }
    };

    /* Functor hash predicate for use with boost unordered */
    struct hash : std::unary_function<NValue, std::size_t> {
        std::size_t operator()(NValue const& x) const {
            std::size_t seed = 0;
            x.hashCombine(seed);
            return seed;
        }
    };

    /* Return a string full of arcana and wonder. */
    std::string debug() const;
    std::string toString() const {
        if (isNull()) {
            return "null";
        }

        std::stringstream value;
        const ValueType type = getValueType();
        switch (type) {
        case VALUE_TYPE_TINYINT:
            // This cast keeps the tiny int from being confused for a char.
            value << static_cast<int>(getTinyInt());
            break;
        case VALUE_TYPE_SMALLINT:
            value << getSmallInt();
            break;
        case VALUE_TYPE_INTEGER:
            value << getInteger();
            break;
        case VALUE_TYPE_BIGINT:
            value << getBigInt();
            break;
        case VALUE_TYPE_DOUBLE:
            // Use the specific standard SQL formatting for float values,
            // which the C/C++ format options don't quite support.
            streamSQLFloatFormat(value, getDouble());
            break;
        case VALUE_TYPE_DECIMAL:
            value << createStringFromDecimal();
            break;
        case VALUE_TYPE_VARCHAR: {
            int32_t length;
            const char* buf = getObject_withoutNull(length);
            return std::string(buf, length);
        }
        case VALUE_TYPE_VARBINARY: {
            int32_t length;
            const char* buf = getObject_withoutNull(length);
            // Allocate 2 hex chars per input byte -- plus a terminator
            // because hexEncodeString expects to terminate the result.
            char *scratchBuffer = new char[length * 2 + 1];
            // Don't leak the scratch buffer.
            boost::scoped_array<char> scratchGuard(scratchBuffer);
            catalog::Catalog::hexEncodeString(buf, scratchBuffer, length);
            std::string retval(scratchBuffer, length * 2);
            return retval;
        }
        case VALUE_TYPE_TIMESTAMP:
            streamTimestamp(value);
            break;
        default:
            throwCastSQLException(type, VALUE_TYPE_VARCHAR);
        }
        return value.str();
    }

    // Constants for Decimal type
    // Precision and scale (inherent in the schema)
    static const uint16_t kMaxDecPrec = 38;
    static const uint16_t kMaxDecScale = 12;
    static const int64_t kMaxScaleFactor = 1000000000000;         // == 10**12
  private:
    // Our maximum scale is 12.  Our maximum precision is 38.  So,
    // the maximum number of decimal digits is 38 - 12 = 26.  We can't
    // represent 10**26 in a 64 bit integer, but we can represent 10**18.
    // So, to test if a TTInt named m is too big we test if
    // m/kMaxWholeDivisor < kMaxWholeFactor
    static const uint64_t kMaxWholeDivisor = 100000000;             // == 10**8
    static const uint64_t kMaxWholeFactor = 1000000000000000000;    // == 10**18
    static bool inline oversizeWholeDecimal(TTInt ii) {
        return (TTInt(kMaxWholeFactor) <= ii / kMaxWholeDivisor);
    }
  public:
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

    // Declared public for cppunit test purposes .
    static int64_t parseTimestampString(const std::string &txt);

    static inline int32_t getCharLength(const char *valueChars, const size_t length) {
        // very efficient code to count characters in UTF string and ASCII string
        int32_t j = 0;
        size_t i = length;
        while (i-- > 0) {
            if ((valueChars[i] & 0xc0) != 0x80) j++;
        }
        return j;
    }

    static inline int32_t getIthCharIndex(const char *valueChars, const int64_t length, const int64_t ith) {
        if (ith <= 0) return -1;
        int32_t i = 0, j = 0;

        while (i < length) {
            if ((valueChars[i] & 0xc0) != 0x80) {
                if (++j == ith) break;
            }
            i++;
        }
        return i;
    }

    // Return the beginning char * place of the ith char.
    // Return the end char* when ith is larger than it has, NULL if ith is less and equal to zero.
    static inline const char* getIthCharPosition(const char *valueChars, const size_t length, const int32_t ith) {
        // very efficient code to count characters in UTF string and ASCII string
        int32_t i = getIthCharIndex(valueChars,length, ith);
        if (i < 0) return NULL;
        return &valueChars[i];
    }

    // Copy a value. If the value is inlined in a source tuple, then allocate
    // memory from the temp string pool and copy data there
    NValue copyNValue() const {
        NValue copy = *this;
        if (getSourceInlined()) {
            // The NValue storage is inlined (a pointer to the backing tuple storage) and needs
            // to be copied to a local storage
            copy.allocateObjectFromPool();
        }
        return copy;
    }

    /** Return the amount of memory needed to store this non-inlined
        value in persistent, relocatable storage, not counting the
        pointer to the StringRef in the tuple. */
    std::size_t getAllocationSizeForObjectInPersistentStorage() const {
        if (isNull()) {
            return 0;
        }
        vassert( ! getSourceInlined());
        const StringRef* sref = getObjectPointer();
        return sref->getAllocatedSizeInPersistentStorage();
    }

    /** Return the amount of memory needed to store this non-inlined
        value in temporary storage, not counting the pointer to the
        StringRef in the tuple. */
    std::size_t getAllocationSizeForObjectInTempStorage() const {
        if (isNull()) {
            return 0;
        }
        vassert( ! getSourceInlined());
        const StringRef* sref = getObjectPointer();
        return sref->getAllocatedSizeInTempStorage();
    }

    /** When a large temp table block is loaded, pointers to
        non-inlined data need to get updated. */
    void relocateNonInlined(std::ptrdiff_t offset) {
        if (isNull()) {
            return;
        }

        StringRef* sr = getObjectPointer();
        sr->relocate(offset);
    }

private:
    /*
     * Private methods are private for a reason. Don't expose the raw
     * data so that it can be operated on directly.
     */

    // Function declarations for NValue.cpp definitions.
    void createDecimalFromString(const std::string &txt);
    std::string createStringFromDecimal() const;

    // Helpers for inList.
    // These are purposely not inlines to avoid exposure of NValueList details.
    void deserializeIntoANewNValueList(SerializeInputBE& input, Pool* tempPool);
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
    // These are the bound of converting decimal
    static TTInt s_maxInt64AsDecimal;
    static TTInt s_minInt64AsDecimal;

    enum AttrBits : uint8_t {
        SOURCE_INLINED = 0x1,
        VOLATILE = 0x2
    };

    /**
     * 16 bytes of storage for NValue data.
     */
    char m_data[16];
    ValueType m_valueType;
    uint8_t m_attributes;

    /**
     * Private constructor that initializes storage and the specifies the type of value
     * that will be stored in this instance
     */
    NValue(ValueType type) {
        ::memset(m_data, 0, 16);
        setValueType(type);
        setDefaultAttributes();
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

    /** Mark this value as referencing storage that is inside a tuple. */
    void setSourceInlined(bool val) {
        setAttribute(SOURCE_INLINED, val);
    }

    /* Tell caller if this NValue's value refers back to VARCHAR or
       VARBINARY data internal to a TableTuple (and not a StringRef) */
    bool getSourceInlined() const {
        return getAttribute(SOURCE_INLINED);
    }

    /** Mark this value as referencing storage that is subject to
        change or deallocation during this value's lifetime. */
    void setVolatile(bool val) {
        setAttribute(VOLATILE, val);
    }

    /** Get the value (0 or 1) of the given attribute bit. */
    bool getAttribute(AttrBits bit) const {
        return (m_attributes & bit) != 0;
    }

    /** Set the value (0 or 1) of the given attribute bit. */
    void setAttribute(AttrBits attrBit, bool value) {
        if (value) {
            m_attributes |= attrBit;
        } else {
            m_attributes &= ~attrBit;
        }
    }

    /** Set the default value for all attributes. */
    void setDefaultAttributes() {
        m_attributes = 0x0;
    }

    void tagAsNull() { m_data[13] = OBJECT_NULL_BIT; }
    void tagAsNotNull() { m_data[13] = 0; }

    /**
     * An Object is something like a String that has a variable length
     * (thus it is length preceded) and can potentially have indirect
     * storage (will always be indirect when referenced via an NValue).
     *
     * This method allocates a StringRef object that refers to newly-allocated
     * memory in the provided pool.  StringRef's create method will copy "length"
     * characters from the source buffer into the memory.  If the source pointer
     * is null, the string ref's memory will remain uninitialized.
     */
    StringRef* createObjectPointer(int32_t length, const char* source, Pool* pool) {
        StringRef* object = StringRef::create(length, source, pool);
        *reinterpret_cast<StringRef**>(m_data) = object;
        //*reinterpret_cast<int32_t *>(&m_data[8]) = length;
        tagAsNotNull();
        return object;
    }

    void setObjectPointer(const StringRef* object) {
        *reinterpret_cast<const StringRef**>(m_data) = object;
        //*reinterpret_cast<int32_t*>(&m_data[8]) = object->getObjectLength();
        tagAsNotNull();
    }

    void setNullObjectPointer() {
        *reinterpret_cast<StringRef**>(m_data) = NULL;
        tagAsNull();
    }

    const StringRef* getObjectPointer() const {
       return *reinterpret_cast<const StringRef* const*>(m_data);
    }

    StringRef* getObjectPointer() {
       return *reinterpret_cast<StringRef**>(m_data);
    }

    const char* getObjectValue_withoutNull() const {
        if (getSourceInlined()) {
            return *reinterpret_cast<const char* const*>(m_data) + SHORT_OBJECT_LENGTHLENGTH;
        } else {
           return getObjectPointer()->getObjectValue();
        }
    }

    const char* getObject_withoutNull(int32_t& lengthOut) const {
        if (getSourceInlined()) {
            const char* storage = *reinterpret_cast<const char* const*>(m_data);
            lengthOut = storage[0]; // one-byte length prefix for inline
            return storage + SHORT_OBJECT_LENGTHLENGTH; // skip prefix.
        } else {
           char const* retVal = getObjectPointer()->getObject(lengthOut);
           vassert(lengthOut >= 0);
           return retVal;
        }
    }

    // getters
    const int8_t& getTinyInt() const {
        vassert(getValueType() == VALUE_TYPE_TINYINT);
        return *reinterpret_cast<const int8_t*>(m_data);
    }

    int8_t& getTinyInt() {
        vassert(getValueType() == VALUE_TYPE_TINYINT);
        return *reinterpret_cast<int8_t*>(m_data);
    }

    const int16_t& getSmallInt() const {
        vassert(getValueType() == VALUE_TYPE_SMALLINT);
        return *reinterpret_cast<const int16_t*>(m_data);
    }

    int16_t& getSmallInt() {
        vassert(getValueType() == VALUE_TYPE_SMALLINT);
        return *reinterpret_cast<int16_t*>(m_data);
    }

    const int32_t& getInteger() const {
        vassert(getValueType() == VALUE_TYPE_INTEGER);
        return *reinterpret_cast<const int32_t*>(m_data);
    }

    int32_t& getInteger() {
        vassert(getValueType() == VALUE_TYPE_INTEGER);
        return *reinterpret_cast<int32_t*>(m_data);
    }

    const int64_t& getBigInt() const {
       vassert((getValueType() == VALUE_TYPE_BIGINT) || (getValueType() == VALUE_TYPE_TIMESTAMP) ||
             (getValueType() == VALUE_TYPE_ADDRESS));
        return *reinterpret_cast<const int64_t*>(m_data);
    }

    int64_t& getBigInt() {
        vassert((getValueType() == VALUE_TYPE_BIGINT) ||
               (getValueType() == VALUE_TYPE_TIMESTAMP) ||
               (getValueType() == VALUE_TYPE_ADDRESS));
        return *reinterpret_cast<int64_t*>(m_data);
    }

    const int64_t& getTimestamp() const {
        vassert(getValueType() == VALUE_TYPE_TIMESTAMP);
        return *reinterpret_cast<const int64_t*>(m_data);
    }

    int64_t& getTimestamp() {
        vassert(getValueType() == VALUE_TYPE_TIMESTAMP);
        return *reinterpret_cast<int64_t*>(m_data);
    }

    const double& getDouble() const {
        vassert(getValueType() == VALUE_TYPE_DOUBLE);
        return *reinterpret_cast<const double*>(m_data);
    }

    double& getDouble() {
        vassert(getValueType() == VALUE_TYPE_DOUBLE);
        return *reinterpret_cast<double*>(m_data);
    }

    const TTInt& getDecimal() const {
        vassert(getValueType() == VALUE_TYPE_DECIMAL);
        const void* retval = reinterpret_cast<const void*>(m_data);
        return *reinterpret_cast<const TTInt*>(retval);
    }

    TTInt& getDecimal() {
        vassert(getValueType() == VALUE_TYPE_DECIMAL);
        void* retval = reinterpret_cast<void*>(m_data);
        return *reinterpret_cast<TTInt*>(retval);
    }

    const bool& getBoolean() const {
        vassert(getValueType() == VALUE_TYPE_BOOLEAN);
        return *reinterpret_cast<const bool*>(m_data);
    }

    bool& getBoolean() {
        vassert(getValueType() == VALUE_TYPE_BOOLEAN);
        return *reinterpret_cast<bool*>(m_data);
    }

    GeographyPointValue& getGeographyPointValue() {
        vassert(getValueType() == VALUE_TYPE_POINT);
        return *reinterpret_cast<GeographyPointValue*>(m_data);
    }

    const GeographyPointValue& getGeographyPointValue() const {

        BOOST_STATIC_ASSERT_MSG(sizeof(GeographyPointValue) <= sizeof(m_data),
                                "Size of Point is too large for NValue m_data");

        vassert(getValueType() == VALUE_TYPE_POINT);
        return *reinterpret_cast<const GeographyPointValue*>(m_data);
    }

    const GeographyValue getGeographyValue() const {
        vassert(getValueType() == VALUE_TYPE_GEOGRAPHY);

        if (isNull()) {
            return GeographyValue();
        } else {
           int32_t objectLength;
           const char* objectValue = getObject_withoutNull(objectLength);
           return GeographyValue(objectValue, objectLength);
        }
    }

    bool isBooleanNULL() const ;

    static void throwCastSQLException(const ValueType origType, const ValueType newType) {
        char msg[1024];
        snprintf(msg, 1024, "Type %s can't be cast as %s",
                 valueToString(origType).c_str(), valueToString(newType).c_str());
        throw SQLException(SQLException::data_exception_most_specific_type_mismatch, msg);
    }

    /** return the whole part of a TTInt*/
    static inline int64_t narrowDecimalToBigInt(TTInt &scaledValue) {
        if (scaledValue > NValue::s_maxInt64AsDecimal || scaledValue < NValue::s_minInt64AsDecimal) {
            throwCastSQLValueOutOfRangeException<TTInt>(scaledValue, VALUE_TYPE_DECIMAL, VALUE_TYPE_BIGINT);
        }
        TTInt whole(scaledValue);
        whole /= kMaxScaleFactor;
        return whole.ToInt();
    }

    /** return the fractional part of a TTInt*/
    static inline int64_t getFractionalPart(TTInt& scaledValue) {
        TTInt fractional(scaledValue);
        fractional %= kMaxScaleFactor;
        return fractional.ToInt();
    }

    /**
     * Implicitly converting function to big integer type
     * DOUBLE, DECIMAL should not be handled here
     */
    int64_t castAsBigIntAndGetValue() const {
       vassert(isNull() == false);

       const ValueType type = getValueType();
       vassert(type != VALUE_TYPE_NULL);
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

    /**
     * Implicitly converting function to integer type
     * DOUBLE, DECIMAL should not be handled here
     */
    int32_t castAsIntegerAndGetValue() const {
        vassert(isNull() == false);

        const ValueType type = getValueType();
        int64_t value;
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
            value = getBigInt();
            if (value > (int64_t)INT32_MAX || value < (int64_t)VOLT_INT32_MIN) {
                throwCastSQLValueOutOfRangeException<int64_t>(value, VALUE_TYPE_BIGINT, VALUE_TYPE_INTEGER);
            }
            return static_cast<int32_t>(value);
        default:
            throwCastSQLException(type, VALUE_TYPE_INTEGER);
            return 0; // NOT REACHED
        }
    }

    double castAsDoubleAndGetValue() const {
        vassert(isNull() == false);
        const ValueType type = getValueType();
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
            TTInt scaledValue = getDecimal();
            // we only deal with the decimal number within int64_t range here
            int64_t whole = narrowDecimalToBigInt(scaledValue);
            int64_t fractional = getFractionalPart(scaledValue);
            return static_cast<double>(whole) + (static_cast<double>(fractional)/static_cast<double>(kMaxScaleFactor));
        }
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_VARBINARY:
        default:
            throwCastSQLException(type, VALUE_TYPE_DOUBLE);
            return 0; // NOT REACHED
        }
    }

    TTInt castAsDecimalAndGetValue() const {
        vassert(isNull() == false);
        const ValueType type = getValueType();
        switch (type) {
          case VALUE_TYPE_TINYINT:
          case VALUE_TYPE_SMALLINT:
          case VALUE_TYPE_INTEGER:
          case VALUE_TYPE_BIGINT:
          case VALUE_TYPE_TIMESTAMP: {
            int64_t value = castAsBigIntAndGetValue();
            TTInt retval(value);
            retval *= kMaxScaleFactor;
            return retval;
          }
          case VALUE_TYPE_DECIMAL:
              return getDecimal();
          case VALUE_TYPE_DOUBLE: {
            int64_t intValue = castAsBigIntAndGetValue();
            TTInt retval(intValue);
            retval *= kMaxScaleFactor;

            double value = getDouble();
            value -= static_cast<double>(intValue); // isolate decimal part
            value *= static_cast<double>(kMaxScaleFactor); // scale up to integer.
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

    /**
     * This function does not check NULL value.
     */
    double getNumberFromString() const {
        vassert(isNull() == false);
        int32_t length;
        const char* buf = getObject_withoutNull(length);
        // Guarantee termination at end of object -- or strtod might not stop there.
        char safeBuffer[length+1];
        memcpy(safeBuffer, buf, length);
        safeBuffer[length] = '\0';
        char * bufferEnd = safeBuffer;
        double result = strtod(safeBuffer, &bufferEnd);
        // Needs to have consumed SOMETHING.
        if (bufferEnd > safeBuffer) {
            // Unconsumed trailing chars are OK if they are whitespace.
            while (bufferEnd < safeBuffer+length && isspace(*bufferEnd)) {
                ++bufferEnd;
            }
            if (bufferEnd == safeBuffer + length) {
                return result;
            }
        }

        std::ostringstream oss;
        oss << "Could not convert to number: '" << safeBuffer << "' contains invalid character value.";
        throw SQLException(SQLException::data_exception_invalid_character_value_for_cast, oss.str().c_str());
    }

    NValue castAsBigInt() const {
        vassert(isNull() == false);

        NValue retval(VALUE_TYPE_BIGINT);
        const ValueType type = getValueType();
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getBigInt() = static_cast<int64_t>(getTinyInt());
            break;
        case VALUE_TYPE_SMALLINT:
            retval.getBigInt() = static_cast<int64_t>(getSmallInt());
            break;
        case VALUE_TYPE_INTEGER:
            retval.getBigInt() = static_cast<int64_t>(getInteger());
            break;
        case VALUE_TYPE_ADDRESS:
            retval.getBigInt() = getBigInt();
            break;
        case VALUE_TYPE_BIGINT:
            return *this;
        case VALUE_TYPE_TIMESTAMP:
            retval.getBigInt() = getTimestamp();
            break;
        case VALUE_TYPE_DOUBLE:
            if (getDouble() > (double)INT64_MAX || getDouble() < (double)VOLT_INT64_MIN) {
                throwCastSQLValueOutOfRangeException<double>(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
            }
            retval.getBigInt() = static_cast<int64_t>(getDouble());
            break;
        case VALUE_TYPE_DECIMAL: {
            TTInt scaledValue = getDecimal();
            retval.getBigInt() = narrowDecimalToBigInt(scaledValue);
            break;
        }
        case VALUE_TYPE_VARCHAR:
            retval.getBigInt() = static_cast<int64_t>(getNumberFromString());
            break;
        case VALUE_TYPE_VARBINARY:
        default:
            throwCastSQLException(type, VALUE_TYPE_BIGINT);
        }
        return retval;
    }

    NValue castAsTimestamp() const {
        vassert(isNull() == false);

        NValue retval(VALUE_TYPE_TIMESTAMP);
        const ValueType type = getValueType();
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getTimestamp() = static_cast<int64_t>(getTinyInt());
            break;
        case VALUE_TYPE_SMALLINT:
            retval.getTimestamp() = static_cast<int64_t>(getSmallInt());
            break;
        case VALUE_TYPE_INTEGER:
            retval.getTimestamp() = static_cast<int64_t>(getInteger());
            break;
        case VALUE_TYPE_BIGINT:
            retval.getTimestamp() = getBigInt();
            break;
        case VALUE_TYPE_TIMESTAMP:
            retval.getTimestamp() = getTimestamp();
            break;
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
            retval.getTimestamp() = static_cast<int64_t>(getDouble());
            break;
        case VALUE_TYPE_DECIMAL: {
            // TODO: Consider just eliminating this switch case to throw a cast exception,
            // or explicitly throwing some other exception here.
            // Direct cast of decimal to timestamp (implemented via intermediate cast to integer, here)
            // is not a SQL standard requirement, may not even make it past the planner's type-checks,
            // or may just be too far a stretch.
            // OR it might be a convenience for some obscure system-generated edge case?

            TTInt scaledValue = getDecimal();
            retval.getTimestamp() = narrowDecimalToBigInt(scaledValue);
            break;
        }
        case VALUE_TYPE_VARCHAR: {
            int32_t length;
            const char* buf = getObject_withoutNull(length);
            const std::string value(buf, length);
            retval.getTimestamp() = parseTimestampString(value);
            break;
        }
        case VALUE_TYPE_VARBINARY:
        default:
            throwCastSQLException(type, VALUE_TYPE_TIMESTAMP);
        }
        return retval;
    }

    template <typename T>
    void narrowToInteger(const T value, ValueType sourceType) {
        if (value > (T)INT32_MAX || value < (T)VOLT_INT32_MIN) {
            throwCastSQLValueOutOfRangeException(value, sourceType, VALUE_TYPE_INTEGER);
        }
        getInteger() = static_cast<int32_t>(value);
    }

    NValue castAsInteger() const {
        NValue retval(VALUE_TYPE_INTEGER);
        const ValueType type = getValueType();
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getInteger() = static_cast<int32_t>(getTinyInt());
            break;
        case VALUE_TYPE_SMALLINT:
            retval.getInteger() = static_cast<int32_t>(getSmallInt());
            break;
        case VALUE_TYPE_INTEGER:
            return *this;
        case VALUE_TYPE_BIGINT:
            retval.narrowToInteger(getBigInt(), type);
            break;
        case VALUE_TYPE_TIMESTAMP:
            retval.narrowToInteger(getTimestamp(), type);
            break;
        case VALUE_TYPE_DOUBLE:
            retval.narrowToInteger(getDouble(), type);
            break;
        case VALUE_TYPE_DECIMAL: {
            TTInt scaledValue = getDecimal();
            // get the whole part of the decimal
            int64_t whole = narrowDecimalToBigInt(scaledValue);
            // try to convert the whole part, which is a int64_t
            retval.narrowToInteger(whole, VALUE_TYPE_BIGINT);
            break;
        }
        case VALUE_TYPE_VARCHAR:
            retval.narrowToInteger(getNumberFromString(), type);
            break;
        case VALUE_TYPE_VARBINARY:
        default:
            throwCastSQLException(type, VALUE_TYPE_INTEGER);
        }
        return retval;
    }

    template <typename T>
    void narrowToSmallInt(const T value, ValueType sourceType) {
        if (value > (T)INT16_MAX || value < (T)VOLT_INT16_MIN) {
            throwCastSQLValueOutOfRangeException(value, sourceType, VALUE_TYPE_SMALLINT);
        }
        getSmallInt() = static_cast<int16_t>(value);
    }

    NValue castAsSmallInt() const {
        vassert(isNull() == false);

        NValue retval(VALUE_TYPE_SMALLINT);
        const ValueType type = getValueType();
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getSmallInt() = static_cast<int16_t>(getTinyInt());
            break;
        case VALUE_TYPE_SMALLINT:
            retval.getSmallInt() = getSmallInt();
            break;
        case VALUE_TYPE_INTEGER:
            retval.narrowToSmallInt(getInteger(), type);
            break;
        case VALUE_TYPE_BIGINT:
            retval.narrowToSmallInt(getBigInt(), type);
            break;
        case VALUE_TYPE_TIMESTAMP:
            retval.narrowToSmallInt(getTimestamp(), type);
            break;
        case VALUE_TYPE_DOUBLE:
            retval.narrowToSmallInt(getDouble(), type);
            break;
        case VALUE_TYPE_DECIMAL: {
            TTInt scaledValue = getDecimal();
            int64_t whole = narrowDecimalToBigInt(scaledValue);
            retval.narrowToSmallInt(whole, VALUE_TYPE_BIGINT);
            break;
        }
        case VALUE_TYPE_VARCHAR:
            retval.narrowToSmallInt(getNumberFromString(), type);
            break;
        case VALUE_TYPE_VARBINARY:
        default:
            throwCastSQLException(type, VALUE_TYPE_SMALLINT);
        }
        return retval;
    }

    template <typename T>
    void narrowToTinyInt(const T value, ValueType sourceType) {
        if (value > (T)INT8_MAX || value < (T)VOLT_INT8_MIN) {
            throwCastSQLValueOutOfRangeException(value, sourceType, VALUE_TYPE_TINYINT);
        }
        getTinyInt() = static_cast<int8_t>(value);
    }

    NValue castAsTinyInt() const {
        vassert(isNull() == false);

        NValue retval(VALUE_TYPE_TINYINT);
        const ValueType type = getValueType();
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getTinyInt() = getTinyInt();
            break;
        case VALUE_TYPE_SMALLINT:
            retval.narrowToTinyInt(getSmallInt(), type);
            break;
        case VALUE_TYPE_INTEGER:
            retval.narrowToTinyInt(getInteger(), type);
            break;
        case VALUE_TYPE_BIGINT:
            retval.narrowToTinyInt(getBigInt(), type);
            break;
        case VALUE_TYPE_TIMESTAMP:
            retval.narrowToTinyInt(getTimestamp(), type);
            break;
        case VALUE_TYPE_DOUBLE:
            retval.narrowToTinyInt(getDouble(), type);
            break;
        case VALUE_TYPE_DECIMAL: {
            TTInt scaledValue = getDecimal();
            int64_t whole = narrowDecimalToBigInt(scaledValue);
            retval.narrowToTinyInt(whole, type);
            break;
        }
        case VALUE_TYPE_VARCHAR:
            retval.narrowToTinyInt(getNumberFromString(), type);
            break;
        case VALUE_TYPE_VARBINARY:
        default:
            throwCastSQLException(type, VALUE_TYPE_TINYINT);
        }
        return retval;
    }

    NValue castAsDouble() const {
        vassert(isNull() == false);

        NValue retval(VALUE_TYPE_DOUBLE);
        const ValueType type = getValueType();
        switch (type) {
        case VALUE_TYPE_TINYINT:
            retval.getDouble() = static_cast<double>(getTinyInt());
            break;
        case VALUE_TYPE_SMALLINT:
            retval.getDouble() = static_cast<double>(getSmallInt());
            break;
        case VALUE_TYPE_INTEGER:
            retval.getDouble() = static_cast<double>(getInteger());
            break;
        case VALUE_TYPE_BIGINT:
            retval.getDouble() = static_cast<double>(getBigInt());
            break;
        case VALUE_TYPE_TIMESTAMP:
            retval.getDouble() = static_cast<double>(getTimestamp());
            break;
        case VALUE_TYPE_DOUBLE:
            retval.getDouble() = getDouble();
            break;
        case VALUE_TYPE_DECIMAL:
            retval.getDouble() = castAsDoubleAndGetValue();
            break;
        case VALUE_TYPE_VARCHAR:
            retval.getDouble() = getNumberFromString();
            break;
        case VALUE_TYPE_VARBINARY:
        default:
            throwCastSQLException(type, VALUE_TYPE_DOUBLE);
        }
        return retval;
    }

    void streamTimestamp(std::stringstream& value) const;

    NValue castAsString() const {
        vassert(isNull() == false);

        std::stringstream value;
        const ValueType type = getValueType();
        switch (type) {
        case VALUE_TYPE_TINYINT:
            // This cast keeps the tiny int from being confused for a char.
            value << static_cast<int>(getTinyInt());
            break;
        case VALUE_TYPE_SMALLINT:
            value << getSmallInt();
            break;
        case VALUE_TYPE_INTEGER:
            value << getInteger();
            break;
        case VALUE_TYPE_BIGINT:
            value << getBigInt();
            break;
        //case VALUE_TYPE_TIMESTAMP:
            //TODO: The SQL standard wants an actual date literal rather than a numeric value, here. See ENG-4284.
            //value << static_cast<double>(getTimestamp());
            //break;
        case VALUE_TYPE_DOUBLE:
            // Use the specific standard SQL formatting for float values,
            // which the C/C++ format options don't quite support.
            streamSQLFloatFormat(value, getDouble());
            break;
        case VALUE_TYPE_DECIMAL:
            value << createStringFromDecimal();
            break;
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_VARBINARY: {
        // note: we allow binary conversion to strings to support
        // byte[] as string parameters...
        // In the future, it would be nice to check this is a decent string here...
            NValue retval(VALUE_TYPE_VARCHAR);
            retval.setSourceInlined(getSourceInlined());
            memcpy(retval.m_data, m_data, sizeof(m_data));
            return retval;
        }
        case VALUE_TYPE_TIMESTAMP: {
            streamTimestamp(value);
            break;
        }
        case VALUE_TYPE_POINT: {
            value << getGeographyPointValue().toWKT();
            break;
        }
        case VALUE_TYPE_GEOGRAPHY: {
            value << getGeographyValue().toWKT();
            break;
        }
        default:
            throwCastSQLException(type, VALUE_TYPE_VARCHAR);
        }
        return getTempStringValue(value.str().c_str(), value.str().length());
    }

    NValue castAsBinary() const {
        vassert(isNull() == false);

        NValue retval(VALUE_TYPE_VARBINARY);
        const ValueType type = getValueType();
        switch (type) {
        case VALUE_TYPE_VARBINARY:
            memcpy(retval.m_data, m_data, sizeof(m_data));
            break;
        default:
            throwCastSQLException(type, VALUE_TYPE_VARBINARY);
        }
        return retval;
    }

    void createDecimalFromInt(int64_t rhsint) {
        TTInt scaled(rhsint);
        scaled *= kMaxScaleFactor;
        getDecimal() = scaled;
    }

    NValue castAsDecimal() const {
        vassert(isNull() == false);
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
            int32_t length;
            const char* buf = getObject_withoutNull(length);
            const std::string value(buf, length);
            retval.createDecimalFromString(value);
            break;
        }
        default:
            throwCastSQLException(type, VALUE_TYPE_DECIMAL);
        }
        return retval;
    }

    NValue castAsGeography() const {
        NValue retval(VALUE_TYPE_GEOGRAPHY);
        if (isNull()) {
            retval.setNull();
            return retval;
        }
        const ValueType type = getValueType();
        switch (type) {
            case VALUE_TYPE_GEOGRAPHY:
                memcpy(retval.m_data, m_data, sizeof(m_data));
                break;
            case VALUE_TYPE_VARCHAR:
            default:
                throwCastSQLException(type, VALUE_TYPE_GEOGRAPHY);
        }
        return retval;
    }

    NValue castAsGeographyPoint() const {

        NValue retval(VALUE_TYPE_POINT);
        if (isNull()) {
            retval.setNull();
            return retval;
        }
        const ValueType type = getValueType();
        switch (type) {
            case VALUE_TYPE_POINT:
                memcpy(retval.m_data, m_data, sizeof(m_data));
                break;
            case VALUE_TYPE_VARCHAR:
            default:
                throwCastSQLException(type, VALUE_TYPE_POINT);
        }
        return retval;
    }
    /**
     * Copy the arbitrary size object that this value points to as an
     * inline object in the provided tuple storage area
     */
    void serializeInlineObjectToTupleStorage(char* storage, int32_t maxLength, bool isInBytes) const {
        if (isNull()) {
            // Always reset all the bits regardless of the actual length of the value
            // 1 additional byte for the length prefix
            ::memset(storage, 0, maxLength + 1);

            /*
             * The 7th bit of the length preceding value
             * is used to indicate that the object is null.
             */
            storage[0] = OBJECT_NULL_BIT;
            return;
        }
        int32_t length;
        const char* buf = getObject_withoutNull(length);
        checkTooWideForVariableLengthType(m_valueType, buf, length, maxLength, isInBytes);

        // Always reset all the bits regardless of the actual length of the value.
        // Offset 1 byte for the length prefix
        ::memset(storage + SHORT_OBJECT_LENGTHLENGTH, 0, maxLength);
        storage[0] = static_cast<char>(length);
        ::memcpy(storage + SHORT_OBJECT_LENGTHLENGTH, buf, length);
    }

    static inline bool validVarcharSize(const char *valueChars, size_t length, int32_t maxLength) {
        int32_t min_continuation_bytes = static_cast<int32_t>(length - maxLength);
        if (min_continuation_bytes <= 0) {
            return true;
        }
        size_t i = length;
        while (i--) {
            if ((valueChars[i] & 0xc0) == 0x80) {
                if (--min_continuation_bytes == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Assuming non-null NValue, validate the size of the variable length data
     */
    static inline void checkTooWideForVariableLengthType(ValueType type, const char* ptr,
          int32_t objLength, int32_t maxLength, bool isInBytes) {
       if (maxLength == 0) {
          throwFatalLogicErrorStreamed("Zero maxLength for object type " << valueToString(type));
       }

       switch (type) {
          case VALUE_TYPE_VARBINARY:
          case VALUE_TYPE_GEOGRAPHY:
             if (objLength > maxLength) {
                std::ostringstream oss;
                oss <<  "The size " << objLength << " of the value exceeds the size of ";
                if (type == VALUE_TYPE_VARBINARY) {
                   oss << "the VARBINARY(" << maxLength << ") column";
                   throw SQLException(SQLException::data_exception_string_data_length_mismatch,
                         oss.str().c_str(), SQLException::TYPE_VAR_LENGTH_MISMATCH);
                } else {
                   oss << "the GEOGRAPHY column (" << maxLength << " bytes)";
                   throw SQLException(SQLException::data_exception_string_data_length_mismatch, oss.str().c_str());
                }
             }
             break;
          case VALUE_TYPE_VARCHAR:
             if (isInBytes) {
                if (objLength > maxLength) {
                   std::string inputValue;
                   if (objLength > FULL_STRING_IN_MESSAGE_THRESHOLD) {
                      inputValue = std::string(ptr, FULL_STRING_IN_MESSAGE_THRESHOLD) + std::string("...");
                   } else {
                      inputValue = std::string(ptr, objLength);
                   }
                   char msg[1024];
                   snprintf(msg, 1024,
                         "The size %d of the value '%s' exceeds the size of the VARCHAR(%d BYTES) column",
                         objLength, inputValue.c_str(), maxLength);
                   throw SQLException(SQLException::data_exception_string_data_length_mismatch,
                         msg, SQLException::TYPE_VAR_LENGTH_MISMATCH);
                }
             } else if (!validVarcharSize(ptr, objLength, maxLength)) {
                const int32_t charLength = getCharLength(ptr, objLength);
                char msg[1024];
                std::string inputValue;
                if (charLength > FULL_STRING_IN_MESSAGE_THRESHOLD) {
                   const char * end = getIthCharPosition(ptr, objLength, FULL_STRING_IN_MESSAGE_THRESHOLD+1);
                   int32_t numBytes = (int32_t)(end - ptr);
                   inputValue = std::string(ptr, numBytes) + std::string("...");
                } else {
                   inputValue = std::string(ptr, objLength);
                }
                snprintf(msg, 1024,
                      "The size %d of the value '%s' exceeds the size of the VARCHAR(%d) column",
                      charLength, inputValue.c_str(), maxLength);

                throw SQLException(SQLException::data_exception_string_data_length_mismatch,
                      msg, SQLException::TYPE_VAR_LENGTH_MISMATCH);
             }
             break;
          default:
             throwFatalLogicErrorStreamed("NValue::checkTooWideForVariableLengthType, Invalid object type " << valueToString(type));
       }
    }

    template<typename T>
    int compareValue (const T lhsValue, const T rhsValue) const {
        if (lhsValue == rhsValue) {
            return VALUE_COMPARE_EQUAL;
        } else if (lhsValue > rhsValue) {
            return VALUE_COMPARE_GREATERTHAN;
        } else {
           return VALUE_COMPARE_LESSTHAN;
        }
    }

    int compareDoubleValue (const double lhsValue, const double rhsValue) const {
        // Treat NaN values as equals and also make them smaller than negative infinity.
        // This breaks IEEE754 for expressions slightly.
        if (std::isnan(lhsValue)) {
            return std::isnan(rhsValue) ? VALUE_COMPARE_EQUAL : VALUE_COMPARE_LESSTHAN;
        } else if (std::isnan(rhsValue)) {
            return VALUE_COMPARE_GREATERTHAN;
        } else if (lhsValue > rhsValue) {
            return VALUE_COMPARE_GREATERTHAN;
        } else if (lhsValue < rhsValue) {
            return VALUE_COMPARE_LESSTHAN;
        } else {
           return VALUE_COMPARE_EQUAL;
        }
    }

    int compareTinyInt (const NValue& rhs) const {
        vassert(m_valueType == VALUE_TYPE_TINYINT);

        // get the right hand side as a bigint
        if (rhs.getValueType() == VALUE_TYPE_DOUBLE) {
            return compareDoubleValue(static_cast<double>(getTinyInt()), rhs.getDouble());
        } else if (rhs.getValueType() == VALUE_TYPE_DECIMAL) {
            const TTInt rhsValue = rhs.getDecimal();
            TTInt lhsValue(static_cast<int64_t>(getTinyInt()));
            lhsValue *= kMaxScaleFactor;
            return compareValue<TTInt>(lhsValue, rhsValue);
        } else {
           int64_t lhsValue, rhsValue;
           lhsValue = static_cast<int64_t>(getTinyInt());
           rhsValue = rhs.castAsBigIntAndGetValue();
           return compareValue<int64_t>(lhsValue, rhsValue);
        }
    }

    int compareSmallInt (const NValue& rhs) const {
        vassert(m_valueType == VALUE_TYPE_SMALLINT);

        // get the right hand side as a bigint
        if (rhs.getValueType() == VALUE_TYPE_DOUBLE) {
            return compareDoubleValue(static_cast<double>(getSmallInt()), rhs.getDouble());
        } else if (rhs.getValueType() == VALUE_TYPE_DECIMAL) {
            const TTInt rhsValue = rhs.getDecimal();
            TTInt lhsValue(static_cast<int64_t>(getSmallInt()));
            lhsValue *= kMaxScaleFactor;
            return compareValue<TTInt>(lhsValue, rhsValue);
        } else {
           int64_t lhsValue, rhsValue;
           lhsValue = static_cast<int64_t>(getSmallInt());
           rhsValue = rhs.castAsBigIntAndGetValue();
           return compareValue<int64_t>(lhsValue, rhsValue);
        }
    }

    int compareInteger (const NValue& rhs) const {
        vassert(m_valueType == VALUE_TYPE_INTEGER);

        // get the right hand side as a bigint
        if (rhs.getValueType() == VALUE_TYPE_DOUBLE) {
            return compareDoubleValue(static_cast<double>(getInteger()), rhs.getDouble());
        } else if (rhs.getValueType() == VALUE_TYPE_DECIMAL) {
            const TTInt rhsValue = rhs.getDecimal();
            TTInt lhsValue(static_cast<int64_t>(getInteger()));
            lhsValue *= kMaxScaleFactor;
            return compareValue<TTInt>(lhsValue, rhsValue);
        } else {
           int64_t lhsValue, rhsValue;
           lhsValue = static_cast<int64_t>(getInteger());
           rhsValue = rhs.castAsBigIntAndGetValue();
           return compareValue<int64_t>(lhsValue, rhsValue);
        }
    }

    int compareBigInt (const NValue& rhs) const {
        vassert(m_valueType == VALUE_TYPE_BIGINT);

        // get the right hand side as a bigint
        if (rhs.getValueType() == VALUE_TYPE_DOUBLE) {
            return compareDoubleValue(static_cast<double>(getBigInt()), rhs.getDouble());
        } else if (rhs.getValueType() == VALUE_TYPE_DECIMAL) {
            const TTInt rhsValue = rhs.getDecimal();
            TTInt lhsValue(getBigInt());
            lhsValue *= kMaxScaleFactor;
            return compareValue<TTInt>(lhsValue, rhsValue);
        }
        int64_t lhsValue, rhsValue;
        lhsValue = getBigInt();
        rhsValue = rhs.castAsBigIntAndGetValue();
        return compareValue<int64_t>(lhsValue, rhsValue);
    }


    int compareBooleanValue (const NValue &rhs) const {
        vassert(m_valueType == VALUE_TYPE_BOOLEAN);

        // get the right hand side as an integer.
        if (rhs.getValueType() == VALUE_TYPE_BOOLEAN) {
            bool rhsValue = rhs.getBoolean();
            bool lhsValue = getBoolean();
            if (lhsValue == rhsValue) {
                return 0;
            }
            // False < True.  So,
            //    compare(False, True)  = 1
            //    compare(True,  False) = -1
            if (lhsValue) {
                return -1;
            } else {
               return 1;
            }
        }
        char message[128];
        snprintf(message, 128,
                 "Type %s cannot be cast for comparison to type %s",
                 valueToString(rhs.getValueType()).c_str(),
                 valueToString(getValueType()).c_str());
         throw SQLException(SQLException::data_exception_most_specific_type_mismatch, message);
         // Not reached
    }

    int compareTimestamp (const NValue& rhs) const {
        vassert(m_valueType == VALUE_TYPE_TIMESTAMP);

        // get the right hand side as a bigint
        if (rhs.getValueType() == VALUE_TYPE_DOUBLE) {
            return compareDoubleValue(static_cast<double>(getTimestamp()), rhs.getDouble());
        } else if (rhs.getValueType() == VALUE_TYPE_DECIMAL) {
            const TTInt rhsValue = rhs.getDecimal();
            TTInt lhsValue(getTimestamp());
            lhsValue *= kMaxScaleFactor;
            return compareValue<TTInt>(lhsValue, rhsValue);
        } else {
           int64_t lhsValue, rhsValue;
           lhsValue = getTimestamp();
           rhsValue = rhs.castAsBigIntAndGetValue();
           return compareValue<int64_t>(lhsValue, rhsValue);
        }
    }

    int compareDoubleValue (const NValue& rhs) const {
        vassert(m_valueType == VALUE_TYPE_DOUBLE);

        const double lhsValue = getDouble();
        double rhsValue;

        switch (rhs.getValueType()) {
        case VALUE_TYPE_DOUBLE:
            rhsValue = rhs.getDouble();
            break;
        case VALUE_TYPE_TINYINT:
            rhsValue = static_cast<double>(rhs.getTinyInt());
            break;
        case VALUE_TYPE_SMALLINT:
            rhsValue = static_cast<double>(rhs.getSmallInt());
            break;
        case VALUE_TYPE_INTEGER:
            rhsValue = static_cast<double>(rhs.getInteger());
            break;
        case VALUE_TYPE_BIGINT:
            rhsValue = static_cast<double>(rhs.getBigInt());
            break;
        case VALUE_TYPE_TIMESTAMP:
            rhsValue = static_cast<double>(rhs.getTimestamp());
            break;
        case VALUE_TYPE_DECIMAL: {
            TTInt scaledValue = rhs.getDecimal();
            TTInt whole(scaledValue);
            TTInt fractional(scaledValue);
            whole /= kMaxScaleFactor;
            fractional %= kMaxScaleFactor;
            rhsValue = static_cast<double>(whole.ToInt()) +
                    (static_cast<double>(fractional.ToInt())/static_cast<double>(kMaxScaleFactor));
            break;
        }
        default:
            char message[128];
            snprintf(message, 128,
                    "Type %s cannot be cast for comparison to type %s",
                    valueToString(rhs.getValueType()).c_str(),
                    valueToString(getValueType()).c_str());
            throw SQLException(SQLException::
                    data_exception_most_specific_type_mismatch,
                    message);
            // Not reached
        }

        return compareDoubleValue(lhsValue, rhsValue);
    }

    int compareStringValue (const NValue& rhs) const {
        vassert(m_valueType == VALUE_TYPE_VARCHAR);

        ValueType rhsType = rhs.getValueType();
        if (rhsType != VALUE_TYPE_VARCHAR) {
            char message[128];
            snprintf(message, 128, "Type %s cannot be cast for comparison to type %s",
                    valueToString(rhsType).c_str(), valueToString(m_valueType).c_str());
            throw SQLException(SQLException::data_exception_most_specific_type_mismatch, message);
        }

        vassert(m_valueType == VALUE_TYPE_VARCHAR);

        int32_t leftLength;
        const char* left = getObject_withoutNull(leftLength);
        int32_t rightLength;
        const char* right = rhs.getObject_withoutNull(rightLength);

        int result = ::strncmp(left, right, std::min(leftLength, rightLength));
        if (result == 0) {
            result = leftLength - rightLength;
        }
        if (result > 0) {
            return VALUE_COMPARE_GREATERTHAN;
        } else if (result < 0) {
            return VALUE_COMPARE_LESSTHAN;
        } else {
           return VALUE_COMPARE_EQUAL;
        }
    }

    int compareBinaryValue (const NValue& rhs) const {
        vassert(m_valueType == VALUE_TYPE_VARBINARY);

        if (rhs.getValueType() != VALUE_TYPE_VARBINARY) {
            char message[128];
            snprintf(message, 128,
                     "Type %s cannot be cast for comparison to type %s",
                     valueToString(rhs.getValueType()).c_str(),
                     valueToString(m_valueType).c_str());
            throw SQLException(SQLException::
                               data_exception_most_specific_type_mismatch,
                               message);
        }
        int32_t leftLength;
        const char* left = getObject_withoutNull(leftLength);
        int32_t rightLength;
        const char* right = rhs.getObject_withoutNull(rightLength);

        const int result = ::memcmp(left, right, std::min(leftLength, rightLength));
        if (result == 0 && leftLength != rightLength) {
            if (leftLength > rightLength) {
                return  VALUE_COMPARE_GREATERTHAN;
            } else {
               return VALUE_COMPARE_LESSTHAN;
            }
        } else if (result > 0) {
            return VALUE_COMPARE_GREATERTHAN;
        } else if (result < 0) {
            return VALUE_COMPARE_LESSTHAN;
        } else {
           return VALUE_COMPARE_EQUAL;
        }
    }

    int compareDecimalValue (const NValue& rhs) const {
        vassert(m_valueType == VALUE_TYPE_DECIMAL);
        switch (rhs.getValueType()) {
        case VALUE_TYPE_DECIMAL:
            return compareValue<TTInt>(getDecimal(), rhs.getDecimal());
        case VALUE_TYPE_DOUBLE: {
            const double rhsValue = rhs.getDouble();
            TTInt scaledValue = getDecimal();
            TTInt whole(scaledValue);
            TTInt fractional(scaledValue);
            whole /= kMaxScaleFactor;
            fractional %= kMaxScaleFactor;
            const double lhsValue = static_cast<double>(whole.ToInt()) +
                    (static_cast<double>(fractional.ToInt())/static_cast<double>(kMaxScaleFactor));

            return compareValue<double>(lhsValue, rhsValue);
        }
        // create the equivalent decimal value
        case VALUE_TYPE_TINYINT: {
            TTInt rhsValue(static_cast<int64_t>(rhs.getTinyInt()));
            rhsValue *= kMaxScaleFactor;
            return compareValue<TTInt>(getDecimal(), rhsValue);
        }
        case VALUE_TYPE_SMALLINT: {
            TTInt rhsValue(static_cast<int64_t>(rhs.getSmallInt()));
            rhsValue *= kMaxScaleFactor;
            return compareValue<TTInt>(getDecimal(), rhsValue);
        }
        case VALUE_TYPE_INTEGER: {
            TTInt rhsValue(static_cast<int64_t>(rhs.getInteger()));
            rhsValue *= kMaxScaleFactor;
            return compareValue<TTInt>(getDecimal(), rhsValue);
        }
        case VALUE_TYPE_BIGINT: {
            TTInt rhsValue(rhs.getBigInt());
            rhsValue *= kMaxScaleFactor;
            return compareValue<TTInt>(getDecimal(), rhsValue);
        }
        case VALUE_TYPE_TIMESTAMP: {
            TTInt rhsValue(rhs.getTimestamp());
            rhsValue *= kMaxScaleFactor;
            return compareValue<TTInt>(getDecimal(), rhsValue);
        }
        default:
            break;
        }
        char message[128];
        snprintf(message, 128,
                 "Type %s cannot be cast for comparison to type %s",
                 valueToString(rhs.getValueType()).c_str(),
                 valueToString(getValueType()).c_str());
        throw SQLException(SQLException::
                data_exception_most_specific_type_mismatch,
                message);
    }

    int comparePointValue (const NValue &rhs) const {
        vassert(m_valueType == VALUE_TYPE_POINT);
        switch (rhs.getValueType()) {
        case VALUE_TYPE_POINT:
            return getGeographyPointValue().compareWith(rhs.getGeographyPointValue());
        default:
            std::ostringstream oss;
            oss << "Type " << valueToString(rhs.getValueType())
                << " cannot be cast for comparison to type "
                << valueToString(getValueType());
            throw SQLException(SQLException::data_exception_most_specific_type_mismatch,
                               oss.str().c_str());
            // Not reached
            return 0;
        }
    }

    int compareGeographyValue (const NValue &rhs) const {
        vassert(m_valueType == VALUE_TYPE_GEOGRAPHY);
        switch (rhs.getValueType()) {
        case VALUE_TYPE_GEOGRAPHY:
            return getGeographyValue().compareWith(rhs.getGeographyValue());
        default:
            std::ostringstream oss;
            oss << "Type " << valueToString(rhs.getValueType())
                << " cannot be cast for comparison to type "
                << valueToString(getValueType());
            throw SQLException(SQLException::data_exception_most_specific_type_mismatch,
                               oss.str().c_str());
            // Not reached
            return 0;
        }
    }

    static NValue opAddBigInts(const int64_t lhs, const int64_t rhs) {
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

    static NValue opSubtractBigInts(const int64_t lhs, const int64_t rhs) {
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

    static int64_t multiplyAndCheckOverflow(const int64_t lhs, const int64_t rhs, bool *overflowed) {
        *overflowed = false;
        //Scary overflow check from https://www.securecoding.cert.org/confluence/display/cplusplus/INT32-CPP.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow
        if (lhs > 0){  /* lhs is positive */
            if (rhs > 0) {  /* lhs and rhs are positive */
                if (lhs > (INT64_MAX / rhs)) {
                    *overflowed = true;
                }
            } /* end if lhs and rhs are positive */
            else { /* lhs positive, rhs non-positive */
                if (rhs < (INT64_MIN / lhs)) {
                    *overflowed = true;
                }
            } /* lhs positive, rhs non-positive */
        } /* end if lhs is positive */
        else { /* lhs is non-positive */
            if (rhs > 0) { /* lhs is non-positive, rhs is positive */
                if (lhs < (INT64_MIN / rhs)) {
                    *overflowed = true;
                }
            } /* end if lhs is non-positive, rhs is positive */
            else { /* lhs and rhs are non-positive */
                if ( (lhs != 0) && (rhs < (INT64_MAX / lhs))) {
                    *overflowed = true;
                }
            } /* end if lhs and rhs non-positive */
        } /* end if lhs is non-positive */

        const int64_t result = lhs * rhs;
        if (result == INT64_NULL) {
            *overflowed = true;
        }

        return result;
    }

    static NValue opMultiplyBigInts(const int64_t lhs, const int64_t rhs) {
        bool overflow = false;
        int64_t result = multiplyAndCheckOverflow(lhs, rhs, &overflow);

        if (overflow) {
            char message[4096];
            snprintf(message, 4096, "Multiplying %jd with %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
            throw SQLException( SQLException::data_exception_numeric_value_out_of_range, message);
        }

        return getBigIntValue(result);
    }

    static NValue opDivideBigInts(const int64_t lhs, const int64_t rhs) {
        if (rhs == 0) {
            char message[4096];
            snprintf(message, 4096, "Attempted to divide %jd by 0", (intmax_t)lhs);
            throw SQLException(SQLException::data_exception_division_by_zero, message);
        }

        /**
         * Because the smallest int64 value is used to represent null (and this is checked for an handled above)
         * it isn't necessary to check for any kind of overflow since none is possible.
         */
        return getBigIntValue(int64_t(lhs / rhs));
    }

    static NValue opAddDoubles(const double lhs, const double rhs) {
        const double result = lhs + rhs;
        throwDataExceptionIfInfiniteOrNaN(result, "'+' operator");
        return getDoubleValue(result);
    }

    static NValue opSubtractDoubles(const double lhs, const double rhs) {
        const double result = lhs - rhs;
        throwDataExceptionIfInfiniteOrNaN(result, "'-' operator");
        return getDoubleValue(result);
    }

    static NValue opMultiplyDoubles(const double lhs, const double rhs) {
        const double result = lhs * rhs;
        throwDataExceptionIfInfiniteOrNaN(result, "'*' operator");
        return getDoubleValue(result);
    }

    static NValue opDivideDoubles(const double lhs, const double rhs) {
        const double result = lhs / rhs;
        throwDataExceptionIfInfiniteOrNaN(result, "'/' operator");
        return getDoubleValue(result);
    }

    static NValue opAddDecimals(const NValue& lhs, const NValue& rhs) {
        vassert(lhs.isNull() == false);
        vassert(rhs.isNull() == false);
        vassert(lhs.getValueType() == VALUE_TYPE_DECIMAL);
        vassert(rhs.getValueType() == VALUE_TYPE_DECIMAL);

        TTInt retval(lhs.getDecimal());
        if (retval.Add(rhs.getDecimal()) || retval > s_maxDecimalValue || retval < s_minDecimalValue) {
            char message[4096];
            snprintf(message, 4096, "Attempted to add %s with %s causing overflow/underflow",
                    lhs.createStringFromDecimal().c_str(), rhs.createStringFromDecimal().c_str());
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range, message);
        }
        return getDecimalValue(retval);
    }

    static NValue opSubtractDecimals(const NValue& lhs, const NValue& rhs) {
        vassert(lhs.isNull() == false);
        vassert(rhs.isNull() == false);
        vassert(lhs.getValueType() == VALUE_TYPE_DECIMAL);
        vassert(rhs.getValueType() == VALUE_TYPE_DECIMAL);

        TTInt retval(lhs.getDecimal());
        if (retval.Sub(rhs.getDecimal()) || retval > s_maxDecimalValue || retval < s_minDecimalValue) {
            char message[4096];
            snprintf(message, 4096, "Attempted to subtract %s from %s causing overflow/underflow",
                    rhs.createStringFromDecimal().c_str(), lhs.createStringFromDecimal().c_str());
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range, message);
        }
        return getDecimalValue(retval);
    }

    /*
     * Avoid scaling both sides if possible. E.g, don't turn dec * 2 into
     * (dec * 2*kMaxScale*E-12). Then the result of simple multiplication
     * is a*b*E-24 and have to further multiply to get back to the assumed
     * E-12, which can overflow unnecessarily at the middle step.
     */
    static NValue opMultiplyDecimals(const NValue& lhs, const NValue& rhs) {
        vassert(lhs.isNull() == false);
        vassert(rhs.isNull() == false);
        vassert(lhs.getValueType() == VALUE_TYPE_DECIMAL);
        vassert(rhs.getValueType() == VALUE_TYPE_DECIMAL);

        TTLInt calc;
        calc.FromInt(lhs.getDecimal());
        calc *= rhs.getDecimal();
        calc /= kMaxScaleFactor;
        TTInt retval;
        if (retval.FromInt(calc)  || retval > s_maxDecimalValue || retval < s_minDecimalValue) {
            char message[4096];
            snprintf(message, 4096, "Attempted to multiply %s by %s causing overflow/underflow. Unscaled result was %s",
                    lhs.createStringFromDecimal().c_str(), rhs.createStringFromDecimal().c_str(),
                    calc.ToString(10).c_str());
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                               message);
        }
        return getDecimalValue(retval);
    }


    /*
     * Divide two decimals and return a correctly scaled decimal.
     * A little cumbersome. Better algorithms welcome.
     *   (1) calculate the quotient and the remainder.
     *   (2) temporarily scale the remainder to 19 digits
     *   (3) divide out remainder to calculate digits after the radix point.
     *   (4) scale remainder to 12 digits (that's the default scale)
     *   (5) scale the quotient back to 19,12.
     *   (6) sum the scaled quotient and remainder.
     *   (7) construct the final decimal.
     */

    static NValue opDivideDecimals(const NValue& lhs, const NValue& rhs) {
        vassert(lhs.isNull() == false);
        vassert(rhs.isNull() == false);
        vassert(lhs.getValueType() == VALUE_TYPE_DECIMAL);
        vassert(rhs.getValueType() == VALUE_TYPE_DECIMAL);

        TTLInt calc;
        calc.FromInt(lhs.getDecimal());
        calc *= kMaxScaleFactor;
        if (calc.Div(rhs.getDecimal())) {
            char message[4096];
            snprintf( message, 4096, "Attempted to divide %s by %s causing overflow/underflow (or divide by zero)",
                    lhs.createStringFromDecimal().c_str(), rhs.createStringFromDecimal().c_str());
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                               message);
        }
        TTInt retval;
        if (retval.FromInt(calc)  || retval > s_maxDecimalValue || retval < s_minDecimalValue) {
            char message[4096];
            snprintf( message, 4096, "Attempted to divide %s by %s causing overflow. Unscaled result was %s",
                    lhs.createStringFromDecimal().c_str(), rhs.createStringFromDecimal().c_str(),
                    calc.ToString(10).c_str());
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                               message);
        }
        return getDecimalValue(retval);
    }

    static NValue getTinyIntValue(int8_t value) {
        NValue retval(VALUE_TYPE_TINYINT);
        retval.getTinyInt() = value;
        if (value == INT8_NULL) {
            retval.tagAsNull();
        }
        return retval;
    }

    static NValue getSmallIntValue(int16_t value) {
        NValue retval(VALUE_TYPE_SMALLINT);
        retval.getSmallInt() = value;
        if (value == INT16_NULL) {
            retval.tagAsNull();
        }
        return retval;
    }

    static NValue getIntegerValue(int32_t value) {
        NValue retval(VALUE_TYPE_INTEGER);
        retval.getInteger() = value;
        if (value == INT32_NULL) {
            retval.tagAsNull();
        }
        return retval;
    }

    static NValue getBigIntValue(int64_t value) {
        NValue retval(VALUE_TYPE_BIGINT);
        retval.getBigInt() = value;
        if (value == INT64_NULL) {
            retval.tagAsNull();
        }
        return retval;
    }

    static NValue getTimestampValue(int64_t value) {
        NValue retval(VALUE_TYPE_TIMESTAMP);
        retval.getTimestamp() = value;
        if (value == INT64_NULL) {
            retval.tagAsNull();
        }
        return retval;
    }

    static NValue getDoubleValue(double value) {
        NValue retval(VALUE_TYPE_DOUBLE);
        retval.getDouble() = value;
        if (value <= DOUBLE_NULL) {
            retval.tagAsNull();
        }
        return retval;
    }

    static NValue getBooleanValue(bool value) {
        NValue retval(VALUE_TYPE_BOOLEAN);
        retval.getBoolean() = value;
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

    static NValue getAllocatedValue(ValueType type, const char* value, size_t size, Pool* stringPool) {
        NValue retval(type);
        retval.createObjectPointer((int32_t)size, value, stringPool);
        return retval;
    }

    char* allocateValueStorage(int32_t length, Pool* stringPool) {
        StringRef* sref = createObjectPointer(length, NULL, stringPool);
        char* storage = sref->getObjectValue();
        return storage;
    }

    static NValue getNullStringValue() {
        NValue retval(VALUE_TYPE_VARCHAR);
        retval.tagAsNull();
        *reinterpret_cast<char**>(retval.m_data) = NULL;
        return retval;
    }

    static NValue getNullBinaryValue() {
        NValue retval(VALUE_TYPE_VARBINARY);
        retval.tagAsNull();
        *reinterpret_cast<char**>(retval.m_data) = NULL;
        return retval;
    }

    static NValue getNullValue() {
        NValue retval(VALUE_TYPE_NULL);
        retval.tagAsNull();
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

    /// Common code to implement variants of the TRIM SQL function: LEADING, TRAILING, or BOTH
    static NValue trimWithOptions(const std::vector<NValue>& arguments, bool leading, bool trailing);

};

/**
 * Public constructor that initializes to an NValue that is unusable
 * with other NValues.  Useful for declaring storage for an NValue.
 */
inline NValue::NValue() {
    ::memset(m_data, 0, 16);
    setValueType(VALUE_TYPE_INVALID);
    setDefaultAttributes();
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
 * Returns C++ true if this NValue is a boolean and is true
 * If it is NULL, return false.
 */
inline bool NValue::isTrue() const {
    if (isBooleanNULL()) {
        return false;
    } else {
       return getBoolean();
    }
}

/**
 * Returns C++ false if this NValue is a boolean and is true
 * If it is NULL, return false.
 */
inline bool NValue::isFalse() const {
    if (isBooleanNULL()) {
        return false;
    } else {
       return !getBoolean();
    }
}

inline bool NValue::isBooleanNULL() const {
    vassert(getValueType() == VALUE_TYPE_BOOLEAN);
    return *reinterpret_cast<const int8_t*>(m_data) == INT8_NULL;
}

/**
 * Objects may have storage allocated for them. Calling free causes the NValue to return the storage allocated for
 * the object to the heap
 */
inline void NValue::free() const {
   StringRef* sref;
   switch (getValueType()) {
      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_VARBINARY:
      case VALUE_TYPE_GEOGRAPHY:
      case VALUE_TYPE_ARRAY:
         vassert(!getSourceInlined());
         sref = *reinterpret_cast<StringRef* const*>(m_data);
         if (sref != NULL) {
            StringRef::destroy(sref);
         }
         break;
      default:
         return;
   }
}

inline void NValue::freeObjectsFromTupleStorage(std::vector<char*> const &oldObjects) {
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
      case VALUE_TYPE_GEOGRAPHY:
        return sizeof(char*);
      case VALUE_TYPE_DECIMAL:
        return sizeof(TTInt);
      case VALUE_TYPE_BOOLEAN:
        return sizeof(bool);
      case VALUE_TYPE_POINT:
        return sizeof(GeographyPointValue);
      default:
          char message[128];
          snprintf(message, 128, "NValue::getTupleStorageSize() unsupported type '%s'",
                   getTypeName(type).c_str());
          throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                        message);
    }
}

/**
 * This null compare function works for GROUP BY, ORDER BY, INDEX KEY, etc,
 * except for comparison expression.
 * comparison expression has different logic for null.
 */
inline int NValue::compareNull(const NValue& rhs) const {
    bool lnull = isNull();
    bool rnull = rhs.isNull();

    if (lnull) {
        if (rnull) {
            return VALUE_COMPARE_EQUAL;
        } else {
            return VALUE_COMPARE_LESSTHAN;
        }
    } else if (rnull) {
        return VALUE_COMPARE_GREATERTHAN;
    } else {
       return VALUE_COMPARE_INVALID;
    }
}

/**
 * Assuming no nulls are in comparison.
 * Compare any two NValues. Comparison is not guaranteed to
 * succeed if the values are incompatible.  Avoid use of
 * comparison in favor of op_*.
 */
inline int NValue::compare_withoutNull(const NValue& rhs) const {
   vassert(isNull() == false && rhs.isNull() == false);

   switch (m_valueType) {
      case VALUE_TYPE_VARCHAR:
         return compareStringValue(rhs);
      case VALUE_TYPE_BIGINT:
         return compareBigInt(rhs);
      case VALUE_TYPE_INTEGER:
         return compareInteger(rhs);
      case VALUE_TYPE_SMALLINT:
         return compareSmallInt(rhs);
      case VALUE_TYPE_TINYINT:
         return compareTinyInt(rhs);
      case VALUE_TYPE_TIMESTAMP:
         return compareTimestamp(rhs);
      case VALUE_TYPE_DOUBLE:
         return compareDoubleValue(rhs);
      case VALUE_TYPE_VARBINARY:
         return compareBinaryValue(rhs);
      case VALUE_TYPE_DECIMAL:
         return compareDecimalValue(rhs);
      case VALUE_TYPE_POINT:
         return comparePointValue(rhs);
      case VALUE_TYPE_GEOGRAPHY:
         return compareGeographyValue(rhs);
      case VALUE_TYPE_BOOLEAN:
         return compareBooleanValue(rhs);
      default: {
                  throwDynamicSQLException(
                        "non comparable types lhs '%s' rhs '%s'",
                        getValueTypeString().c_str(),
                        rhs.getValueTypeString().c_str());
               }
               /* no break */
   }
}

/**
 * Compare any two NValues. Comparison is not guaranteed to
 * succeed if the values are incompatible.  Avoid use of
 * comparison in favor of op_*.
 */
inline int NValue::compare(const NValue& rhs) const {
    int hasNullCompare = compareNull(rhs);
    if (hasNullCompare != VALUE_COMPARE_INVALID) {
        return hasNullCompare;
    }

    return compare_withoutNull(rhs);
}

/**
 * Set this NValue to null.
 */
inline void NValue::setNull() {
   tagAsNull(); // This gets overwritten for DECIMAL and POINT -- but that's OK.
   switch (getValueType()) {
      case VALUE_TYPE_BOOLEAN:
         // HACK BOOL NULL
         *reinterpret_cast<int8_t*>(m_data) = INT8_NULL;
         break;
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
      case VALUE_TYPE_GEOGRAPHY:
         *reinterpret_cast<void**>(m_data) = NULL;
         break;
      case VALUE_TYPE_DECIMAL:
         getDecimal().SetMin();
         break;
      case VALUE_TYPE_POINT:
         getGeographyPointValue() = GeographyPointValue();
         break;
      default:
         throwDynamicSQLException("NValue::setNull() called with unsupported ValueType '%d'", getValueType());
   }
}

inline NValue NValue::initFromTupleStorage(const void *storage, ValueType type, bool isInlined, bool isVolatile) {
    NValue retval(type);
    switch (type) {
    case VALUE_TYPE_INTEGER:
        if ((retval.getInteger() = *reinterpret_cast<const int32_t*>(storage)) == INT32_NULL) {
            retval.tagAsNull();
        }
        break;
    case VALUE_TYPE_BIGINT:
        if ((retval.getBigInt() = *reinterpret_cast<const int64_t*>(storage)) == INT64_NULL) {
            retval.tagAsNull();
        }
        break;
    case VALUE_TYPE_DOUBLE:
        if ((retval.getDouble() = *reinterpret_cast<const double*>(storage)) <= DOUBLE_NULL) {
            retval.tagAsNull();
        }
        break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_GEOGRAPHY: {
        retval.setVolatile(isVolatile);
        //Potentially non-inlined type requires special handling
        if (isInlined) {
            //If it is inlined the storage area contains the actual data so copy a reference
            //to the storage area
            vassert(type != VALUE_TYPE_GEOGRAPHY);
            const char* inline_data = reinterpret_cast<const char*>(storage);
            *reinterpret_cast<const char**>(retval.m_data) = inline_data;
            retval.setSourceInlined(true);
            /**
             * If a string is inlined in its storage location there will be no pointer to
             * check for NULL. The length prefix value must be used instead.
             */
            if ((inline_data[0] & OBJECT_NULL_BIT) != 0) {
                retval.tagAsNull();
            }
            break;
        }

        // If it isn't inlined the storage area contains a pointer to the
        // StringRef object containing the string's memory
        StringRef* sref = *reinterpret_cast<StringRef**>(const_cast<void*>(storage));
        // If the StringRef pointer is null, that's because this
        // was a null value; otherwise get the right char* from the StringRef
        if (sref == NULL) {
            retval.setNullObjectPointer();
        } else {
            retval.setObjectPointer(sref);
        }
        break;
    }
    case VALUE_TYPE_TIMESTAMP:
        if ((retval.getTimestamp() = *reinterpret_cast<const int64_t*>(storage)) == INT64_NULL) {
            retval.tagAsNull();
        }
        break;
    case VALUE_TYPE_TINYINT:
        if ((retval.getTinyInt() = *reinterpret_cast<const int8_t*>(storage)) == INT8_NULL) {
            retval.tagAsNull();
        }
        break;
    case VALUE_TYPE_SMALLINT:
        if ((retval.getSmallInt() = *reinterpret_cast<const int16_t*>(storage)) == INT16_NULL) {
            retval.tagAsNull();
        }
        break;
    case VALUE_TYPE_DECIMAL:
        ::memcpy(retval.m_data, storage, sizeof(TTInt));
        break;
    case VALUE_TYPE_POINT:
        retval.getGeographyPointValue() = *reinterpret_cast<const GeographyPointValue*>(storage);
        break;
    default:
        throwDynamicSQLException("NValue::initFromTupleStorage() invalid column type '%s'",
              getTypeName(type).c_str());
        /* no break */
    }
    return retval;
}

template<class POOL>
inline void NValue::serializeToTupleStorage(void *storage, bool isInlined,
      int32_t maxLength, bool isInBytes, bool allocateObjects, POOL* tempPool) const {
    const ValueType type = getValueType();
    switch (type) {
    case VALUE_TYPE_TIMESTAMP:
        *reinterpret_cast<int64_t*>(storage) = getTimestamp();
        return;
    case VALUE_TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(storage) = getTinyInt();
        return;
    case VALUE_TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(storage) = getSmallInt();
        return;
    case VALUE_TYPE_INTEGER:
        *reinterpret_cast<int32_t*>(storage) = getInteger();
        return;
    case VALUE_TYPE_BIGINT:
        *reinterpret_cast<int64_t*>(storage) = getBigInt();
        return;
    case VALUE_TYPE_DOUBLE:
        *reinterpret_cast<double*>(storage) = getDouble();
        return;
    case VALUE_TYPE_DECIMAL:
        ::memcpy(storage, m_data, sizeof(TTInt));
        return;
    case VALUE_TYPE_POINT:
        ::memcpy(storage, m_data, sizeof(GeographyPointValue));
        return;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_GEOGRAPHY: {
        //Potentially non-inlined type requires special handling
        if (isInlined) {
            vassert(type != VALUE_TYPE_GEOGRAPHY);
            serializeInlineObjectToTupleStorage(static_cast<char*>(storage), maxLength, isInBytes);
            return;
        }
        if (isNull()) {
            *reinterpret_cast<void**>(storage) = NULL;
            return;
        }
        int32_t length;
        const char* buf = getObject_withoutNull(length);
        checkTooWideForVariableLengthType(m_valueType, buf, length, maxLength, isInBytes);

        const StringRef* sref;
        if (allocateObjects) {
            // Need to copy a StringRef pointer.
            sref = StringRef::create(length, buf, tempPool);
        } else if (getSourceInlined()) {
            sref = StringRef::create(length, buf, getTempStringPool());
        } else {
            sref = getObjectPointer();
        }
        *reinterpret_cast<const StringRef**>(storage) = sref;
        return;
    }
    default:
        break;
    }
    char message[128];
    snprintf(message, 128, "NValue::serializeToTupleStorage() unrecognized type '%s'",
             getTypeName(type).c_str());
    throw SQLException(SQLException::data_exception_most_specific_type_mismatch,
                       message);
}


/**
 * Deserialize a scalar value of the specified type from the
 * SerializeInput directly into the tuple storage area
 * provided. This function will perform memory allocations for
 * Object types as necessary using the provided data pool or the
 * heap. This is used to deserialize tables.
 */
inline void NValue::deserializeFrom(SerializeInputBE& input, Pool* tempPool, char *storage,
        const ValueType type, bool isInlined, int32_t maxLength, bool isInBytes) {
    deserializeFrom<TUPLE_SERIALIZATION_NATIVE>(input, tempPool, storage,
                                                type, isInlined, maxLength, isInBytes);
}

template <TupleSerializationFormat F, Endianess E> inline void NValue::deserializeFrom(
        SerializeInput<E>& input, Pool* tempPool, char *storage,
        ValueType type, bool isInlined, int32_t maxLength, bool isInBytes) {
    switch (type) {
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
        *reinterpret_cast<int64_t*>(storage) = input.readLong();
        return;
    case VALUE_TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(storage) = input.readByte();
        return;
    case VALUE_TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(storage) = input.readShort();
        return;
    case VALUE_TYPE_INTEGER:
        *reinterpret_cast<int32_t*>(storage) = input.readInt();
        return;
    case VALUE_TYPE_DOUBLE:
        *reinterpret_cast<double* >(storage) = input.readDouble();
        return;
    case VALUE_TYPE_POINT:
        *reinterpret_cast<GeographyPointValue*>(storage) = GeographyPointValue::deserializeFrom(input);
        return;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_GEOGRAPHY: {
        const int32_t length = input.readInt();
        if (length < -1) {
            throw SQLException(SQLException::dynamic_sql_error, "Object length cannot be < -1");
        }
        if (isInlined) {
            vassert(type != VALUE_TYPE_GEOGRAPHY);
            vassert(length <= OBJECT_MAX_LENGTH_SHORT_LENGTH);
            // Always reset the bits regardless of how long the actual value is.
            storage[0] = static_cast<char>(length);
            ::memset(storage+1, 0, maxLength);

            if (length == OBJECTLENGTH_NULL) {
                return;
            }
            const char *data = reinterpret_cast<const char*>(input.getRawPointer(length));
            checkTooWideForVariableLengthType(type, data, length, maxLength, isInBytes);
            ::memcpy(storage + SHORT_OBJECT_LENGTHLENGTH, data, length);
            return;
        }
        if (length == OBJECTLENGTH_NULL) {
            *reinterpret_cast<void**>(storage) = NULL;
            return;
        }

        StringRef* sref = NULL;
        if (type != VALUE_TYPE_GEOGRAPHY) {
            // This advances input past the end of the string
            const char *data = reinterpret_cast<const char*>(input.getRawPointer(length));
            checkTooWideForVariableLengthType(type, data, length, maxLength, isInBytes);
            sref = StringRef::create(length, data, tempPool);
        } else {
            // This gets a pointer to the start of data without advancing
            // input stream, so we can read loops and vertices.
            const char* data = reinterpret_cast<const char*>(input.getRawPointer());
            checkTooWideForVariableLengthType(type, data, length, maxLength, isInBytes);
            // Create the string ref without passing the source pointer, so we can use
            // GeographyValue's deserialize method to initialize it.
            sref = StringRef::create(length, NULL, tempPool);
            GeographyValue::deserializeFrom(input, sref->getObjectValue(), length);
        }

        *reinterpret_cast<StringRef**>(storage) = sref;
        return;
    }
    case VALUE_TYPE_DECIMAL: {
        if (F == TUPLE_SERIALIZATION_DR) {
            const int scale = input.readByte();
            const int precisionBytes = input.readByte();
            if (scale != kMaxDecScale) {
                throwFatalException("Unexpected scale %d", scale);
            }
            if (precisionBytes != 16) {
                throwFatalException("Unexpected number of precision bytes %d", precisionBytes);
            }
        }
        uint64_t *longStorage = reinterpret_cast<uint64_t*>(storage);
        //Reverse order for Java BigDecimal BigEndian
        longStorage[1] = input.readLong();
        longStorage[0] = input.readLong();

        if (F == TUPLE_SERIALIZATION_DR) {
            // Serialize to export serializes them in network byte order, have to reverse them here
            longStorage[0] = ntohll(longStorage[0]);
            longStorage[1] = ntohll(longStorage[1]);
        }
        return;
    }
    default:
        break;
    }
    char message[128];
    snprintf(message, 128, "NValue::deserializeFrom() unrecognized type '%s'",
             getTypeName(type).c_str());
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  message);
}

/**
 * Deserialize a scalar value of the specified type from the
 * provided SerializeInput and perform allocations as necessary.
 * This is used to deserialize parameter sets.
 */
inline void NValue::deserializeFromAllocateForStorage(SerializeInputBE& input, Pool* tempPool) {
    const ValueType type = static_cast<ValueType>(input.readByte());
    deserializeFromAllocateForStorage(type, input, tempPool);
}

inline void NValue::deserializeFromAllocateForStorage(ValueType type, SerializeInputBE& input, Pool* tempPool) {
   setValueType(type);
   // Parameter array NValue elements are reused from one executor call to the next,
   // so these NValues need to forget they were ever null.
   tagAsNotNull();
   switch (type) {
      case VALUE_TYPE_BIGINT:
         getBigInt() = input.readLong();
         if (getBigInt() == INT64_NULL) {
            tagAsNull();
         }
         return;
      case VALUE_TYPE_TIMESTAMP:
         getTimestamp() = input.readLong();
         if (getTimestamp() == INT64_NULL) {
            tagAsNull();
         }
         return;
      case VALUE_TYPE_TINYINT:
         getTinyInt() = input.readByte();
         if (getTinyInt() == INT8_NULL) {
            tagAsNull();
         }
         return;
      case VALUE_TYPE_SMALLINT:
         getSmallInt() = input.readShort();
         if (getSmallInt() == INT16_NULL) {
            tagAsNull();
         }
         return;
      case VALUE_TYPE_INTEGER:
         getInteger() = input.readInt();
         if (getInteger() == INT32_NULL) {
            tagAsNull();
         }
         return;
      case VALUE_TYPE_DOUBLE:
         getDouble() = input.readDouble();
         if (getDouble() <= DOUBLE_NULL) {
            tagAsNull();
         }
         return;
      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_VARBINARY:
      case VALUE_TYPE_GEOGRAPHY:
         {
            const int32_t length = input.readInt();
            // the NULL SQL string is a NULL C pointer
            if (length == OBJECTLENGTH_NULL) {
               setNull();
               return;
            }

            if (type != VALUE_TYPE_GEOGRAPHY) {
               const char *str = (const char*) input.getRawPointer(length);
               createObjectPointer(length, str, tempPool);
            }
            else {
               StringRef* sref = createObjectPointer(length, NULL, tempPool);
               GeographyValue::deserializeFrom(input, sref->getObjectValue(), length);
            }

            return;
         }
      case VALUE_TYPE_DECIMAL:
         getDecimal().table[1] = input.readLong();
         getDecimal().table[0] = input.readLong();
         return;
      case VALUE_TYPE_POINT:
         getGeographyPointValue() = GeographyPointValue::deserializeFrom(input);
         return;
      case VALUE_TYPE_NULL:
         setNull();
         return;
      case VALUE_TYPE_ARRAY:
         deserializeIntoANewNValueList(input, tempPool);
         return;
      default:
         break;
   }

   throwDynamicSQLException("NValue::deserializeFromAllocateForStorage() unrecognized type '%s'",
         getTypeName(type).c_str());
}

/**
 * Serialize this NValue to the provided SerializeOutput
 */
inline void NValue::serializeTo(SerializeOutput &output) const {
    const ValueType type = getValueType();
    switch (type) {
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_GEOGRAPHY: {
        if (isNull()) {
            output.writeInt(OBJECTLENGTH_NULL);
            return;
        }
        int32_t length;
        const char* buf = getObject_withoutNull(length);
        if (length <= OBJECTLENGTH_NULL) {
            throwDynamicSQLException("Attempted to serialize an NValue with a negative length");
        }
        output.writeInt(length);

        // Not a null string: write it out
        if (type != VALUE_TYPE_GEOGRAPHY) {
            output.writeBytes(buf, length);
        } else {
            // geography gets its own serialization to deal with
            // byteswapping and endianness
            getGeographyValue().serializeTo(output);
        }
        return;
    }
    case VALUE_TYPE_TINYINT:
        output.writeByte(getTinyInt());
        return;

    case VALUE_TYPE_SMALLINT:
        output.writeShort(getSmallInt());
        return;

    case VALUE_TYPE_INTEGER:
        output.writeInt(getInteger());
        return;

    case VALUE_TYPE_TIMESTAMP:
        output.writeLong(getTimestamp());
        return;

    case VALUE_TYPE_BIGINT:
        output.writeLong(getBigInt());
        return;

    case VALUE_TYPE_DOUBLE:
        output.writeDouble(getDouble());
        return;

    case VALUE_TYPE_DECIMAL:
        output.writeLong(getDecimal().table[1]);
        output.writeLong(getDecimal().table[0]);
        return;

    case VALUE_TYPE_POINT:
        getGeographyPointValue().serializeTo(output);
        return;

    default:
        break;
    }

    throwDynamicSQLException("NValue::serializeTo() found a column with ValueType '%s' that is not handled",
          getValueTypeString().c_str());
}


inline size_t NValue::serializeToExport_withoutNull(ExportSerializeOutput &io) const {
    size_t sz = 0;
    vassert(isNull() == false);
    const ValueType type = getValueType();
    switch (type) {
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_GEOGRAPHY: {
        int32_t length;
        const char* buf = getObject_withoutNull(length);
        if (type == VALUE_TYPE_GEOGRAPHY) {
            sz += io.writeInt(length);
            // geography gets its own serialization to deal with byte-swapping and endianness
            getGeographyValue().serializeTo(io);
        }
        else {
            sz += io.writeBinaryString(buf, length);
        }
        return sz;
    }
    case VALUE_TYPE_TINYINT:
        sz += io.writeByte(getTinyInt());
        return sz;
    case VALUE_TYPE_SMALLINT:
        sz += io.writeShort(getSmallInt());
        return sz;
    case VALUE_TYPE_INTEGER:
        sz += io.writeInt(getInteger());
        return sz;
    case VALUE_TYPE_TIMESTAMP:
        sz += io.writeLong(getTimestamp());
        return sz;
    case VALUE_TYPE_BIGINT:
        sz += io.writeLong(getBigInt());
        return sz;
    case VALUE_TYPE_DOUBLE:
        sz += io.writeDouble(getDouble());
        return sz;
    case VALUE_TYPE_DECIMAL:
        sz += io.writeByte((int8_t)kMaxDecScale);
        sz += io.writeByte((int8_t)16);  //number of bytes in decimal
        sz += io.writeLong(htonll(getDecimal().table[1]));
        sz += io.writeLong(htonll(getDecimal().table[0]));
        return sz;
    case VALUE_TYPE_POINT:
        getGeographyPointValue().serializeTo(io);
        return sz;
    case VALUE_TYPE_INVALID:
    case VALUE_TYPE_NULL:
    case VALUE_TYPE_BOOLEAN:
    case VALUE_TYPE_ADDRESS:
    case VALUE_TYPE_ARRAY:
    case VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC: {
        char message[128];
        snprintf(message, sizeof(message), "Invalid type in serializeToExport: %s",
                 getTypeName(getValueType()).c_str());
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
    default:
        break;
    }
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                  "Invalid type in serializeToExport");
}

/** Reformat an object-typed value from its current form to its
 *  allocated non-inlined form.  Use the pool specified by the caller,
 *  or the temp string pool if none was supplied. **/
inline void NValue::allocateObjectFromPool(Pool* pool) {
    if (m_valueType == VALUE_TYPE_NULL || m_valueType == VALUE_TYPE_INVALID) {
        return;
    }

    vassert(isVariableLengthType(m_valueType));

    if (isNull()) {
        *reinterpret_cast<void**>(m_data) = NULL;
        // serializeToTupleStorage fusses about this inline flag being set, even for NULLs
        setSourceInlined(false);
        setVolatile(false);
        return;
    }

    if (pool == NULL) {
        pool = getTempStringPool();
    }

    int32_t length;
    const char* source = getObject_withoutNull(length);

    createObjectPointer(length, source, pool);
    setSourceInlined(false);
    setVolatile(false);
}

inline bool NValue::isNull() const {
    // DECIMAL and POINT don't use the
    // OBJECT_NULL_BIT, because they have a 16-byte
    // representation in m_data, and the object null bit
    // (if set) lives in m_data[13].
    if (getValueType() == VALUE_TYPE_DECIMAL) {
        TTInt min;
        min.SetMin();
        return getDecimal() == min;
    } else if (getValueType() == VALUE_TYPE_POINT) {
        return getGeographyPointValue().isNull();
    }

    return m_data[13] == OBJECT_NULL_BIT;
}

inline bool NValue::isNaN() const {
    if (getValueType() == VALUE_TYPE_DOUBLE) {
        return std::isnan(getDouble());
    } else {
       return false;
    }
}

// general full comparison
inline NValue NValue::op_equals(const NValue& rhs) const {
    return compare(rhs) == 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_notEquals(const NValue& rhs) const {
    return compare(rhs) != 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_lessThan(const NValue& rhs) const {
    return compare(rhs) < 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_lessThanOrEqual(const NValue& rhs) const {
    return compare(rhs) <= 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_greaterThan(const NValue& rhs) const {
    return compare(rhs) > 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_greaterThanOrEqual(const NValue& rhs) const {
    return compare(rhs) >= 0 ? getTrue() : getFalse();
}

// without null comparison
inline NValue NValue::op_equals_withoutNull(const NValue& rhs) const {
    return compare_withoutNull(rhs) == 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_notEquals_withoutNull(const NValue& rhs) const {
    return compare_withoutNull(rhs) != 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_lessThan_withoutNull(const NValue& rhs) const {
    return compare_withoutNull(rhs) < 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_lessThanOrEqual_withoutNull(const NValue& rhs) const {
    return compare_withoutNull(rhs) <= 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_greaterThan_withoutNull(const NValue& rhs) const {
    return compare_withoutNull(rhs) > 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_greaterThanOrEqual_withoutNull(const NValue& rhs) const {
    return compare_withoutNull(rhs) >= 0 ? getTrue() : getFalse();
}

inline NValue NValue::op_max(const NValue& rhs) const {
    if (compare(rhs) > 0) {
        return *this;
    } else {
       return rhs;
    }
}

inline NValue NValue::op_min(const NValue& rhs) const {
    if (compare(rhs) < 0) {
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
        boost::hash_combine(seed, getTinyInt());
        return;
    case VALUE_TYPE_SMALLINT:
        boost::hash_combine(seed, getSmallInt());
        return;
    case VALUE_TYPE_INTEGER:
        boost::hash_combine(seed, getInteger());
        return;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
        boost::hash_combine(seed, getBigInt());
        return;
    case VALUE_TYPE_DOUBLE:
        if (isNull()) {
            // A range of values for FLOAT are considered to be
            // NULL---anything less than or equal to DOUBLE_NULL.  Any
            // two FLOAT NULL values may have a different bit pattern,
            // but they should still hash to the same thing.
            //
            // Just use INT64_NULL here to force them to all be the same.
            boost::hash_combine(seed, INT64_NULL);
        } else {
            MiscUtil::hashCombineFloatingPoint(seed, getDouble());
        }
        return;
    case VALUE_TYPE_VARCHAR:
    {
        if (isNull()) {
            boost::hash_combine( seed, std::string(""));
            return;
        }
        int32_t length;
        const char* buf = getObject_withoutNull(length);
        boost::hash_combine(seed, std::string(buf, length));
        return;
    }
    case VALUE_TYPE_VARBINARY:
    {
        if (isNull()) {
            boost::hash_combine( seed, std::string(""));
            return;
        }
        int32_t length;
        const char* buf = getObject_withoutNull(length);
        for (int32_t i = 0; i < length; i++) {
            boost::hash_combine(seed, buf[i]);
        }
        return;
    }
    case VALUE_TYPE_DECIMAL:
        getDecimal().hash(seed);
        return;
    case VALUE_TYPE_POINT:
        getGeographyPointValue().hashCombine(seed);
        return;
    case VALUE_TYPE_GEOGRAPHY:
        getGeographyValue().hashCombine(seed);
        return;
    default:
        break;
    }

    throwDynamicSQLException( "NValue::hashCombine unknown type %s", getValueTypeString().c_str());
}

inline NValue NValue::castAs(ValueType type) const {
    if (getValueType() == type) {
        return *this;
    }
    if (isNull()) {
        return getNullValue(type);
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
    case VALUE_TYPE_POINT:
        return castAsGeographyPoint();
    case VALUE_TYPE_GEOGRAPHY:
        return castAsGeography();
    default:
        break;
    }
    DEBUG_IGNORE_OR_THROW_OR_CRASH("Fallout from planner error."
                                   " The invalid target value type for a cast is " <<
                                   getTypeName(type));
    char message[128];
    snprintf(message, 128, "Type %d not a recognized type for casting",
             (int) type);
    throw SQLException(SQLException::data_exception_most_specific_type_mismatch, message);
}

inline void* NValue::castAsAddress() const {
    const ValueType type = getValueType();
    switch (type) {
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_ADDRESS:
        return *reinterpret_cast<void* const*>(m_data);
    default:
        break;
    }
    throwDynamicSQLException("Type %s not a recognized type for casting as an address", getValueTypeString().c_str());
}

inline NValue NValue::op_unary_minus() const {
    const ValueType type = getValueType();
    NValue retval(type);
    switch(type) {
    case VALUE_TYPE_TINYINT:
        retval.getTinyInt() = static_cast<int8_t>(-getTinyInt());
        break;
    case VALUE_TYPE_SMALLINT:
        retval.getSmallInt() = static_cast<int16_t>(-getSmallInt());
        break;
    case VALUE_TYPE_INTEGER:
        retval.getInteger() = -getInteger();
        break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
        retval.getBigInt() = -getBigInt();
        break;
    case VALUE_TYPE_DECIMAL:
        retval.getDecimal() = -getDecimal();
        break;
    case VALUE_TYPE_DOUBLE:
        retval.getDouble() = -getDouble();
        break;
    default:
        throwDynamicSQLException( "unary minus cannot be applied to type %s", getValueTypeString().c_str());
        break;
    }
    return retval;
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
        retval.getTinyInt() = static_cast<int8_t>(getTinyInt() + 1);
        break;
    case VALUE_TYPE_SMALLINT:
        if (getSmallInt() == INT16_MAX) {
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                    "Incrementing this SmallInt results in a value out of range");
        }
        retval.getSmallInt() = static_cast<int16_t>(getSmallInt() + 1);
        break;
    case VALUE_TYPE_INTEGER:
        if (getInteger() == INT32_MAX) {
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                    "Incrementing this Integer results in a value out of range");
        }
        retval.getInteger() = getInteger() + 1;
        break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
        if (getBigInt() == INT64_MAX) {
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                    "Incrementing this BigInt/Timestamp results in a value out of range");
        }
        retval.getBigInt() = getBigInt() + 1;
        break;
    case VALUE_TYPE_DOUBLE:
        retval.getDouble() = getDouble() + 1;
        break;
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
        retval.getTinyInt() = static_cast<int8_t>(getTinyInt() - 1);
        break;
    case VALUE_TYPE_SMALLINT:
        if (getSmallInt() == VOLT_INT16_MIN) {
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                    "Decrementing this SmallInt results in a value out of range");
        }
        retval.getSmallInt() = static_cast<int16_t>(getSmallInt() - 1);
        break;
    case VALUE_TYPE_INTEGER:
        if (getInteger() == VOLT_INT32_MIN) {
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                    "Decrementing this Integer results in a value out of range");
        }
        retval.getInteger() = getInteger() - 1;
        break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
        if (getBigInt() == VOLT_INT64_MIN) {
            throw SQLException(SQLException::data_exception_numeric_value_out_of_range,
                    "Decrementing this BigInt/Timestamp results in a value out of range");
        }
        retval.getBigInt() = getBigInt() - 1;
        break;
    case VALUE_TYPE_DOUBLE:
        retval.getDouble() = getDouble() - 1;
        break;
    default:
        throwDynamicSQLException( "type %s is not decrementable", getValueTypeString().c_str());
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
        break;
    }
    throwDynamicSQLException("type %s is not a numeric type that implements isZero()",
                             getValueTypeString().c_str());
}

inline NValue NValue::op_subtract(const NValue& rhs) const {
    ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
    if (isNull() || rhs.isNull()) {
        return getNullValue(vt);
    }

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

inline NValue NValue::op_add(const NValue& rhs) const {
    ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
    if (isNull() || rhs.isNull()) {
        return getNullValue(vt);
    }

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

inline NValue NValue::op_multiply(const NValue& rhs) const {
    ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
    if (isNull() || rhs.isNull()) {
        return getNullValue(vt);
    }

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
        return opMultiplyDecimals(castAsDecimal(),
                rhs.castAsDecimal());

    default:
        break;
    }
    throwDynamicSQLException("Promotion of %s and %s failed in op_multiply.",
            getValueTypeString().c_str(),
            rhs.getValueTypeString().c_str());
}

inline NValue NValue::op_divide(const NValue& rhs) const {
    ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
    if (isNull() || rhs.isNull()) {
        return getNullValue(vt);
    }

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

inline int32_t NValue::murmurHash3() const {
    const ValueType type = getValueType();
    switch(type) {
    case VALUE_TYPE_TIMESTAMP:
    case VALUE_TYPE_DOUBLE:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_POINT:
        return MurmurHash3_x64_128(castAsBigIntAndGetValue());
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_VARCHAR:
    {
        if (isNull()) {
            // Use NULL check first to be able to get rid of checks inside of other functions.
            // Maybe it is impossible to be null here. -xin
            throw SQLException(SQLException::dynamic_sql_error,
                    "Must not ask for object length on sql null object.");
        }
        int32_t length;
        const char* buf = getObject_withoutNull(length);
        return MurmurHash3_x64_128(buf, length, 0);
    }
    default:
        break;
    }
    throwFatalException("Unknown type for murmur hashing %d", type);
}

/*
 * The LHS (this) should always be the string being compared
 * and the RHS should always be the LIKE expression.
 * The planner or EE needs to enforce this.
 *
 * Null check should have been handled already.
 */
inline NValue NValue::like(const NValue& rhs) const {
    /*
     * Validate that all params are VARCHAR
     */
    const ValueType mType = getValueType();
    if (mType != VALUE_TYPE_VARCHAR) {
        throwDynamicSQLException(
                "The left operand of the LIKE expression is %s not %s",
                getValueTypeString().c_str(),
                getTypeName(VALUE_TYPE_VARCHAR).c_str());
    }

    const ValueType rhsType = rhs.getValueType();
    if (rhsType != VALUE_TYPE_VARCHAR) {
        throwDynamicSQLException(
                "The right operand of the LIKE expression is %s not %s",
                rhs.getValueTypeString().c_str(),
                getTypeName(VALUE_TYPE_VARCHAR).c_str());
    }

    int32_t valueUTF8Length;
    const char* valueChars = getObject_withoutNull(valueUTF8Length);
    int32_t patternUTF8Length;
    const char* patternChars = rhs.getObject_withoutNull(patternUTF8Length);

    if (0 == patternUTF8Length) {
        if (0 == valueUTF8Length) {
            return getTrue();
        } else {
            return getFalse();
        }
    }

    vassert(valueChars);
    vassert(patternChars);

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
        Liker(const char *valueChars, const char* patternChars,
              int32_t valueUTF8Length, int32_t patternUTF8Length)
            : m_value(valueChars, valueChars + valueUTF8Length)
            , m_pattern(patternChars, patternChars + patternUTF8Length)
        { }

        bool like() {
            while ( ! m_pattern.atEnd()) {
                const uint32_t nextPatternCodePoint = m_pattern.extractCodePoint();
                switch (nextPatternCodePoint) {
                case '%': {
                    if (m_pattern.atEnd()) {
                        return true;
                    }

                    const char *postPercentPatternIterator = m_pattern.getCursor();
                    uint32_t nextPatternCodePointAfterPercent = m_pattern.extractCodePoint();

                    // ENG-14485 handle two or more consecutive '%' characters at the end of the pattern
                    if (m_value.atEnd()) {
                        while (nextPatternCodePointAfterPercent == '%') {
                            if (m_pattern.atEnd()) {
                                return true;
                            }
                            nextPatternCodePointAfterPercent = m_pattern.extractCodePoint();
                        }
                        return false;
                    }

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
                    if (m_value.atEnd()) {
                        return false;
                    }
                    //Extract a code point to consume a character
                    m_value.extractCodePoint();
                    break;
                }
                default: {
                    if (m_value.atEnd()) {
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

/*
 * This function checks to see if a VARCHAR string starts with the given prefix pattern.
 *
 * The LHS (this) should always be the string being checked
 * and the RHS should always be a plain string used as the pattern.
 * The funtion returns NValue: true if rhs is a prefix of lhs, o/w NValue: false.
 *
 * Null check should have been handled in comparisonexpression.h already.
 */
inline NValue NValue::startsWith(const NValue& rhs) const {
    /*
     * Validate that all params are VARCHAR
     */
    const ValueType mType = getValueType();
    if (mType != VALUE_TYPE_VARCHAR) {
        throwDynamicSQLException(
                "The left operand of the STARTS WITH expression is %s not %s",
                getValueTypeString().c_str(),
                getTypeName(VALUE_TYPE_VARCHAR).c_str());
    }

    const ValueType rhsType = rhs.getValueType();
    if (rhsType != VALUE_TYPE_VARCHAR) {
        throwDynamicSQLException(
                "The right operand of the STARTS WITH expression is %s not %s",
                rhs.getValueTypeString().c_str(),
                getTypeName(VALUE_TYPE_VARCHAR).c_str());
    }

    int32_t valueUTF8Length;
    const char* valueChars = getObject_withoutNull(valueUTF8Length);
    int32_t patternUTF8Length;
    const char* patternChars = rhs.getObject_withoutNull(patternUTF8Length);

    /*
     * The case if pattern is an empty string.
     * Return true only if the left string is also an empty string.
     */
    if (0 == patternUTF8Length) {
        if (0 == valueUTF8Length) {
            return getTrue();
        } else {
            return getFalse();
        }
    }

    UTF8Iterator m_value(valueChars, valueChars + valueUTF8Length);
    UTF8Iterator m_pattern(patternChars, patternChars + patternUTF8Length);

    /*
     * Go through the pattern per single code point to see if pattern is the prefix
     */
    while (! m_pattern.atEnd()) {
        const uint32_t nextPatternCodePoint = m_pattern.extractCodePoint();
        if (m_value.atEnd()) { // if the pattern is longer than the value being checked
            return getFalse();
        }
        const uint32_t nextValueCodePoint = m_value.extractCodePoint();
        if (nextPatternCodePoint != nextValueCodePoint) { // if the current char is not the same
            return getFalse();
        }
    }
    // Have checked the pattern is the prefix of left string, return true
    return getTrue();
}

} // namespace voltdb

