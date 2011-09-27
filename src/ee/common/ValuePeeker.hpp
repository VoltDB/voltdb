/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#ifndef VALUEPEEKER_HPP_
#define VALUEPEEKER_HPP_
#include "common/NValue.hpp"
#include "common/types.h"
#include <cassert>

namespace voltdb {
/*
 * A class for peeking into an NValue and converting its data to a
 * native C++ type. It is necessary for some classes to have access to
 * the actual value in order to serialize, format for printing, or run
 * tests. Moving the functionality for accessing the private data into
 * these static methods allows NValue to define ValuePeeker as its
 * only friend class.  Anything that uses this class is a possible
 * candidate for having its functionality moved into NValue to ensure
 * consistency.
 */
class ValuePeeker {
public:
    static inline double peekDouble(const NValue value) {
        assert(value.getValueType() == VALUE_TYPE_DOUBLE);
        return value.getDouble();
    }

    static inline int8_t peekTinyInt(const NValue value) {
        assert(value.getValueType() == VALUE_TYPE_TINYINT);
        return value.getTinyInt();
    }

    static inline int16_t peekSmallInt(const NValue value) {
        assert(value.getValueType() == VALUE_TYPE_SMALLINT);
        return value.getSmallInt();
    }

    static inline int32_t peekInteger(const NValue value) {
        assert(value.getValueType() == VALUE_TYPE_INTEGER);
        return value.getInteger();
    }

    // cast as int and peek at value. this is used by index code that need a
    // real number from a tuple and the limit node code used to get the limit
    // from an expression.
    static inline int32_t peekAsInteger(const NValue value) {
        return value.castAsInteger().getInteger();
    }

    static inline int64_t peekBigInt(const NValue value) {
        assert(value.getValueType() == VALUE_TYPE_BIGINT);
        return value.getBigInt();
    }

    static inline int64_t peekTimestamp(const NValue value) {
        assert(value.getValueType() == VALUE_TYPE_TIMESTAMP);
        return value.getTimestamp();
    }

    static inline void* peekObjectValue(const NValue value) {
        assert((value.getValueType() == VALUE_TYPE_VARCHAR) ||
               (value.getValueType() == VALUE_TYPE_VARBINARY));
        return value.getObjectValue();
    }

    static inline int32_t peekObjectLength(const NValue value) {
        assert((value.getValueType() == VALUE_TYPE_VARCHAR) ||
               (value.getValueType() == VALUE_TYPE_VARBINARY));
        return value.getObjectLength();
    }

    static inline ValueType peekValueType(const NValue value) {
        return value.getValueType();
    }

    static inline TTInt peekDecimal(const NValue value) {
        return value.getDecimal();
    }

    // exists for test.
    static inline std::string peekDecimalString(const NValue value) {
        return value.createStringFromDecimal();
    }

    // cast as big int and peek at value. this is used by
    // index code that need a real number from a tuple.
    static inline int64_t peekAsBigInt(const NValue value) {
        return value.castAsBigIntAndGetValue();
    }

    static inline int64_t peekAsRawInt64(const NValue value) {
        return value.castAsRawInt64AndGetValue();
    }
};
}

#endif /* VALUEPEEKER_HPP_ */
