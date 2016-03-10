/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
    static double peekDouble(const NValue& value) {
        assert(value.getValueType() == VALUE_TYPE_DOUBLE);
        return value.getDouble();
    }

    static int8_t peekTinyInt(const NValue& value) {
        assert(value.getValueType() == VALUE_TYPE_TINYINT);
        return value.getTinyInt();
    }

    static int16_t peekSmallInt(const NValue& value) {
        assert(value.getValueType() == VALUE_TYPE_SMALLINT);
        return value.getSmallInt();
    }

    static int32_t peekInteger(const NValue& value) {
        assert(value.getValueType() == VALUE_TYPE_INTEGER);
        return value.getInteger();
    }

    static bool peekBoolean(const NValue& value) {
        assert(value.getValueType() == VALUE_TYPE_BOOLEAN);
        return value.getBoolean();
    }

    // cast as int and peek at value. this is used by index code that need a
    // real number from a tuple and the limit node code used to get the limit
    // from an expression.
    static int32_t peekAsInteger(const NValue& value) {
        return value.castAsInteger().getInteger();
    }

    static int64_t peekBigInt(const NValue& value) {
        assert(value.getValueType() == VALUE_TYPE_BIGINT);
        return value.getBigInt();
    }

    static int64_t peekTimestamp(const NValue& value) {
        assert(value.getValueType() == VALUE_TYPE_TIMESTAMP);
        return value.getTimestamp();
    }

    static const char* peekObjectValue(const NValue& value) {
        assert(isVariableLengthType(value.getValueType()));
        if (value.isNull()) {
            return NULL;
        }
        return value.getObjectValue_withoutNull();
    }

    static const char* peekObject_withoutNull(const NValue& value, int32_t* lengthOut) {
        assert(isVariableLengthType(value.getValueType()));
        // NEEDS WORK
        return value.getObject_withoutNull(lengthOut);
    }

    static ValueType peekValueType(const NValue& value) {
        return value.getValueType();
    }

    static TTInt peekDecimal(const NValue& value) {
        return value.getDecimal();
    }

    static const GeographyValue peekGeographyValue(const NValue& value) {
        return value.getGeographyValue();
    }

    static const GeographyPointValue peekGeographyPointValue(const NValue& value) {
        return value.getGeographyPointValue();
    }

    // exists for test.
    static std::string peekDecimalString(const NValue& value) {
        return value.createStringFromDecimal();
    }

    // cast as big int and peek at value. this is used by
    // index code that need a real number from a tuple.
    static int64_t peekAsBigInt(const NValue& value) {
        if (value.isNull()) {
            return INT64_NULL;
        }
        return value.castAsBigIntAndGetValue();
    }

    static int64_t peekAsRawInt64(const NValue& value) {
        return value.castAsBigIntAndGetValue();
    }

    /// Given an NValue, return a pointer to its data bytes.  Also return
    /// The length of the data bytes via output parameter.
    ///
    /// Assumes that value is not null!!
    static const char* peekPointerToDataBytes(const NValue &value, int32_t *length) {
        ValueType vt = value.getValueType();
        switch (vt) {
        case VALUE_TYPE_TINYINT:
        case VALUE_TYPE_SMALLINT:
        case VALUE_TYPE_INTEGER:
        case VALUE_TYPE_BIGINT:
        case VALUE_TYPE_TIMESTAMP:
        case VALUE_TYPE_DECIMAL:
        case VALUE_TYPE_BOOLEAN:
            *length = static_cast<int32_t>(NValue::getTupleStorageSize(vt));
            return value.m_data;

        default:
            assert(false);
            return NULL;
        }
    }
};

} // end namespace voltdb

#endif /* VALUEPEEKER_HPP_ */
