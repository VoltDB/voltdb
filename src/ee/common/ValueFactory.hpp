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

#ifndef VALUEFACTORY_HPP_
#define VALUEFACTORY_HPP_

#include "common/NValue.hpp"

namespace voltdb {
class ValueFactory {
public:

    static inline NValue getTinyIntValue(int8_t value) {
        return NValue::getTinyIntValue(value);
    }

    static inline NValue getSmallIntValue(int16_t value) {
        return NValue::getSmallIntValue(value);
    }

    static inline NValue getIntegerValue(int32_t value) {
        return NValue::getIntegerValue(value);
    }

    static inline NValue getBigIntValue(int64_t value) {
        return NValue::getBigIntValue(value);
    }

    static inline NValue getTimestampValue(int64_t value) {
        return NValue::getTimestampValue(value);
    }

    static inline NValue getDoubleValue(double value) {
        return NValue::getDoubleValue(value);
    }

    static inline NValue getStringValue(std::string value) {
        return NValue::getStringValue(value);
    }

    static inline NValue getNullStringValue() {
        return NValue::getNullStringValue();
    }

    static inline NValue getBinaryValue(std::string value) {
        // uses hex encoding
        return NValue::getBinaryValue(value);
    }

    static inline NValue getBinaryValue(unsigned char* value, int32_t len) {
        return NValue::getBinaryValue(value, len);
    }

    static inline NValue getNullBinaryValue() {
        return NValue::getNullBinaryValue();
    }

    /** Returns valuetype = VALUE_TYPE_NULL. Careful with this! */
    static inline NValue getNullValue() {
        return NValue::getNullValue();
    }

    static inline NValue getDecimalValueFromString(const std::string &txt) {
        return NValue::getDecimalValueFromString(txt);
    }

    static inline NValue getAddressValue(void *address) {
        return NValue::getAddressValue(address);
    }

    // What follows exists for test only!

    static inline NValue castAsBigInt(NValue value) {
        return value.castAsBigInt();
    }

    static inline NValue castAsInteger(NValue value) {
        return value.castAsInteger();
    }

    static inline NValue castAsSmallInt(NValue value) {
        return value.castAsSmallInt();
    }

    static inline NValue castAsTinyInt(NValue value) {
        return value.castAsTinyInt();
    }

    static inline NValue castAsDouble(NValue value) {
        return value.castAsDouble();
    }

    static inline NValue castAsDecimal(NValue value) {
        return value.castAsDecimal();
    }

    static inline NValue castAsString(NValue value) {
        return value.castAsString();
    }
};
}
#endif /* VALUEFACTORY_HPP_ */
