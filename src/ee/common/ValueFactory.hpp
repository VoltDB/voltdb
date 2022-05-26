/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include "common/NValue.hpp"

namespace voltdb {
class ValueFactory {
public:

    static NValue getTinyIntValue(int8_t value) {
        return NValue::getTinyIntValue(value);
    }

    static NValue getSmallIntValue(int16_t value) {
        return NValue::getSmallIntValue(value);
    }

    static NValue getIntegerValue(int32_t value) {
        return NValue::getIntegerValue(value);
    }

    static NValue getBigIntValue(int64_t value) {
        return NValue::getBigIntValue(value);
    }

    static NValue getTimestampValue(int64_t value) {
        return NValue::getTimestampValue(value);
    }

    static NValue getDoubleValue(double value) {
        return NValue::getDoubleValue(value);
    }

    static NValue getDecimalValue(double value) {
        NValue doubleNVal = ValueFactory::getDoubleValue(value);
        return doubleNVal.castAsDecimal();
    }

    static NValue getBooleanValue(bool value) {
        return NValue::getBooleanValue(value);
    }

    /// Constructs a value copied into long-lived pooled memory (or the heap)
    /// that will require an explicit NValue::free.
    static NValue getStringValue(const char *value, Pool* pool = NULL) {
        return NValue::getAllocatedValue(ValueType::tVARCHAR, value, (size_t)(value ? strlen(value) : 0), pool);
    }

    /// Constructs a value copied into long-lived pooled memory (or the heap)
    /// that will require an explicit NValue::free.
    static NValue getStringValue(const std::string& value, Pool* pool = NULL) {
        return NValue::getAllocatedValue(ValueType::tVARCHAR, value.c_str(), value.length(), pool);
    }

    /// Constructs a value copied into temporary thread-local storage.
    static NValue getTempStringValue(const std::string& value) {
        return NValue::getTempStringValue(value.c_str(), value.length());
    }

    static NValue getTempStringValue(const char *value, size_t length) {
        return NValue::getTempStringValue(value, length);
    }

    static NValue getNullStringValue() {
        return NValue::getNullStringValue();
    }

    /// Constructs a value copied into long-lived pooled memory (or the heap)
    /// that will require an explicit NValue::free.
    /// Assumes hex-encoded input
    static NValue getBinaryValue(const std::string& value, Pool* pool = NULL) {
        size_t rawLength = value.length() / 2;
        unsigned char rawBuf[rawLength];
        hexDecodeToBinary(rawBuf, value.c_str());
        return getBinaryValue(rawBuf, (int32_t)rawLength, pool);
    }

    /// Constructs a value copied into temporary string pool
    /// Assumes hex-encoded input
    static NValue getTempBinaryValue(const std::string& value) {
        size_t rawLength = value.length() / 2;
        unsigned char rawBuf[rawLength];
        hexDecodeToBinary(rawBuf, value.c_str());
        return getBinaryValue(rawBuf, static_cast<int32_t>(rawLength),
                              NValue::getTempStringPool());
    }

    /// Constructs a varbinary value copied into temporary string
    /// pool.  Arguments provide a pointer to the raw bytes and the
    /// size of the value.
    static NValue getTempBinaryValue(const char* rawBuf, int32_t rawLength) {
        return NValue::getAllocatedValue(ValueType::tVARBINARY, rawBuf, rawLength,
                                         NValue::getTempStringPool());
    }

    /// Constructs a value copied into long-lived pooled memory (or the heap)
    /// that will require an explicit NValue::free.
    /// Assumes raw byte input
    static NValue getBinaryValue(const unsigned char* rawBuf, int32_t rawLength, Pool* pool = NULL) {
        return NValue::getAllocatedValue(ValueType::tVARBINARY,
                                         reinterpret_cast<const char*>(rawBuf),
                                         (size_t)rawLength,
                                         pool);
    }

    static NValue getNullBinaryValue() {
        return NValue::getNullBinaryValue();
    }

    /// Returns an NValue of type Geography that points to an uninitialized temp buffer of the given size
    static inline NValue getUninitializedTempGeographyValue(int32_t length) {
        NValue retval(ValueType::tGEOGRAPHY);
        retval.allocateValueStorage(length, NValue::getTempStringPool());
        return retval;
    }

    // Constructs a geography point NValue from value
    static inline NValue getGeographyPointValue(const GeographyPointValue* value) {
        NValue retval(ValueType::tPOINT);
        if (value == nullptr) {
            retval.setNull();
        } else {
            retval.getGeographyPointValue() = *value;
        }
        return retval;
    }

    // Constructs a geography NValue from value using pool if provided
    static inline NValue getGeographyValue(const Polygon* value, Pool* pool = nullptr) {
        NValue retval(ValueType::tGEOGRAPHY);
        if (value == nullptr) {
            retval.setNull();
        } else {
            size_t len = value->serializedLength();
            char *data = retval.allocateValueStorage(len, pool == nullptr ? NValue::getTempStringPool() : pool);
            SimpleOutputSerializer output(data, len);
            value->saveToBuffer(output);
        }
        return retval;
    }

    /** Returns valuetype = tNULL. Careful with this! */
    static NValue getNullValue() {
        return NValue::getNullValue();
    }

    static NValue getDecimalValueFromString(const std::string &txt) {
        return NValue::getDecimalValueFromString(txt);
    }

    static NValue getArrayValueFromSizeAndType(size_t elementCount,
                                               ValueType elementType) {
        return NValue::getAllocatedArrayValueFromSizeAndType(elementCount, elementType);
    }

    static NValue getAddressValue(void *address) {
        return NValue::getAddressValue(address);
    }

    // What follows exists for test only!

    static NValue castAsBigInt(const NValue& value) {
        if (value.isNull()) {
            return NValue::getNullValue(ValueType::tBIGINT);
        }
        return value.castAsBigInt();
    }

    static NValue castAsInteger(const NValue& value) {
        if (value.isNull()) {
            NValue retval(ValueType::tINTEGER);
            retval.setNull();
            return retval;
        }

        return value.castAsInteger();
    }

    static NValue castAsSmallInt(const NValue& value) {
        if (value.isNull()) {
            NValue retval(ValueType::tSMALLINT);
            retval.setNull();
            return retval;
        }

        return value.castAsSmallInt();
    }

    static NValue castAsTinyInt(const NValue& value) {
        if (value.isNull()) {
            NValue retval(ValueType::tTINYINT);
            retval.setNull();
            return retval;
        }

        return value.castAsTinyInt();
    }

    static NValue castAsDouble(const NValue& value) {
        if (value.isNull()) {
            NValue retval(ValueType::tDOUBLE);
            retval.setNull();
            return retval;
        }

        return value.castAsDouble();
    }

    static NValue castAsDecimal(const NValue& value) {
        if (value.isNull()) {
            NValue retval(ValueType::tDECIMAL);
            retval.setNull();
            return retval;
        }
        return value.castAsDecimal();
    }

    static NValue castAsString(const NValue& value) {
        return value.castAsString();
    }

    // Get an empty NValue with specified data type.
    static NValue getNValueOfType(const ValueType type) {
        NValue retval(type);
        return retval;
    }

    static NValue nvalueFromSQLDefaultType(const ValueType type, const std::string &value, Pool* pool) {
        switch (type) {
            case ValueType::tNULL:
                return getNullValue();
            case ValueType::tTINYINT:
            case ValueType::tSMALLINT:
            case ValueType::tINTEGER:
            case ValueType::tBIGINT:
            case ValueType::tTIMESTAMP:
            {
                NValue retval(ValueType::tBIGINT);
                int64_t ival = atol(value.c_str());
                retval = getBigIntValue(ival);
                return retval.castAs(type);
            }
            case ValueType::tDECIMAL:
                return getDecimalValueFromString(value);
            case ValueType::tDOUBLE:
            {
                double dval = atof(value.c_str());
                return getDoubleValue(dval);
            }
            case ValueType::tVARCHAR:
                return getStringValue(value.c_str(), pool);
            case ValueType::tVARBINARY:
                return getBinaryValue(value, pool);
            default:; // skip to throw
        }
        throwDynamicSQLException("Default value parsing error.");
    }

    static NValue getRandomValue(ValueType type, uint32_t maxLength, Pool* pool = NULL);
};
}
