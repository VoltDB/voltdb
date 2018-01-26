/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#include "common/ValueFactory.hpp"

namespace voltdb {

NValue ValueFactory::getRandomValue(ValueType type,
                                    uint32_t maxLength,
                                    Pool* pool) {
    switch (type) {
        case VALUE_TYPE_TIMESTAMP:
            return ValueFactory::getTimestampValue(static_cast<int64_t>(time(NULL)));
        case VALUE_TYPE_TINYINT:
            return ValueFactory::getTinyIntValue(static_cast<int8_t>(rand() % 128));
        case VALUE_TYPE_SMALLINT:
            return ValueFactory::getSmallIntValue(static_cast<int16_t>(rand() % 32768));
        case VALUE_TYPE_INTEGER:
            return ValueFactory::getIntegerValue(rand() % (1 << 31));
        case VALUE_TYPE_BIGINT:
            return ValueFactory::getBigIntValue(rand());
        case VALUE_TYPE_DECIMAL: {
            char characters[29];
            int i;
            for (i = 0; i < 15; ++i) {
                characters[i] = (char)(48 + (rand() % 10));
            }
            characters[i] = '.';
            for (i = 16; i < 28; ++i) {
                characters[i] = (char)(48 + (rand() % 10));
            }
            characters[i] = '\0';
            return ValueFactory::getDecimalValueFromString(std::string(characters));
        }
        case VALUE_TYPE_DOUBLE:
            return ValueFactory::getDoubleValue((rand() % 10000) / double((rand() % 10000) + 1));
        case VALUE_TYPE_VARCHAR: {
            int length = (rand() % maxLength);
            char characters[maxLength];
            for (int ii = 0; ii < length; ii++) {
                characters[ii] = char(32 + (rand() % 94)); //printable characters
            }
            characters[length] = '\0';
            return ValueFactory::getStringValue(string(characters), pool);
        }
        case VALUE_TYPE_VARBINARY: {
            int length = (rand() % maxLength);
            unsigned char bytes[maxLength];
            for (int ii = 0; ii < length; ii++) {
                bytes[ii] = static_cast<unsigned char>(rand() % 256);
            }
            bytes[length] = '\0';
            return ValueFactory::getBinaryValue(bytes, length, pool);
        }
            break;
        case VALUE_TYPE_ARRAY:
        default: {
            throwSerializableEEException("Attempted to get a random value of unsupported value type %d", type);
        }
    }
}

}
