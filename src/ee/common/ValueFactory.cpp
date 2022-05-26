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

#include "common/ValueFactory.hpp"

namespace voltdb {

NValue ValueFactory::getRandomValue(ValueType type,
                                    uint32_t maxLength,
                                    Pool* pool) {
    switch (type) {
        case ValueType::tTIMESTAMP:
            return ValueFactory::getTimestampValue(static_cast<int64_t>(time(NULL)));
        case ValueType::tTINYINT:
            return ValueFactory::getTinyIntValue(static_cast<int8_t>(rand() % 128));
        case ValueType::tSMALLINT:
            return ValueFactory::getSmallIntValue(static_cast<int16_t>(rand() % 32768));
        case ValueType::tINTEGER:
            return ValueFactory::getIntegerValue(rand() % (1 << 31));
        case ValueType::tBIGINT:
            return ValueFactory::getBigIntValue(rand());
        case ValueType::tDECIMAL:
            {
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
        case ValueType::tDOUBLE:
            return ValueFactory::getDoubleValue((rand() % 10000) / double((rand() % 10000) + 1));
        case ValueType::tVARCHAR:
            {
                int length = (rand() % maxLength);
                char characters[maxLength];
                for (int ii = 0; ii < length; ii++) {
                    characters[ii] = char(32 + (rand() % 94)); //printable characters
                }
                characters[length] = '\0';
                return ValueFactory::getStringValue(string(characters), pool);
            }
        case ValueType::tVARBINARY:
            {
                int length = (rand() % maxLength);
                unsigned char bytes[maxLength];
                for (int ii = 0; ii < length; ii++) {
                    bytes[ii] = static_cast<unsigned char>(rand() % 256);
                }
                bytes[length] = '\0';
                return ValueFactory::getBinaryValue(bytes, length, pool);
            }
            break;
        case ValueType::tARRAY:
        default:
            throwSerializableEEException("Attempted to get a random value of unsupported value type %s",
                    getTypeName(type).c_str());
    }
}

}
