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

#include "common/ValueFactory.hpp"

namespace voltdb {

NValue ValueFactory::getRandomValue(ValueType type, uint32_t maxLength, Pool* pool) {
    auto const r = rand();
    switch (type) {
        case ValueType::tTIMESTAMP:
            return getTimestampValue(time(nullptr));
        case ValueType::tTINYINT:
            return getTinyIntValue(r % 128);
        case ValueType::tSMALLINT:
            return getSmallIntValue(r % 32768);
        case ValueType::tINTEGER:
            return getIntegerValue(r % (1 << 31));
        case ValueType::tBIGINT:
            return getBigIntValue(r);
        case ValueType::tDECIMAL:
            {
                char characters[29];
                int i;
                for (i = 0; i < 15; ++i) {
                    characters[i] = static_cast<char>(48 + rand() % 10);
                }
                characters[i] = '.';
                for (i = 16; i < 28; ++i) {
                    characters[i] = static_cast<char>(48 + rand() % 10);
                }
                characters[i] = '\0';
                return getDecimalValueFromString(characters);
            }
        case ValueType::tDOUBLE:
            return getDoubleValue((r % 10000) / (rand() % 10000 + 1.));
        case ValueType::tVARCHAR:
            {
                int const length = r % maxLength;
                char characters[maxLength];
                for (int ii = 0; ii < length; ii++) {
                    characters[ii] = static_cast<char>(32 + rand() % 94); //printable characters
                }
                characters[length] = '\0';
                return getStringValue(characters, pool);
            }
        case ValueType::tVARBINARY:
            {
                int const length = r % maxLength;
                unsigned char bytes[maxLength];
                for (int ii = 0; ii < length; ii++) {
                    bytes[ii] = static_cast<unsigned char>(rand() % 256);
                }
                bytes[length] = '\0';
                return getBinaryValue(bytes, length, pool);
            }
            break;
        case ValueType::tARRAY:
        default:
            throwSerializableEEException(
                    "Attempted to get a random value of unsupported value type %s",
                    getTypeName(type).c_str());
    }
}

}
