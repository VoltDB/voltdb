/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

namespace voltdb {

/** implement the SQL ABS (absolute value) function for all numeric types */
template<> inline NValue NValue::callUnary<FUNC_ABS>() const {
    const ValueType type = getValueType();
    NValue retval(type);
    switch(type) {
    case VALUE_TYPE_TINYINT:
        retval.getTinyInt() = static_cast<int8_t>(std::abs(getTinyInt())); break;
    case VALUE_TYPE_SMALLINT:
        retval.getSmallInt() = static_cast<int16_t>(std::abs(getSmallInt())); break;
    case VALUE_TYPE_INTEGER:
        retval.getInteger() = std::abs(getInteger()); break;
    case VALUE_TYPE_BIGINT:
        retval.getBigInt() = std::abs(getBigInt()); break;
    case VALUE_TYPE_DOUBLE:
        retval.getDouble() = std::abs(getDouble()); break;
    case VALUE_TYPE_DECIMAL: {
        retval.getDecimal() = getDecimal();
        retval.getDecimal().Abs(); // updates in place!
    }
    break;
    case VALUE_TYPE_TIMESTAMP:
    default:
        throwCastSQLException (type, VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC);
        break;
    }
    return retval;
}

}
