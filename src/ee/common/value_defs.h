/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#ifndef VALUE_DEFS_H_
#define VALUE_DEFS_H_

// We use these values for NULL to make it compact and fast
// "-1" is a hack to shut up gcc warning

// null DECIMAL is private. Use VoltDecimal.isNull()

#define INT8_NULL (-127 - 1)
#define INT16_NULL (-32767 - 1)
#define INT32_NULL (-2147483647L - 1)
#define INT64_NULL (INT64_MIN)

//Minimum value user can represent that is not null
#define VOLT_INT8_MIN INT8_NULL + 1
#define VOLT_INT16_MIN INT16_NULL + 1
#define VOLT_INT32_MIN INT32_NULL + 1
#define VOLT_INT64_MIN INT64_NULL + 1
#define VOLT_DECIMAL_MIN -9999999
#define VOLT_DECIMAL_MAX 9999999

// float/double less than these values are null
#define FLOAT_NULL -3.4e+38f
#define DOUBLE_NULL -1.7E+308

// values to be substituted as null
#define FLOAT_MIN -3.40282347e+38f
#define DOUBLE_MIN -1.7976931348623157E+308

// objects (i.e., varchar) with length prefix of -1 are null
#define OBJECTLENGTH_NULL -1

#define VALUE_COMPARE_LESSTHAN -1
#define VALUE_COMPARE_EQUAL 0
#define VALUE_COMPARE_GREATERTHAN 1
#define VALUE_COMPARE_INVALID -2

#endif
