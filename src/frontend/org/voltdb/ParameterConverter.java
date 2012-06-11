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

package org.voltdb;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.Timestamp;
import java.util.Date;

import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.Encoder;

/**
 * ParameterConverter provides a static helper to convert a deserialized
 * procedure invocation parameter to the correct Object required by a
 * stored procedure's parameter type.
 *
 */
public class ParameterConverter {

    /**
     * @throws Exception with a message describing why the types are incompatible.
     */
    public static Object tryToMakeCompatible(
            final boolean isPrimitive,
            final boolean isArray,
            final Class<?> paramType,
            final Class<?> paramTypeComponentType,
            final Object param)
    throws Exception
    {
        if (param == null ||
            param == VoltType.NULL_STRING_OR_VARBINARY ||
            param == VoltType.NULL_DECIMAL) {
            if (isPrimitive) {
                VoltType type = VoltType.typeFromClass(paramType);
                switch (type) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                    return type.getNullValue();
                }
            }

            // Pass null reference to the procedure run() method. These null values will be
            // converted to a serialize-able NULL representation for the EE in getCleanParams()
            // when the parameters are serialized for the plan fragment.
            return null;
        }

        if (param instanceof SystemProcedureExecutionContext) {
            return param;
        }

        Class<?> pclass = param.getClass();

        // hack to make strings work with input as byte[]
        if ((paramType == String.class) && (pclass == byte[].class)) {
            String sparam = null;
            sparam = new String((byte[]) param, "UTF-8");
            return sparam;
        }

        // hack to make varbinary work with input as string
        if ((paramType == byte[].class) && (pclass == String.class)) {
            return Encoder.hexDecode((String) param);
        }

        if (isArray != pclass.isArray()) {
            throw new Exception("Array / Scalar parameter mismatch");
        }

        if (isArray) {
            Class<?> pSubCls = pclass.getComponentType();
            Class<?> sSubCls = paramTypeComponentType;
            if (pSubCls == sSubCls) {
                return param;
            }
            // if it's an empty array, let it through
            // this is a bit ugly as it might hide passing
            //  arrays of the wrong type, but it "does the right thing"
            //  more often that not I guess...
            else if (Array.getLength(param) == 0) {
                return Array.newInstance(sSubCls, 0);
            }
            else {
                /*
                 * Arrays can be quite large so it doesn't make sense to silently do the conversion
                 * and incur the performance hit. The client should serialize the correct invocation
                 * parameters
                 */
                throw new Exception(
                        "tryScalarMakeCompatible: Unable to match parameter array:"
                        + sSubCls.getName() + " to provided " + pSubCls.getName());
            }
        }

        /*
         * inline tryScalarMakeCompatible so we can save on reflection
         */
        final Class<?> slot = paramType;
        if ((slot == long.class) && (pclass == Long.class || pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == int.class) && (pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == short.class) && (pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == byte.class) && (pclass == Byte.class)) return param;
        if ((slot == double.class) && (param instanceof Number)) return ((Number)param).doubleValue();
        if ((slot == float.class) && (param instanceof Number)) return ((Number)param).floatValue();
        if ((slot == String.class) && (pclass == String.class)) return param;
        if (slot == TimestampType.class) {
            if (pclass == Long.class) return new TimestampType((Long)param);
            if (pclass == TimestampType.class) return param;
            if (pclass == Date.class) return new TimestampType((Date) param);
            // if a string is given for a date, use java's JDBC parsing
            if (pclass == String.class) {
            	String longtime = ((String) param).trim();
                try {
                	return new java.sql.Timestamp(Long.parseLong(longtime));
                } catch (IllegalArgumentException e) {
                	// Defer errors to the generic Exception throw below, if it's not the right format
                }
                try {
                    return new TimestampType(longtime);
                }
                catch (IllegalArgumentException e) {
                    // Defer errors to the generic Exception throw below, if it's not the right format
                }
            }
        }
        else if (slot == java.sql.Timestamp.class) {
            if (param instanceof java.sql.Timestamp) return param;
            if (param instanceof java.util.Date) return new java.sql.Timestamp(((java.util.Date) param).getTime());
            if (param instanceof TimestampType) return ((TimestampType) param).asJavaTimestamp();
            // If a string is given for a date, use java's JDBC parsing.
            if (pclass == String.class) {
            	String longtime = ((String) param).trim();
                try {
                	return new java.sql.Timestamp(Long.parseLong(longtime));
                }
                catch (IllegalArgumentException e) {
                    // Defer errors to the generic Exception throw below, if it's not the right format
                }
                try {
                	return java.sql.Timestamp.valueOf(longtime);
                } catch (IllegalArgumentException e) {
                	// Defer errors to the generic Exception throw below, if it's not the right format
                }
                
            }
        }
        else if (slot == java.sql.Date.class) {
            if (param instanceof java.sql.Date) return param; // covers java.sql.Date and java.sql.Timestamp
            if (param instanceof java.util.Date) return new java.sql.Date(((java.util.Date) param).getTime());
            if (param instanceof TimestampType) return ((TimestampType) param).asExactJavaSqlDate();
            // If a string is given for a date, use java's JDBC parsing.
            if (pclass == String.class) {
                try {
                    return new java.sql.Date(TimestampType.millisFromJDBCformat((String) param));
                }
                catch (IllegalArgumentException e) {
                    // Defer errors to the generic Exception throw below, if it's not the right format
                }
            }
        }
        else if (slot == java.util.Date.class) {
            if (param instanceof java.util.Date) return param; // covers java.sql.Date and java.sql.Timestamp
            if (param instanceof TimestampType) return ((TimestampType) param).asExactJavaDate();
            // If a string is given for a date, use the default format parser for the default locale.
            if (pclass == String.class) {
                try {
                    return new java.util.Date(TimestampType.millisFromJDBCformat((String) param));
                }
                catch (IllegalArgumentException e) {
                    // Defer errors to the generic Exception throw below, if it's not the right format
                }
            }
        }
        else if (slot == BigDecimal.class) {
            if ((pclass == Long.class) || (pclass == Integer.class) ||
                (pclass == Short.class) || (pclass == Byte.class)) {
                BigInteger bi = new BigInteger(param.toString());
                BigDecimal bd = new BigDecimal(bi);
                bd.setScale(4, BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
            if (pclass == BigDecimal.class) {
                BigDecimal bd = (BigDecimal) param;
                bd.setScale(4, BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
            if (pclass == String.class) {
                BigDecimal bd = VoltDecimalHelper.deserializeBigDecimalFromString((String) param);
                return bd;
            }
        }
        else if (slot == VoltTable.class && pclass == VoltTable.class) {
            return param;
        }

        // handle truncation for integers

        // Long targeting int parameter
        else if ((slot == int.class) && (pclass == Long.class)) {
            long val = ((Number) param).longValue();

            // if it's in the right range, and not null (target null), crop the value and return
            if ((val <= Integer.MAX_VALUE) && (val >= Integer.MIN_VALUE) && (val != VoltType.NULL_INTEGER))
                return ((Number) param).intValue();
        }

        // Long or Integer targeting short parameter
        else if ((slot == short.class) && (pclass == Long.class || pclass == Integer.class)) {
            long val = ((Number) param).longValue();

            // if it's in the right range, and not null (target null), crop the value and return
            if ((val <= Short.MAX_VALUE) && (val >= Short.MIN_VALUE) && (val != VoltType.NULL_SMALLINT))
                return ((Number) param).shortValue();
        }

        // Long, Integer or Short targeting byte parameter
        else if ((slot == byte.class) && (pclass == Long.class || pclass == Integer.class || pclass == Short.class)) {
            long val = ((Number) param).longValue();

            // if it's in the right range, and not null (target null), crop the value and return
            if ((val <= Byte.MAX_VALUE) && (val >= Byte.MIN_VALUE) && (val != VoltType.NULL_TINYINT))
                return ((Number) param).byteValue();
        }

        // Coerce strings to primitive numbers.
        else if (pclass == String.class) {
            try {
            	String value = ((String) param).trim();
            	value = value.replaceAll("\\,","");
            	if (slot == byte.class) {
                    return Byte.parseByte(value);
                }
                if (slot == short.class) {
                    return Short.parseShort(value);
                }
                if (slot == int.class) {
                    return Integer.parseInt(value);
                }
                if (slot == long.class) {
                    return Long.parseLong(value);
                }
                if (slot == double.class) {
                	return Double.parseDouble(value);
                }
                if (slot == float.class) {
                	return Float.parseFloat(value);
                }
            }
            catch (NumberFormatException nfe) {
                throw new Exception(
                        "tryToMakeCompatible: Unable to convert string "
                        + (String)param + " to "  + slot.getName()
                        + " value for target parameter " + slot.getName());
            }
        }

        throw new Exception(
                "tryToMakeCompatible: The provided value: (" + param.toString() + ") of type: " + pclass.getName() +
                " is not a match or is out of range for the target parameter type: " + slot.getName());
    }


    /**
     * Convert string inputs to Longs for TheHashinator if possible
     * @param param
     * @param slot
     * @return Object parsed as Number or null if types not compatible
     * @throws Exception if a parse error occurs (consistent with above).
     */
    public static Object stringToLong(Object param, Class<?> slot)
    throws Exception
    {
        try {
            if (slot == byte.class ||
                slot == short.class ||
                slot == int.class ||
                slot == long.class)
            {
                return Long.parseLong((String)param);
            }
            return null;
        }
        catch (NumberFormatException nfe) {
            throw new Exception(
                    "tryToMakeCompatible: Unable to convert string "
                    + (String)param + " to "  + slot.getName()
                    + " value for target parameter " + slot.getName());
        }
    }


}

