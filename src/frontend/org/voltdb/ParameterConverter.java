/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.Encoder;

import com.google.common.base.Charsets;

/**
 * ParameterConverter provides a static helper to convert a deserialized
 * procedure invocation parameter to the correct Object required by a
 * stored procedure's parameter type.
 *
 */
public class ParameterConverter {

    /**
     * Get the appropriate and compatible null value for a given
     * parameter type.
     */
    private static Object nullValueForType(final Class<?> paramType)
    {
        if (paramType == long.class) {
            return VoltType.BIGINT.getNullValue();
        }
        else if (paramType == int.class) {
            return VoltType.INTEGER.getNullValue();
        }
        else if (paramType == short.class) {
            return VoltType.SMALLINT.getNullValue();
        }
        else if (paramType == byte.class) {
            return VoltType.TINYINT.getNullValue();
        }
        else if (paramType == double.class) {
            return VoltType.FLOAT.getNullValue();
        }

        // all non-primitive types can handle null
        return null;
    }

    /**
     * Assertion-heavy method to verify the type tryToMakeCompatible returns
     * is exactly the type asked for (or null in some cases).
     */
    public static boolean verifyParameterConversion(
            Object value,
            final Class<?> paramType)
    {
        // skip this (used for sysprocs)
        if (value instanceof SystemProcedureExecutionContext)
            return true;

        if (paramType == long.class) {
            assert(value != null);
            assert(value.getClass() == Long.class);
        }
        else if (paramType == int.class) {
            assert(value != null);
            assert(value.getClass() == Integer.class);
        }
        else if (paramType == short.class) {
            assert(value != null);
            assert(value.getClass() == Short.class);
        }
        else if (paramType == byte.class) {
            assert(value != null);
            assert(value.getClass() == Byte.class);
        }
        else if (paramType == double.class) {
            assert(value != null);
            assert(value.getClass() == Double.class);
        }
        else if (value != null) {
            assert(value.getClass() == paramType);
            if (paramType.isArray()) {
                assert(value.getClass().getComponentType() == paramType.getComponentType());
            }
        }
        return true;
    }

    /**
     *
     * @param value
     * @param paramType
     * @return
     * @throws Exception
     */
    private static Object convertStringToPrimitive(String value, final Class<?> paramType)
    throws Exception
    {
        value = value.trim();
        // detect CSV null
        if (value.equals(VoltTable.CSV_NULL)) return null;
        // remove commas and escape chars
        value = value.replaceAll("\\,","");

        try {
            if (paramType == long.class) {
                return Long.parseLong(value);
            }
            if (paramType == int.class) {
                return Integer.parseInt(value);
            }
            if (paramType == short.class) {
                return Short.parseShort(value);
            }
            if (paramType == byte.class) {
                return Byte.parseByte(value);
            }
            if (paramType == double.class) {
                return Double.parseDouble(value);
            }
        }
        // ignore the exception and fail through below
        catch (NumberFormatException nfe) {}

        throw new Exception(
                "tryToMakeCompatible: Unable to convert string "
                + value + " to "  + paramType.getName()
                + " value for target parameter.");
    }

    /**
     * Factored out code to handle array parameter types.
     *
     * @throws Exception with a message describing why the types are incompatible.
     */
    private static Object tryToMakeCompatibleArray(
            final Class<?> paramTypeComponentType,
            final Class<?> pclass,
            Object param)
    throws Exception
    {
        Class<?> pSubCls = pclass.getComponentType();
        Class<?> sSubCls = paramTypeComponentType;
        int inputLength = Array.getLength(param);

        if (pSubCls == sSubCls) {
            return param;
        }
        // if it's an empty array, let it through
        // this is a bit ugly as it might hide passing
        //  arrays of the wrong type, but it "does the right thing"
        //  more often that not I guess...
        else if (inputLength == 0) {
            return Array.newInstance(sSubCls, 0);
        }
        // hack to make strings work with input as bytes
        else if ((pSubCls == byte[].class) && (sSubCls == String.class)) {
            String[] values = new String[inputLength];
            for (int i = 0; i < inputLength; i++) {
                values[i] = new String((byte[]) Array.get(param, i), "UTF-8");
            }
            return values;
        }
        // hack to make varbinary work with input as hex string
        else if ((pSubCls == String.class) && (sSubCls == byte[].class)) {
            byte[][] values = new byte[inputLength][];
            for (int i = 0; i < inputLength; i++) {
                values[i] = Encoder.hexDecode((String) Array.get(param, i));
            }
            return values;
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

    /**
     * Convert the given value to the type given, if possible.
     *
     * This function is in the performance path, so some effort has been made to order
     * the giant string of branches such that most likely things are first, and that
     * if the type is already correct, it should move very quickly through the logic.
     * Some clarity has been sacrificed for performance, but perfect clarity is pretty
     * elusive with complicated logic like this anyway.
     *
     * @throws Exception with a message describing why the types are incompatible.
     */
    public static Object tryToMakeCompatible(final Class<?> paramType, final Object param)
    throws Exception
    {
        // uncomment for debugging
        /*System.err.printf("Converting %s of type %s to type %s\n",
                String.valueOf(param),
                param == null ? "NULL" : param.getClass().getName(),
                paramType.getName());
        System.err.flush();*/

        // Get blatant null out of the way fast, as it avoids some inline checks
        // There are some suble null values that aren't java null coming up, but wait until
        // after the basics to check for those.
        if (param == null) {
            return nullValueForType(paramType);
        }

        Class<?> pclass = param.getClass();

        // If we make it through this first block, memoize a number value for some range checks later
        Number numberParam = null;

        // This first code block tries to hit as many common cases as possible
        // Specifically, it does primitive types and strings, which are the most common param types.
        // Downconversions (e.g. long to short) happen later, but can use the memoized numberParam value.
        // Notice this block switches on the type of the given value (different later).

        if (pclass == Long.class) {
            if (paramType == long.class) return param;
            if ((Long) param == VoltType.NULL_BIGINT) return nullValueForType(paramType);
            numberParam = (Number) param;
        }
        else if (pclass == Integer.class) {
            if (paramType == int.class) return param;
            if ((Integer) param == VoltType.NULL_INTEGER) return nullValueForType(paramType);
            if (paramType == long.class) return ((Integer) param).longValue();
            numberParam = (Number) param;
        }
        else if (pclass == Short.class) {
            if (paramType == short.class) return param;
            if ((Short) param == VoltType.NULL_SMALLINT) return nullValueForType(paramType);
            if (paramType == long.class) return ((Short) param).longValue();
            if (paramType == int.class) return ((Short) param).intValue();
            numberParam = (Number) param;
        }
        else if (pclass == Byte.class) {
            if (paramType == byte.class) return param;
            if ((Byte) param == VoltType.NULL_TINYINT) return nullValueForType(paramType);
            if (paramType == long.class) return ((Byte) param).longValue();
            if (paramType == int.class) return ((Byte) param).intValue();
            if (paramType == short.class) return ((Byte) param).shortValue();
            numberParam = (Number) param;
        }
        else if (pclass == Double.class) {
            if (paramType == double.class) return param;
            if ((Double) param == VoltType.NULL_FLOAT) return nullValueForType(paramType);
        }
        else if (pclass == String.class) {
            if (((String) param).equals(VoltTable.CSV_NULL)) return nullValueForType(paramType);
            else if (paramType == String.class) return param;
            // Hack allows hex-encoded strings to be passed into byte[] params
            else if (paramType == byte[].class) {
                return Encoder.hexDecode((String) param);
            }
            // We allow all values to be passed as strings for csv loading, json, etc...
            // This code handles primitive types. Complex types come later.
            if (paramType.isPrimitive()) {
                return convertStringToPrimitive((String) param, paramType);
            }
        }
        else if (pclass == byte[].class) {
            if (paramType == byte[].class) return param;
            // allow byte arrays to be passed into string parameters
            else if (paramType == String.class) {
                String value = new String((byte[]) param, Charsets.UTF_8);
                if (value.equals(VoltTable.CSV_NULL)) return nullValueForType(paramType);
                else return value;
            }
        }
        // null sigil
        else if (param == VoltType.NULL_STRING_OR_VARBINARY) {
            return nullValueForType(paramType);
        }
        // null sigil
        else if (param == VoltType.NULL_DECIMAL) {
            return nullValueForType(paramType);
        }
        // these are used by system procedures and are ignored here
        else if (param instanceof SystemProcedureExecutionContext) {
            return param;
        }

        // make sure we get the array/scalar match
        if (paramType.isArray() != pclass.isArray()) {
            throw new Exception(String.format("Array / Scalar parameter mismatch (%s to %s)",
                    pclass.getName(), paramType.getName()));
        }

        // handle arrays in a factored-out method
        if (paramType.isArray()) {
            return tryToMakeCompatibleArray(paramType.getComponentType(), pclass, param);
        }

        // The following block switches on the type of the paramter desired.
        // It handles all of the paths not trapped in the code above. We can assume
        // values are not null and that most sane primitive stuff has been handled.
        // Downcasting is handled here (e.g. long => short).
        // Time (in many forms) and Decimal are also handled below.

        if ((paramType == int.class) && (numberParam != null)) {
            long val = numberParam.longValue();
            if (val == VoltType.NULL_INTEGER) {
                throw new Exception("tryToMakeCompatible: The provided long value: ("
                        + param.toString() + ") might be interpreted as integer null. " +
                                "Try explicitly using a int parameter.");
            }
            // if it's in the right range, crop the value and return
            if ((val <= Integer.MAX_VALUE) && (val >= Integer.MIN_VALUE))
                return numberParam.intValue();
        }
        else if ((paramType == short.class) && (numberParam != null)) {
            if ((pclass == Long.class) || (pclass == Integer.class)) {
                long val = numberParam.longValue();
                if (val == VoltType.NULL_SMALLINT) {
                    throw new Exception("tryToMakeCompatible: The provided int or long value: ("
                            + param.toString() + ") might be interpreted as smallint null. " +
                                    "Try explicitly using a short parameter.");
                }
                // if it's in the right range, crop the value and return
                if ((val <= Short.MAX_VALUE) && (val >= Short.MIN_VALUE))
                    return numberParam.shortValue();
            }
        }
        else if ((paramType == byte.class) && (numberParam != null)) {
            if ((pclass == Long.class) || (pclass == Integer.class) || (pclass == Short.class)) {
                long val = numberParam.longValue();
                if (val == VoltType.NULL_TINYINT) {
                    throw new Exception("tryToMakeCompatible: The provided short, int or long value: ("
                            + param.toString() + ") might be interpreted as tinyint null. " +
                                    "Try explicitly using a byte parameter.");
                }
                // if it's in the right range, crop the value and return
                if ((val <= Byte.MAX_VALUE) && (val >= Byte.MIN_VALUE))
                    return numberParam.byteValue();
            }
        }
        else if ((paramType == double.class) && (numberParam != null)) {
            return numberParam.doubleValue();
        }
        else if (paramType == TimestampType.class) {
            if (pclass == Long.class) return new TimestampType((Long)param); // null values safe
            if (pclass == TimestampType.class) return param;
            if (pclass == Date.class) return new TimestampType((Date) param);
            // if a string is given for a date, use java's JDBC parsing
            if (pclass == String.class) {
                String longtime = ((String) param).trim();
                try {
                    return new TimestampType(Long.parseLong(longtime));
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
        else if (paramType == java.sql.Timestamp.class) {
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
        else if (paramType == java.sql.Date.class) {
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
        else if (paramType == java.util.Date.class) {
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
        else if (paramType == BigDecimal.class) {
            if (numberParam != null) {
                BigInteger bi = new BigInteger(param.toString());
                BigDecimal bd = new BigDecimal(bi);
                bd = bd.setScale(12, BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
            if (pclass == BigDecimal.class) {
                BigDecimal bd = (BigDecimal) param;
                bd = bd.setScale(12 ,BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
            if (paramType == BigDecimal.class) {
                return VoltDecimalHelper.deserializeBigDecimalFromString((String) param);
            }
        }
        else if (paramType == VoltTable.class && pclass == VoltTable.class) {
            return param;
        }

        throw new Exception(
                "tryToMakeCompatible: The provided value: (" + param.toString() + ") of type: " + pclass.getName() +
                " is not a match or is out of range for the target parameter type: " + paramType.getName());
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

