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

package org.voltdb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.regex.Pattern;

import org.voltdb.common.Constants;
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
     * Get the appropriate and compatible null value for a given
     * parameter type.
     */
    private static Object nullValueForType(final Class<?> expectedClz)
    {
        if (expectedClz == long.class) {
            return VoltType.NULL_BIGINT;
        }
        else if (expectedClz == int.class) {
            return VoltType.NULL_INTEGER;
        }
        else if (expectedClz == short.class) {
            return VoltType.NULL_SMALLINT;
        }
        else if (expectedClz == byte.class) {
            return VoltType.NULL_TINYINT;
        }
        else if (expectedClz == double.class) {
            return VoltType.NULL_FLOAT;
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
            final Class<?> expectedClz)
    {
        if (expectedClz == long.class) {
            assert(value != null);
            assert(value.getClass() == Long.class);
        }
        else if (expectedClz == int.class) {
            assert(value != null);
            assert(value.getClass() == Integer.class);
        }
        else if (expectedClz == short.class) {
            assert(value != null);
            assert(value.getClass() == Short.class);
        }
        else if (expectedClz == byte.class) {
            assert(value != null);
            assert(value.getClass() == Byte.class);
        }
        else if (expectedClz == double.class) {
            assert(value != null);
            assert(value.getClass() == Double.class);
        }
        else if (value != null) {
            Class<?> clz = value.getClass();
            if (clz != expectedClz) {
                // skip this without linking to it (used for sysprocs)
                return expectedClz.getSimpleName().equals("SystemProcedureExecutionContext") &&
                        expectedClz.isAssignableFrom(clz);
            }
            if (expectedClz.isArray()) {
                assert(clz.getComponentType() == expectedClz.getComponentType());
            }
        }
        return true;
    }

    private static final Pattern thousandSeparator = Pattern.compile("\\,");
    /**
     * Given a string, covert it to a primitive type or return null.
     */
    private static Object convertStringToPrimitive(String value, final Class<?> expectedClz)
    throws VoltTypeException
    {
        value = value.trim();
        // detect CSV null
        if (value.equals(Constants.CSV_NULL)) return nullValueForType(expectedClz);
        // remove commas and escape chars
        value = thousandSeparator.matcher(value).replaceAll("");

        try {
            if (expectedClz == long.class) {
                return Long.parseLong(value);
            }
            if (expectedClz == int.class) {
                return Integer.parseInt(value);
            }
            if (expectedClz == short.class) {
                return Short.parseShort(value);
            }
            if (expectedClz == byte.class) {
                return Byte.parseByte(value);
            }
            if (expectedClz == double.class) {
                return Double.parseDouble(value);
            }
        }
        // ignore the exception and fail through below
        catch (NumberFormatException nfe) {}

        throw new VoltTypeException(
                "tryToMakeCompatible: Unable to convert string "
                + value + " to "  + expectedClz.getName()
                + " value for target parameter.");
    }

    /**
     * Factored out code to handle array parameter types.
     *
     * @throws Exception with a message describing why the types are incompatible.
     */
    private static Object tryToMakeCompatibleArray(
            final Class<?> expectedComponentClz,
            final Class<?> inputComponentClz,
            Object param)
    throws VoltTypeException
    {
        int inputLength = Array.getLength(param);

        if (inputComponentClz == expectedComponentClz) {
            return param;
        }
        // if it's an empty array, let it through
        // this is a bit ugly as it might hide passing
        //  arrays of the wrong type, but it "does the right thing"
        //  more often that not I guess...
        else if (inputLength == 0) {
            return Array.newInstance(expectedComponentClz, 0);
        }
        // hack to make strings work with input as bytes
        else if ((inputComponentClz == byte[].class) && (expectedComponentClz == String.class)) {
            String[] values = new String[inputLength];
            for (int i = 0; i < inputLength; i++) {
                try {
                    values[i] = new String((byte[]) Array.get(param, i), "UTF-8");
                } catch (UnsupportedEncodingException ex) {
                    throw new VoltTypeException(
                            "tryScalarMakeCompatible: Unsupported encoding:"
                            + expectedComponentClz.getName() + " to provided " + inputComponentClz.getName());
                }
            }
            return values;
        }
        // hack to make varbinary work with input as hex string
        else if ((inputComponentClz == String.class) && (expectedComponentClz == byte[].class)) {
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
            throw new VoltTypeException(
                    "tryScalarMakeCompatible: Unable to match parameter array:"
                    + expectedComponentClz.getName() + " to provided " + inputComponentClz.getName());
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
    public static Object tryToMakeCompatible(final Class<?> expectedClz, final Object param)
    throws VoltTypeException
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
            return nullValueForType(expectedClz);
        }

        Class<?> inputClz = param.getClass();

        // If we make it through this first block, memoize a number value for some range checks later
        Number numberParam = null;

        // This first code block tries to hit as many common cases as possible
        // Specifically, it does primitive types and strings, which are the most common param types.
        // Downconversions (e.g. long => short) happen later, but can use the memoized numberParam value.
        // Notice this block switches on the type of the given value (different later).

        if (inputClz == Long.class) {
            if (expectedClz == long.class) return param;
            if ((Long) param == VoltType.NULL_BIGINT) return nullValueForType(expectedClz);
            numberParam = (Number) param;
        }
        else if (inputClz == Integer.class) {
            if (expectedClz == int.class) return param;
            if ((Integer) param == VoltType.NULL_INTEGER) return nullValueForType(expectedClz);
            if (expectedClz == long.class) return ((Integer) param).longValue();
            numberParam = (Number) param;
        }
        else if (inputClz == Short.class) {
            if (expectedClz == short.class) return param;
            if ((Short) param == VoltType.NULL_SMALLINT) return nullValueForType(expectedClz);
            if (expectedClz == long.class) return ((Short) param).longValue();
            if (expectedClz == int.class) return ((Short) param).intValue();
            numberParam = (Number) param;
        }
        else if (inputClz == Byte.class) {
            if (expectedClz == byte.class) return param;
            if ((Byte) param == VoltType.NULL_TINYINT) return nullValueForType(expectedClz);
            if (expectedClz == long.class) return ((Byte) param).longValue();
            if (expectedClz == int.class) return ((Byte) param).intValue();
            if (expectedClz == short.class) return ((Byte) param).shortValue();
            numberParam = (Number) param;
        }
        else if (inputClz == Double.class) {
            if (expectedClz == double.class) return param;
            if ((Double) param == VoltType.NULL_FLOAT) return nullValueForType(expectedClz);
        }
        else if (inputClz == String.class) {
            if (((String) param).equals(Constants.CSV_NULL)) return nullValueForType(expectedClz);
            else if (expectedClz == String.class) return param;
            // Hack allows hex-encoded strings to be passed into byte[] params
            else if (expectedClz == byte[].class) {
                return Encoder.hexDecode((String) param);
            }
            // We allow all values to be passed as strings for csv loading, json, etc...
            // This code handles primitive types. Complex types come later.
            if (expectedClz.isPrimitive()) {
                return convertStringToPrimitive((String) param, expectedClz);
            }
        }
        else if (inputClz == byte[].class) {
            if (expectedClz == byte[].class) return param;
            // allow byte arrays to be passed into string parameters
            else if (expectedClz == String.class) {
                String value = new String((byte[]) param, Constants.UTF8ENCODING);
                if (value.equals(Constants.CSV_NULL)) return nullValueForType(expectedClz);
                else return value;
            }
        }
        // null sigils. (ning - if we're not checking if the sigil matches the expected type,
        // why do we have three sigils for three types??)
        else if (param == VoltType.NULL_TIMESTAMP ||
                 param == VoltType.NULL_STRING_OR_VARBINARY ||
                 param == VoltType.NULL_DECIMAL) {
            return nullValueForType(expectedClz);
        }

        // make sure we get the array/scalar match
        if (expectedClz.isArray() != inputClz.isArray()) {
            throw new VoltTypeException(String.format("Array / Scalar parameter mismatch (%s to %s)",
                    inputClz.getName(), expectedClz.getName()));
        }

        // handle arrays in a factored-out method
        if (expectedClz.isArray()) {
            return tryToMakeCompatibleArray(expectedClz.getComponentType(), inputClz.getComponentType(), param);
        }

        // The following block switches on the type of the paramter desired.
        // It handles all of the paths not trapped in the code above. We can assume
        // values are not null and that most sane primitive stuff has been handled.
        // Downcasting is handled here (e.g. long => short).
        // Time (in many forms) and Decimal are also handled below.

        if ((expectedClz == int.class) && (numberParam != null)) {
            long val = numberParam.longValue();
            if (val == VoltType.NULL_INTEGER) {
                throw new VoltTypeException("tryToMakeCompatible: The provided long value: ("
                        + param.toString() + ") might be interpreted as integer null. " +
                                "Try explicitly using a int parameter.");
            }
            // if it's in the right range, crop the value and return
            if ((val <= Integer.MAX_VALUE) && (val >= Integer.MIN_VALUE))
                return numberParam.intValue();
        }
        else if ((expectedClz == short.class) && (numberParam != null)) {
            if ((inputClz == Long.class) || (inputClz == Integer.class)) {
                long val = numberParam.longValue();
                if (val == VoltType.NULL_SMALLINT) {
                    throw new VoltTypeException("tryToMakeCompatible: The provided int or long value: ("
                            + param.toString() + ") might be interpreted as smallint null. " +
                                    "Try explicitly using a short parameter.");
                }
                // if it's in the right range, crop the value and return
                if ((val <= Short.MAX_VALUE) && (val >= Short.MIN_VALUE))
                    return numberParam.shortValue();
            }
        }
        else if ((expectedClz == byte.class) && (numberParam != null)) {
            if ((inputClz == Long.class) || (inputClz == Integer.class) || (inputClz == Short.class)) {
                long val = numberParam.longValue();
                if (val == VoltType.NULL_TINYINT) {
                    throw new VoltTypeException("tryToMakeCompatible: The provided short, int or long value: ("
                            + param.toString() + ") might be interpreted as tinyint null. " +
                                    "Try explicitly using a byte parameter.");
                }
                // if it's in the right range, crop the value and return
                if ((val <= Byte.MAX_VALUE) && (val >= Byte.MIN_VALUE))
                    return numberParam.byteValue();
            }
        }
        else if ((expectedClz == double.class) && (numberParam != null)) {
            return numberParam.doubleValue();
        }
        else if (expectedClz == TimestampType.class) {
            if (inputClz == Integer.class) return new TimestampType((Integer)param); // null values safe
            if (inputClz == Long.class) return new TimestampType((Long)param); // null values safe
            if (inputClz == TimestampType.class) return param;
            if (inputClz == Date.class) return new TimestampType((Date) param);
            // if a string is given for a date, use java's JDBC parsing
            if (inputClz == String.class) {
                String timestring = ((String) param).trim();
                try {
                    return new TimestampType(Long.parseLong(timestring));
                } catch (IllegalArgumentException e) {
                    // Defer errors to the generic Exception throw below, if it's not the right format
                }
                try {
                    return new TimestampType(timestring);
                }
                catch (IllegalArgumentException e) {
                    // Defer errors to the generic Exception throw below, if it's not the right format
                }
            }
        }
        else if (expectedClz == java.sql.Timestamp.class) {
            if (param instanceof java.sql.Timestamp) return param;
            if (param instanceof java.util.Date) return new java.sql.Timestamp(((java.util.Date) param).getTime());
            if (param instanceof TimestampType) return ((TimestampType) param).asJavaTimestamp();
            // If a string is given for a date, use java's JDBC parsing.
            if (inputClz == String.class) {
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
        else if (expectedClz == java.sql.Date.class) {
            if (param instanceof java.sql.Date) return param; // covers java.sql.Date and java.sql.Timestamp
            if (param instanceof java.util.Date) return new java.sql.Date(((java.util.Date) param).getTime());
            if (param instanceof TimestampType) return ((TimestampType) param).asExactJavaSqlDate();
            // If a string is given for a date, use java's JDBC parsing.
            if (inputClz == String.class) {
                try {
                    return new java.sql.Date(TimestampType.millisFromJDBCformat((String) param));
                }
                catch (IllegalArgumentException e) {
                    // Defer errors to the generic Exception throw below, if it's not the right format
                }
            }
        }
        else if (expectedClz == java.util.Date.class) {
            if (param instanceof java.util.Date) return param; // covers java.sql.Date and java.sql.Timestamp
            if (param instanceof TimestampType) return ((TimestampType) param).asExactJavaDate();
            // If a string is given for a date, use the default format parser for the default locale.
            if (inputClz == String.class) {
                try {
                    return new java.util.Date(TimestampType.millisFromJDBCformat((String) param));
                }
                catch (IllegalArgumentException e) {
                    // Defer errors to the generic Exception throw below, if it's not the right format
                }
            }
        }
        else if (expectedClz == BigDecimal.class) {
            if (numberParam != null) {
                BigInteger bi = new BigInteger(param.toString());
                BigDecimal bd = new BigDecimal(bi);
                bd = VoltDecimalHelper.setDefaultScale(bd);
                return bd;
            }
            if (inputClz == BigDecimal.class) {
                BigDecimal bd = (BigDecimal) param;
                bd = VoltDecimalHelper.setDefaultScale(bd);
                return bd;
            }
            if (inputClz == Float.class || inputClz == Double.class) {
                try {
                    return VoltDecimalHelper.deserializeBigDecimalFromString(String.format("%.12f", param));
                } catch (IOException ex) {
                    throw new VoltTypeException(String.format("deserialize Float from string failed. (%s to %s)",
                            inputClz.getName(), expectedClz.getName()));
                }
            }
            try {
                return VoltDecimalHelper.deserializeBigDecimalFromString(String.valueOf(param));
            } catch (IOException ex) {
                throw new VoltTypeException(String.format("deserialize BigDecimal from string failed. (%s to %s)",
                        inputClz.getName(), expectedClz.getName()));
            }
        } else if (expectedClz == VoltTable.class && inputClz == VoltTable.class) {
            return param;
        } else if (expectedClz == String.class) {
            //For VARCHAR columns if not null or not an array send toString value.
            if (!param.getClass().isArray()) {
                return String.valueOf(param);
            }
        }

        // handle SystemProcedureExecutionContext without linking to it
        // these are used by system procedures and are ignored here
        if (expectedClz.getSimpleName().equals("SystemProcedureExecutionContext")) {
            if (expectedClz.isAssignableFrom(inputClz)) {
                return param;
            }
        }

        throw new VoltTypeException(
                "tryToMakeCompatible: The provided value: (" + param.toString() + ") of type: " + inputClz.getName() +
                " is not a match or is out of range for the target parameter type: " + expectedClz.getName());
    }


    /**
     * Convert string inputs to Longs for TheHashinator if possible
     * @param param
     * @param slot
     * @return Object parsed as Number or null if types not compatible
     * @throws Exception if a parse error occurs (consistent with above).
     */
    public static Object stringToLong(Object param, Class<?> slot)
    throws VoltTypeException
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
            throw new VoltTypeException(
                    "tryToMakeCompatible: Unable to convert string "
                    + (String)param + " to "  + slot.getName()
                    + " value for target parameter " + slot.getName());
        }
    }
}
