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

package org.voltdb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.nio.ByteBuffer;

import org.apache.commons.lang3.ArrayUtils;
import org.voltdb.common.Constants;
import org.voltdb.parser.SQLParser;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
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

    private static boolean isByteClass(Class<?> clz) { return clz == Byte.class || clz == byte.class; }
    private static boolean isShortClass(Class<?> clz) { return clz == Short.class || clz == short.class; }
    private static boolean isIntClass(Class<?> clz) { return clz == Integer.class || clz == int.class; }
    private static boolean isLongClass(Class<?> clz) { return clz == Long.class || clz == long.class; }
    private static boolean isDoubleClass(Class<?> clz) { return clz == Double.class || clz == double.class; }
    private static boolean isByteArrayClass(Class<?> clz) { return clz == Byte[].class || clz == byte[].class; }

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
            if (ByteBuffer.class.isAssignableFrom(clz) && ByteBuffer.class.isAssignableFrom(expectedClz)) {
                return true;
            }
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

    /**
     * Given a string, covert it to a primitive type or boxed type of the primitive type or return null.
     *
     * If the string value is a VARBINARY constant of the form X'00ABCD', and the
     * expected class is one of byte, short, int or long, then we interpret the
     * string as specifying bits of a 64-bit signed integer (padded with zeroes if
     * there are fewer than 16 digits).
     * Corresponding code for handling hex literals appears in HSQL's ExpressionValue class
     * and in voltdb.expressions.ConstantValueExpression.
     */
    private static Object convertStringToPrimitiveOrPrimitiveWrapper(String value, final Class<?> expectedClz)
    throws VoltTypeException
    {
        value = value.trim();
        // detect CSV null
        if (value.equals(Constants.CSV_NULL)) return nullValueForType(expectedClz);

        // Remove commas.  Doing this seems kind of dubious since it lets strings like
        //    ,,,3.1,4,,e,+,,16
        // be parsed as a valid double value (for example).
        String commaFreeValue = value.contains(",") ? value.replace(",", "") : value;

        try {
            // autoboxing converts to boxed types since this method returns a java Object
            if (isLongClass(expectedClz)) {
                return Long.parseLong(commaFreeValue);
            }
            if (isIntClass(expectedClz)) {
                return Integer.parseInt(commaFreeValue);
            }
            if (isShortClass(expectedClz)) {
                return Short.parseShort(commaFreeValue);
            }
            if (isByteClass(expectedClz)) {
                return Byte.parseByte(commaFreeValue);
            }
            if (isDoubleClass(expectedClz)) {
                return Double.parseDouble(commaFreeValue);
            }
        }
        // ignore the exception and fail through below
        catch (NumberFormatException nfe) {

            // If we failed to parse the string in decimal form it could still
            // be a numeric value specified as X'....'
            //
            // Do this only after trying to parse a decimal literal, which is the
            // most common case.
            if (expectedClz != double.class) {
                String hexDigits = SQLParser.getDigitsFromHexLiteral(value);
                if (hexDigits != null) {
                    try {
                        return SQLParser.hexDigitsToLong(hexDigits);
                    }
                    catch (SQLParser.Exception spe) {
                    }
                }
            }
        }

        throw new VoltTypeException(
                "Unable to convert string "
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
            Object param,
            boolean keepParamsImmutable)
    throws VoltTypeException
    {
        int inputLength = Array.getLength(param);

        // if it's an empty array, let it through
        // this is a bit ugly as it might hide passing
        //  arrays of the wrong type, but it "does the right thing"
        //  more often that not I guess...
        if (inputLength == 0) {
            return Array.newInstance(expectedComponentClz, 0);
        }
        else if (inputComponentClz == expectedComponentClz) {
            if( !keepParamsImmutable ) {
                return param;
            }

            // if both input and output types in the array stay same,
            // no additional transformation happens. Hence we need to copy
            // arrays since they, potentially, can be modified within the store procedure code.

            VoltType type;
            try {
                type = VoltType.typeFromClass(inputComponentClz);
            } catch (VoltTypeException e) {
                // BZ looks like a hack. Do we allow objects of different type in an array?
                Object obj = ParameterSet.getAKosherArray((Object[]) param);
                type = VoltType.typeFromClass(obj.getClass().getComponentType());
            }

            switch (type) {
                case TINYINT:
                    if (param instanceof Byte[]) {
                        return Arrays.copyOf((Byte[]) param, inputLength);
                    } else {
                        return Arrays.copyOf((byte[]) param, inputLength);
                    }
                case SMALLINT:
                    if (param instanceof Short[]) {
                        return Arrays.copyOf((Short[]) param, inputLength);
                    } else {
                        return Arrays.copyOf((short[]) param, inputLength);
                    }
                case INTEGER:
                    if (param instanceof Integer[]) {
                        return Arrays.copyOf((Integer[]) param, inputLength);
                    } else {
                        return Arrays.copyOf((int[]) param, inputLength);
                    }
                case BIGINT:
                    if (param instanceof Long[]) {
                        return Arrays.copyOf((Long[]) param, inputLength);
                    } else {
                        return Arrays.copyOf((long[]) param, inputLength);
                    }
                case FLOAT:
                    if (param instanceof Double[]) {
                        return Arrays.copyOf((Double[]) param, inputLength);
                    } else {
                        return Arrays.copyOf((double[]) param, inputLength);
                    }
                case STRING:
                    return param;
                case DECIMAL:
                    return Arrays.copyOf((BigDecimal[])param, inputLength);
                case VOLTTABLE:
                    return Arrays.copyOf((VoltTable[])param, inputLength);
                case VARBINARY:
                    if (param instanceof Byte[][]) {
                        Byte[][] obj = new Byte[inputLength][];
                        for (int ii = 0; ii < inputLength; ii++) {
                            obj[ii] = Arrays.copyOf(((Byte[][]) param)[ii], ((Byte[][]) param)[ii].length);
                        }
                        return obj;
                    }
                    else {
                        byte[][] obj = new byte[inputLength][];
                        for (int ii = 0; ii < inputLength; ii++) {
                            obj[ii] = Arrays.copyOf(((byte[][]) param)[ii], ((byte[][]) param)[ii].length);
                        }
                        return obj;
                    }
                case GEOGRAPHY_POINT:
                    return Arrays.copyOf((GeographyPointValue[])param, inputLength);
                case GEOGRAPHY:
                    return Arrays.copyOf((GeographyValue[])param, inputLength);
                default:
                    return param;
            }
        }
        // hack to make strings work with input as bytes
        else if (isByteArrayClass(inputComponentClz)
                && (expectedComponentClz == String.class)) {
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
        else if ((inputComponentClz == String.class) &&
                (expectedComponentClz == byte[].class)) {
            byte[][] values = new byte[inputLength][];
            for (int i = 0; i < inputLength; i++) {
                values[i] = Encoder.hexDecode((String) Array.get(param, i));
            }
            return values;
        } else if ((inputComponentClz == String.class) &&
                (expectedComponentClz == Byte[].class)) {
            Byte[][] boxvalues = new Byte[inputLength][];
            for (int i = 0; i < inputLength; i++) {
                boxvalues[i] = ArrayUtils.toObject(
                        Encoder.hexDecode((String) Array.get(param, i)) );
            }
            return boxvalues;
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
     * Wraps tryToMakeCompatible
     */
    public static Object tryToMakeCompatible(final Class<?> expectedClz, final Object param)
            throws VoltTypeException
    {
        return tryToMakeCompatible(expectedClz, param, false);
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
    public static Object tryToMakeCompatible(final Class<?> expectedClz, final Object param, boolean keepParamsImmutable)
    throws VoltTypeException
    {
        /* uncomment for debugging
        System.err.printf("Converting %s of type %s to type %s\n",

                String.valueOf(param),
                param == null ? "NULL" : param.getClass().getName(),
                expectedClz.getName());
        System.err.flush();
         */

        // Get blatant null out of the way fast, as it avoids some inline checks
        // There are some subtle null values that aren't java null coming up, but wait until
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
            if (expectedClz == Long.class) return param;
            numberParam = (Number) param;
        }
        else if (inputClz == Integer.class) {
            if (expectedClz == int.class) return param;
            if ((Integer) param == VoltType.NULL_INTEGER) return nullValueForType(expectedClz);
            if (expectedClz == Integer.class) return param;
            if (isLongClass(expectedClz)) return ((Integer) param).longValue();
            numberParam = (Number) param;
        }
        else if (inputClz == Short.class) {
            if (expectedClz == short.class) return param;
            if ((Short) param == VoltType.NULL_SMALLINT) return nullValueForType(expectedClz);
            if (expectedClz == Short.class) return param;
            if (isLongClass(expectedClz)) return ((Short) param).longValue();
            if (isIntClass(expectedClz)) return ((Short) param).intValue();
            numberParam = (Number) param;
        }
        else if (inputClz == Byte.class) {
            if (expectedClz == byte.class) return param;
            if ((Byte) param == VoltType.NULL_TINYINT) return nullValueForType(expectedClz);
            if (expectedClz == Byte.class) return param;
            if (isLongClass(expectedClz)) return ((Byte) param).longValue();
            if (isIntClass(expectedClz)) return ((Byte) param).intValue();
            if (isShortClass(expectedClz)) return ((Byte) param).shortValue();
            numberParam = (Number) param;
        }
        else if (inputClz == Double.class) {
            if (expectedClz == double.class) return param;
            if ((Double) param == VoltType.NULL_FLOAT) return nullValueForType(expectedClz);
            if (expectedClz == Double.class) return param;
        }
        else if (inputClz == String.class) {
            String stringParam = (String)param;
            if (stringParam.equals(Constants.CSV_NULL)) return nullValueForType(expectedClz);
            else if (expectedClz == String.class) return param;
            // Hack allows hex-encoded strings to be passed into byte[] params
            else if (isByteArrayClass(expectedClz)) {
                // regular expressions can be expensive, so don't invoke SQLParser
                // unless the param really looks like an x-quoted literal
                if (stringParam.startsWith("X") || stringParam.startsWith("x")) {
                    String hexDigits = SQLParser.getDigitsFromHexLiteral(stringParam);
                    if (hexDigits != null) {
                        stringParam = hexDigits;
                    }
                }
                byte[] inpArray =  Encoder.hexDecode(stringParam);
                if (expectedClz == byte[].class)
                    return inpArray;
                if (expectedClz == Byte[].class)
                    return ArrayUtils.toObject(inpArray);
            }
            // We allow all values to be passed as strings for csv loading, json, etc...
            // This code handles primitive types and their wrapped types. Complex types come later.
            if (expectedClz.isPrimitive() || expectedClz == Long.class || expectedClz == Integer.class
                    || expectedClz == Byte.class || expectedClz == Double.class || expectedClz == Short.class) {
                return convertStringToPrimitiveOrPrimitiveWrapper(stringParam, expectedClz);
            }
        }
        else if (inputClz == byte[].class) {
            if (expectedClz == Byte[].class) {
                return ArrayUtils.toObject((byte[]) param);
            }
            // allow byte arrays to be passed into string parameters
            if (expectedClz == String.class) {
                String value = new String((byte[]) param, Constants.UTF8ENCODING);
                if (value.equals(Constants.CSV_NULL)) {
                    return nullValueForType(expectedClz);
                } else {
                    return value;
                }
            }
            if (ByteBuffer.class.isAssignableFrom(expectedClz)) {
                return ByteBuffer.wrap((byte[])param);
            }
        }
        // null sigils. (ning - if we're not checking if the sigil matches the expected type,
        // why do we have three sigils for three types??)
        else if (param == VoltType.NULL_TIMESTAMP ||
                param == VoltType.NULL_STRING_OR_VARBINARY ||
                param == VoltType.NULL_GEOGRAPHY ||
                param == VoltType.NULL_POINT ||
                param == VoltType.NULL_DECIMAL) {
            return nullValueForType(expectedClz);
        }
        // [ENG-12522] BigDecimal should be able to be converted to long / double or other
        // primitive numeric types if possible
        // If it cannot be converted (say out of range), just display the error message.
        else if (inputClz == BigDecimal.class) {
            // Only conversion to primitive numeric types are considered
            BigDecimal pBigDecimal = (BigDecimal) param;
            if (expectedClz == BigDecimal.class) {
                return VoltDecimalHelper.setDefaultScale(pBigDecimal);
            }

            if (isLongClass(expectedClz)) {
                try {
                    long result = pBigDecimal.longValueExact();
                    return result;
                } catch (ArithmeticException e) {}  // The error will be re-thrown below
            } else if (isDoubleClass(expectedClz)) {
                // This conversion could potentially lose information, should a warning be
                // given at a higher level ?
                double result = pBigDecimal.doubleValue();
                // The converted double could be infinity if out of range
                if (result != Double.POSITIVE_INFINITY && result != Double.NEGATIVE_INFINITY) {
                    return result;
                }
            } else if (isIntClass(expectedClz)) {
                try {
                    int result = pBigDecimal.intValueExact();
                    return result;
                } catch (ArithmeticException e) {}  // The error will be re-thrown below
            } else if (isShortClass(expectedClz)) {
                try {
                    short result = pBigDecimal.shortValueExact();
                    return result;
                } catch (ArithmeticException e) {} // The error will be re-thrown below
            } else if (isByteClass(expectedClz)) {
                try {
                    byte result = pBigDecimal.byteValueExact();
                    return result;
                } catch (ArithmeticException e) {}
            }
            throw new VoltTypeException(
                            "The provided value: (" + param.toString() +
                            ") of type: " + inputClz.getName() +
                            " is out of range for the target parameter type: " +
                            expectedClz.getName());
        }

        // make sure we get the array/scalar match
        if (expectedClz.isArray() != inputClz.isArray()) {
            throw new VoltTypeException(String.format("Array / Scalar parameter mismatch (%s to %s)",
                    inputClz.getName(), expectedClz.getName()));
        }

        // handle arrays in a factored-out method
        if (expectedClz.isArray()) {
            return tryToMakeCompatibleArray(expectedClz.getComponentType(), inputClz.getComponentType(), param, keepParamsImmutable);
        }

        // The following block switches on the type of the parameter desired.
        // It handles all of the paths not trapped in the code above. We can assume
        // values are not null and that most sane primitive stuff has been handled.
        // Downcasting is handled here (e.g. long => short).
        // Time (in many forms) and Decimal are also handled below.

        if (isIntClass(expectedClz) && (numberParam != null)) {
            long val = numberParam.longValue();
            if (val == VoltType.NULL_INTEGER) {
                throw new VoltTypeException("The provided long value: ("
                        + param.toString() + ") might be interpreted as integer null. " +
                                "Try explicitly using a int parameter.");
            }
            // if it's in the right range, crop the value and return
            if ((val <= Integer.MAX_VALUE) && (val >= Integer.MIN_VALUE))
                return numberParam.intValue();
        }
        else if (isShortClass(expectedClz) && (numberParam != null)) {
            if ((inputClz == Long.class) || (inputClz == Integer.class)) {
                long val = numberParam.longValue();
                if (val == VoltType.NULL_SMALLINT) {
                    throw new VoltTypeException("The provided int or long value: ("
                            + param.toString() + ") might be interpreted as smallint null. " +
                                    "Try explicitly using a short parameter.");
                }
                // if it's in the right range, crop the value and return
                if ((val <= Short.MAX_VALUE) && (val >= Short.MIN_VALUE))
                    return numberParam.shortValue();
            }
        }
        else if (isByteClass(expectedClz) && (numberParam != null)) {
            if ((inputClz == Long.class) || (inputClz == Integer.class) || (inputClz == Short.class)) {
                long val = numberParam.longValue();
                if (val == VoltType.NULL_TINYINT) {
                    throw new VoltTypeException("The provided short, int or long value: ("
                            + param.toString() + ") might be interpreted as tinyint null. " +
                                    "Try explicitly using a byte parameter.");
                }
                // if it's in the right range, crop the value and return
                if ((val <= Byte.MAX_VALUE) && (val >= Byte.MIN_VALUE))
                    return numberParam.byteValue();
            }
        }
        else if (isDoubleClass(expectedClz) && (numberParam != null)) {
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
                    return SQLParser.parseDate(timestring);
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
                BigDecimal bd = VoltDecimalHelper.stringToDecimal(param.toString());
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
        } else if (expectedClz == GeographyPointValue.class) {
            // Is it a point already?  If so, just return it.
            if (inputClz == GeographyPointValue.class) {
                return param;
            }
            // Is it a string from which we can construct a point?
            // If so, return the newly constructed point.
            if (inputClz == String.class) {
                try {
                    GeographyPointValue pt = GeographyPointValue.fromWKT((String)param);
                    return pt;
                } catch (IllegalArgumentException e) {
                    throw new VoltTypeException(String.format("deserialize GeographyPointValue from string failed (string %s)",
                                                              (String)param));
                }
            }
        } else if (expectedClz == GeographyValue.class) {
            if (inputClz == GeographyValue.class) {
                return param;
            }
            if (inputClz == String.class) {
                String paramStr = (String)param;
                try {
                    GeographyValue gv = GeographyValue.fromWKT(paramStr);
                    return gv;
                } catch (IllegalArgumentException e) {
                    throw new VoltTypeException(String.format("deserialize GeographyValue from string failed (string %s)",
                                                              paramStr));
                }
            }
        } else if (expectedClz == VoltTable.class && inputClz == VoltTable.class) {
            return param;
        } else if (expectedClz == String.class) {
            //For VARCHAR columns if not null or not an array send toString value.
            if (!param.getClass().isArray()) {
                return String.valueOf(param);
            }
        }
        // this is for NT sysprocs with variable arguments
        // they do their own validation
        else if (expectedClz == ParameterSet.class && inputClz == ParameterSet.class) {
            return param;
        }

        // handle SystemProcedureExecutionContext without linking to it
        // these are used by system procedures and are ignored here
        if (expectedClz.getSimpleName().equals("SystemProcedureExecutionContext")) {
            if (expectedClz.isAssignableFrom(inputClz)) {
                return param;
            }
        }

        throw new VoltTypeException(
                "The provided value: (" + param.toString() + ") of type: " + inputClz.getName() +
                " is not a match or is out of range for the target parameter type: " + expectedClz.getName());
    }

    /**
     * Given the results of a procedure, convert it into a sensible array of VoltTables.
     * @throws InvocationTargetException
     */
    final static public VoltTable[] getResultsFromRawResults(String procedureName, Object result) throws InvocationTargetException {
        if (result == null) {
            return new VoltTable[0];
        }
        if (result instanceof VoltTable[]) {
            VoltTable[] retval = (VoltTable[]) result;
            for (VoltTable table : retval) {
                if (table == null) {
                    Exception e = new RuntimeException("VoltTable arrays with non-zero length cannot contain null values.");
                    throw new InvocationTargetException(e);
                }
                // Make sure this table does not use an ee cache buffer
                table.convertToHeapBuffer();
            }

            return retval;
        }
        if (result instanceof VoltTable) {
            VoltTable vt = (VoltTable) result;
            // Make sure this table does not use an ee cache buffer
            vt.convertToHeapBuffer();
            return new VoltTable[] { vt };
        }
        if (result instanceof Long) {
            VoltTable t = new VoltTable(new VoltTable.ColumnInfo("", VoltType.BIGINT));
            t.addRow(result);
            return new VoltTable[] { t };
        }
        throw new RuntimeException(String.format("Procedure %s unsupported procedure return type %s.",
                procedureName, result.getClass().getSimpleName()));
    }
}
