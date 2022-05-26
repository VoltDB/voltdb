/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package udfbenchmark;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;

/** Contains a bunch of UDF's (user-defined functions) used for testing. */
public class UDFLib {
    // Change this to true, to get a very altered version of most of these
    // UDF's (specifically, the ones with "add" or "concat" in their names);
    // useful for testing of @UpdateClasses (see ENG-12856)
    private static final boolean USE_ALTERNATIVE_VERSION = false;

    /** A simple user-defined (runtime) exception, used to test UDF's
     *  (user-defined exceptions) that throw such exceptions. */
    public class UserDefinedTestException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public UserDefinedTestException() {
        }
        public UserDefinedTestException(String message) {
            super(message);
        }
        public UserDefinedTestException(Throwable cause) {
            super(cause);
        }
        public UserDefinedTestException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // We start with private methods that are called by (some of) the test
    // UDF's (user-defined functions) below:

    /**
     * Returns the VoltDB <b>null</b> value that corresponds to the data type
     * of the input <i>value</i>; for example, -128 for a TINYINT (byte) value,
     * or -32768 for a SMALLINT (short) value. However, if <i>useTrueNullValue</i>
     * is true, or if the <i>value</i> is not of a recognized numeric type, a
     * true Java <b>null</b> is returned.
     */
    private Number getNullValue(Number value, boolean useTrueNullValue) {
        if (useTrueNullValue) {
            return null;
        }

        switch (value.getClass().toString()) {
        case "class java.lang.Byte":        return VoltType.NULL_TINYINT;   // the TINYINT (byte) null value (-128)
        case "class java.lang.Short":       return VoltType.NULL_SMALLINT;  // the SMALLINT (short) null value (-32768)
        case "class java.lang.Integer":     return VoltType.NULL_INTEGER;   // the INTEGER (int) null value (-2147483648)
        case "class java.lang.Long":        return VoltType.NULL_BIGINT;    // the BIGINT (long) null value (-9223372036854775808L)
        case "class java.lang.Double":      return VoltType.NULL_FLOAT;     // the FLOAT (double) null value (-1.7E+308D)
        default:                            return null;
        }
    }

    public static class UDF_TEST {
        public static final int RETURN_DATA_TYPE_NULL = -100;
        public static final int RETURN_JAVA_NULL      = -101;
        public static final int RETURN_TINYINT_NULL   = -102;
        public static final int RETURN_SMALLINT_NULL  = -103;
        public static final int RETURN_INTEGER_NULL   = -104;
        public static final int RETURN_BIGINT_NULL    = -105;
        public static final int RETURN_FLOAT_NULL     = -106;
        public static final int RETURN_DECIMAL_MIN    = -107;
        public static final int RETURN_DECIMAL_MAX    = -108;
        public static final int RETURN_NaN            = -109;
        public static final int THROW_NullPointerException           = -110;
        public static final int THROW_IllegalArgumentException       = -111;
        public static final int THROW_NumberFormatException          = -112;
        public static final int THROW_ArrayIndexOutOfBoundsException = -113;
        public static final int THROW_ClassCastException             = -114;
        public static final int THROW_ArithmeticException            = -115;
        public static final int THROW_UnsupportedOperationException  = -116;
        public static final int THROW_VoltTypeException              = -117;
        public static final int THROW_UserDefinedTestException       = -118;
    }

    /** Usually just returns the input value; but certain special input values
     *  (generally between -100 and -120) trigger an exception to be thrown,
     *  or special VoltDB "null" values to be returned. */
    private Number testExceptionsByValue(Number value) {
        return testExceptionsByValue(value, false);
    }

    /** Usually just returns the input value; but certain special input values
     *  (generally between -100 and -120) trigger an exception to be thrown,
     *  or special VoltDB "null" values to be returned. When <i>useTrueNullValue</i>
     *  is true, and the special input value -100 is given, a true Java <b>null</b>
     *  is returned. */
    @SuppressWarnings("null")
    private Number testExceptionsByValue(Number value, boolean useTrueNullValue) {
        if (value == null) {
            return null;
        }
        Integer uninitialized = null;
        Integer[] array = {1, 2, 3, 4, 5};
        int intValue = value.intValue();

        switch (intValue) {
        case UDF_TEST.RETURN_DATA_TYPE_NULL:    return getNullValue(value, useTrueNullValue);
        case UDF_TEST.RETURN_JAVA_NULL:         return null;
        case UDF_TEST.RETURN_TINYINT_NULL:      return VoltType.NULL_TINYINT;   // the TINYINT (byte) null value (-128)
        case UDF_TEST.RETURN_SMALLINT_NULL:     return VoltType.NULL_SMALLINT;  // the SMALLINT (short) null value (-32768)
        case UDF_TEST.RETURN_INTEGER_NULL:      return VoltType.NULL_INTEGER;   // the INTEGER (int) null value (-2147483648)
        case UDF_TEST.RETURN_BIGINT_NULL:       return VoltType.NULL_BIGINT;    // the BIGINT (long) null value (-9223372036854775808L)
        case UDF_TEST.RETURN_FLOAT_NULL:        return VoltType.NULL_FLOAT;     // the FLOAT (double) null value (-1.7E+308D)
        case UDF_TEST.RETURN_DECIMAL_MIN:       return new BigDecimal("-99999999999999999999999999.999999999999");  // the DECIMAL (BigDecimal) minimum value
        case UDF_TEST.RETURN_DECIMAL_MAX:       return new BigDecimal( "99999999999999999999999999.999999999999");  // the DECIMAL (BigDecimal) maximum value
        case UDF_TEST.RETURN_NaN:               return Math.log(value.doubleValue());   // Return NaN
        case UDF_TEST.THROW_NullPointerException:           return uninitialized.intValue();        // Throw a NullPointerException (NPE)
        case UDF_TEST.THROW_IllegalArgumentException:       Character.toChars(intValue);            // Throw an IllegalArgumentException
        case UDF_TEST.THROW_NumberFormatException:          return new Byte("nonnumeric");          // Throw a NumberFormatException
        case UDF_TEST.THROW_ArrayIndexOutOfBoundsException: return array[-1];                       // Throw an ArrayIndexOutOfBoundsException
        case UDF_TEST.THROW_ClassCastException:             return (Byte) value + (Short) value;    // Throw a ClassCastException
        case UDF_TEST.THROW_ArithmeticException:            return intValue / 0;                    // Throw an ArithmeticException (/ by zero)
        case UDF_TEST.THROW_UnsupportedOperationException:  Arrays.asList(array).add(0);            // Throw an UnsupportedOperationException
        case UDF_TEST.THROW_VoltTypeException:              throw new VoltTypeException("Test UDF's that throw a VoltTypeException.");
        case UDF_TEST.THROW_UserDefinedTestException:       throw new UserDefinedTestException("Test UDF's that throw a user-defined (runtime) exception.");
        default:                                return value;
        }
    }

    /** Usually just returns the input value; but certain special input values
     *  (generally between -100 and -120) trigger an exception to be thrown,
     *  or special VoltDB "null" values to be returned. When <i>useTrueNullValue</i>
     *  is true, and the special input value -100 is given, a true Java <b>null</b>
     *  is returned. */
    private Byte testExceptions(Byte value, boolean useTrueNullValue) {
        Number result = testExceptionsByValue(value, useTrueNullValue);
        return (result == null ? null : result.byteValue());
    }

    /** Usually just returns the input value; but certain special input values
     *  (generally between -100 and -120) trigger an exception to be thrown,
     *  or special VoltDB "null" values to be returned. */
    private Byte testExceptions(Byte value) {
        return testExceptions(value, false);
    }

    /** Usually just returns the input value; but certain special input values
     *  (generally between -100 and -120) trigger an exception to be thrown,
     *  or special VoltDB "null" values to be returned. */
    private Short testExceptions(Short value) {
        Number result = testExceptionsByValue(value);
        return (result == null ? null : result.shortValue());
    }

    /** Usually just returns the input value; but certain special input values
     *  (generally between -100 and -120) trigger an exception to be thrown,
     *  or special VoltDB "null" values to be returned. When <i>useTrueNullValue</i>
     *  is true, and the special input value -100 is given, a true Java <b>null</b>
     *  is returned. */
    private Integer testExceptions(Integer value, boolean useTrueNullValue) {
        Number result = testExceptionsByValue(value, useTrueNullValue);
        return (result == null ? null : result.intValue());
    }

    /** Usually just returns the input value; but certain special input values
     *  (generally between -100 and -120) trigger an exception to be thrown,
     *  or special VoltDB "null" values to be returned. */
    private Integer testExceptions(Integer value) {
        return testExceptions(value, false);
    }

    /** Usually just returns the input value; but certain special input values
     *  (generally between -100 and -120) trigger an exception to be thrown,
     *  or special VoltDB "null" values to be returned. */
    private Long testExceptions(Long value) {
        Number result = testExceptionsByValue(value);
        return (result == null ? null : result.longValue());
    }

    /** Usually just returns the input value; but certain special input values
     *  (generally between -100 and -120) trigger an exception to be thrown,
     *  or special VoltDB "null" values to be returned. When <i>useTrueNullValue</i>
     *  is true, and the special input value -100 is given, a true Java <b>null</b>
     *  is returned. */
    private Double testExceptions(Double value, boolean useTrueNullValue) {
        Number result = testExceptionsByValue(value, useTrueNullValue);
        return (result == null ? null : result.doubleValue());
    }

    /** Usually just returns the input value; but certain special input values
     *  (generally between -100 and -120) trigger an exception to be thrown,
     *  or special VoltDB "null" values to be returned. */
    private Double testExceptions(Double value) {
        return testExceptions(value, false);
    }

    /** Usually just returns the input value; but certain special input values
     *  (generally between -100 and -120) trigger an exception to be thrown,
     *  or special VoltDB "null" values to be returned. */
    private BigDecimal testExceptions(BigDecimal value) {
        Number n = testExceptionsByValue(value);
        if (n == null) {
            return null;
        } else if (n.getClass() == BigDecimal.class) {
            return (BigDecimal) n;
        } else if (n.getClass() == Integer.class) {
            return new BigDecimal (n.intValue());
        } else if (n.getClass() == Long.class) {
            return new BigDecimal (n.longValue());
        } else {
            return new BigDecimal (n.doubleValue());
        }
    }

    /** Usually just returns the input value; but certain special input (int)
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be returned. */
    private String testExceptions(String value) {
        if (value == null) {
            return null;
        }
        try {
            Integer intValue = testExceptions(Integer.parseInt(value), true);
            return (intValue == null ? null : intValue.toString());
        } catch (NumberFormatException e) {
            return value;
        }
    }

    /** Usually just returns the input value; but certain special values within
     *  the input array (generally between -100 and -120) trigger an exception
     *  to be thrown, or special VoltDB "null" values to be returned. */
    private byte[] testExceptions(byte[] value) {
        if (value == null) {
            return null;
        }
        byte[] result = new byte[value.length];
        for (int i=0; i < value.length; i++) {
            Byte test_i = testExceptions(value[i], true);
            if (test_i == null) {
                return null;
            } else {
                result[i] = test_i;
            }
        }
        return result;
    }

    /** Usually just returns the input value; but certain special values within
     *  the input array (generally between -100 and -120) trigger an exception
     *  to be thrown, or special VoltDB "null" values to be returned. */
    // TODO: I'm not certain whether Byte[] (as opposed to byte[]) is a valid
    // way to represent VARBINARY
    private Byte[] testExceptions(Byte[] value) {
        if (value == null) {
            return null;
        }
        Byte[] result = new Byte[value.length];
        for (int i=0; i < value.length; i++) {
            Byte test_i = testExceptions(value[i], true);
            if (test_i == null) {
                return null;
            } else {
                result[i] = test_i;
            }
        }
        return result;
    }

    /** Usually just returns the input value; but input values with certain
     *  special years (generally between 1780 and 1800) trigger an exception
     *  to be thrown, or special VoltDB "null" values to be returned. */
    private TimestampType testExceptions(TimestampType value) {
        if (value == null) {
            return null;
        }
        Date date = value.asExactJavaDate();
        Integer year = testExceptions(date.getYear(), true);
        if (year == null) {
            return null;
        }
        date.setYear(year);
        return new TimestampType(date);
    }

    /** Usually just returns the input value; but input values with certain
     *  special years (generally between 1780 and 1800) trigger an exception
     *  to be thrown, or special VoltDB "null" values to be returned. */
    private TimestampType testExceptions(Date value) {
        if (value == null) {
            return null;
        }
        return testExceptions(new TimestampType(value));
    }

    /** Usually just returns the input value; but input values with certain
     *  special longitude values (generally between -100 and -120) trigger
     *  an exception to be thrown, or special VoltDB "null" values to be
     *  returned. */
    private GeographyPointValue testExceptions(GeographyPointValue value) {
        if (value == null) {
            return null;
        }
        // We don't bother to "test" Latitude, because it must be between -90
        // and 90, so it cannot have any of the "interesting" values, such as
        // -100, -101, etc.
        Double longitude = testExceptions(value.getLongitude(), true);
        return (longitude == null ? null : new GeographyPointValue(Math.min(180, Math.max(-180, longitude)), value.getLatitude() ) );
    }

    /** Usually just returns the input value; but input values with certain
     *  special longitude values (generally between -100 and -120) trigger
     *  an exception to be thrown, or special VoltDB "null" values to be
     *  returned. */
    private GeographyValue testExceptions(GeographyValue value) {
        if (value == null) {
            return null;
        }
        // We "test" every GeographyPoint in the Geography (polygon), but again,
        // we don't bother to "test" Latitude, because it must be between -90
        // and 90, so it cannot have any of the "interesting" values, such as
        // -100, -101, etc.
        List<List<GeographyPointValue>> rings = value.getRings();
        for (int i=0; i < rings.size(); i++) {
            List<GeographyPointValue> ring = rings.get(i);
            for (int j=0; j < ring.size(); j++) {
                if (testExceptions(ring.get(j).getLongitude(), true) == null) {
                    return null;
                }
            }
        }
        return value;
    }

    /** Private method called by the various test UDF's that involve addition
     *  of two integers; note that there is no null checking. However, if
     *  USE_ALTERNATIVE_VERSION is true, it subtracts, rather than adds, the
     *  two integers. */
    private Long add(Long i, Long j) {
        if (USE_ALTERNATIVE_VERSION) {
            return i - j;
        }
        return i + j;
    }

    // More private methods, using various integer data types, that simply call
    // the method above, with the necessary conversions:
    private Byte add(Byte i, Byte j) {
        return add(i.longValue(), j.longValue()).byteValue();
    }
    private Short add(Short i, Short j) {
        return add(i.longValue(), j.longValue()).shortValue();
    }
    private Integer add(Integer i, Integer j) {
        return add(i.longValue(), j.longValue()).intValue();
    }

    /** Private method called by the various test UDF's that involve addition
     *  of two (Double) numbers; note that there is no null checking. However,
     *  if USE_ALTERNATIVE_VERSION is true, it subtracts, rather than adds, the
     *  two integers. */
    private Double add(Double x, Double y) {
        if (USE_ALTERNATIVE_VERSION) {
            return x - y;
        }
        return x + y;
    }

    /** Private method called by the test UDF that involves addition of two
     *  (BigDecimal) numbers; note that there is no null checking. However,
     *  if USE_ALTERNATIVE_VERSION is true, it subtracts, rather than adds,
     *  the two integers. */
    private BigDecimal add(BigDecimal x, BigDecimal y) {
        if (USE_ALTERNATIVE_VERSION) {
            return x.subtract(y);
        }
        return x.add(y);
    }

    /** Private method called by the test UDF that involves "addition" of a
     *  GeographyPointValue to a GeographyValue; note that there is no null
     *  checking. However, if USE_ALTERNATIVE_VERSION is true, it "subtracts",
     *  rather than "adds", the GeographyPointValue. */
    private GeographyValue add(GeographyValue g, GeographyPointValue p) {
        if (USE_ALTERNATIVE_VERSION) {
            GeographyPointValue p2 = new GeographyPointValue(0.0 - p.getLongitude(),
                                                             0.0 - p.getLatitude());
            return g.add(p2);
        }
        return g.add(p);
    }

    /** Private method called by the various test UDF's that involve
     *  concatenation of two or more strings. However, if
     *  USE_ALTERNATIVE_VERSION is true, it concatenates them in
     *  reverse order. */
    private String concatenate(String... s) {
        if (s == null) {
            return null;
        }
        StringBuffer result = new StringBuffer();
        int len = s.length;
        for (int i=0; i < len; i++) {
            if (s[i] == null) {
                return null;
            }
            if (USE_ALTERNATIVE_VERSION) {
                result.append(s[len-1 - i]);
            } else {
                result.append(s[i]);
            }
        }
        return result.toString();
    }


    // Test UDF's (user-defined functions) that can be used to test UDF's that
    // throw various exceptions, or return various null values (including the
    // various numerical VoltDB null values, like -128 and -32768), when certain
    // special input values (generally between -100 and -120) are used; most
    // of these simply add or concatenate two input values:


    /** Simple test UDF (user-defined function) that adds two TINYINT
     *  (primitive, or unboxed, byte) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used. */
    public byte add2Tinyint(byte i, byte j) {
        if (i == VoltType.NULL_TINYINT || j == VoltType.NULL_TINYINT) {
            return VoltType.NULL_TINYINT;
        }
        return add(testExceptions(i), testExceptions(j));
    }

    /** Simple test UDF (user-defined function) that adds two TINYINT
     *  (boxed Byte) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public Byte add2TinyintBoxed(Byte i, Byte j) {
        Byte test_i = testExceptions(i);
        Byte test_j = testExceptions(j);
        if (test_i == null || test_j == null || test_i.equals(VoltType.NULL_TINYINT)
                || test_j.equals(VoltType.NULL_TINYINT)) {
            return null;
        }
        return (byte) add(test_i, test_j);
    }

    /** Simple test UDF (user-defined function) that adds two SMALLINT
     *  (primitive, or unboxed, short) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used. */
    public short add2Smallint(short i, short j) {
        if (i == VoltType.NULL_SMALLINT || j == VoltType.NULL_SMALLINT) {
            return VoltType.NULL_SMALLINT;
        }
        return add(testExceptions(i), testExceptions(j));
    }

    /** Simple test UDF (user-defined function) that adds two SMALLINT
     *  (boxed Short) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public Short add2SmallintBoxed(Short i, Short j) {
        Short test_i = testExceptions(i);
        Short test_j = testExceptions(j);
        if (test_i == null || test_j == null || test_i.equals(VoltType.NULL_SMALLINT)
                || test_j.equals(VoltType.NULL_SMALLINT)) {
            return null;
        }
        return (short) add(test_i, test_j);
    }

    /** Simple test UDF (user-defined function) that adds two INTEGER
     *  (primitive, or unboxed, int) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used. */
    public int add2Integer(int i, int j) {
        if (i == VoltType.NULL_INTEGER || j == VoltType.NULL_INTEGER) {
            return VoltType.NULL_INTEGER;
        }
        return add(testExceptions(i), testExceptions(j));
    }

    /** Simple test UDF (user-defined function) that adds two INTEGER
     *  (boxed Integer) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public Integer add2IntegerBoxed(Integer i, Integer j) {
        Integer test_i = testExceptions(i);
        Integer test_j = testExceptions(j);
        if (test_i == null || test_j == null || test_i.equals(VoltType.NULL_INTEGER)
                || test_j.equals(VoltType.NULL_INTEGER)) {
            return null;
        }
        return add(test_i, test_j);
    }

    /** Simple test UDF (user-defined function) that adds two BIGINT
     *  (primitive, or unboxed, long) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used. */
    public long add2Bigint(long i, long j) {
        if (i == VoltType.NULL_BIGINT || j == VoltType.NULL_BIGINT) {
            return VoltType.NULL_BIGINT;
        }
        return add(testExceptions(i), testExceptions(j));
    }

    /** Simple test UDF (user-defined function) that adds two BIGINT
     *  (boxed Long) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public Long add2BigintBoxed(Long i, Long j) {
        Long test_i = testExceptions(i);
        Long test_j = testExceptions(j);
        if (test_i == null || test_j == null || test_i.equals(VoltType.NULL_BIGINT)
                || test_j.equals(VoltType.NULL_BIGINT)) {
            return null;
        }
        return add(test_i, test_j);
    }

    /** Simple test UDF (user-defined function) that adds two FLOAT
     *  (primitive, or unboxed, double) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used. */
    public double add2Float(double x, double y) {
        if (x <= VoltType.NULL_FLOAT || y <= VoltType.NULL_FLOAT) {
            return VoltType.NULL_FLOAT;
        }
        return add(testExceptions(x), testExceptions(y));
    }

    /** Simple test UDF (user-defined function) that adds two FLOAT
     *  (boxed Double) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public Double add2FloatBoxed(Double x, Double y) {
        Double test_x = testExceptions(x);
        Double test_y = testExceptions(y);
        if (test_x == null || test_y == null || test_x <= VoltType.NULL_FLOAT
                || test_y <= VoltType.NULL_FLOAT) {
            return null;
        }
        return add(test_x, test_y);
    }

    /** Simple test UDF (user-defined function) that adds two DECIMAL
     *  (BigDecimal) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public BigDecimal add2Decimal(BigDecimal x, BigDecimal y) {
        BigDecimal test_x = testExceptions(x);
        BigDecimal test_y = testExceptions(y);
        if (test_x == null || test_y == null) {
            return null;
        }
        return add(test_x, test_y);
    }

    /** Simple test UDF (user-defined function) that concatenates two VARCHAR
     *  (String) values; except, certain special input (int) values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public String add2Varchar(String s, String t) {
        return concatenate(testExceptions(s), testExceptions(t));
    }

    /** Simple test UDF (user-defined function) that "adds" two VARBINARY
     *  (primitive, or unboxed, byte array) values; that is, it adds each byte
     *  value in the two arrays; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public byte[] add2Varbinary(byte[] a, byte[] b) {
        byte[] test_a = testExceptions(a);
        byte[] test_b = testExceptions(b);
        if (test_a == null || test_b == null) {
            return null;
        }
        int length = Math.max(a.length, b.length);
        byte[] result = new byte[length];
        for (int i=0; i < length; i++) {
            result[i] = add( (i < a.length ? test_a[i] : 0),
                                    (i < b.length ? test_b[i] : 0) );
        }
        return result;
    }

    /** Simple test UDF (user-defined function) that adds two VARBINARY
     *  (boxed Byte array) values; that is, it adds each byte value in the
     *  two arrays; except, certain special input values (generally between
     *  -100 and -120) trigger an exception to be thrown, or special VoltDB
     *  "null" values to be used. */
    // TODO: I'm not certain whether Byte[] (as opposed to byte[]) is a valid
    // way to represent VARBINARY
    public Byte[] add2VarbinaryBoxed(Byte[] a, Byte[] b) {
        Byte[] test_a = testExceptions(a);
        Byte[] test_b = testExceptions(b);
        if (test_a == null || test_b == null) {
            return null;
        }
        int length = Math.max(a.length, b.length);
        Byte[] result = new Byte[length];
        for (int i=0; i < length; i++) {
            result[i] = (byte) add( (i < a.length ? test_a[i] : 0),
                                    (i < b.length ? test_b[i] : 0) );
        }
        return result;
    }

    /**
     * A user-defined function that returns a byte array with given length.
     * This is used in the testUDFBufferEnlargement test case to return a
     * larger-than-buffer-size byte array.
     */
    public byte[] getByteArrayOfSize(int size) {
        return new byte[size];
    }

    /** Simple test UDF (user-defined function) that adds a specified number
     *  of years to a (VoltDB) Timestamp value; except, certain special input
     *  year values (generally between 1780 and 1800) trigger an exception to
     *  be thrown, or special VoltDB "null" values to be used. */
    public TimestampType addYearsToTimestamp(TimestampType t, Integer numYears) {
        TimestampType t2 = testExceptions(t);
        if (t2 == null || numYears == null) {
            return null;
        }
        Date d = t2.asExactJavaDate();
        d.setYear(add(d.getYear(), numYears));
        return new TimestampType(d);
    }

    /** A convenience method, allowing the above method to be called using a Date
     *  value, and it will construct a (VoltDB) Timestamp value to be used. */
    private TimestampType addYearsToTimestamp(Date d, int numYears) {
        return addYearsToTimestamp(new TimestampType(d), numYears);
    }

    /** Simple test UDF (user-defined function) that "adds" two (VoltDB)
     *  GeographyPoint values; that is, it adds their longitudes and latitudes;
     *  except, certain special longitude input values (generally between -100
     *  and -120) trigger an exception to be thrown, or special VoltDB "null"
     *  values to be used. */
    public GeographyPointValue add2GeographyPoint(GeographyPointValue p, GeographyPointValue q) {
        if (p == null || q == null) {
            return null;
        }
        // We don't bother to "test" Latitude, because it must be between -90
        // and 90, so it cannot have any of the "interesting" values, such as
        // -101, -102, etc.
        Double p_long = testExceptions(p.getLongitude(), true);
        Double q_long = testExceptions(q.getLongitude(), true);
        if (p_long == null || q_long == null) {
            return null;
        }
        return new GeographyPointValue( Math.min(180, Math.max(-180, add(p_long, q_long))), add(p.getLatitude(), q.getLatitude()) );
    }

    /** Simple test UDF (user-defined function) that "adds" a (VoltDB)
     *  GeographyPoint to a (VoltDB) Geography; that is, it adds the
     *  GeographyPoint's longitude and latitude to each vertex of the
     *  Geography; except, certain special longitude input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public GeographyValue addGeographyPointToGeography(GeographyValue g, GeographyPointValue p) {
        GeographyValue g2 = testExceptions(g);
        if (g2 == null || p == null) {
            return null;
        }
        return add(g2, p);
    }


    // Test UDF's (user-defined functions) that are similar to (some of) the
    // above UDF's, but without null checking, so slightly odd things can happen,
    // such as null plus one equals a number

    /** Simple test UDF (user-defined function) that adds two TINYINT
     *  (primitive, or unboxed, byte) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used; but this version
     *  has no null checking. */
    public byte add2TinyintWithoutNullCheck(byte i, byte j) {
        return add(testExceptions(i), testExceptions(j));
    }
    /** Simple test UDF (user-defined function) that adds two TINYINT
     *  (boxed Byte) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used; but this version has no null
     *  checking. */
    public Byte add2TinyintBoxedWithoutNullCheck(Byte i, Byte j) {
        return (byte) add(testExceptions(i), testExceptions(j));
    }
    /** Simple test UDF (user-defined function) that adds two SMALLINT
     *  (primitive, or unboxed, short) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used; but this version
     *  has no null checking. */
    public short add2SmallintWithoutNullCheck(short i, short j) {
        return add(testExceptions(i), testExceptions(j));
    }
    /** Simple test UDF (user-defined function) that adds two SMALLINT
     *  (boxed Short) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used; but this version has no null
     *  checking. */
    public Short add2SmallintBoxedWithoutNullCheck(Short i, Short j) {
        return (short) add(testExceptions(i), testExceptions(j));
    }
    /** Simple test UDF (user-defined function) that adds two INTEGER
     *  (primitive, or unboxed, int) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used; but this version
     *  has no null checking. */
    public int add2IntegerWithoutNullCheck(int i, int j) {
        return add(testExceptions(i), testExceptions(j));
    }
    /** Simple test UDF (user-defined function) that adds two INTEGER
     *  (boxed Integer) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used; but this version has no null
     *  checking. */
    public Integer add2IntegerBoxedWithoutNullCheck(Integer i, Integer j) {
        return add(testExceptions(i), testExceptions(j));
    }
    /** Simple test UDF (user-defined function) that adds two BIGINT
     *  (primitive, or unboxed, long) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used; but this version
     *  has no null checking. */
    public long add2BigintWithoutNullCheck(long i, long j) {
        return add(testExceptions(i), testExceptions(j));
    }
    /** Simple test UDF (user-defined function) that adds two BIGINT
     *  (boxed Long) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used; but this version has no null
     *  checking. */
    public Long add2BigintBoxedWithoutNullCheck(Long i, Long j) {
        return add(testExceptions(i), testExceptions(j));
    }
    /** Simple test UDF (user-defined function) that adds two FLOAT
     *  (primitive, or unboxed, double) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used; but this version
     *  has no null checking. */
    public double add2FloatWithoutNullCheck(double i, double j) {
        return add(testExceptions(i), testExceptions(j));
    }
    /** Simple test UDF (user-defined function) that adds two FLOAT
     *  (boxed Double) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used; but this version has no null
     *  checking. */
    public Double add2FloatBoxedWithoutNullCheck(Double i, Double j) {
        return add(testExceptions(i), testExceptions(j));
    }


    // Test UDF's (user-defined functions) that can easily be translated into
    // PostgreSQL functions, and are therefore suitable for use in SqlCoverage:

    // Functions with zero arguments...

    /** Simple test UDF (user-defined function) that just returns the value of pi. */
    public double piUdf() {
        return Math.PI;
    }
    /** Simple test UDF (user-defined function) that just returns the value of pi. */
    public Double piUdfBoxed() {
        return Math.PI;
    }

    // Functions with one argument...

    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public byte absTinyint(byte i) {
        if (i == VoltType.NULL_TINYINT) {
            return VoltType.NULL_TINYINT;
        }
        return (byte) Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public Byte absTinyintBoxed(Byte i) {
        if (i == null || i.equals(VoltType.NULL_TINYINT)) {
            return null;
        }
        return (byte) Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public short absSmallint(short i) {
        if (i == VoltType.NULL_SMALLINT) {
            return VoltType.NULL_SMALLINT;
        }
        return (short) Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public Short absSmallintBoxed(Short i) {
        if (i == null || i.equals(VoltType.NULL_SMALLINT)) {
            return null;
        }
        return (short) Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public int absInteger(int i) {
        if (i == VoltType.NULL_INTEGER) {
            return VoltType.NULL_INTEGER;
        }
        return Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public Integer absIntegerBoxed(Integer i) {
        if (i == null || i.equals(VoltType.NULL_INTEGER)) {
            return null;
        }
        return Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public long absBigint(long i) {
        if (i == VoltType.NULL_BIGINT) {
            return VoltType.NULL_BIGINT;
        }
        return Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public Long absBigintBoxed(Long i) {
        if (i == null || i.equals(VoltType.NULL_BIGINT)) {
            return null;
        }
        return Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public double absFloat(double x) {
        if (x <= VoltType.NULL_FLOAT) {
            return VoltType.NULL_FLOAT;
        }
        return Math.abs(x);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public Double absFloatBoxed(Double x) {
        if (x == null || x <= VoltType.NULL_FLOAT) {
            return null;
        }
        return Math.abs(x);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public BigDecimal absDecimal(BigDecimal x) {
        if (x == null) {
            return null;
        }
        return x.abs();
    }

    /** Simple test UDF (user-defined function) that returns the input String
     *  reversed (i.e., with the characters in reverse order). */
    public String reverse(String s) {
        if (s == null) {
            return null;
        }
        StringBuffer result = new StringBuffer();
        for (int i=s.length()-1; i >= 0; i--) {
            result.append(s.charAt(i));
        }
        return result.toString();
    }

    /** Simple test UDF (user-defined function) that returns the number of rings
     *  (including the exterior ring) in the input GEOGRAPHY. */
    public int numRings(GeographyValue g) {
        if (g == null) {
            return 0;
        }
        return g.getRings().size();
    }

    /** Simple test UDF (user-defined function) that returns the number of
     *  points in all rings (including the exterior ring) in the input
     *  GEOGRAPHY. */
    public int numPointsUdf(GeographyValue g) {
        if (g == null) {
            return 0;
        }
        int num_points = 0;
        for (List<GeographyPointValue> ring : g.getRings()) {
            num_points += ring.size();
        }
        return num_points;
    }

    // Functions with two arguments...

    // TODO: add more test UDF's: LATITUDE, LONGITUDE, AsText?; DECODE, ENCODE;
    // and perhaps: ZERO, EMPTY (0 args); DISTANCE, STRPOS, POSITION (2 args);
    // TRANSLATE, WIDTH_BUCKET, SUBSTR, MAKE_TIMESTAMP (3+ args)

    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public byte modTinyint(byte i, byte j) {
        if (i == VoltType.NULL_TINYINT || j == VoltType.NULL_TINYINT) {
            return VoltType.NULL_TINYINT;
        }
        return (byte) (i % j);
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public Byte modTinyintBoxed(Byte i, Byte j) {
        if (i == null || j == null || i.equals(VoltType.NULL_TINYINT)
                || j.equals(VoltType.NULL_TINYINT)) {
            return null;
        }
        return (byte) (i % j);
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public short modSmallint(short i, short j) {
        if (i == VoltType.NULL_SMALLINT || j == VoltType.NULL_SMALLINT) {
            return VoltType.NULL_SMALLINT;
        }
        return (short) (i % j);
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public Short modSmallintBoxed(Short i, Short j) {
        if (i == null || j == null || i.equals(VoltType.NULL_SMALLINT)
                || j.equals(VoltType.NULL_SMALLINT)) {
            return null;
        }
        return (short) (i % j);
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public int modInteger(int i, int j) {
        if (i == VoltType.NULL_INTEGER || j == VoltType.NULL_INTEGER) {
            return VoltType.NULL_INTEGER;
        }
        return i % j;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public Integer modIntegerBoxed(Integer i, Integer j) {
        if (i == null || j == null || i.equals(VoltType.NULL_INTEGER)
                || j.equals(VoltType.NULL_INTEGER)) {
            return null;
        }
        return i % j;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public long modBigint(long i, long j) {
        if (i == VoltType.NULL_BIGINT || j == VoltType.NULL_BIGINT) {
            return VoltType.NULL_BIGINT;
        }
        return i % j;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public Long modBigintBoxed(Long i, Long j) {
        if (i == null || j == null || i.equals(VoltType.NULL_BIGINT)
                || j.equals(VoltType.NULL_BIGINT)) {
            return null;
        }
        return i % j;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public double modFloat(double x, double y) {
        if (x <= VoltType.NULL_FLOAT || y <= VoltType.NULL_FLOAT) {
            return VoltType.NULL_FLOAT;
        }
        return x % y;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public Double modFloatBoxed(Double x, Double y) {
        if (x == null || y == null || x <= VoltType.NULL_BIGINT
                || y <= VoltType.NULL_BIGINT) {
            return null;
        }
        return x % y;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public BigDecimal modDecimal(BigDecimal x, BigDecimal y) {
        if (x == null || y == null) {
            return null;
        }
        return x.remainder(y);
    }

    /** Simple test UDF (user-defined function) that returns the first VARBINARY
     *  (byte array) argument, with any of the bytes from the second VARBINARY
     *  (byte array) argument trimmed from the beginning and end. */
    public byte[] btrim(byte[] byteString, byte[] bytesToTrim) {
        if (byteString == null || bytesToTrim == null) {
            return null;
        }
        int startIndex = 0;
        for (int i=0; i < byteString.length; i++) {
            for (int j=0; j < bytesToTrim.length; j++) {
                if (byteString[i] == bytesToTrim[j]) {
                    startIndex = i + 1;
                    break;
                }
            }
            if (startIndex <= i) {
                break;
            }
        }
        int endIndex = byteString.length;
        for (int i = byteString.length - 1; i > startIndex; i--) {
            for (int j=0; j < bytesToTrim.length; j++) {
                if (byteString[i] == bytesToTrim[j]) {
                    endIndex = i;
                    break;
                }
            }
            if (endIndex > i) {
                break;
            }
        }
        byte[] result = new byte[endIndex - startIndex];
        for (int i=0; i < endIndex - startIndex; i++) {
            result[i] = byteString[startIndex+i];
        }
        return result;
    }

    /** Simple test UDF (user-defined function) that returns the first VARBINARY
     *  (byte array) argument, with any of the bytes from the second VARBINARY
     *  (byte array) argument trimmed from the beginning and end. */
    // TODO: I'm not certain whether Byte[] (as opposed to byte[]) is a valid
    // way to represent VARBINARY
    public Byte[] btrimBoxed(Byte[] byteString, Byte[] bytesToTrim) {
        if (byteString == null || bytesToTrim == null) {
            return null;
        }
        int startIndex = 0;
        for (int i=0; i < byteString.length; i++) {
            for (int j=0; j < bytesToTrim.length; j++) {
                if (byteString[i] == bytesToTrim[j]) {
                    startIndex = i + 1;
                    break;
                }
            }
            if (startIndex <= i) {
                break;
            }
        }
        int endIndex = byteString.length;
        for (int i = byteString.length - 1; i > startIndex; i--) {
            for (int j=0; j < bytesToTrim.length; j++) {
                if (byteString[i] == bytesToTrim[j]) {
                    endIndex = i;
                    break;
                }
            }
            if (endIndex > i) {
                break;
            }
        }
        Byte[] result = new Byte[endIndex - startIndex];
        for (int i=0; i < endIndex - startIndex; i++) {
            result[i] = byteString[startIndex+i];
        }
        return result;
    }

    /** Simple test UDF (user-defined function) that concatenates two VARCHAR
     *  (String) values. */
    public String concat2Varchar(String s1, String s2) {
        return concatenate(s1, s2);
    }

    // Functions with three or more arguments...

    /** Simple test UDF (user-defined function) that concatenates three VARCHAR
     *  (String) values. */
    public String concat3Varchar(String s1, String s2, String s3) {
        return concatenate(s1, s2, s3);
    }

    /** Simple test UDF (user-defined function) that concatenates four VARCHAR
     *  (String) values. */
    public String concat4Varchar(String s1, String s2, String s3, String s4) {
        return concatenate(s1, s2, s3, s4);
    }

}
