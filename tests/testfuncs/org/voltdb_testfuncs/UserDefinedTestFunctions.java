/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb_testfuncs;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;

/** Contains a bunch of UDF's (user-defined functions) used for testing. */
public class UserDefinedTestFunctions {

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
        case "class java.math.BigDecimal":  return new BigDecimal("-100000000000000000000000000");  // a DECIMAL (BigDecimal) null value
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
        public static final int RETURN_DECIMAL_NULL   = -107;
        public static final int RETURN_DECIMAL_MIN    = -108;
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
        case UDF_TEST.RETURN_DECIMAL_NULL:      return new BigDecimal("-100000000000000000000000000");  // a DECIMAL (BigDecimal) null value
        case UDF_TEST.RETURN_DECIMAL_MIN:       return new BigDecimal("-99999999999999999999999999.999999999999");  // the DECIMAL (BigDecimal) minimum value
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
     *  or special VoltDB "null" values to be returned. */
    private Double testExceptions(Double value) {
        Number result = testExceptionsByValue(value);
        return (result == null ? null : result.doubleValue());
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
            result[i] = testExceptions(value[i]);
        }
        return result;
    }

    /** Usually just returns the input value; but certain special values within
     *  the input array (generally between -100 and -120) trigger an exception
     *  to be thrown, or special VoltDB "null" values to be returned. */
    private Byte[] testExceptions(Byte[] value) {
        if (value == null) {
            return null;
        }
        Byte[] result = new Byte[value.length];
        for (int i=0; i < value.length; i++) {
            Byte test_i = testExceptions(value[i]);
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
        // -101, -102, etc.
        Double longitude = testExceptions(value.getLongitude());
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
                if (testExceptions(ring.get(j).getLongitude()) == null) {
                    return null;
                }
            }
        }
        return value;
    }

    /** Private method called by the various test UDF's that involve
     *  concatenation of two or more strings. */
    private String concatenate(String... s) {
        if (s == null) {
            return null;
        }
        StringBuffer result = new StringBuffer();
        for (int i=0; i < s.length; i++) {
            if (s[i] == null) {
                return null;
            }
            result.append(s[i]);
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
        return (byte) (testExceptions(i) + testExceptions(j));
    }

    /** Simple test UDF (user-defined function) that adds two TINYINT
     *  (boxed Byte) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public Byte add2TinyintBoxed(Byte i, Byte j) {
        Byte test_i = testExceptions(i);
        Byte test_j = testExceptions(j);
        if (test_i == null || test_j == null) {
            return null;
        }
        return (byte) (test_i + test_j);
    }

    /** Simple test UDF (user-defined function) that adds two SMALLINT
     *  (primitive, or unboxed, short) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used. */
    public short add2Smallint(short i, short j) {
        return (short) (testExceptions(i) + testExceptions(j));
    }

    /** Simple test UDF (user-defined function) that adds two SMALLINT
     *  (boxed Short) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public Short add2SmallintBoxed(Short i, Short j) {
        Short test_i = testExceptions(i);
        Short test_j = testExceptions(j);
        if (test_i == null || test_j == null) {
            return null;
        }
        return (short) (test_i + test_j);
    }

    /** Simple test UDF (user-defined function) that adds two INTEGER
     *  (primitive, or unboxed, int) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used. */
    public int add2Integer(int i, int j) {
        return testExceptions(i) + testExceptions(j);
    }

    /** Simple test UDF (user-defined function) that adds two INTEGER
     *  (boxed Integer) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public Integer add2IntegerBoxed(Integer i, Integer j) {
        Integer test_i = testExceptions(i);
        Integer test_j = testExceptions(j);
        if (test_i == null || test_j == null) {
            return null;
        }
        return test_i + test_j;
    }

    /** Simple test UDF (user-defined function) that adds two BIGINT
     *  (primitive, or unboxed, long) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used. */
    public long add2Bigint(long i, long j) {
        return testExceptions(i) + testExceptions(j);
    }

    /** Simple test UDF (user-defined function) that adds two BIGINT
     *  (boxed Long) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public Long add2BigintBoxed(Long i, Long j) {
        Long test_i = testExceptions(i);
        Long test_j = testExceptions(j);
        if (test_i == null || test_j == null) {
            return null;
        }
        return test_i + test_j;
    }

    /** Simple test UDF (user-defined function) that adds two FLOAT
     *  (primitive, or unboxed, double) values; except, certain special input
     *  values (generally between -100 and -120) trigger an exception to be
     *  thrown, or special VoltDB "null" values to be used. */
    public double add2Float(double x, double y) {
        return testExceptions(x) + testExceptions(y);
    }

    /** Simple test UDF (user-defined function) that adds two FLOAT
     *  (boxed Double) values; except, certain special input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public Double add2FloatBoxed(Double x, Double y) {
        Double test_x = testExceptions(x);
        Double test_y = testExceptions(y);
        if (test_x == null || test_y == null) {
            return null;
        }
        return test_x + test_y;
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
        return test_x.add(test_y);
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
            result[i] = (byte) ( (i < a.length ? test_a[i] : 0)
                               + (i < b.length ? test_b[i] : 0) );
        }
        return result;
    }

    /** Simple test UDF (user-defined function) that adds two VARBINARY
     *  (boxed Byte array) values; that is, it adds each byte value in the
     *  two arrays; except, certain special input values (generally between
     *  -100 and -120) trigger an exception to be thrown, or special VoltDB
     *  "null" values to be used. */
    public Byte[] add2VarbinaryBoxed(Byte[] a, Byte[] b) {
        Byte[] test_a = testExceptions(a);
        Byte[] test_b = testExceptions(b);
        if (test_a == null || test_b == null) {
            return null;
        }
        int length = Math.max(a.length, b.length);
        Byte[] result = new Byte[length];
        for (int i=0; i < length; i++) {
            result[i] = (byte) ( (i < a.length ? test_a[i] : 0)
                               + (i < b.length ? test_b[i] : 0) );
        }
        return result;
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
        d.setYear(d.getYear() + numYears);
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
        Double p_long = testExceptions(p.getLongitude());
        Double q_long = testExceptions(q.getLongitude());
        if (p_long == null || q_long == null) {
            return null;
        }
        return new GeographyPointValue( Math.min(180, Math.max(-180, p_long + q_long)), p.getLatitude() + q.getLatitude() );
    }

    /** Simple test UDF (user-defined function) that "adds" a (VoltDB)
     *  GeographyPoint to a (VoltDB) Geography; that is, it adds the
     *  GeographyPoint's longitude and latitude to each vertex of the
     *  Geography; except, certain special longitude input values (generally
     *  between -100 and -120) trigger an exception to be thrown, or special
     *  VoltDB "null" values to be used. */
    public GeographyValue addGeographyPointToGeography(GeographyValue g, GeographyPointValue p) {
        if (g == null || p == null) {
            return null;
        }
        GeographyValue g2 = testExceptions(g);
        if (g2 == null) {
            return null;
        }
        return g2.add(p);
    }


    // Test UDF's (user-defined functions) that can easily be translated into
    // PostgreSQL functions, and are therefore suitable for use in SqlCoverage:

    // Functions with zero arguments...

    /** Simple test UDF (user-defined function) that just returns the value of pi. */
    public double pi_UDF() {
        return Math.PI;
    }
    /** Simple test UDF (user-defined function) that just returns the value of pi. */
    public Double pi_UDF_Boxed() {
        return Math.PI;
    }

    // Functions with one argument...

    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public byte abs_TINYINT(byte i) {
        return (byte) Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public Byte abs_TINYINT_Boxed(Byte i) {
        if (i == null) {
            return null;
        }
        return (Byte) (byte) Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public short abs_SMALLINT(short i) {
        return (short) Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public Short abs_SMALLINT_Boxed(Short i) {
        if (i == null) {
            return null;
        }
        return (Short) (short) Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public int abs_INTEGER(int i) {
        return Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public Integer abs_INTEGER_Boxed(Integer i) {
        if (i == null) {
            return null;
        }
        return Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public long abs_BIGINT(long i) {
        return Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public Long abs_BIGINT_Boxed(Long i) {
        if (i == null) {
            return null;
        }
        return Math.abs(i);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public double abs_FLOAT(double x) {
        return Math.abs(x);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public Double abs_FLOAT_Boxed(Double x) {
        if (x == null) {
            return null;
        }
        return Math.abs(x);
    }
    /** Simple test UDF (user-defined function) that just returns the absolute
     *  value of the input value. */
    public BigDecimal abs_DECIMAL(BigDecimal x) {
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
    public int numPoints_UDF(GeographyValue g) {
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
    public byte mod_TINYINT(byte i, byte j) {
        return (byte) (i % j);
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public Byte mod_TINYINT_Boxed(Byte i, Byte j) {
        if (i == null || j == null) {
            return null;
        }
        return (Byte) (byte) (i % j);
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public short mod_SMALLINT(short i, short j) {
        return (short) (i % j);
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public Short mod_SMALLINT_Boxed(Short i, Short j) {
        if (i == null || j == null) {
            return null;
        }
        return (Short) (short) (i % j);
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public int mod_INTEGER(int i, int j) {
        return i % j;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public Integer mod_INTEGER_Boxed(Integer i, Integer j) {
        if (i == null || j == null) {
            return null;
        }
        return i % j;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public long mod_BIGINT(long i, long j) {
        return i % j;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public Long mod_BIGINT_Boxed(Long i, Long j) {
        if (i == null || j == null) {
            return null;
        }
        return i % j;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public double mod_FLOAT(double x, double y) {
        return x % y;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public Double mod_FLOAT_Boxed(Double x, Double y) {
        if (x == null || y == null) {
            return null;
        }
        return x % y;
    }
    /** Simple test UDF (user-defined function) that just returns the mod
     *  (remainder) of the input values. */
    public BigDecimal mod_DECIMAL(BigDecimal x, BigDecimal y) {
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
    public Byte[] btrim_Boxed(Byte[] byteString, Byte[] bytesToTrim) {
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


    // This main method is just used for manual testing of the example UDF's
    // (user defined functions) above, until they are fully supported in VoltDB.
    public static void main(String[] args) {
        UserDefinedTestFunctions udtf = new UserDefinedTestFunctions();
        Integer[] intInputs = {0, 0, 2, 2, 4, 4, 7, 5, 9, 5,
                -100, 0, -101, 0, -102, 0, -103, 0, -104, 0, -105, 0, -106, 0, -107, 0, -108, 0, -109, 0,
                -110, 0, -111, 0, -112, 0, -113, 0, -114, 0, -115, 0, -116, 0, -117, 0, -118, 0, -119, 0};
        int NUM_SIMPLE_TESTS = 16;

        System.out.println("Running tests in UserDefinedTestFunctions.main...");

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            try {
                System.out.println("add2Bigint("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.add2Bigint(intInputs[i].longValue(), intInputs[i+1].longValue()));
            } catch (Throwable e) {
                System.out.println("add2Bigint("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            try {
                System.out.println("add2BigintBoxed("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.add2BigintBoxed((Long)intInputs[i].longValue(), (Long)intInputs[i+1].longValue()));
            } catch (Throwable e) {
                System.out.println("add2BigintBoxed("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("add2BigintBoxed(null,null): " + udtf.add2BigintBoxed(null, null));
        } catch (Throwable e) {
            System.out.println("add2BigintBoxed(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            try {
                System.out.println("add2Integer("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.add2Integer(intInputs[i], intInputs[i+1]));
            } catch (Throwable e) {
                System.out.println("add2Integer("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            try {
                System.out.println("add2IntegerBoxed("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.add2IntegerBoxed(intInputs[i], intInputs[i+1]));
            } catch (Throwable e) {
                System.out.println("add2IntegerBoxed("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("add2IntegerBoxed(null,null): " + udtf.add2IntegerBoxed(null, null));
        } catch (Throwable e) {
            System.out.println("add2IntegerBoxed(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            try {
                System.out.println("add2Smallint("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.add2Smallint(intInputs[i].shortValue(), intInputs[i+1].shortValue()));
            } catch (Throwable e) {
                System.out.println("add2Smallint("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            try {
                System.out.println("add2SmallintBoxed("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.add2SmallintBoxed((Short)intInputs[i].shortValue(), (Short)intInputs[i+1].shortValue()));
            } catch (Throwable e) {
                System.out.println("add2SmallintBoxed("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("add2SmallintBoxed(null,null): " + udtf.add2SmallintBoxed(null, null));
        } catch (Throwable e) {
            System.out.println("add2SmallintBoxed(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            try {
                System.out.println("add2Tinyint("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.add2Tinyint(intInputs[i].byteValue(), intInputs[i+1].byteValue()));
            } catch (Throwable e) {
                System.out.println("add2Tinyint("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            try {
                System.out.println("add2TinyintBoxed("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.add2TinyintBoxed((Byte)intInputs[i].byteValue(), (Byte)intInputs[i+1].byteValue()));
            } catch (Throwable e) {
                System.out.println("add2TinyintBoxed("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("add2TinyintBoxed(null,null): " + udtf.add2TinyintBoxed(null, null));
        } catch (Throwable e) {
            System.out.println("add2TinyintBoxed(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            try {
                System.out.println("add2Float("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.add2Float(intInputs[i].doubleValue(), intInputs[i+1].doubleValue()));
            } catch (Throwable e) {
                System.out.println("add2Float("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            try {
                System.out.println("add2FloatBoxed("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.add2FloatBoxed((Double)intInputs[i].doubleValue(), (Double)intInputs[i+1].doubleValue()));
            } catch (Throwable e) {
                System.out.println("add2FloatBoxed("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("add2FloatBoxed(null,null): " + udtf.add2FloatBoxed(null, null));
        } catch (Throwable e) {
            System.out.println("add2FloatBoxed(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            try {
                System.out.println("add2Decimal("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.add2Decimal(new BigDecimal(intInputs[i]), new BigDecimal(intInputs[i+1])));
            } catch (Throwable e) {
                System.out.println("add2Decimal("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("add2Decimal(null,null): " + udtf.add2Decimal(null, null));
        } catch (Throwable e) {
            System.out.println("add2Decimal(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            String s = intInputs[i].toString();
            String t = intInputs[i+1].toString();
            try {
                System.out.println("add2Varchar("+s+","+t+"): " + udtf.add2Varchar(s, t));
            } catch (Throwable e) {
                System.out.println("add2Varchar("+s+","+t+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("add2Varchar(null,null): " + udtf.add2Varchar(null, null));
        } catch (Throwable e) {
            System.out.println("add2Varchar(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            byte[] a = new byte[4];
            byte[] b = new byte[i];
            a[0] = intInputs[i].byteValue();
            a[1] = 0;
            a[2] = intInputs[i].byteValue();
            a[3] = -1;
            for (byte j=0; j < b.length; j++) {
                b[j] = (byte) ( intInputs[i+1].byteValue() + j );
            }
            try {
                System.out.println("add2Varbinary("+Arrays.toString(a)+","+Arrays.toString(b)+"): " + Arrays.toString(udtf.add2Varbinary(a, b)));
            } catch (Throwable e) {
                System.out.println("add2Varbinary("+Arrays.toString(a)+","+Arrays.toString(b)+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("add2Varbinary(null,null): " + udtf.add2Varbinary(null, null));
        } catch (Throwable e) {
            System.out.println("add2Varbinary(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            Byte[] a = new Byte[4];
            Byte[] b = new Byte[i];
            a[0] = intInputs[i].byteValue();
            a[1] = 0;
            a[2] = intInputs[i].byteValue();
            a[3] = -1;
            for (byte j=0; j < b.length; j++) {
                b[j] = (byte) ( intInputs[i+1].byteValue() + j );
            }
            try {
                System.out.println("add2VarbinaryBoxed("+Arrays.toString(a)+","+Arrays.toString(b)+"): " + Arrays.toString(udtf.add2VarbinaryBoxed(a, b)));
            } catch (Throwable e) {
                System.out.println("add2VarbinaryBoxed("+Arrays.toString(a)+","+Arrays.toString(b)+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("add2VarbinaryBoxed(null,null): " + udtf.add2VarbinaryBoxed(null, null));
        } catch (Throwable e) {
            System.out.println("add2VarbinaryBoxed(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            Date d = new Date();
            d.setYear(intInputs[i]);
            try {
                System.out.println("addYearsToTimestamp("+d+","+intInputs[i+1]+"): " + udtf.addYearsToTimestamp(d, intInputs[i+1]));
            } catch (Throwable e) {
                System.out.println("addYearsToTimestamp("+d+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("addYearsToTimestamp(null,null): " + udtf.addYearsToTimestamp(null, null));
        } catch (Throwable e) {
            System.out.println("addYearsToTimestamp(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            GeographyPointValue p = new GeographyPointValue(intInputs[i],   -intInputs[i]/2);
            GeographyPointValue q = new GeographyPointValue(intInputs[i+1], -intInputs[i+1]/2);
            try {
                System.out.println("add2GeographyPoint("+p+","+q+"): " + udtf.add2GeographyPoint(p, q));
            } catch (Throwable e) {
                System.out.println("add2GeographyPoint("+p+","+q+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("add2GeographyPoint(null,null): " + udtf.add2GeographyPoint(null, null));
        } catch (Throwable e) {
            System.out.println("add2GeographyPoint(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        for (int i=0; i < intInputs.length; i+=2) {
            GeographyValue      g = new GeographyValue("POLYGON((0 0, "+intInputs[i]+" 0, 0 "+intInputs[i]/2+", 0 0))");
            GeographyPointValue p = new GeographyPointValue(intInputs[i+1], -intInputs[i+1]/2);
            try {
                System.out.println("addGeographyPointToGeography("+g+","+p+"): " + udtf.addGeographyPointToGeography(g, p));
            } catch (Throwable e) {
                System.out.println("addGeographyPointToGeography("+g+","+p+") threw Exception:\n"+e);
            }
        }
        try {
            System.out.println("addGeographyPointToGeography(null,null): " + udtf.addGeographyPointToGeography(null, null));
        } catch (Throwable e) {
            System.out.println("addGeographyPointToGeography(null,null) threw Exception:\n"+e);
        }

        System.out.println();
        System.out.println("Tests of (PostgreSQL-compatible) UDF's with 0 or 1 args:");
        System.out.println();
        System.out.println("pi_UDF(): " + udtf.pi_UDF());
        System.out.println("pi_UDF_Boxed(): " + udtf.pi_UDF_Boxed());

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("abs_BIGINT("+intInputs[i]+"): " + udtf.abs_BIGINT(intInputs[i].longValue()));
        }
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("abs_INTEGER("+intInputs[i]+"): " + udtf.abs_INTEGER(intInputs[i]));
        }
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("abs_SMALLINT("+intInputs[i]+"): " + udtf.abs_SMALLINT(intInputs[i].shortValue()));
        }
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("abs_TINYINT("+intInputs[i]+"): " + udtf.abs_TINYINT(intInputs[i].byteValue()));
        }
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("abs_FLOAT("+intInputs[i]+"): " + udtf.abs_FLOAT(intInputs[i].doubleValue()));
        }
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("abs_BIGINT_Boxed("+intInputs[i]+"): " + udtf.abs_BIGINT_Boxed(intInputs[i].longValue()));
        }
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("abs_INTEGER_Boxed("+intInputs[i]+"): " + udtf.abs_INTEGER_Boxed(intInputs[i]));
        }
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("abs_SMALLINT_Boxed("+intInputs[i]+"): " + udtf.abs_SMALLINT_Boxed(intInputs[i].shortValue()));
        }
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("abs_TINYINT_Boxed("+intInputs[i]+"): " + udtf.abs_TINYINT_Boxed(intInputs[i].byteValue()));
        }
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("abs_FLOAT_Boxed("+intInputs[i]+"): " + udtf.abs_FLOAT_Boxed(intInputs[i].doubleValue()));
        }
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("abs_DECIMAL("+intInputs[i]+"): " + udtf.abs_DECIMAL(new BigDecimal(intInputs[i])));
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            System.out.println("reverse("+intInputs[i]+"): " + udtf.reverse(intInputs[i].toString()));
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            String polygon = "POLYGON((0 0, "+intInputs[i]+" 0, 0 "+intInputs[i]/2+", 0 0))";
            for (int j=0; j < i; j++) {
                polygon = polygon.replace("))", "),("+j+" "+j+", "+intInputs[i]/2+" "+j+", "+j+" "+intInputs[i]/4+", "+j+" "+j+"))");
            }
            GeographyValue g = new GeographyValue(polygon);
            System.out.println("numRings     ( "+g+" ): " + udtf.numRings(g));
            System.out.println("numPoints_UDF( "+g+" ): " + udtf.numPoints_UDF(g));
        }

        System.out.println();
        System.out.println("Tests of (PostgreSQL-compatible) UDF's with 2 args:");
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            try {
                System.out.println("mod_BIGINT("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.mod_BIGINT(intInputs[i].longValue(), intInputs[i+1].longValue()));
            } catch (Throwable e) {
                System.out.println("mod_BIGINT("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            try {
                System.out.println("mod_INTEGER("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.mod_INTEGER(intInputs[i], intInputs[i+1]));
            } catch (Throwable e) {
                System.out.println("mod_INTEGER("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            try {
                System.out.println("mod_SMALLINT("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.mod_SMALLINT(intInputs[i].shortValue(), intInputs[i+1].shortValue()));
            } catch (Throwable e) {
                System.out.println("mod_SMALLINT("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            try {
                System.out.println("mod_TINYINT("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.mod_TINYINT(intInputs[i].byteValue(), intInputs[i+1].byteValue()));
            } catch (Throwable e) {
                System.out.println("mod_TINYINT("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            try {
                System.out.println("mod_FLOAT("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.mod_FLOAT(intInputs[i].doubleValue(), intInputs[i+1].doubleValue()));
            } catch (Throwable e) {
                System.out.println("mod_FLOAT("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            try {
                System.out.println("mod_BIGINT_Boxed("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.mod_BIGINT_Boxed(intInputs[i].longValue(), intInputs[i+1].longValue()));
            } catch (Throwable e) {
                System.out.println("mod_BIGINT_Boxed("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            try {
                System.out.println("mod_INTEGER_Boxed("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.mod_INTEGER_Boxed(intInputs[i], intInputs[i+1]));
            } catch (Throwable e) {
                System.out.println("mod_INTEGER_Boxed("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            try {
                System.out.println("mod_SMALLINT_Boxed("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.mod_SMALLINT_Boxed(intInputs[i].shortValue(), intInputs[i+1].shortValue()));
            } catch (Throwable e) {
                System.out.println("mod_SMALLINT_Boxed("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            try {
                System.out.println("mod_TINYINT_Boxed("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.mod_TINYINT_Boxed(intInputs[i].byteValue(), intInputs[i+1].byteValue()));
            } catch (Throwable e) {
                System.out.println("mod_TINYINT_Boxed("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            try {
                System.out.println("mod_FLOAT_Boxed("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.mod_FLOAT_Boxed(intInputs[i].doubleValue(), intInputs[i+1].doubleValue()));
            } catch (Throwable e) {
                System.out.println("mod_FLOAT_Boxed("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            try {
                System.out.println("mod_DECIMAL("+intInputs[i]+","+intInputs[i+1]+"): " + udtf.mod_DECIMAL(new BigDecimal(intInputs[i]), new BigDecimal(intInputs[i+1])));
            } catch (Throwable e) {
                System.out.println("mod_DECIMAL("+intInputs[i]+","+intInputs[i+1]+") threw Exception:\n"+e);
            }
        }

        System.out.println();
        byte[] a = new byte[19];
        for (int i=0; i < 8; i++) {
            a[i] = (byte) i;
            a[18-i] = (byte) i;
        }
        a[9] = 0;
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            a[8]  = intInputs[i].byteValue();
            a[10] = (byte) (intInputs[i+1] - 1);
            byte[] b = new byte[i];
            for (byte j=0; j < b.length; j++) {
                b[j] = (byte) j;
            }
            System.out.println("btrim("+Arrays.toString(a)+","+Arrays.toString(b)+"): " + Arrays.toString(udtf.btrim(a, b)));
        }

        System.out.println();
        Byte[] c = new Byte[19];
        for (byte i=0; i < 8; i++) {
            c[i] = (Byte) i;
            c[18-i] = (Byte) i;
        }
        c[9] = 0;
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            c[8]  = intInputs[i].byteValue();
            c[10] = (Byte) (byte) (intInputs[i+1] - 1);
            Byte[] d = new Byte[i];
            for (byte j=0; j < d.length; j++) {
                d[j] = (Byte) j;
            }
            System.out.println("btrim_Boxed("+Arrays.toString(c)+","+Arrays.toString(d)+"): " + Arrays.toString(udtf.btrim_Boxed(c, d)));
        }

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            String s1 = intInputs[i].toString();
            String s2 = intInputs[i+1].toString();
            System.out.println("concat2Varchar("+s1+","+s2+"): " + udtf.concat2Varchar(s1, s2));
        }
        System.out.println("concat2Varchar(null,null): " + udtf.concat2Varchar(null, null));

        System.out.println();
        System.out.println("Tests of (PostgreSQL-compatible) UDF's with 3+ args:");
        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            String s1 = intInputs[i].toString();
            String s2 = intInputs[i+1].toString();
            String s3 = intInputs[i].toString();
            System.out.println("concat3Varchar("+s1+","+s2+","+s3+"): " + udtf.concat3Varchar(s1, s2, s3));
        }
        System.out.println("concat3Varchar(null,null,null): " + udtf.concat3Varchar(null, null, null));

        System.out.println();
        for (int i=0; i < NUM_SIMPLE_TESTS; i+=2) {
            String s1 = intInputs[i].toString();
            String s2 = intInputs[i+1].toString();
            String s3 = intInputs[i].toString();
            String s4 = intInputs[i+1].toString();
            System.out.println("concat4Varchar("+s1+","+s2+","+s3+","+s4+"): " + udtf.concat4Varchar(s1, s2, s3, s4));
        }
        System.out.println("concat4Varchar(null,null,null,null): " + udtf.concat4Varchar(null, null, null, null));

    }

}
