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
package exportbenchmark;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

public class SampleRecord
{
    public final long rowid;
    public final Object rowid_group;
    public final Object type_null_tinyint;
    public final Object type_not_null_tinyint;
    public final Object type_null_smallint;
    public final Object type_not_null_smallint;
    public final Object type_null_integer;
    public final Object type_not_null_integer;
    public final Object type_null_bigint;
    public final Object type_not_null_bigint;
    public final Object type_null_timestamp;
    public final Object type_not_null_timestamp;
    public final Object type_null_float;
    public final Object type_not_null_float;
    public final Object type_null_decimal;
    public final Object type_not_null_decimal;
    public final Object type_null_varchar25;
    public final Object type_not_null_varchar25;
    public final Object type_null_varchar128;
    public final Object type_not_null_varchar128;
    public final Object type_null_varchar1024;
    public final Object type_not_null_varchar1024;
    public SampleRecord(long rowid, Random rand)
    {
        this.rowid = rowid;
        this.rowid_group = (byte)((rowid % 255) - 127);
        this.type_null_tinyint          = nextTinyint(rand, true);
        this.type_not_null_tinyint      = nextTinyint(rand);
        this.type_null_smallint         = nextSmallint(rand, true);
        this.type_not_null_smallint     = nextSmallint(rand);
        this.type_null_integer          = nextInteger(rand, true);
        this.type_not_null_integer      = nextInteger(rand);
        this.type_null_bigint           = nextBigint(rand, true);
        this.type_not_null_bigint       = nextBigint(rand);
        this.type_null_timestamp        = nextTimestamp(rand, true);
        this.type_not_null_timestamp    = nextTimestamp(rand);
        this.type_null_float            = nextFloat(rand, true);
        this.type_not_null_float        = nextFloat(rand);
        this.type_null_decimal          = nextDecimal(rand, true);
        this.type_not_null_decimal      = nextDecimal(rand);
        this.type_null_varchar25        = nextVarchar(rand, true, 1, 25);
        this.type_not_null_varchar25    = nextVarchar(rand, 1, 25);
        this.type_null_varchar128       = nextVarchar(rand, true, 25, 128);
        this.type_not_null_varchar128   = nextVarchar(rand, 25, 128);
        this.type_null_varchar1024      = nextVarchar(rand, true, 128, 1024);
        this.type_not_null_varchar1024  = nextVarchar(rand, 128, 1024);
    }

    private static Object nextTinyint(Random rand)
    {
        return nextTinyint(rand, false);
    }
    private static Object nextTinyint(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        byte result;
        do { result = (new Integer(rand.nextInt())).byteValue(); } while(result == VoltType.NULL_TINYINT);
        return result;
    }

    private static Object nextSmallint(Random rand)
    {
        return nextSmallint(rand, false);
    }
    private static Object nextSmallint(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        short result;
        do { result = (new Integer(rand.nextInt())).shortValue(); } while(result == VoltType.NULL_SMALLINT);
        return result;
    }

    private static Object nextInteger(Random rand)
    {
        return nextInteger(rand, false);
    }
    private static Object nextInteger(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        int result;
        do { result = rand.nextInt(); } while(result == VoltType.NULL_INTEGER);
        return result;
    }

    private static Object nextBigint(Random rand)
    {
        return nextBigint(rand, false);
    }
    private static Object nextBigint(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        long result;
        do { result = rand.nextLong(); } while(result == VoltType.NULL_BIGINT);
        return result;
    }

    private static Object nextTimestamp(Random rand)
    {
        return nextTimestamp(rand, false);
    }
    private static Object nextTimestamp(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        return new TimestampType(Math.abs(rand.nextInt())*1000l);
    }

    private static Object nextFloat(Random rand)
    {
        return nextFloat(rand, false);
    }
    private static Object nextFloat(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        double result; // Inconsistent naming (!)  Underlying database type is Double
        do { result = rand.nextDouble(); } while(result == VoltType.NULL_FLOAT);
        return result;
    }

    private static Object nextDecimal(Random rand)
    {
        return nextDecimal(rand, false);
    }
    private static Object nextDecimal(Random rand, boolean isNullable)
    {
        if (isNullable && rand.nextBoolean()) return null;
        return (new BigDecimal(rand.nextDouble()*rand.nextLong())).setScale(12, RoundingMode.HALF_EVEN);
    }

    private static Object nextVarchar(Random rand, int minLength, int maxLength)
    {
        return nextVarchar(rand, false, minLength, maxLength);
    }
    private static Object nextVarchar(Random rand, boolean isNullable, int minLength, int maxLength)
    {
        if (isNullable && rand.nextBoolean()) return null;
        int length = (maxLength==minLength)?maxLength:rand.nextInt(maxLength-minLength)+minLength;
        StringBuilder result = new StringBuilder(length);
        while(result.length() < length)
            result.append(Long.toBinaryString(rand.nextLong()));
        return result.toString().substring(0,Math.min(result.length(), length)-1);
    }
}
