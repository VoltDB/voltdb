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

package org.voltdb.test.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.voltdb.VoltType;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.PolygonFactory;

/**
 * A subclass of {@link Random} which implements {@link TestRule} so that the seed being used by this instance is
 * printed to standard out so that the seed can be set if needed to try and reproduce a test run.
 */
public class RandomTestRule extends Random implements TestRule {
    private static final long serialVersionUID = 1L;

    private boolean m_needInitialize = true;
    private String m_name = "all tests";

    public RandomTestRule() {
    }

    public RandomTestRule(long seed) {
        initializeSeed(seed);
    }

    public void nextBytes(ByteBuffer buffer) {
        while (buffer.remaining() > Integer.BYTES) {
            buffer.putInt(nextInt());
        }
        if (buffer.hasRemaining()) {
            int value = nextInt();
            do {
                buffer.put((byte) value);
                value >>>= Byte.SIZE;
            } while (buffer.hasRemaining());
        }
    }

    /**
     * @param origin inclusive lower bound of the returned value
     * @param bound  exclusive upper bound of the returned value
     * @return a pseudorandom value between {@code origin} and {@code bound}
     */
    public int nextInt(int origin, int bound) {
        if (origin >= bound) {
            throw new IllegalArgumentException("origin must be less than bound");
        }
        return nextInt(bound - origin) + origin;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        setMethodName(description.getDisplayName());
        return base;
    }

    public void setMethodName(String name) {
        m_name = name;
    }

    /**
     * Generate multiple rows of random objects. About {@code 5%} of the objects will be null
     *
     * @param rows  number of rows to generate
     * @param types in each row
     * @return rows of randomly generated objects
     */
    public Object[][] nextValues(int rows, List<VoltType> types) {
        Object[][] result = new Object[rows][];
        for (int i = 0; i < rows; ++i) {
            result[i] = nextValues(types);
        }
        return result;
    }

    /**
     * Generate a row of random objects. About {@code 5%} of the objects will be null
     *
     * @param types in each row
     * @return a row of randomly generated objects
     */
    public Object[] nextValues(List<VoltType> types) {
        Object[] result = new Object[types.size()];
        for (int i = 0; i < result.length; ++i) {
            result[i] = nextValue(types.get(i));
        }
        return result;
    }

    /**
     * Generate a new random object of {@code type} or {@code null} for {@code 5%} of the returned values
     *
     * @param type of object to return
     * @return a randomly generated object of {@code type} or {@code null}
     */
    public Object nextValue(VoltType type) {
        return nextValue(type, 0.05);
    }

    /**
     * Generate a new random object of {@code type} or {@code null} when a randomly generated {@code double} is less
     * than {@code nullRatio}
     *
     * @param type      of object to return
     * @param nullRatio ratio of values returned which are null. Between {@code 1.0} and {@code 0.0}
     * @return a randomly generated object of {@code type} or {@code null}
     */
    public Object nextValue(VoltType type, double nullRatio) {
        if (nextDouble() < nullRatio) {
            return null;
        }

        switch (type) {
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
            case TINYINT:
                do {
                    byte value = (byte) nextInt();
                    if (value != Byte.MIN_VALUE) {
                        return value;
                    }
                } while (true);
            case SMALLINT:
                do {
                    short value = (short) nextInt();
                    if (value != Short.MIN_VALUE) {
                        return value;
                    }
                } while (true);
            case INTEGER:
                return nextInt() * (nextBoolean() ? 1 : -1);
            case BIGINT:
                return nextLong() * (nextBoolean() ? 1 : -1);
            case FLOAT:
                return nextDouble() * (nextBoolean() ? 1 : -1);
            case DECIMAL:
                return VoltDecimalHelper.setDefaultScale(
                        new BigDecimal(new BigInteger(nextInt(64), this), nextBoolean() ? 1 : -1));
            case TIMESTAMP:
                return new TimestampType((nextLong() >> 20) * 1000);
            case STRING:
                return RandomStringUtils.random(nextInt(4096), 0, 0, false, false, null, this);
            case VARBINARY:
                byte[] data = new byte[nextInt(4096)];
                nextBytes(data);
                return data;
            case GEOGRAPHY_POINT:
                return nextGeographyPointValue();
            case GEOGRAPHY:
                return nextBoolean()
                        ? PolygonFactory.CreateRegularConvex(nextGeographyPointValue(), nextGeographyPointValue(),
                                nextInt(10) + 3, nextDouble())
                        : PolygonFactory.CreateStar(nextGeographyPointValue(), nextGeographyPointValue(),
                                nextInt(10) + 3, nextDouble(), nextDouble());
        }
    }

    private GeographyPointValue nextGeographyPointValue() {
        return new GeographyPointValue(nextInt(36000) / 100.0 - 180, nextInt(18000) / 100.0 - 90);
    }

    @Override
    protected int next(int bits) {
        if (m_needInitialize) {
            initializeSeed(((long) (super.next(32)) << 32) + super.next(32));
        }
        return super.next(bits);
    }

    private void initializeSeed(long seed) {
        super.setSeed(seed);
        System.out.println("Seed being used for " + m_name + ": " + seed);
        m_needInitialize = false;
    }
}
