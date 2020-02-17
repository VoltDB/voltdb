/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

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
        m_name = description.getDisplayName();
        return base;
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
