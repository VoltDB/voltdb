/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/*
 * Copyright (C) 2012 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* This code was originally sourced from https://github.com/addthis/stream-lib
   in December 2014. */

package org.voltdb_hll;

import java.io.IOException;

/**
 * Java implementation of HyperLogLog (HLL) algorithm from this paper:
 * <p/>
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 * <p/>
 * HLL is an improved version of LogLog that is capable of estimating
 * the cardinality of a set with accuracy = 1.04/sqrt(m) where
 * m = 2^b.  So we can control accuracy vs space usage by increasing
 * or decreasing b.
 * <p/>
 * The main benefit of using HLL over LL is that it only requires 64%
 * of the space that LL does to get the same accuracy.
 * <p/>
 * This implementation implements a single counter.  If a large (millions)
 * number of counters are required you may want to refer to:
 * <p/>
 * http://dsiutils.dsi.unimi.it/
 * <p/>
 * It has a more complex implementation of HLL that supports multiple counters
 * in a single object, drastically reducing the java overhead from creating
 * a large number of objects.
 * <p/>
 * This implementation leveraged a javascript implementation that Yammer has
 * been working on:
 * <p/>
 * https://github.com/yammer/probablyjs
 * <p>
 * Note that this implementation does not include the long range correction function
 * defined in the original paper.  Empirical evidence shows that the correction
 * function causes more harm than good.
 * </p>
 * <p/>
 * <p>
 * Users have different motivations to use different types of hashing functions.
 * Rather than try to keep up with all available hash functions and to remove
 * the concern of causing future binary incompatibilities this class allows clients
 * to offer the value in hashed int or long form.  This way clients are free
 * to change their hash function on their own time line.  We recommend using Google's
 * Guava Murmur3_128 implementation as it provides good performance and speed when
 * high precision is required.  In our tests the 32bit MurmurHash function included
 * in this project is faster and produces better results than the 32 bit murmur3
 * implementation google provides.
 * </p>
 */
public class HyperLogLog {

    private final RegisterSet registerSet;
    private final int log2m;

    private static void validateLog2m(int log2m) {
        if (log2m < 0 || log2m > 30) {
            throw new IllegalArgumentException("log2m argument is "
                                               + log2m + " and is outside the range [0, 30]");
        }
    }

    public static HyperLogLog fromBytes(byte[] bytes) throws IOException {
        RegisterSet registerSet = new RegisterSet(bytes);
        for (int log2m = 0; log2m <= 30; log2m++) {
            if ((1 << log2m) == registerSet.count()) {
                return new HyperLogLog(log2m, registerSet);
            }
        }
        throw new RuntimeException("Couldn't find log2m value for byte array");
    }

    public byte[] toBytes() throws IOException {
        return registerSet.toBytes();
    }

    public boolean getDirty() {
        return registerSet.getDirty();
    }

    /**
     * Create a new HyperLogLog instance.  The log2m parameter defines the accuracy of
     * the counter.  The larger the log2m the better the accuracy.
     * <p/>
     * accuracy = 1.04/sqrt(2^log2m)
     *
     * @param log2m - the number of bits to use as the basis for the HLL instance
     */
    public HyperLogLog(int log2m) {
        this(log2m, new RegisterSet(1 << log2m));
    }

    /**
     * Creates a new HyperLogLog instance using the given registers.  Used for unmarshalling a serialized
     * instance and for merging multiple counters together.
     *
     * @param registerSet - the initial values for the register set
     */
    HyperLogLog(int log2m, RegisterSet registerSet) {
        validateLog2m(log2m);
        this.registerSet = registerSet;
        this.log2m = log2m;
    }

    public boolean offer(Object o) {
        final int x = MurmurHash.hash(o);
        return offerHashed(x);
    }

    public boolean offerHashed(long hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = (int) (hashedValue >>> (Long.SIZE - log2m));
        final int r = Long.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return registerSet.updateIfGreater(j, r);
    }

    public boolean offerHashed(int hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = hashedValue >>> (Integer.SIZE - log2m);
        final int r = Integer.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return registerSet.updateIfGreater(j, r);
    }

    public long cardinality() {
        double registerSum = 0;
        int count = registerSet.count();
        double zeros = 0.0;
        for (int j = 0; j < count; j++) {
            int val = registerSet.get(j);
            if (val == 0) {
                registerSum++;
                zeros++;
            }
            else {
                registerSum += 1.0 / (1 << val);
            }
        }

        double estimate = getAlphaMM() * (1.0 / registerSum);

        if (estimate <= (5.0 / 2.0) * count) {
            // Small Range Estimate
            return Math.round(linearCounting(count, zeros));
        } else {
            return Math.round(estimate);
        }
    }

    public int sizeof() {
        return registerSet.size();
    }

    /**
     * Add all the elements of the other set to this set.
     * <p/>
     * This operation does not imply a loss of precision.
     *
     * @param other A compatible Hyperloglog instance (same log2m)
     * @throws CardinalityMergeException if other is not compatible
     */
    public void addAll(HyperLogLog other) {
        if (this.sizeof() != other.sizeof()) {
            throw new RuntimeException("Cannot merge estimators of different sizes");
        }
        registerSet.merge(other.registerSet);
    }

    public HyperLogLog merge(HyperLogLog... estimators) {
        HyperLogLog merged = new HyperLogLog(log2m, new RegisterSet(this.registerSet.count()));
        merged.addAll(this);

        if (estimators == null) {
            return merged;
        }

        for (HyperLogLog estimator : estimators) {
            if (!(estimator instanceof HyperLogLog)) {
                throw new RuntimeException("Cannot merge estimators of different class");
            }
            HyperLogLog hll = estimator;
            merged.addAll(hll);
        }

        return merged;
    }

    protected double getAlphaMM() {
        final int m = 1 << log2m;

        // See the paper.
        switch (log2m) {
            case 4:
                return 0.673 * m * m;
            case 5:
                return 0.697 * m * m;
            case 6:
                return 0.709 * m * m;
            default:
                return (0.7213 / (1 + 1.079 / m)) * m * m;
        }
    }

    protected static double linearCounting(int m, double V) {
        return m * Math.log(m / V);
    }
}
