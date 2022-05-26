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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package com;

import java.util.Random;

/** A TPC-C random generator. */
public abstract class RandomGenerator {
    /** Constants for the NURand function. */
    public static class NURandC {
        public NURandC(int cLast, int cId, int orderLineItemId) {
            this.cLast = cLast;
            this.cId = cId;
            this.orderLineItemId = orderLineItemId;
        }

        /** Create random NURand constants, appropriate for loading the database. */
        public static NURandC makeForLoad(RandomGenerator generator) {
            return new NURandC(generator.number(0, 255), generator.number(0, 1023),
                    generator.number(0, 8191));
        }

        /** @returns true if the cRun value is valid for running. See TPC-C 2.1.6.1 (page 20). */
        private static boolean validCRun(int cRun, int cLoad) {
            int cDelta = Math.abs(cRun - cLoad);
            return 65 <= cDelta && cDelta <= 119 && cDelta != 96 && cDelta != 112;
        }

        /** Create random NURand constants for running TPC-C. TPC-C 2.1.6.1. (page 20) specifies the
        valid range for these constants. */
        public static NURandC makeForRun(RandomGenerator generator, NURandC loadC) {
            int cRun = generator.number(0, 255);
            while (!validCRun(cRun, loadC.cLast)) {
                cRun = generator.number(0, 255);
            }
            assert validCRun(cRun, loadC.cLast);

            return new NURandC(cRun, generator.number(0, 1023), generator.number(0, 8191));
        }

        public final int cLast;
        public final int cId;
        public final int orderLineItemId;
    }

    /** @returns a int in the range [minimum, maximum]. Note that this is inclusive. */
    public abstract int number(int minimum, int maximum);

    /**
     * Returns a random int in a skewed gaussian distribution of the range
     * Note that the range is inclusive
     * A skew factor of 0.0 means that it's a uniform distribution
     * The greater the skew factor the higher the probability the selected random
     * value will be closer to the mean of the range
     *
     * @param minimum the minimum random number
     * @param maximum the maximum random number
     * @param skewFactor the factor to skew the stddev of the gaussian distribution
     */
    public abstract int skewedNumber(int minimum, int maximum, double skewFactor);

    /** @returns an int in the range [minimum, maximum], excluding excluding. */
    public int numberExcluding(int minimum, int maximum, int excluding) {
        assert minimum < maximum;
        assert minimum <= excluding && excluding <= maximum;

        // Generate 1 less number than the range
        int num = number(minimum, maximum-1);

        // Adjust the numbers to remove excluding
        if (num >= excluding) {
            num += 1;
        }
        assert minimum <= num && num <= maximum && num != excluding;
        return num;
    }

    public double fixedPoint(int decimal_places, double minimum, double maximum) {
        assert decimal_places > 0;
        assert minimum < maximum;

        int multiplier = 1;
        for (int i = 0; i < decimal_places; ++i) {
            multiplier *= 10;
        }

        int int_min = (int)(minimum * multiplier + 0.5);
        int int_max = (int)(maximum * multiplier + 0.5);

        return (double) number(int_min, int_max) / (double) multiplier;
    }

    /** @returns a last name as defined by TPC-C 4.3.2.3. Not actually random. */
    public String makeLastName(int number) {
        final String SYLLABLES[] = {
                "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
        };

        assert 0 <= number && number <= 999;
        int indicies[] = { number/100, (number/10)%10, number%10 };

        String name = "";
        for (int i = 0; i < indicies.length; ++i) {
            name += SYLLABLES[indicies[i]];
        }
        return name;
    }

    /** @returns a non-uniform random last name, as defined by TPC-C 4.3.2.3. The name will be
    limited to maxCID. */
    public String makeRandomLastName(int maxCID) {
        int min = 999;
        if (maxCID - 1 < min) min = maxCID - 1;
        return makeLastName(NURand(255, 0, min));
    }

    /** @returns a non-uniform random number, as defined by TPC-C 2.1.6. (page 20). */
    public int NURand(int A, int x, int y) {
        assert x <= y;
        int C;
        switch (A) {
            case 255:
                C = cValues.cLast;
                break;
            case 1023:
                C = cValues.cId;
                break;
            case 8191:
                C = cValues.orderLineItemId;
                break;
            default:
                throw new IllegalArgumentException("A = " + A + " is not a supported value");
        }

        return (((number(0, A) | number(x, y)) + C) % (y - x + 1)) + x;
    }

    public void setC(NURandC cValues) { this.cValues = cValues; }

    /** @returns a random alphabetic string with length in range [minimum_length, maximum_length].
    */
    public String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'a', 26);
    }

    /** @returns a random numeric string with length in range [minimum_length, maximum_length].
    */
    public String nstring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    private String randomString(int minimum_length, int maximum_length, char base,
            int numCharacters) {
        int length = number(minimum_length, maximum_length);
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte)(baseByte + number(0, numCharacters-1));
        }
        return new String(bytes);
    }

    private NURandC cValues = new NURandC(0, 0, 0);

    /** A RandomGenerator implementation using java.util.Random. */
    public static class Implementation extends RandomGenerator {
        /** Seeds the random number generator using the default Random() constructor. */
        public Implementation() { rng = new Random(); }
        /** Seeds the random number generator with seed. */
        public Implementation(long seed) { rng = new Random(seed); }

        public int number(int minimum, int maximum) {
            assert minimum <= maximum;
            int range_size = maximum - minimum + 1;
            int value = rng.nextInt(range_size);
            value += minimum;
            assert minimum <= value && value <= maximum;
            return value;
        }

        @Override
        public int skewedNumber(int minimum, int maximum, double skewFactor) {
            // Calling number() when the skewFactor is zero will likely be faster
            // than using our Gaussian distribution method below
            if (skewFactor == 0) return (this.number(minimum, maximum));

            assert minimum <= maximum;
            int range_size = maximum - minimum + 1;
            int mean = range_size / 2;
            double stddev = range_size - ((range_size / 1.1) * skewFactor);
            int value = -1;
            while (value < 0 || value >= range_size) {
                value = (int) Math.round(rng.nextGaussian() * stddev) + mean;
            }
            value += minimum;
            assert minimum <= value && value <= maximum;
            return value;
        }

        private final Random rng;
    }
}
