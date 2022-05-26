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
package org.voltdb.client.exampleutils;

/**
 * A simple utility class providing support for Greater Common Divisor calculation.
 *
 * @author Seb Coursol
 * @since 2.0
 */
public class MathEx
{
    /**
     * Recursively calculates the GCD of two integers.
     *
     * @param p the first integer of the pair to evaluate.
     * @param q the second integer of the pair to evaluate.
     * @return the GCD of the integer pair.
     */
    public static int gcd(int p, int q)
    {
        if (q == 0)
            return p;
        return gcd(q, p % q);
    }

    /**
     * Recursively calculates the GCD of two longs.
     *
     * @param p the first long of the pair to evaluate.
     * @param q the second long of the pair to evaluate.
     * @return the GCD of the long pair.
     */
    public static long gcd(long p, long q)
    {
        if (q == 0)
            return p;
        return gcd(q, p % q);
    }
}
