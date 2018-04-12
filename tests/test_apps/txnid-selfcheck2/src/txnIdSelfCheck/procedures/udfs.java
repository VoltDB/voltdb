/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package txnIdSelfCheck.procedures;

public class udfs {

    public long add2Bigint(long i, long j) {
        // add two big ints
        return i + j;
    }

    public long identityInt(long i) {
        return i;
    }

    public byte[] identityVarbin(byte[] z) {
        // return a varbinary without change
        return z;
    }

    public long badUDF(long i) {
        // a function which always throws an exception
        return i/0;
    }
    /**
     * Compute (t, exponent) -> ((|t|  2) * 10**exponent).
     * This is used to create a family of UDFs to test
     * that UDFs return the right values, and that some
     * HSQL tables are not corrupted by database shutdowns,
     * joins and rejoins.
     *
     * @param exponent
     * @param t
     * @return
     */

    private long getExpected(long t, int exponent) {
        return (Math.abs(t) + 2) * (long)Math.pow(10, exponent);
    }

    /**
     * We want to call these functions, simpleUDF* from stored procedures.
     * We want to ensure that the HSQL mappings from names
     * to signatures and function IDs are recovered correctly
     * while the database is being stopped, started, recovered,
     * joined and rejoined.
     *
     * This computes (t) -> ((|t|  2) * 10^2).
     *
     * @param t Any long value.
     * @return (|t|2)*10**2
     */

    public long simpleUDF2(long t) {
        return getExpected(t, 2);
    }

    /**
     * @see simpleUDF2.
     * @param t
     * @return (|t|2)*10**3
     */

    public long simpleUDF3(long t) {
        return getExpected(t, 3);
    }
    /**
     * @see simpleUDF2.
     * @param t
     * @return (|t|2)*10**4
     */

    public long simpleUDF4(long t) {
        return getExpected(t, 4);
    }
    /**
     * @see simpleUDF2.
     * @param t
     * @return (|t|2)*10**5
     */

    public long simpleUDF5(long t) {
        return getExpected(t, 5);
    }
    /**
     * @see simpleUDF2.
     * @param t
     * @return (|t|2)*10**6
     */

    public long simpleUDF6(long t) {
        return getExpected(t, 6);
    }
    /**
     * @see simpleUDF2.
     * @param t
     * @return (|t|2)*10**7.
     */

    public long simpleUDF7(long t) {
        return getExpected(t, 7);
    }
    /**
     * @see simpleUDF2.
     * @param t
     * @return (|t|2)*10**8
     */

    public long simpleUDF8(long t) {
        return getExpected(t, 8);
    }
    /**
     * @see simpleUDF2.
     * @param t
     * @return (|t|2)*10**9
     */

    public long simpleUDF9(long t) {
        return getExpected(t, 9);
    }

    /**
     * @see simpleUDF2.
     * @param t
     * @return (|t|2)*10**10
     */

    public long simpleUDF10(long t) {
        return getExpected(t, 10);
    }
}
