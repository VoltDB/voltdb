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

package org.voltdb_testfuncs;

import java.io.Serializable;

import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltUDAggregate;

public class UminWithException implements Serializable, VoltUDAggregate<Double, UminWithException> {
    private double min = Double.POSITIVE_INFINITY;

    @Override
    public void start() {
    }

    @Override
    public void assemble (Double value) {
        if (value < min) {
            min = value;
            if (min < 0) {
                throw new VoltAbortException("Minimum value negative");
            }
        }
    }

    @Override
    public void combine (UminWithException other) {
        if (other.min < min) {
            min = other.min;
        }
    }

    @Override
    public Double end () {
        return min;
    }
}
