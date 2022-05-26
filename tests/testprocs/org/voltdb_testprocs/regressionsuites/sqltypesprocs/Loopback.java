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

package org.voltdb_testprocs.regressionsuites.sqltypesprocs;

import java.math.BigDecimal;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;

public class Loopback extends VoltProcedure {

    protected final SQLStmt i_no_nulls = new SQLStmt
    ("INSERT INTO LOOPBACK_NO_NULLS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public VoltTable[] run(
            int pkey,
            long a_tinyint,
            long a_smallint,
            long a_integer,
            long a_bigint,
            double a_float,
            TimestampType a_timestamp,
            String a_inline_s1,
            String a_inline_s2,
            String a_pool_s,
            String a_pool_max_s,
            byte[] b_inline,
            byte[] b_pool,
            BigDecimal a_decimal,
            GeographyPointValue a_geography_point,
            GeographyValue a_geography
            )
    {
        voltQueueSQL(i_no_nulls, pkey, a_tinyint, a_smallint, a_integer,
                     a_bigint, a_float, a_timestamp, a_inline_s1, a_inline_s2,
                     a_pool_s, a_pool_max_s, b_inline, b_pool, a_decimal,
                     a_geography_point, a_geography);

        return voltExecuteSQL();
    }
}
