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

public class Update extends VoltProcedure {

    public final SQLStmt u_no_nulls = new SQLStmt
    ("UPDATE NO_NULLS SET A_TINYINT = ?, A_SMALLINT = ?, A_INTEGER = ?, A_BIGINT = ?, A_FLOAT = ?, A_TIMESTAMP = ?, A_INLINE_S1 = ?, A_INLINE_S2 = ?, A_POOL_S = ?, A_POOL_MAX_S = ?, A_INLINE_B = ?, A_POOL_B = ?, A_DECIMAL = ?, A_GEOGRAPHY_POINT = ?, A_GEOGRAPHY = ? WHERE PKEY = ?;");

    public final SQLStmt u_allow_nulls = new SQLStmt
    ("UPDATE ALLOW_NULLS SET A_TINYINT = ?, A_SMALLINT = ?, A_INTEGER = ?, A_BIGINT = ?, A_FLOAT = ?, A_TIMESTAMP = ?, A_INLINE_S1 = ?, A_INLINE_S2 = ?, A_POOL_S = ?, A_POOL_MAX_S = ?, A_INLINE_B = ?, A_POOL_B = ?, A_DECIMAL = ?, A_GEOGRAPHY_POINT = ?, A_GEOGRAPHY = ?  WHERE PKEY = ?;");

    public final SQLStmt u_jumbo_row = new SQLStmt
    ("UPDATE JUMBO_ROW SET STRING1 = ?, STRING2 = ? WHERE PKEY = ?;");

    public VoltTable[] run(
            String tablename,
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
        if (tablename.equals("NO_NULLS")) {
            voltQueueSQL(u_no_nulls, a_tinyint, a_smallint, a_integer,
                         a_bigint, a_float, a_timestamp, a_inline_s1, a_inline_s2,
                         a_pool_s, a_pool_max_s, b_inline, b_pool, a_decimal, a_geography_point, a_geography, pkey);
        }
        else if (tablename.equals("ALLOW_NULLS")) {
            voltQueueSQL(u_allow_nulls, a_tinyint, a_smallint, a_integer,
                         a_bigint, a_float, a_timestamp, a_inline_s1, a_inline_s2,
                         a_pool_s, a_pool_max_s, b_inline, b_pool, a_decimal, a_geography_point, a_geography, pkey);
        } else if (tablename.equals("JUMBO_ROW")) {
            voltQueueSQL(u_jumbo_row, a_inline_s1, a_inline_s2, 0);
        }

        return voltExecuteSQL();
    }
}
