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

/**
 * This procedure performs an insert and then rolls it back;
 * used in conjunction with the nearly identical Insert.java
 * to test Export with rollback for TestExportSuite.java
 */

public class RollbackInsert extends VoltProcedure {

    public final SQLStmt i_no_nulls = new SQLStmt
    ("INSERT INTO NO_NULLS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public final SQLStmt i_allow_nulls = new SQLStmt
    ("INSERT INTO ALLOW_NULLS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt i_with_defaults = new SQLStmt
    ("INSERT INTO WITH_DEFAULTS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt i_with_null_defaults = new SQLStmt
    ("INSERT INTO WITH_NULL_DEFAULTS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt i_expressions_with_nulls = new SQLStmt
    ("INSERT INTO EXPRESSIONS_WITH_NULLS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt i_expressions_no_nulls = new SQLStmt
    ("INSERT INTO EXPRESSIONS_NO_NULLS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

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
    ) throws VoltAbortException
    {

        // these types are converted to instances of Long when processed
        // from the wire protocol serialization to the stored procedure
        // run prototype arguments. Convert them back to the underlying
        // java mappings of the SQL types here to test EE handling of non-long
        // values.

        byte v_tinyint = new Long(a_tinyint).byteValue();
        short v_smallint = new Long(a_smallint).shortValue();
        int v_integer = new Long(a_integer).intValue();

        if (tablename.equals("NO_NULLS")) {
            voltQueueSQL(i_no_nulls, pkey, v_tinyint, v_smallint, v_integer,
                         a_bigint, a_float, a_timestamp, a_inline_s1, a_inline_s2,
                         a_pool_s, a_pool_max_s, b_inline, b_pool, a_decimal,
                         a_geography_point, a_geography);
        }
        else if (tablename.equals("ALLOW_NULLS")) {
            voltQueueSQL(i_allow_nulls, pkey, v_tinyint, v_smallint, v_integer,
                         a_bigint, a_float, a_timestamp, a_inline_s1, a_inline_s2,
                         a_pool_s, a_pool_max_s, b_inline, b_pool, a_decimal,
                         a_geography_point, a_geography);
        }
        else if (tablename.equals("WITH_DEFAULTS")) {
            voltQueueSQL(i_with_defaults, pkey);
        }
        else if (tablename.equals("WITH_NULL_DEFAULTS")) {
            voltQueueSQL(i_with_null_defaults, pkey);
        }
        else if (tablename.equals("EXPRESSIONS_WITH_NULLS")) {
            voltQueueSQL(i_expressions_with_nulls, pkey, v_tinyint, v_smallint, v_integer,
                         a_bigint, a_float, a_timestamp, a_inline_s1, a_inline_s2,
                         a_pool_s, a_pool_max_s, b_inline, b_pool, a_decimal,
                         a_geography_point, a_geography);
        }
        else if (tablename.equals("EXPRESSIONS_NO_NULLS")) {
            voltQueueSQL(i_expressions_no_nulls, pkey, v_tinyint, v_smallint, v_integer,
                         a_bigint, a_float, a_timestamp, a_inline_s1, a_inline_s2,
                         a_pool_s, a_pool_max_s, b_inline, b_pool, a_decimal,
                         a_geography_point, a_geography);
        }

        VoltTable[] results =  voltExecuteSQL();
        if (results.length < 1) {
            throw new VoltAbortException("ERROR");
        }

        throw new VoltAbortException("OK");

    }
}
