/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package server;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Base class for SP or MP invocations: each priority level uses different SP and MP procedures per priority in order
 * to distinguish them in the stats. Each SP operates on a different table, allowing to separate the results. All MP procedures
 * operates on the same table with constant numbe of rows in order to have constant MP execution time
 */
public class TestInvocationBase extends VoltProcedure {
    final long m_seed = Long.MAX_VALUE / 8;

    protected final VoltLogger LOG = new VoltLogger("FOO");

    // SP invocation inserts row into TABLExx using parameter as primary key 'rowid'
    final String m_spTemplate = "INSERT INTO TABLE%02d (rowid, rowid_group, type_null_tinyint, type_not_null_tinyint, type_null_smallint, type_not_null_smallint, "
            + "type_null_integer, type_not_null_integer, type_null_bigint, type_not_null_bigint, type_null_timestamp, type_not_null_timestamp, type_null_float, type_not_null_float, "
            + "type_null_decimal, type_not_null_decimal, type_null_varchar25, type_not_null_varchar25, type_null_varchar128, type_not_null_varchar128, type_null_varchar1024, "
            + "type_not_null_varchar1024) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    // MP invocation updates MP_TABLE with constant increment
    String m_mpTemplate = "UPDATE MP_TABLE SET bigint = bigint + ?, timestamp = now()";

    final SQLStmt m_spStatement;
    final SQLStmt m_mpStatement;
    final int m_priority;
    final boolean m_isMp;

    public TestInvocationBase(int priority, boolean isMp) {
        m_spStatement = new SQLStmt(String.format(m_spTemplate, priority));
        m_mpStatement = new SQLStmt(m_mpTemplate);
        m_priority = priority;
        m_isMp = isMp;
    }

    public VoltTable[] doRun (long parameter, long delay) {

        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (Exception e) {
                // ignore
            }
        }

        if (m_isMp) {
            voltQueueSQL(m_mpStatement, parameter);
        }
        else {
            // Note: with a Random initialized from the same seed on each call
            // we get the same record contents. This is intentional for now in order
            // to produce a 'square' test where every inserted record is the same (beyond the rowid).
            Random rand = new Random(m_seed);
            TestRecord record = new TestRecord(parameter, rand);
            voltQueueSQL(m_spStatement
                    , parameter
                    , record.rowid_group
                    , record.type_null_tinyint
                    , record.type_not_null_tinyint
                    , record.type_null_smallint
                    , record.type_not_null_smallint
                    , record.type_null_integer
                    , record.type_not_null_integer
                    , record.type_null_bigint
                    , record.type_not_null_bigint
                    , record.type_null_timestamp
                    , record.type_not_null_timestamp
                    , record.type_null_float
                    , record.type_not_null_float
                    , record.type_null_decimal
                    , record.type_not_null_decimal
                    , record.type_null_varchar25
                    , record.type_not_null_varchar25
                    , record.type_null_varchar128
                    , record.type_not_null_varchar128
                    , record.type_null_varchar1024
                    , record.type_not_null_varchar1024
                    );
        }
        return voltExecuteSQL();
    }
}
