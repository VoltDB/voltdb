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
package exportbenchmark;

import java.util.Random;

import org.voltdb.DeprecatedProcedureAPIAccess;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class InsertExport extends VoltProcedure {

    String template = "INSERT INTO ALL_VALUES (txnid, rowid, rowid_group, type_null_tinyint, type_not_null_tinyint, type_null_smallint, type_not_null_smallint, "
            + "type_null_integer, type_not_null_integer, type_null_bigint, type_not_null_bigint, type_null_timestamp, type_not_null_timestamp, type_null_float, type_not_null_float, "
            + "type_null_decimal, type_not_null_decimal, type_null_varchar25, type_not_null_varchar25, type_null_varchar128, type_not_null_varchar128, type_null_varchar1024, "
            + "type_not_null_varchar1024) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    String front = "INSERT INTO ALL_VALUES";
    String back = " (txnid, rowid, rowid_group, type_null_tinyint, type_not_null_tinyint, type_null_smallint, type_not_null_smallint, "
            + "type_null_integer, type_not_null_integer, type_null_bigint, type_not_null_bigint, type_null_timestamp, type_not_null_timestamp, type_null_float, type_not_null_float, "
            + "type_null_decimal, type_not_null_decimal, type_null_varchar25, type_not_null_varchar25, type_null_varchar128, type_not_null_varchar128, type_null_varchar1024, "
            + "type_not_null_varchar1024) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public final SQLStmt export = new SQLStmt(template);
    public final SQLStmt export1 = new SQLStmt(template.replace("ALL_VALUES", "ALL_VALUES1"));
    public final SQLStmt export2 = new SQLStmt(template.replace("ALL_VALUES", "ALL_VALUES2"));
    public final SQLStmt export3 = new SQLStmt(template.replace("ALL_VALUES", "ALL_VALUES3"));
    public final SQLStmt export4 = new SQLStmt(template.replace("ALL_VALUES", "ALL_VALUES4"));
    public final SQLStmt export5 = new SQLStmt(template.replace("ALL_VALUES", "ALL_VALUES5"));
    public final SQLStmt export6 = new SQLStmt(template.replace("ALL_VALUES", "ALL_VALUES6"));
    public final SQLStmt export7 = new SQLStmt(template.replace("ALL_VALUES", "ALL_VALUES7"));
    public final SQLStmt export8 = new SQLStmt(template.replace("ALL_VALUES", "ALL_VALUES8"));
    public final SQLStmt export9 = new SQLStmt(template.replace("ALL_VALUES", "ALL_VALUES9"));
    public final SQLStmt export10 = new SQLStmt(template.replace("ALL_VALUES", "ALL_VALUES10"));


    public long run(long rowid, int multiply, int targets)
    {
        @SuppressWarnings("deprecation")
        long txid = DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this);

        // Critical for proper determinism: get a cluster-wide consistent Random instance
        Random rand = new Random(txid);

        // Check multiply factor and adjust to reasonable value
        if (multiply <= 0) {
            multiply = 1;
        }

        // Insert a new record with olptional multiply factor
        SampleRecord record = new SampleRecord(rowid, rand);

        SQLStmt stmt = null;
        for (int j = 0; j < targets; j++)
            for (int i = 0; i < multiply; i++) {
                // int mod = (i*j) % targets;
                switch(j) {
                case 0 :
                    stmt = export;
                    break;
                case 1 :
                    stmt = export1;
                    break;
                case 2 :
                    stmt = export2;
                    break;
                case 3 :
                    stmt = export3;
                    break;
                case 4 :
                    stmt = export4;
                    break;
                case 5 :
                    stmt = export5;
                    break;
                case 6 :
                    stmt = export6;
                    break;
                case 7 :
                    stmt = export7;
                    break;
                case 8 :
                    stmt = export8;
                    break;
                case 9 :
                    stmt = export9;
                    break;
                case 10 :
                    stmt = export10;
                    break;
                default :
                    stmt = export;


                }

                // voltQueueSQL(
                //        stmt
                SQLStmt s = new SQLStmt(front + j + back);
                voltQueueSQL(s
                        , txid
                        , rowid
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

        // Execute last statement batch
        // voltExecuteSQL(true);
        voltExecuteSQL();

        // Retun to caller
        return txid;
    }
}
