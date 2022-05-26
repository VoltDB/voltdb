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

/*
 * Stored procedure for Kafka import
 *
 * If incoming data is in the mirror table, delete that row.
 *
 * Else add to import table as a record of rows that didn't get
 * into the mirror table, a major error!
 */

package kafkaimporter.db.procedures;

import java.math.BigDecimal;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

public class InsertImport2 extends VoltProcedure {
    public final SQLStmt selectCounts = new SQLStmt("SELECT key FROM importcounts ORDER BY key LIMIT 1");
    public final SQLStmt insertCounts = new SQLStmt("INSERT INTO importcounts(KEY, TOTAL_ROWS_DELETED) VALUES (?, ?)");
    public final SQLStmt updateCounts = new SQLStmt("UPDATE importcounts SET total_rows_deleted=total_rows_deleted+? where key = ?");
    public final SQLStmt updateMismatch = new SQLStmt("INSERT INTO importcounts VALUES(?, ?, ?)");
    public final SQLStmt selectMirrorRow = new SQLStmt("SELECT * FROM kafkamirrortable2 WHERE key = ? AND value = ? LIMIT 1");
    public final String sqlSuffix =
            "(key, value, rowid_group, type_null_tinyint, type_not_null_tinyint, " +
                    "type_null_smallint, type_not_null_smallint, type_null_integer, " +
                    "type_not_null_integer, type_null_bigint, type_not_null_bigint, " +
                    "type_null_timestamp, type_not_null_timestamp, type_null_float, " +
                    "type_not_null_float, type_null_decimal, type_not_null_decimal, " +
                    "type_null_varchar25, type_not_null_varchar25, type_null_varchar128, " +
                    "type_not_null_varchar128, type_null_varchar1024, type_not_null_varchar1024)" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    public final SQLStmt importInsert = new SQLStmt("INSERT INTO kafkaImportTable2 " + sqlSuffix);
    public final SQLStmt deleteMirrorRow = new SQLStmt(
            "DELETE FROM kafkaMirrorTable2 WHERE key = ? AND value = ?"
            );

    public long run(
            long key, long value, byte rowid_group,
            byte type_null_tinyint, byte type_not_null_tinyint,
            short type_null_smallint, short type_not_null_smallint,
            int type_null_integer, int type_not_null_integer,
            long type_null_bigint, long type_not_null_bigint,
            TimestampType type_null_timestamp, TimestampType type_not_null_timestamp,
            double type_null_float, double type_not_null_float,
            BigDecimal type_null_decimal, BigDecimal type_not_null_decimal,
            String type_null_varchar25, String type_not_null_varchar25,
            String type_null_varchar128, String type_not_null_varchar128,
            String type_null_varchar1024, String type_not_null_varchar1024) {

        // column positions, used in "get" calls below
        final int TYPE_NULL_TINYINT = 3;
        final int TYPE_NOT_NULL_TINYINT = 4;
        final int TYPE_NULL_SMALLINT = 5;
        final int TYPE_NOT_NULL_SMALLINT = 6;
        final int TYPE_NULL_INTEGER = 7;
        final int TYPE_NOT_NULL_INTEGER = 8;
        final int TYPE_NULL_BIGINT = 9;
        final int TYPE_NOT_NULL_BIGINT = 10;
        final int TYPE_NULL_TIMESTAMP = 11;
        final int TYPE_NOT_NULL_TIMESTAMP = 12;
        final int TYPE_NULL_FLOAT = 13;
        final int TYPE_NOT_NULL_FLOAT = 14;
        final int TYPE_NULL_DECIMAL = 15;
        final int TYPE_NOT_NULL_DECIMAL = 16;
        final int TYPE_NULL_VARCHAR25 = 17;
        final int TYPE_NOT_NULL_VARCHAR25 = 18;
        final int TYPE_NULL_VARCHAR128 = 19;
        final int TYPE_NOT_NULL_VARCHAR128 = 20;
        final int TYPE_NULL_VARCHAR1024 = 21;
        final int TYPE_NOT_NULL_VARCHAR1024 = 22;

        // find mirror row that matches import
        voltQueueSQL(selectMirrorRow, key, value);
        VoltTable[] mirrorResults = voltExecuteSQL();
        VoltTable rowData = mirrorResults[0];

        long deletedCount = 0;
        boolean rowCheckOk = true;
        if (rowData.getRowCount() == 1) {
            // we already checked key and value via SELECT; now work through the rest of the types
            // not_null rows are simple compares. nullable types need to check for null as well

            byte ntiVal = (byte) rowData.fetchRow(0).get(TYPE_NULL_TINYINT, VoltType.TINYINT);
            if (ntiVal != type_null_tinyint) {
                rowCheckOk =  reportMismatch("type_null_tinyint", String.valueOf(type_null_tinyint), String.valueOf(ntiVal));
            }

            byte tiVal = (byte) rowData.fetchRow(0).get(TYPE_NOT_NULL_TINYINT, VoltType.TINYINT);
            if (tiVal != type_not_null_tinyint) {
                rowCheckOk =  reportMismatch("type_not_null_tinyint", String.valueOf(type_not_null_tinyint), String.valueOf(tiVal));
            }

            short nsiVal = (short) rowData.fetchRow(0).get(TYPE_NULL_SMALLINT, VoltType.SMALLINT);
            if (nsiVal != type_null_smallint)  {
                rowCheckOk =  reportMismatch("type_null_smallint", String.valueOf(type_null_smallint), String.valueOf(nsiVal));
            }

            short siVal = (short) rowData.fetchRow(0).get(TYPE_NOT_NULL_SMALLINT, VoltType.SMALLINT);
            if (siVal != type_not_null_smallint ) {
                rowCheckOk =  reportMismatch("type_not_null_smallint", String.valueOf(type_not_null_smallint), String.valueOf(siVal));
            }

            int nintVal = (int) rowData.fetchRow(0).get(TYPE_NULL_INTEGER, VoltType.INTEGER);
            if (nintVal != type_null_integer ) {
                rowCheckOk =  reportMismatch("type_null_integer", String.valueOf(type_null_integer), String.valueOf(nintVal));
            }

            int intVal = (int) rowData.fetchRow(0).get(TYPE_NOT_NULL_INTEGER, VoltType.INTEGER);
            if (intVal != type_not_null_integer ) {
                rowCheckOk =  reportMismatch("type_not_null_integer", String.valueOf(type_not_null_integer), String.valueOf(intVal));
            }

            long nbigVal = (long) rowData.fetchRow(0).get(TYPE_NULL_BIGINT, VoltType.BIGINT);
            if (nbigVal != type_null_bigint ) {
                rowCheckOk =  reportMismatch("type_null_bigint", String.valueOf(type_null_bigint), String.valueOf(nbigVal));
            }

            long bigVal = (long) rowData.fetchRow(0).get(TYPE_NOT_NULL_BIGINT, VoltType.BIGINT);
            if (bigVal != type_not_null_bigint ) {
                rowCheckOk =  reportMismatch("type_not_null_bigint", String.valueOf(type_not_null_bigint), String.valueOf(bigVal));
            }

            TimestampType ntsVal = (TimestampType) rowData.fetchRow(0).get(TYPE_NULL_TIMESTAMP, VoltType.TIMESTAMP);
            if (!(ntsVal == null && type_null_timestamp == null) && !ntsVal.equals(type_null_timestamp)) {
                rowCheckOk =  reportMismatch("type_null_timestamp", type_null_timestamp.toString(), ntsVal.toString());
            }

            TimestampType tsVal = (TimestampType) rowData.fetchRow(0).get(TYPE_NOT_NULL_TIMESTAMP, VoltType.TIMESTAMP);
            if (!tsVal.equals(type_not_null_timestamp)) {
                rowCheckOk =  reportMismatch("type_not_null_timestamp", type_not_null_timestamp.toString(), tsVal.toString());
            }

            double nfloatVal = (double) rowData.fetchRow(0).get(TYPE_NULL_FLOAT, VoltType.FLOAT);
            if (nfloatVal != type_null_float) {
                rowCheckOk =  reportMismatch("type_null_float", String.valueOf(type_null_float), String.valueOf(nfloatVal));
            }

            double floatVal = (double) rowData.fetchRow(0).get(TYPE_NOT_NULL_FLOAT, VoltType.FLOAT);
            if (floatVal != type_not_null_float ) {
                rowCheckOk =  reportMismatch("type_not_null_float", String.valueOf(type_not_null_float), String.valueOf(floatVal));
            }

            BigDecimal ndecimalVal = (BigDecimal) rowData.fetchRow(0).get(TYPE_NULL_DECIMAL, VoltType.DECIMAL);
            if (!(ndecimalVal == null && type_null_decimal == null) && !ndecimalVal.equals(type_null_decimal)) {
                rowCheckOk =  reportMismatch("type_null_decimal", type_null_decimal.toString(), ndecimalVal.toString());
            }

            BigDecimal decimalVal = (BigDecimal) rowData.fetchRow(0).get(TYPE_NOT_NULL_DECIMAL, VoltType.DECIMAL);
            if (!decimalVal.equals(type_not_null_decimal)) {
                rowCheckOk =  reportMismatch("type_not_null_decimal", type_not_null_decimal.toString(), decimalVal.toString());
            }

            String nstring25Val = (String) rowData.fetchRow(0).get(TYPE_NULL_VARCHAR25, VoltType.STRING);
            if (!(nstring25Val == null && type_null_varchar25 == null) && !nstring25Val.equals(type_null_varchar25)) {
                rowCheckOk =  reportMismatch("type_null_varchar25", type_null_varchar25, nstring25Val);
            }

            String string25Val = (String) rowData.fetchRow(0).get(TYPE_NOT_NULL_VARCHAR25, VoltType.STRING);
            if (!string25Val.equals(type_not_null_varchar25)) {
                rowCheckOk =  reportMismatch("type_not_null_varchar25", type_not_null_varchar25, string25Val);
            }

            String nstring128Val = (String) rowData.fetchRow(0).get(TYPE_NULL_VARCHAR128, VoltType.STRING);
            if (!(nstring128Val == null && type_null_varchar128 == null) && ! nstring128Val.equals(type_null_varchar128)) {
                rowCheckOk =  reportMismatch("type_null_varchar128", type_null_varchar128, nstring128Val);
            }

            String string128Val = (String) rowData.fetchRow(0).get(TYPE_NOT_NULL_VARCHAR128, VoltType.STRING);
            if (!string128Val.equals(type_not_null_varchar128)) {
                rowCheckOk =  reportMismatch("type_not_null_varchar128", type_not_null_varchar128, string128Val);
            }

            String nstring1024Val = (String) rowData.fetchRow(0).get(TYPE_NULL_VARCHAR1024, VoltType.STRING);
            if (!(nstring1024Val == null && type_null_varchar1024 == null) && !nstring1024Val.equals(type_null_varchar1024)) {
                rowCheckOk =  reportMismatch("type_null_varchar1024", type_null_varchar1024, nstring1024Val);
            }

            String string1024Val = (String) rowData.fetchRow(0).get(TYPE_NOT_NULL_VARCHAR1024, VoltType.STRING);
            if (!string1024Val.equals(type_not_null_varchar1024)) {
                rowCheckOk = reportMismatch("type_not_null_varchar1024", type_not_null_varchar1024, string1024Val);
            }

            if (rowCheckOk) {  // delete the row
                voltQueueSQL(deleteMirrorRow, EXPECT_SCALAR_LONG, key, value);
                deletedCount = voltExecuteSQL()[0].asScalarLong();
            } else { // there was a data mismatch; set VALUE_MISMATCH, which will be noticed by client
                voltQueueSQL(updateMismatch, key, value, 1);
                voltExecuteSQL();
                return 0;
            }

            if (deletedCount != 1) {
                System.out.println("Rows deleted: " + deletedCount + ", key: " + key + ", value: " + value);
            }
        } else {           // add to import table as indicator of dupe or mismatch
            voltQueueSQL(importInsert,
                    key, value, rowid_group,
                    type_null_tinyint, type_not_null_tinyint,
                    type_null_smallint, type_not_null_smallint,
                    type_null_integer, type_not_null_integer,
                    type_null_bigint, type_not_null_bigint,
                    type_null_timestamp, type_not_null_timestamp,
                    type_null_float, type_not_null_float,
                    type_null_decimal, type_not_null_decimal,
                    type_null_varchar25, type_not_null_varchar25,
                    type_null_varchar128, type_not_null_varchar128,
                    type_null_varchar1024, type_not_null_varchar1024);
            voltExecuteSQL();
        }

        // update the counts tables so we can track progress and results
        // since the SP can't return results to the client
        voltQueueSQL(selectCounts);
        VoltTable[] result = voltExecuteSQL();
        VoltTable data = result[0];
        long nrows = data.getRowCount();
        if (nrows > 0) {
            long ck = data.fetchRow(0).getLong(0);
            voltQueueSQL(updateCounts, deletedCount, ck);
            voltExecuteSQL(true);
        } else {
            voltQueueSQL(insertCounts, key, deletedCount);
            voltExecuteSQL(true);
        }
        return 0;
    }

    private boolean reportMismatch(String typeName, String mirrorVal, String importVal) {
        System.out.println("Mirror " + typeName + " not equal to import " + typeName + ":" + mirrorVal + " != " + importVal);
        return false;
    }
}
