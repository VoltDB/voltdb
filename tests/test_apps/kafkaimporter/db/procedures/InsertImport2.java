/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
	 public final SQLStmt selectCounts = new SQLStmt("SELECT key FROM importcounts LIMIT 1");
	 public final SQLStmt insertCounts = new SQLStmt("INSERT INTO importcounts VALUES (?, ?)");
	 public final SQLStmt updateCounts = new SQLStmt("UPDATE importcounts SET total_rows_deleted=total_rows_deleted+? where key = ?");
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
    public final SQLStmt deleteMirrorRow = new SQLStmt("DELETE FROM kafkaMirrorTable2 WHERE " +
            "key = ? AND value = ?"
//          "key = ? AND " + "value = ? AND " + "rowid_group = ? AND " +
//          "type_null_tinyint = ? AND " + "type_not_null_tinyint = ? AND " +
//          "type_null_smallint = ? AND " + "type_not_null_smallint = ? AND " +
//          "type_null_integer = ? AND " + "type_not_null_integer = ? AND " +
//          "type_null_bigint = ? AND " + "type_not_null_bigint = ? AND " +
//          "type_null_timestamp = ? AND " + "type_not_null_timestamp = ? AND " +
//          "type_null_float = ? AND " + "type_not_null_float = ? AND " +
//          "type_null_decimal = ? AND " + "type_not_null_decimal = ? AND " +
//          "type_null_varchar25 = ?  AND " + "type_not_null_varchar25 = ? AND " +
//          "type_null_varchar128 = ? AND " + "type_not_null_varchar128 = ? AND " +
//          "type_null_varchar1024 = ? AND " + "type_not_null_varchar1024 = ?;"
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


        voltQueueSQL(selectMirrorRow, key, value);
        VoltTable[] mirrorResults= voltExecuteSQL();
        System.out.println("mirrorResults: " + mirrorResults.toString());
        VoltTable rowData = mirrorResults[0];
        System.out.println("rowData: " + rowData.toString() + ". rowData.getRowCount(): " + rowData.getRowCount());

        long deletedCount = 0;
        boolean rowCheckOk = true;
        if (rowData.getRowCount() == 1) {
        	// we already checked key and value via SELECT; now work through the rest of the types
        	// not_null rows are simple compares. nullable types need to check for null as well

            byte ntiVal = (byte) rowData.fetchRow(0).get(TYPE_NULL_TINYINT, VoltType.TINYINT);
            if (ntiVal == type_null_tinyint) {
                System.out.println("type_null_tinyint match");
            } else {
                System.out.println("Mirror type_null_tinyint (" + type_null_tinyint + ") " +
                    "not equal to import type_null_tinyint (" + ntiVal + ")");
                rowCheckOk = false;
            }

            byte tiVal = (byte) rowData.fetchRow(0).get(TYPE_NOT_NULL_TINYINT, VoltType.TINYINT);
            if (tiVal == type_not_null_tinyint) {
                System.out.println("type_not_null_tinyint match!");
            } else {
                System.out.println("Mirror type_not_null_tinyint (" + type_not_null_tinyint + ") " +
                    "not equal to import type_not_null_tinyint (" + tiVal + ")");
                rowCheckOk = false;
            }

            short nsiVal = (short) rowData.fetchRow(0).get(TYPE_NULL_SMALLINT, VoltType.SMALLINT);
            if (nsiVal == type_null_smallint)  {
                System.out.println("type_null_smallint match!");
            } else {
                System.out.println("Mirror type_null_smallint (" + type_null_smallint + ") " +
                    "not equal to import type_null_smallint (" + nsiVal + ")");
                rowCheckOk = false;
            }

            short siVal = (short) rowData.fetchRow(0).get(TYPE_NOT_NULL_SMALLINT, VoltType.SMALLINT);
            if (siVal == type_not_null_smallint ) {
                System.out.println("type_not_null_smallint match!");
            } else {
                System.out.println("Mirror type_not_null_smallint (" + type_not_null_smallint + ") " +
                    "not equal to import type_not_null_smallint (" + siVal + ")");
                rowCheckOk = false;
            }

            int nintVal = (int) rowData.fetchRow(0).get(TYPE_NULL_INTEGER, VoltType.INTEGER);
            if (nintVal == type_null_integer ) {
                System.out.println("type_null_integer match!!");
            } else {
                System.out.println("Mirror type_null_integer (" + type_null_integer + ") " +
                    "not equal to import type_null_integer (" + nintVal + ")");
                rowCheckOk = false;
            }

            int intVal = (int) rowData.fetchRow(0).get(TYPE_NOT_NULL_INTEGER, VoltType.INTEGER);
            if (intVal == type_not_null_integer ) {
                System.out.println("type_not_null_integer match!!");
            } else {
                System.out.println("Mirror type_not_null_integer (" + type_not_null_integer + ") " +
                    "not equal to import type_not_null_integer (" + intVal + ")");
                rowCheckOk = false;
            }

            long nbigVal = (long) rowData.fetchRow(0).get(TYPE_NULL_BIGINT, VoltType.BIGINT);
            if (nbigVal == type_null_bigint ) {
                System.out.println("type_null_bigint match!");
            } else {
                System.out.println("Mirror type_null_bigint (" + type_null_bigint + ") " +
                    "not equal to import type_null_bigint (" + nbigVal + ")");
                rowCheckOk = false;
            }

            long bigVal = (long) rowData.fetchRow(0).get(TYPE_NOT_NULL_BIGINT, VoltType.BIGINT);
            if (bigVal == type_not_null_bigint ) {
                System.out.println("type_not_null_bigint match!");
            } else {
                System.out.println("Mirror type_not_null_bigint (" + type_not_null_bigint + ") " +
                    "not equal to import type_not_null_bigint (" + bigVal + ")");
                rowCheckOk = false;
            }

            TimestampType ntsVal = (TimestampType) rowData.fetchRow(0).get(TYPE_NULL_TIMESTAMP, VoltType.TIMESTAMP);
            if (ntsVal.equals(type_null_timestamp) ||
            		(ntsVal.toString().equals("null") && type_null_timestamp.toString().equals("null"))) {
                System.out.println("type_null_timestamp match!!");
            } else {
                System.out.println("Mirror type_null_timestamp (" + type_null_timestamp + ") " +
                    "not equal to import type_null_timestamp (" + ntsVal + ")");
                rowCheckOk = false;
            }

            TimestampType tsVal = (TimestampType) rowData.fetchRow(0).get(TYPE_NOT_NULL_TIMESTAMP, VoltType.TIMESTAMP);
            if (tsVal.equals(type_not_null_timestamp)) {
                System.out.println("type_not_null_timestamp match!!");
            } else {
                System.out.println("Mirror type_not_null_timestamp (" + type_not_null_timestamp + ") " +
                    "not equal to import type_not_null_timestamp (" + tsVal + ")");
                rowCheckOk = false;
            }

            double nfloatVal = (double) rowData.fetchRow(0).get(TYPE_NULL_FLOAT, VoltType.FLOAT);
            if (nfloatVal == type_null_float) {
                System.out.println("type_null_float match!!");
            } else {
                System.out.println("Mirror type_null_float (" + type_null_float + ") " +
                    "not equal to import type_null_float (" + nfloatVal + ")");
                rowCheckOk = false;
            }

            double floatVal = (double) rowData.fetchRow(0).get(TYPE_NOT_NULL_FLOAT, VoltType.FLOAT);
            if (floatVal == type_not_null_float ) {
                System.out.println("type_not_null_float match!!");
            } else {
                System.out.println("Mirror type_not_null_float (" + type_not_null_float + ") " +
                    "not equal to import type_not_null_float (" + floatVal + ")");
                rowCheckOk = false;
            }

            BigDecimal ndecimalVal = (BigDecimal) rowData.fetchRow(0).get(TYPE_NULL_DECIMAL, VoltType.DECIMAL);
            if (ndecimalVal.equals(type_null_decimal) ||
            		(ndecimalVal.toString().equals("null") && type_null_decimal.toString().equals("null"))) {
                System.out.println("type_null_decimal match!!");
            } else {
                System.out.println("Mirror type_null_decimal (" + type_null_decimal + ") " +
                    "not equal to import type_null_decimal (" + ndecimalVal + ")");
                rowCheckOk = false;
            }

            BigDecimal decimalVal = (BigDecimal) rowData.fetchRow(0).get(TYPE_NOT_NULL_DECIMAL, VoltType.DECIMAL);
            if (decimalVal.equals(type_not_null_decimal)) {
                System.out.println("type_not_null_decimal match!!");
            } else {
                System.out.println("Mirror type_not_null_decimal (" + type_not_null_decimal + ") " +
                    "not equal to import type_not_null_decimal (" + decimalVal + ")");
                rowCheckOk = false;
            }

            String nstring25Val = (String) rowData.fetchRow(0).get(TYPE_NULL_VARCHAR25, VoltType.STRING);
            if (nstring25Val.equals(type_null_varchar25) ||
            		(nstring25Val.toString().equals("null") && type_null_varchar25.toString().equals("null"))) {
                System.out.println("type_null_varchar25 match!!");
            } else {
                System.out.println("Mirror type_null_varchar25 (" + type_null_varchar25 + ") " +
                    "not equal to import type_null_varchar25 (" + nstring25Val + ")");
                rowCheckOk = false;
            }

            String string25Val = (String) rowData.fetchRow(0).get(TYPE_NOT_NULL_VARCHAR25, VoltType.STRING);
            if (string25Val.equals(type_not_null_varchar25)) {
                System.out.println("type_not_null_varchar25 match!!");
            } else {
                System.out.println("Mirror type_not_null_varchar25 (" + type_not_null_varchar25 + ") " +
                    "not equal to import type_not_null_varchar25 (" + string25Val + ")");
                rowCheckOk = false;
            }

            String nstring128Val = (String) rowData.fetchRow(0).get(TYPE_NULL_VARCHAR128, VoltType.STRING);
            if (nstring128Val.equals(type_null_varchar128) ||
            		(nstring128Val.toString().equals("null") && type_null_varchar128.toString().equals("null"))) {
                System.out.println("type_null_varchar128 match!!");
            } else {
                System.out.println("Mirror type_null_varchar128 (" + type_null_varchar128 + ") " +
                    "not equal to import type_null_varchar128 (" + nstring128Val + ")");
                rowCheckOk = false;
            }

            String string128Val = (String) rowData.fetchRow(0).get(TYPE_NOT_NULL_VARCHAR128, VoltType.STRING);
            if (string128Val.equals(type_not_null_varchar128)) {
                System.out.println("type_not_null_varchar128 match!!");
            } else {
                System.out.println("Mirror type_not_null_varchar128 (" + type_not_null_varchar128 + ") " +
                    "not equal to import type_not_null_varchar128 (" + string128Val + ")");
                rowCheckOk = false;
            }

            String nstring1024Val = (String) rowData.fetchRow(0).get(TYPE_NULL_VARCHAR1024, VoltType.STRING);
            if (nstring1024Val.toString().equals(type_null_varchar1024) ||
            		(nstring1024Val.toString().equals("null") && type_null_varchar1024.toString().equals("null"))) {
                System.out.println("type_null_varchar1024 match!!");
            } else {
                System.out.println("Mirror type_null_varchar1024 (" + type_null_varchar1024 + ") " +
                    "not equal to import type_null_varchar1024 (" + nstring1024Val + ")");
                rowCheckOk = false;
            }

            String string1024Val = (String) rowData.fetchRow(0).get(TYPE_NOT_NULL_VARCHAR1024, VoltType.STRING);
            if (string1024Val.equals(type_not_null_varchar1024)) {
                System.out.println("type_not_null_varchar1024 match!!");
            } else {
                System.out.println("Mirror type_not_null_varchar1024 (" + type_not_null_varchar1024 + ") " +
                    "not equal to import type_not_null_varchar1024 (" + string1024Val + ")");
                rowCheckOk = false;
            }

            if (rowCheckOk) {  // delete the row
                voltQueueSQL(deleteMirrorRow, EXPECT_SCALAR_LONG, key, value);
                deletedCount = voltExecuteSQL()[0].asScalarLong();
            }


        	// System.out.println("ntiVal: " + ntiVal);
        	// System.out.println("tiVal: " + tiVal);
        	// System.out.println("nsiVal: " + nsiVal);
        	// System.out.println("siVal: " + siVal);
        	// System.out.println("nintVal: " + nintVal);
        	// System.out.println("intVal: " + intVal);
        	// System.out.println("nbigVal: " + nbigVal);
        	// System.out.println("bigVal: " + bigVal);
        	// System.out.println("ntsVal: " + ntsVal);
        	// System.out.println("tsVal: " + tsVal);
        	// System.out.println("nfloatVal: " + nfloatVal);
        	// System.out.println("floatVal: " + floatVal);
        	// System.out.println("ndecimalVal: " + ndecimalVal);
        	// System.out.println("decimalVal: " + decimalVal);
        	// System.out.println("nstring25Val: " + nstring25Val);
        	// System.out.println("string25Val: " + string25Val);
        	// System.out.println("nstring128Val: " + nstring128Val);
        	// System.out.println("string128Val: " + string128Val);
        	// System.out.println("nstring1024: " + nstring1024Val);
        	// System.out.println("string1024: " + string1024Val);

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
    	//voltQueueSQL(deleteMirrorRow, EXPECT_SCALAR_LONG,
        //        key, value, rowid_group,
         //       type_null_tinyint, type_not_null_tinyint, type_null_smallint,
          //      type_not_null_smallint, type_null_integer, type_not_null_integer,
           //     type_null_bigint, type_not_null_bigint, type_null_timestamp,
            //    type_not_null_timestamp, type_null_float, type_not_null_float,
             //   type_null_decimal, type_not_null_decimal, type_null_varchar25,
              //  type_not_null_varchar25, type_null_varchar128, type_not_null_varchar128,
               // type_null_varchar1024, type_not_null_varchar1024);
        //long deletedCount = voltExecuteSQL()[0].asScalarLong();

//      if (deletedCount == 0) {
//          voltQueueSQL(importInsert,
//              key, value, rowid_group,
//              type_null_tinyint, type_not_null_tinyint,
//              type_null_smallint, type_not_null_smallint,
//              type_null_integer, type_not_null_integer,
//              type_null_bigint, type_not_null_bigint,
//              type_null_timestamp, type_not_null_timestamp,
//              type_null_float, type_not_null_float,
//              type_null_decimal, type_not_null_decimal,
//              type_null_varchar25, type_not_null_varchar25,
//              type_null_varchar128, type_not_null_varchar128,
//              type_null_varchar1024, type_not_null_varchar1024);
//          voltExecuteSQL(true);

//            System.out.println("Delete 0 path. Values: " +
//                "key=" + key + "\n" + "value=" + value + "\n" + "rowid_group=" + rowid_group + "\n" +
//                "type_null_tinyint=" + type_null_tinyint + "\n" + "type_not_null_tinyint=" + type_not_null_tinyint + "\n" +
//                "type_null_smallint=" + type_null_smallint + "\n" + "type_not_null_smallint=" + type_not_null_smallint + "\n" +
//                "type_null_integer=" + type_null_integer + "\n" + "type_not_null_integer=" + type_not_null_integer + "\n" +
//                "type_null_bigint=" + type_null_bigint + "\n" + "type_not_null_bigint=" + type_not_null_bigint + "\n" +
//                "type_null_timestamp=" + type_null_timestamp + "\n" + "type_not_null_timestamp=" + type_not_null_timestamp + "\n" +
//                "type_null_float=" + type_null_float + "\n" + "type_not_null_float=" + type_not_null_float + "\n" +
//                "type_null_decimal=" + type_null_decimal + "\n" + "type_not_null_decimal=" + type_not_null_decimal + "\n" +
//                "type_null_varchar25="+type_null_varchar25 + "\n" + "type_not_null_varchar25="+type_not_null_varchar25 + "\n" +
//                "type_null_varchar128=" + type_null_varchar128 + "\n" + "type_not_null_varchar128=" + type_not_null_varchar128 + "\n" +
//                "type_null_varchar1024=" + type_null_varchar1024 + "\n" + "type_not_null_varchar1024=" + type_not_null_varchar1024);

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
}
