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

/* * add something useful here */

package genqa;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

public class RowCompare {

    // column positions, used in "get" calls below
    final static int TYPE_NULL_TINYINT = 4;
    final static int TYPE_NOT_NULL_TINYINT = 5;
    final static int TYPE_NULL_SMALLINT = 6;
    final static int TYPE_NOT_NULL_SMALLINT = 7;
    final static int TYPE_NULL_INTEGER = 8;
    final static int TYPE_NOT_NULL_INTEGER = 9;
    final static int TYPE_NULL_BIGINT = 10;
    final static int TYPE_NOT_NULL_BIGINT = 11;
    final static int TYPE_NULL_TIMESTAMP = 12;
    final static int TYPE_NOT_NULL_TIMESTAMP = 13;
    final static int TYPE_NULL_FLOAT = 14;
    final static int TYPE_NOT_NULL_FLOAT = 15;
    final static int TYPE_NULL_DECIMAL = 16;
    final static int TYPE_NOT_NULL_DECIMAL = 17;
    final static int TYPE_NULL_VARCHAR25 = 18;
    final static int TYPE_NOT_NULL_VARCHAR25 = 19;
    final static int TYPE_NULL_VARCHAR128 = 20;
    final static int TYPE_NOT_NULL_VARCHAR128 = 21;
    final static int TYPE_NULL_VARCHAR1024 = 22;
    final static int TYPE_NOT_NULL_VARCHAR1024 = 23;

    static int rowcompare(VoltTable t, ResultSet rs) throws SQLException {

        int colMisCount = 0;
        if (rs.next()) {
            // we already checked key and value via SELECT; now work through the rest of the types
            // not_null rows are simple compares. nullable types need to check for null as well

            byte ntiVal = (byte) t.get("TYPE_NULL_TINYINT", VoltType.TINYINT);
            // System.out.println("Volt TYPE_NULL_TINYINT: " + ntiVal);
            byte type_null_tinyint = rs.getByte("TYPE_NULL_TINYINT");
            if (rs.wasNull()) {
                type_null_tinyint = org.voltdb.VoltType.NULL_TINYINT;
            }
            // System.out.println("JDBC TYPE_NULL_TINYINT: " + type_null_tinyint);
            if (ntiVal != type_null_tinyint) {
                colMisCount +=  reportMismatch("type_null_tinyint", String.valueOf(type_null_tinyint), String.valueOf(ntiVal));
            }

            byte tiVal = (byte) t.get("TYPE_NOT_NULL_TINYINT", VoltType.TINYINT);
            byte type_not_null_tinyint = rs.getByte("TYPE_NOT_NULL_TINYINT");
            if (tiVal != type_not_null_tinyint) {
                colMisCount +=  reportMismatch("type_not_null_tinyint", String.valueOf(type_not_null_tinyint), String.valueOf(tiVal));
            }

            short nsiVal = (short) t.get("TYPE_NULL_SMALLINT", VoltType.SMALLINT);
            short type_null_smallint = rs.getShort("TYPE_NULL_SMALLINT");
            if (rs.wasNull()) {
                type_null_smallint = org.voltdb.VoltType.NULL_SMALLINT;
            }
            if (nsiVal != type_null_smallint)  {
                colMisCount +=  reportMismatch("type_null_smallint", String.valueOf(type_null_smallint), String.valueOf(nsiVal));
            }

            short siVal = (short) t.get("TYPE_NOT_NULL_SMALLINT", VoltType.SMALLINT);
            short type_not_null_smallint = rs.getShort("TYPE_NOT_NULL_SMALLINT");
            if (siVal != type_not_null_smallint ) {
                colMisCount +=  reportMismatch("type_not_null_smallint", String.valueOf(type_not_null_smallint), String.valueOf(siVal));
            }

            int nintVal = (int) t.get("TYPE_NULL_INTEGER", VoltType.INTEGER);
            int type_null_integer = rs.getInt("TYPE_NULL_INTEGER");
            if (rs.wasNull()) {
                type_null_integer = org.voltdb.VoltType.NULL_INTEGER;
            }
            if (nintVal != type_null_integer ) {
                colMisCount +=  reportMismatch("type_null_integer", String.valueOf(type_null_integer), String.valueOf(nintVal));
            }

            int intVal = (int) t.get("TYPE_NOT_NULL_INTEGER", VoltType.INTEGER);
            int type_not_null_integer = rs.getInt("TYPE_NOT_NULL_INTEGER");
            if (intVal != type_not_null_integer ) {
                colMisCount +=  reportMismatch("type_not_null_integer", String.valueOf(type_not_null_integer), String.valueOf(intVal));
            }

            long nbigVal = (long) t.get("TYPE_NULL_BIGINT", VoltType.BIGINT);
            long type_null_bigint = rs.getLong("TYPE_NULL_BIGINT");
            if (rs.wasNull()) {
                type_null_bigint = org.voltdb.VoltType.NULL_BIGINT;
            }
            if (nbigVal != type_null_bigint ) {
                colMisCount +=  reportMismatch("type_null_bigint", String.valueOf(type_null_bigint), String.valueOf(nbigVal));
            }

            long bigVal = (long) t.get("TYPE_NOT_NULL_BIGINT", VoltType.BIGINT);
            long type_not_null_bigint = rs.getLong("TYPE_NOT_NULL_BIGINT");
            if (bigVal != type_not_null_bigint ) {
                colMisCount +=  reportMismatch("type_not_null_bigint", String.valueOf(type_not_null_bigint), String.valueOf(bigVal));
            }

            // a direct string comparision of volt TimeStampType.toString()
            // and jdbc String type via ResultSet.getString()
            // isn't consistent, convert it to JDBC Timestamps and then compare.
            // ex: volt syntax: 1970-01-10 13:56:40.549-05
            // postgres syntax: 1970-01-10 13:56:40.549000

            TimestampType ntsVal = (TimestampType) t.get("TYPE_NULL_TIMESTAMP", VoltType.TIMESTAMP);
            if ( ntsVal != null ) {
                // compare it as string values
                Timestamp voltTS = ntsVal.asJavaTimestamp();
                Timestamp jdbcTS = rs.getTimestamp("TYPE_NULL_TIMESTAMP");

                if (! voltTS.toString().equals(jdbcTS.toString())) {
                    colMisCount +=  reportMismatch("type_null_timestamp strings", jdbcTS.toString(), voltTS.toString());
                }
                // compare it as microsecond time values
                if (jdbcTS.getTime() * 1000 != ntsVal.getTime()) {
                    colMisCount += reportMismatch("type_null_timestamp microseconds", String.valueOf(jdbcTS.getTime()*1000),
                            String.valueOf(ntsVal.getTime()));
                }

            } else {
                // if volt is null, jdbc should be null also
                Timestamp jdbcTS = rs.getTimestamp("TYPE_NULL_TIMESTAMP");
                if ( rs.getTimestamp("TYPE_NULL_TIMESTAMP") != null ) {
                    colMisCount += reportMismatch("type_null_timestamp is null", String.valueOf(jdbcTS),
                            String.valueOf(ntsVal));
                }
            }

            double nfloatVal = (double) t.get("TYPE_NULL_FLOAT", VoltType.FLOAT);
            double type_null_float = (double)rs.getFloat("TYPE_NULL_FLOAT");
            if (rs.wasNull()) {
                type_null_float = org.voltdb.VoltType.NULL_FLOAT;
            }
            if (Math.abs(nfloatVal - type_null_float) > 0.0001) {
                colMisCount +=  reportMismatch("type_null_float", String.valueOf(type_null_float), String.valueOf(nfloatVal));
            }

            double floatVal = (double) t.get("TYPE_NOT_NULL_FLOAT", VoltType.FLOAT);
            double type_not_null_float = (double)rs.getFloat("TYPE_NOT_NULL_FLOAT");
            if (Math.abs(floatVal - type_not_null_float) > 0.0001) {
                colMisCount +=  reportMismatch("type_not_null_float", String.valueOf(type_not_null_float), String.valueOf(floatVal));
            }

            BigDecimal ndecimalVal = (BigDecimal) t.get("TYPE_NULL_DECIMAL", VoltType.DECIMAL);
            BigDecimal type_null_decimal = rs.getBigDecimal("TYPE_NULL_DECIMAL");
            if (!(ndecimalVal == null && rs.wasNull()) && !ndecimalVal.equals(type_null_decimal)) {
                colMisCount +=  reportMismatch("type_null_decimal", type_null_decimal.toString(), ndecimalVal.toString());
            }

            BigDecimal decimalVal = (BigDecimal) t.get("TYPE_NOT_NULL_DECIMAL", VoltType.DECIMAL);
            BigDecimal type_not_null_decimal = rs.getBigDecimal("TYPE_NOT_NULL_DECIMAL");
            if (!decimalVal.equals(type_not_null_decimal)) {
                colMisCount +=  reportMismatch("type_not_null_decimal", type_not_null_decimal.toString(), decimalVal.toString());
            }

            String nstring25Val = (String) t.get("TYPE_NULL_VARCHAR25", VoltType.STRING);
            String type_null_varchar25 = rs.getString("TYPE_NULL_VARCHAR25");
            if (!(nstring25Val == null && rs.wasNull()) && !nstring25Val.equals(type_null_varchar25)) {
                colMisCount +=  reportMismatch("type_null_varchar25", type_null_varchar25, nstring25Val);
            }

            String string25Val = (String) t.get("TYPE_NOT_NULL_VARCHAR25", VoltType.STRING);
            String type_not_null_varchar25 = rs.getString("TYPE_NOT_NULL_VARCHAR25");
            if (!string25Val.equals(type_not_null_varchar25)) {
                colMisCount +=  reportMismatch("type_not_null_varchar25", type_not_null_varchar25, string25Val);
            }

            String nstring128Val = (String) t.get("TYPE_NULL_VARCHAR128", VoltType.STRING);
            String type_null_varchar128 = rs.getString("TYPE_NULL_VARCHAR128");
            if (!(nstring128Val == null && rs.wasNull()) && ! nstring128Val.equals(type_null_varchar128)) {
                colMisCount +=  reportMismatch("type_null_varchar128", type_null_varchar128, nstring128Val);
            }

            String string128Val = (String) t.get("TYPE_NOT_NULL_VARCHAR128", VoltType.STRING);
            String type_not_null_varchar128 = rs.getString("TYPE_NOT_NULL_VARCHAR128");
            if (!string128Val.equals(type_not_null_varchar128)) {
                colMisCount +=  reportMismatch("type_not_null_varchar128", type_not_null_varchar128, string128Val);
            }

            String nstring1024Val = (String) t.get("TYPE_NULL_VARCHAR1024", VoltType.STRING);
            String type_null_varchar1024 = rs.getString("TYPE_NULL_VARCHAR1024");
            if (!(nstring1024Val == null && rs.wasNull()) && !nstring1024Val.equals(type_null_varchar1024)) {
                colMisCount +=  reportMismatch("type_null_varchar1024", type_null_varchar1024, nstring1024Val);
            }

            String string1024Val = (String) t.get("TYPE_NOT_NULL_VARCHAR1024", VoltType.STRING);
            String type_not_null_varchar1024 = rs.getString("TYPE_NOT_NULL_VARCHAR1024");
            if (!string1024Val.equals(type_not_null_varchar1024)) {
                colMisCount += reportMismatch("type_not_null_varchar1024", type_not_null_varchar1024, string1024Val);
            }
        } else {
            System.out.println("In rowRowCompare:compare: no JDBC row available");
        }

        // handle the colMisCount in the calling function
        return colMisCount;
    }

    private static int reportMismatch(String typeName, String jdbcVal, String voltVal) {
        System.out.println("JDBC " + typeName + " not equal to Volt " + typeName + ":" + jdbcVal + " != " + voltVal);
        return 1;
    }
}
