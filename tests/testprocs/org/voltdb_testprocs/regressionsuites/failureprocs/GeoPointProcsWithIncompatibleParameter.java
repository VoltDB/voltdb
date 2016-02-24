/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb_testprocs.regressionsuites.failureprocs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.GeographyPointValue;

public class GeoPointProcsWithIncompatibleParameter extends VoltProcedure {
    // Form an enum entry for different geo functions that take in GeographyPointValue as argument. This enum
    // entry stores sql statements that parameterize geography point value and legal unqiue point wkt string 
    // which is used as input parameter for the given stored procedure to voltQueueSql(). Logic is crafted verify 
    // the wkt to Geography Point value conversion does not happen implicitly and results in EE exception.
    public final static SQLStmt containsGeo = new SQLStmt("select borders.name from borders "
                + "  where Contains(borders.region, ?) order by borders.pk;");

    public final static SQLStmt pointsEqual= new SQLStmt("select places.name from places where places.loc = ? "
                + "  order by places.pk;");

    public final static SQLStmt pointsLessThn = new SQLStmt("select places.name from places where places.loc < ? "
                + "  order by places.pk;");

    public final static SQLStmt pointsGreaterThn = new SQLStmt("select places.name from places where places.loc > ? "
                + "  order by places.pk;");

    public final static SQLStmt pointLongitude = new SQLStmt("select places.name from places where longitude(?) < longitude(places.loc) "
                + "  order by places.pk;");


    public static enum TestEntries {
        CompareEquals(pointsEqual,
                     "POINT(-104.959 39.704)",
                     "Type VARCHAR cannot be cast for comparison to type POINT"),
        CompareLessThn(pointsLessThn,
                      "POINT(-104.959 39.705)",
                      "Type VARCHAR cannot be cast for comparison to type POINT"),
        CompareGreaterThn(pointsGreaterThn,
                         "POINT(-104.959 39.714)",
                          "Type VARCHAR cannot be cast for comparison to type POINT"),
        Contains(containsGeo,
                "POINT(-104.959 39.712)",
                "Type VARCHAR can't be cast as POINT"),

        Latitude(pointLongitude ,
                "POINT(-104.959 39.732)",
                "Type VARCHAR can't be cast as POINT");

        private final SQLStmt stmt;
        private final String paramWkt;
        private final String failureMsg;
        private final GeographyPointValue point;

        TestEntries(SQLStmt stmt, String wkt, String msg) {
            //this.id = id;
            this.stmt = stmt;
            paramWkt = wkt;
            failureMsg = msg;
            point = GeographyPointValue.fromWKT(wkt);
        }

        public String getFailureMsg() {
            return failureMsg;
        }

        public SQLStmt getStmt() {
            return stmt;
        }

        public String getParam() {
            return paramWkt;
        }

    };

    public VoltTable[] run(GeographyPointValue value) {

        for (TestEntries entry : TestEntries.values()) {
            if (entry.point.equals(value)) {
                voltQueueSQL(entry.stmt, entry.paramWkt);
            }
        }
        return voltExecuteSQL();
    }
}
