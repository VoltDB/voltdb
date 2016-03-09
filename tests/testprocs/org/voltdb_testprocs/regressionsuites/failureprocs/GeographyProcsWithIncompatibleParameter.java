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
import org.voltdb.types.GeographyValue;

public class GeographyProcsWithIncompatibleParameter extends VoltProcedure {

    // Form an enum entry for different geo functions that take in GeographyValue as argument. This enum
    // entry stores sql statements that parameterize geography value and legal unqiue polygon wkt string
    // which is used as input parameter for the given stored procedure to voltQueueSql(). Logic is crafted
    // verify the wkt to Geography value conversion does not happen implicitly and results in EE exception.
    public final static SQLStmt containsGeo = new SQLStmt("select places.name from places "
                + "  where Contains(?, places.loc) order by places.pk;");

    public final static SQLStmt polygonsEqual= new SQLStmt("select borders.name from borders where borders.region = ? "
                 + "  order by borders.pk;");

    public final static SQLStmt polygonsLessThn = new SQLStmt("select borders.name from borders where borders.region < ? "
                + "  order by borders.pk;");

    public final static SQLStmt polygonsGreaterThn = new SQLStmt("select borders.name from borders where borders.region > ? "
                + "  order by borders.pk;");

    // create dummy statements
    public final static SQLStmt polygonsValid= new SQLStmt("select borders.name from borders where isValid(?) and borders.pk = 0"
                + "  order by borders.pk;");

    public final static SQLStmt polygonsArea = new SQLStmt("select borders.name from borders where area(?) < area(borders.region) "
            + "  order by borders.pk;");

    public static enum TestEntries {
        CompareEquals(polygonsEqual,
                     "POLYGON((-102.052 41.002, -109.045 41.002, -109.045 36.333, -102.052 36.999, -102.052 41.002))",
                     "can not convert type 'STRING' to 'GEOGRAPHY' for arg 0 for SQL stmt"),
        CompareLessThn(polygonsLessThn,
                      "POLYGON((-102.052 41.002, -109.045 41.002, -109.045 36.444, -102.052 36.999, -102.052 41.002))",
                      "can not convert type 'STRING' to 'GEOGRAPHY' for arg 0 for SQL stmt"),
        CompareGreaterThn(polygonsGreaterThn,
                         "POLYGON((-102.052 41.002, -109.045 41.002, -109.045 36.555, -102.052 36.999, -102.052 41.002))",
                          "can not convert type 'STRING' to 'GEOGRAPHY' for arg 0 for SQL stmt"),
        Contains(containsGeo,
                "POLYGON((-102.052 41.002, -109.045 41.002, -109.045 36.666, -102.052 36.999, -102.052 41.002))",
                "can not convert type 'STRING' to 'GEOGRAPHY' for arg 0 for SQL stmt"),
        Area(polygonsArea,
            "POLYGON((-102.052 41.002, -109.045 41.002, -109.045 36.667, -102.045 36.999, -102.052 41.002))",
            "can not convert type 'STRING' to 'GEOGRAPHY' for arg 0 for SQL stmt"),
        IsValid(polygonsValid,
               "POLYGON((-102.052 41.002, -109.045 41.002, -109.045 36.777, -102.052 36.999, -102.052 41.002))",
               "can not convert type 'STRING' to 'GEOGRAPHY' for arg 0 for SQL stmt");

        private final SQLStmt stmt;
        private final String paramWkt;
        private final String failureMsg;
        private final GeographyValue geo;

        TestEntries(SQLStmt stmt, String wkt, String msg) {
            //this.id = id;
            this.stmt = stmt;
            paramWkt = wkt;
            failureMsg = msg;
            geo = new GeographyValue(wkt);
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

    public VoltTable[] run(GeographyValue value) {
        for (TestEntries entry : TestEntries.values()) {
            if (entry.geo.equals(value)) {
                voltQueueSQL(entry.stmt, entry.paramWkt);
            }
        }
        return voltExecuteSQL();
    }
}
