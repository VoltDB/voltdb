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

import java.math.BigDecimal;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;


public class ProcToTestTypeConversion extends VoltProcedure {

    private final byte m_byteVal = 1;
    private final short m_shortVal = 2;
    private final int m_intVal = 3;
    private final long m_bigIntVal = 4;
    private final Long m_longInst = new Long(123);
    private final double m_doubleVal = 1.1;
    private final Double m_doubleInst = new Double (1.11);
    private final Float m_floatInst = new Float(5.01);
    private final float m_floatVal = 5.55F;
    private final BigDecimal m_bigDecVal = new BigDecimal(1.1111);
    private final TimestampType m_tsVal = new TimestampType(1);
    private final String m_strNum = "1";
    private final String m_strTs = "2012-12-01";
    private final byte[] m_binVal = {'1', '0'};
    private final static GeographyPointValue m_pt = new GeographyPointValue(0, 0);
    private final static GeographyValue m_poly = new GeographyValue("POLYGON((-102.052 41.002, -109.045 41.002, -109.045 36.333, -102.052 36.999, -102.052 41.002))");
    private final static int m_colIndOfTimeStamp = 6;
    private final static VoltType[] m_tableColTypeVal = {
            VoltType.TINYINT,
            VoltType.SMALLINT,
            VoltType.INTEGER,
            VoltType.BIGINT,
            VoltType.FLOAT,
            VoltType.DECIMAL,
            VoltType.TIMESTAMP,
            VoltType.STRING,
            VoltType.VARBINARY,
            VoltType.GEOGRAPHY_POINT,
            VoltType.GEOGRAPHY
    };

    private final static SQLStmt insert = new SQLStmt ("Insert into T "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public final static SQLStmt compare = new SQLStmt ("Select * from T "
                                                      + " where T.ti = ? OR "
                                                      + " T.si = ? OR "
                                                      + " T.int = ? OR "
                                                      + " T.bi = ? OR "
                                                      + " T.flt = ? OR "
                                                      + " T.dec = ? OR "
                                                      + " T.ts = ? OR "
                                                      + " T.str = ? OR "
                                                      + " T.bin = ? OR "
                                                      + " T.pt = ? OR "
                                                      + " T.pol = ? "
                                                      + " order by int;");

    public final static SQLStmt compareInList = new SQLStmt ("Select * from T where "
                                                    + " T.ti IN ? OR "
                                                    + " T.si IN ? OR "
                                                    + " T.int IN ? OR "
                                                    + " T.bi IN ? OR "
                                                    + " T.flt IN ? OR "
                                                    + " T.dec IN ? OR "
                                                    + " T.ts IN ? OR "
                                                    + " T.str IN ? OR "
                                                    + " T.bin IN ? OR "
                                                    + " T.pt IN ? OR "
                                                    + " T.pol IN ? "
                                                    + " order by int;");

    // sql statements to test conversion for array of parameters
    private final static SQLStmt m_tinyInList = new SQLStmt("Select * from T where ti IN ? ;");
    private final static SQLStmt m_smallInList = new SQLStmt("Select * from T where si IN ? ;");
    private final static SQLStmt m_intInList = new SQLStmt("Select * from T where int IN ? ;");
    private final static SQLStmt m_bigIntInList = new SQLStmt("Select * from T where bi IN ? ;");
    private final static SQLStmt m_fltInList = new SQLStmt("Select * from T where flt IN ? ;");
    private final static SQLStmt m_bigDecInList = new SQLStmt("Select * from T where dec IN ? ;");
    private final static SQLStmt m_tsInList = new SQLStmt("Select * from T where ts IN ? ;");
    private final static SQLStmt m_strInList = new SQLStmt("Select * from T where str IN ? ;");
    private final static SQLStmt m_binInList = new SQLStmt("Select * from T where bin IN ? ;");
    private final static SQLStmt m_ptInList = new SQLStmt("Select * from T where pt IN ? ;");
    private final static SQLStmt m_polyInList = new SQLStmt("Select * from T where pol IN ? ;");

    // flag to test positive cases of type conversion using inserts and selects with comparison statements
    public final static byte TestAllAllowedTypeConv = 0;
    // flag to test type-conversion by using insert statements in automated fashion using type conversion
    // specified by user. Used to test cases at voltqueuesql level whether type conversion is feasible
    // or not.
    public final static byte TestTypeConvWithInsertProc = 1;
    // flag to test type-conversion of array parameters by using select statement with IN predicate in
    // automated fashion using type conversion specified by user. Used to test cases at voltqueuesql
    // level whether type conversion is feasible or not.
    public final static byte TestTypesInList = 2;

    private Object[] getUpdatedRowToInsert(int colIndex, byte valueTypeToPopulateWith, boolean strTs) {
        Object[] rowToInsert = {m_byteVal, m_shortVal, m_intVal, m_bigIntVal, m_doubleVal, m_bigDecVal, m_tsVal, m_strTs, m_binVal, m_pt, m_poly};
        assert(colIndex < rowToInsert.length);
        VoltType toType = VoltType.get(valueTypeToPopulateWith);

        switch (toType) {
            case TINYINT:
                rowToInsert[colIndex] = m_byteVal;
                break;
            case SMALLINT:
                rowToInsert[colIndex] = m_shortVal;
                break;
            case INTEGER:
                rowToInsert[colIndex] = m_intVal;
                break;
            case BIGINT:
                rowToInsert[colIndex] = m_bigIntVal;
                break;
            case FLOAT:
                rowToInsert[colIndex] = m_doubleVal;
                break;
            case DECIMAL:
                rowToInsert[colIndex] = m_bigDecVal;
                break;
            case TIMESTAMP:
                rowToInsert[colIndex] = m_tsVal;
                break;
            case STRING:
                rowToInsert[colIndex] = strTs ? m_strTs : m_strNum;
                break;
            case VARBINARY:
                rowToInsert[colIndex] = m_binVal;
                break;
            case GEOGRAPHY_POINT:
                rowToInsert[colIndex] = m_pt;
                break;
            case GEOGRAPHY:
                rowToInsert[colIndex] = m_poly;
                break;
            default:
                assert(false);
                break;
        }
        return rowToInsert;
    }

    private Object getUpdatedRow4InList(byte valueTypeToPopulateWith, boolean strTs) {
        VoltType toType = VoltType.get(valueTypeToPopulateWith);

        switch (toType) {
            case TINYINT:
                byte[] tiValue = {m_byteVal, m_byteVal};
                return tiValue;
            case SMALLINT:
                short[] shortValue = {m_shortVal, m_shortVal};
                return shortValue;
            case INTEGER:
                int[] intValue = {m_intVal, m_intVal};
                return intValue;
            case BIGINT:
                long[] bigintValue = {m_bigIntVal, m_bigIntVal};
                return bigintValue;
            case FLOAT:
                double[] floatValue = {m_floatVal, m_floatVal};
                return floatValue;
            case DECIMAL:
                BigDecimal[] bigdecValue = {m_bigDecVal, m_bigDecVal};
                return bigdecValue;
            case TIMESTAMP:
                TimestampType[] tsValue = {m_tsVal, m_tsVal};
                return tsValue;
            case STRING:
                String str = strTs ? m_strTs : m_strNum;
                String[] strValue = {str, str};
                return strValue;
            case VARBINARY:
                byte[][] binValue = {{m_byteVal, m_byteVal}, {m_byteVal, m_byteVal}};
                return binValue;
            case GEOGRAPHY_POINT:
                GeographyPointValue[] ptValue = {m_pt, m_pt};
                return ptValue;
            case GEOGRAPHY:
                GeographyValue[] polyValue = {m_poly, m_poly};
                return polyValue;
            default:
                assert(false);
                break;
        }
        return null;
    }

    public VoltTable[] run (int operation, int indOfTargetType, byte valueOfTypeToPopulateWith) {
        assert(m_tableColTypeVal.length > indOfTargetType);
        Object rowToInsert[];
        boolean useStrTs = (m_tableColTypeVal[indOfTargetType] == VoltType.TIMESTAMP) ? true : false;
        switch(operation) {
        case TestAllAllowedTypeConv:

            // queues insert and select statements for which type conversion is allowed
            voltQueueSQL(insert, null, null, null, m_bigIntVal, null, null,
                                 null, null, null, null, null, null);

            // TinyInt
            voltQueueSQL(insert, m_byteVal, m_byteVal, m_byteVal, m_byteVal, m_byteVal, m_byteVal,
                                 m_byteVal, m_byteVal, m_binVal, m_pt, m_poly);
            // ShortInt
            voltQueueSQL(insert, m_shortVal, m_shortVal, m_shortVal, m_shortVal, m_shortVal, m_shortVal,
                                 m_shortVal, m_shortVal, m_binVal, m_pt, m_poly);

            // Integer
            voltQueueSQL(insert, m_intVal, m_intVal, m_intVal, m_intVal, m_intVal, m_intVal,
                                 m_intVal, m_intVal, m_binVal, m_pt, m_poly);

            // BigInt
            voltQueueSQL(insert, m_bigIntVal, m_bigIntVal, m_bigIntVal, m_bigIntVal, m_bigIntVal, m_bigIntVal,
                                 m_bigIntVal, m_bigIntVal, m_binVal, m_pt, m_poly);
            voltQueueSQL(insert, m_longInst, m_longInst, m_longInst, m_longInst, m_longInst, m_longInst,
                                 m_longInst, m_longInst, m_binVal, m_pt, m_poly);

            // Float
            voltQueueSQL(insert, m_doubleVal, m_doubleVal, m_doubleVal, m_doubleVal, m_doubleVal, m_doubleVal,
                                 m_doubleVal, m_doubleVal, m_binVal, m_pt, m_poly);
            voltQueueSQL(insert, m_doubleInst, m_doubleInst, m_doubleInst, m_doubleInst, m_doubleInst, m_doubleInst,
                                 m_doubleInst, m_doubleInst, m_binVal, m_pt, m_poly);
            voltQueueSQL(insert, m_floatVal, m_floatVal, m_floatVal, m_floatVal, m_floatVal, m_floatVal,
                                 m_floatVal, m_floatVal, m_binVal, m_pt, m_poly);
            voltQueueSQL(insert, m_floatInst, m_floatInst, m_floatInst, m_floatInst, m_floatInst, m_floatInst,
                                 m_floatInst, m_floatInst, m_binVal, m_pt, m_poly);

            // Decimal
            voltQueueSQL(insert, m_bigDecVal, m_bigDecVal, m_bigDecVal, m_bigDecVal, m_bigDecVal, m_bigDecVal,
                                 m_bigDecVal, m_bigDecVal, m_binVal, m_pt, m_poly);

            // TimeStamp
            voltQueueSQL(insert, m_tsVal, m_tsVal, m_tsVal, m_tsVal, m_tsVal, m_bigDecVal,
                                 m_tsVal, m_tsVal, m_binVal, m_pt, m_poly);
            // String/Varchar
            voltQueueSQL(insert, m_strNum, m_strNum, m_strNum, m_strNum, m_strNum, m_strNum,
                                 m_strTs, m_strTs, m_binVal, m_pt, m_poly);

            // Varbinary
            voltQueueSQL(insert, m_byteVal, m_shortVal, m_intVal, m_floatVal, m_bigIntVal, m_bigDecVal,
                                 m_tsVal, m_binVal, m_binVal, m_pt, m_poly);

            // Compare stored proc. comparison expression supports broadest type conversion for select statement
            voltQueueSQL(compare, null, null, null, m_bigIntVal, null, null,
                                  null, null, null, null, null, null);

            voltQueueSQL(compare, m_byteVal, m_shortVal, m_intVal, m_floatVal, m_bigIntVal, m_bigDecVal,
                                  m_tsVal, m_strTs, m_binVal, m_pt, m_poly);

            // TinyInt
            voltQueueSQL(compare, m_byteVal, m_byteVal, m_byteVal, m_byteVal, m_byteVal, m_byteVal,
                                 m_byteVal, m_strTs, m_binVal, m_pt, m_poly);
            // ShortInt
            voltQueueSQL(compare, m_shortVal, m_shortVal, m_shortVal, m_shortVal, m_shortVal, m_shortVal,
                                  m_shortVal, m_strTs, m_binVal, m_pt, m_poly);

            // Integer
            voltQueueSQL(compare, m_intVal, m_intVal, m_intVal, m_intVal, m_intVal, m_intVal,
                                  m_intVal, m_strTs, m_binVal, m_pt, m_poly);

            // BigInt
            voltQueueSQL(compare, m_bigIntVal, m_bigIntVal, m_bigIntVal, m_bigIntVal, m_bigIntVal, m_bigIntVal,
                                  m_bigIntVal, m_strTs, m_binVal, m_pt, m_poly);
            voltQueueSQL(compare, m_longInst, m_longInst, m_longInst, m_longInst, m_longInst, m_longInst,
                                  m_longInst, m_strTs, m_binVal, m_pt, m_poly);

            // Float
            voltQueueSQL(compare, m_doubleVal, m_doubleVal, m_doubleVal, m_doubleVal, m_doubleVal, m_doubleVal,
                                  m_doubleVal, m_strTs, m_binVal, m_pt, m_poly);
            voltQueueSQL(compare, m_doubleInst, m_doubleInst, m_doubleInst, m_doubleInst, m_doubleInst, m_doubleInst,
                                 m_doubleInst, m_strTs, m_binVal, m_pt, m_poly);
            voltQueueSQL(compare, m_floatVal, m_floatVal, m_floatVal, m_floatVal, m_floatVal, m_floatVal,
                                  m_floatVal, m_strTs, m_binVal, m_pt, m_poly);
            voltQueueSQL(compare, m_floatInst, m_floatInst, m_floatInst, m_floatInst, m_floatInst, m_floatInst,
                                  m_floatInst, m_strTs, m_binVal, m_pt, m_poly);

            // Decimal
            voltQueueSQL(compare, m_bigDecVal, m_bigDecVal, m_bigDecVal, m_bigDecVal, m_bigDecVal, m_bigDecVal,
                                  m_bigDecVal, m_strTs, m_binVal, m_pt, m_poly);

            // TimeStamp
            voltQueueSQL(compare, m_tsVal, m_tsVal, m_tsVal, m_tsVal, m_tsVal, m_tsVal,
                                  m_tsVal, m_strTs, m_binVal, m_pt, m_poly);

            break;
        case TestTypeConvWithInsertProc:

            // if column to test is of timestamp (ts) type, use ts string for string value
            // which can be used for inserting into ts column
            rowToInsert = getUpdatedRowToInsert(indOfTargetType, valueOfTypeToPopulateWith, useStrTs);
            voltQueueSQL(insert, rowToInsert);
            break;

        case TestTypesInList:
            // if column to test is of timestamp (ts) type, use ts string for string value
            // which can be used for inserting into ts column
            Object value = getUpdatedRow4InList(valueOfTypeToPopulateWith, useStrTs);
            switch (m_tableColTypeVal[indOfTargetType]) {
            case TINYINT:
                voltQueueSQL(m_tinyInList, value);
                break;
            case SMALLINT:
                voltQueueSQL(m_smallInList, value);
                break;
            case INTEGER:
                voltQueueSQL(m_intInList, value);
                break;
            case BIGINT:
                voltQueueSQL(m_bigIntInList, value);
                break;
            case FLOAT:
                voltQueueSQL(m_fltInList, value);
                break;
            case DECIMAL:
                voltQueueSQL(m_bigDecInList, value);
                break;
            case TIMESTAMP:
                voltQueueSQL(m_tsInList, value);
                break;
            case STRING:
                voltQueueSQL(m_strInList, value);
                break;
            case VARBINARY:
                voltQueueSQL(m_binInList, value);
                break;
            case GEOGRAPHY_POINT:
                voltQueueSQL(m_ptInList, value);
                break;
            case GEOGRAPHY:
                voltQueueSQL(m_polyInList, value);
                break;
            default:
                assert(false);
            }
            break;
        }
        return voltExecuteSQL();
    }
}
