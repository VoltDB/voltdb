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

    private final static byte m_byteVal = 1;
    private final static short m_shortVal = 2;
    private final static int m_intVal = 3;
    private final static long m_bigIntVal = 4;
    private final static Long m_longInst = new Long(123);
    private final static double m_doubleVal = 1.1;
    private final static Double m_doubleInst = new Double (1.11);
    private final static Float m_floatInst = new Float(5.01);
    private final static float m_floatVal = 5.55F;
    private final static BigDecimal m_bigDecVal = new BigDecimal(1.1111);
    private final static TimestampType m_tsVal = new TimestampType(1);
    private final static String m_strNum = "1";
    private final static String m_strTs = "2012-12-01";
    private final static byte[] m_binVal = {'1', '0'};
    private final static GeographyPointValue m_pt = new GeographyPointValue(0, 0);
    private final static GeographyValue m_poly = new GeographyValue("POLYGON((-102.052 41.002, -109.045 41.002, -109.045 36.333, -102.052 36.999, -102.052 41.002))");
    private final static VoltType[] m_tableColTypeVal = {VoltType.TINYINT,
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

    // TODO: IN LISTS are not supported with these column types -- these statements should not even compile.
    final static SQLStmt m_fltInList = new SQLStmt("Select * from T where flt IN ? ;");
    final static SQLStmt m_bigDecInList = new SQLStmt("Select * from T where dec IN ? ;");

    private final static SQLStmt m_tsInList = new SQLStmt("Select * from T where ts IN ? ;");
    private final static SQLStmt m_strInList = new SQLStmt("Select * from T where str IN ? ;");

    // TODO: IN LISTS are not supported with these column types -- these statements should not even compile.
    final static SQLStmt m_binInList = new SQLStmt("Select * from T where bin IN ? ;");
    final static SQLStmt m_ptInList = new SQLStmt("Select * from T where pt IN ? ;");
    final static SQLStmt m_polyInList = new SQLStmt("Select * from T where pol IN ? ;");

    private final static SQLStmt m_tinyCompare = new SQLStmt("Select * from T where ti = ? ;");
    private final static SQLStmt m_smallCompare = new SQLStmt("Select * from T where si = ? ;");
    private final static SQLStmt m_intCompare = new SQLStmt("Select * from T where int = ? ;");
    private final static SQLStmt m_bigIntCompare = new SQLStmt("Select * from T where bi = ? ;");
    private final static SQLStmt m_fltCompare = new SQLStmt("Select * from T where flt = ? ;");
    private final static SQLStmt m_bigDecCompare= new SQLStmt("Select * from T where dec = ? ;");
    private final static SQLStmt m_tsCompare = new SQLStmt("Select * from T where ts = ? ;");
    private final static SQLStmt m_strCompare = new SQLStmt("Select * from T where str = ? ;");
    private final static SQLStmt m_binCompare = new SQLStmt("Select * from T where bin = ? ;");
    private final static SQLStmt m_ptCompare = new SQLStmt("Select * from T where pt = ? ;");
    private final static SQLStmt m_polyCompare = new SQLStmt("Select * from T where pol = ? ;");

    // flag to test positive cases of type conversion using inserts and selects with comparison statements
    public static final byte TestAllAllowedTypeConv = 0;
    // flag to test type-conversion by using insert statements in automated fashion using type conversion
    // specified by user. Used to test cases at voltqueuesql level whether type conversion is feasible
    // or not.
    public static final byte TestTypeConvWithInsertProc = 1;
    // flag to test type-conversion of array parameters by using select statement with IN predicate in
    // automated fashion using type conversion specified by user. Used to test cases at voltqueuesql
    // level whether type conversion is feasible or not.
    public static final byte TestTypesInList = 2;
    public static final byte TestFailingTypesInList = 3;
    public static final byte TestFailingArrayTypeCompare = 4;

    private Object[] getUpdatedRowToInsert(int colIndex, byte valueTypeToPopulateWith, boolean strTs) {
        Object[] rowToInsert = {m_byteVal, m_shortVal, m_intVal, m_bigIntVal, m_doubleVal, m_bigDecVal, m_tsVal, m_strTs, m_binVal, m_pt, m_poly};
        assert(colIndex < rowToInsert.length);
        rowToInsert[colIndex] = getColumnValue(valueTypeToPopulateWith, strTs);
        return rowToInsert;
    }

    private Object getColumnValue(byte valueTypeToPopulateWith, boolean strTs) {
        VoltType toType = VoltType.get(valueTypeToPopulateWith);

        switch (toType) {
            case TINYINT:
                return m_byteVal;
            case SMALLINT:
                return m_shortVal;
            case INTEGER:
                return m_intVal;
            case BIGINT:
                return m_bigIntVal;
            case FLOAT:
                return m_doubleVal;
            case DECIMAL:
                return m_bigDecVal;
            case TIMESTAMP:
                return m_tsVal;
            case STRING:
                return strTs ? m_strTs : m_strNum;
            case VARBINARY:
                return m_binVal;
            case GEOGRAPHY_POINT:
                return m_pt;
            case GEOGRAPHY:
                return m_poly;
            default:
                assert(false);
                break;
        }
        return null;
    }

    private Object getInListParameter(byte valueTypeToPopulateWith, boolean strTs) {
        VoltType toType = VoltType.get(valueTypeToPopulateWith);

        switch (toType) {
            case TINYINT:
                byte[] tinyValue = {m_byteVal, m_byteVal};
                return tinyValue;
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
                                 m_tsVal, m_strTs, m_binVal, m_pt, m_poly);

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
            Object value = getInListParameter(valueOfTypeToPopulateWith, useStrTs);
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
            case DECIMAL:
                // IN LISTs of these types are not supported.
                return null;
            case TIMESTAMP:
                voltQueueSQL(m_tsInList, value);
                break;
            case STRING:
                voltQueueSQL(m_strInList, value);
                break;
            case VARBINARY:
            case GEOGRAPHY_POINT:
            case GEOGRAPHY:
                // IN LISTs of these types are not supported.
                return null;
            default:
                assert(false);
            }
            break;

    case TestFailingTypesInList:
        // if column to test is of timestamp (ts) type, use ts string for string value
        // which can be used for inserting into ts column
        Object failValue = getColumnValue(valueOfTypeToPopulateWith, useStrTs);
        switch (m_tableColTypeVal[indOfTargetType]) {
        case TINYINT:
            voltQueueSQL(m_tinyInList, failValue);
            break;
        case SMALLINT:
            voltQueueSQL(m_smallInList, failValue);
            break;
        case INTEGER:
            voltQueueSQL(m_intInList, failValue);
            break;
        case BIGINT:
            voltQueueSQL(m_bigIntInList, failValue);
            break;
        case FLOAT:
        case DECIMAL:
            // IN LISTs of these types are not supported.
            return null;
        case TIMESTAMP:
            voltQueueSQL(m_tsInList, failValue);
            break;
        case STRING:
            voltQueueSQL(m_strInList, failValue);
            break;
        case VARBINARY:
        case GEOGRAPHY_POINT:
        case GEOGRAPHY:
            // IN LISTs of these types are not supported.
            return null;
        default:
            assert(false);
        }
        break;

    case TestFailingArrayTypeCompare:
        // if column to test is of timestamp (ts) type, use ts string for string value
        // which can be used for inserting into ts column
        Object arrayValue = getInListParameter(valueOfTypeToPopulateWith, useStrTs);
        switch (m_tableColTypeVal[indOfTargetType]) {
        case TINYINT:
            voltQueueSQL(m_tinyCompare, arrayValue);
            break;
        case SMALLINT:
            voltQueueSQL(m_smallCompare, arrayValue);
            break;
        case INTEGER:
            voltQueueSQL(m_intCompare, arrayValue);
            break;
        case BIGINT:
            voltQueueSQL(m_bigIntCompare, arrayValue);
            break;
        case FLOAT:
            voltQueueSQL(m_fltCompare, arrayValue);
            break;
        case DECIMAL:
            voltQueueSQL(m_bigDecCompare, arrayValue);
            break;
        case TIMESTAMP:
            voltQueueSQL(m_tsCompare, arrayValue);
            break;
        case STRING:
            voltQueueSQL(m_strCompare, arrayValue);
            break;
        case VARBINARY:
            voltQueueSQL(m_binCompare, arrayValue);
            break;
        case GEOGRAPHY_POINT:
            voltQueueSQL(m_ptCompare, arrayValue);
            break;
        case GEOGRAPHY:
            voltQueueSQL(m_polyCompare, arrayValue);
            break;
        default:
            assert(false);
        }
        break;
        }

        return voltExecuteSQL();
    }
}
