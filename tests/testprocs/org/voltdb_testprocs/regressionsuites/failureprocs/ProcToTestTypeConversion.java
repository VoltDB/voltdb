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
import org.voltdb.utils.VoltTypeUtil;


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
    private int colIndOfTimeStamp = 6;

    public final static SQLStmt compare = new SQLStmt ("Select * from T "
                                                      + " where T.col = ? OR "
                                                      + " T.dummy0 = ? OR "
                                                      + " T.dummy1 = ? OR "
                                                      + " T.dummy2 = ? OR "
                                                      + " T.dummy3 = ? OR "
                                                      + " T.dummy4 = ? OR "
                                                      + " T.dummy5 = ? OR "
                                                      + " T.dummy6 = ? OR "
                                                      + " T.dummy7 = ? OR "
                                                      + " T.dummy8 = ? OR "
                                                      + " T.dummy9 = ? "
                                                      + " order by dummy1;");

    public final static SQLStmt insert = new SQLStmt ("Insert into T "
                                            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public final static byte TestHCTypeConvAllowed = 0;
    public final static byte TestTypeConvWithInsertProc = 1;

    private Object[] getUpdatedRowToInsert(int colIndex, byte valueTypeToPopulateWith, boolean strTs) {
        //Object[] rowToInsert = m_defaultRowValue;
        // this should match with the numbers of columns and values with it's type of table T
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
                rowToInsert[colIndex] = VoltTypeUtil.getRandomValue(toType);
                break;
        }

        return rowToInsert;
    }

    public VoltTable[] run (int operation, int colIndOfTargetType, byte valueOfTypeToPopulateWith) {

        Object rowToInsert[];
        boolean useStrTs;
        switch(operation) {
        case TestHCTypeConvAllowed:

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
            useStrTs = (colIndOfTargetType == colIndOfTimeStamp) ? true : false;
            rowToInsert = getUpdatedRowToInsert(colIndOfTargetType, valueOfTypeToPopulateWith, useStrTs);
            voltQueueSQL(insert, rowToInsert);
            break;
        }
        return voltExecuteSQL();
    }

}
