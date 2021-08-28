/* This file is part of VoltDB.
 * Copyright (C) 2021 VoltDB Inc.
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

package procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;

import java.math.BigDecimal;


public class InsertOneRow extends VoltProcedure {

    // We don't want to have to change these in more than one place (besides the DDL file)
    final static String COLUMN_NAMES_NO_ID = "MOD_ID, TINY, SMALL, INTEG, BIG, FLOT, DECML, TIMESTMP,"
            + " VCHAR_INLINE,  VCHAR_INLINE_MAX,  VCHAR_OUTLINE_MIN,  VCHAR_OUTLINE,  VCHAR_DEFAULT,"
            + "VARBIN_INLINE, VARBIN_INLINE_MAX, VARBIN_OUTLINE_MIN, VARBIN_OUTLINE, VARBIN_DEFAULT,"
            + "POINT, POLYGON";
    private final static String COLUMN_NAME_LIST = "ID, " + COLUMN_NAMES_NO_ID;
    private final static String VALUES_LIST = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?";

    // Declare the SQL Statements to be used
    public final static SQLStmt INSERT_VALUES_DUSB_R1 = new SQLStmt(
            "INSERT INTO DUSB_R1 ( "+COLUMN_NAME_LIST+" ) VALUES ( "+VALUES_LIST+" );");
    public final static SQLStmt INSERT_VALUES_DUSB_P1 = new SQLStmt(
            "INSERT INTO DUSB_P1 ( "+COLUMN_NAME_LIST+" ) VALUES ( "+VALUES_LIST+" );");


    // The run() method, as required for each VoltProcedure
    public VoltTable[] run(String tableName, int id,
            String[] columnNames, String[] columnValues)
            throws VoltAbortException
    {

        int numColumns = columnNames.length;
        if (numColumns != columnValues.length) {
            throw new VoltAbortException("Different lengths for columnNames ("+numColumns
                    + ") and columnValues ("+columnValues.length+") parameters.");
        }

        // Initialize all column values as null, by default
        byte tiny = (byte) VoltType.TINYINT.getNullValue();
        short small = (short) VoltType.SMALLINT.getNullValue();
        int integ = (int) VoltType.INTEGER.getNullValue();
        long big = (long) VoltType.BIGINT.getNullValue();
        double flot = (double) VoltType.FLOAT.getNullValue();
        BigDecimal dec = null;
        String time = null;
        String vcharInline = null;
        String vcharInlineMax = null;
        String vcharOutlineMin = null;
        String vcharOutline = null;
        String vcharDefault = null;
        String varbarInline = null;
        String varbarInlineMax = null;
        String varbarOutlineMin = null;
        String varbarOutline = null;
        String varbarDefault = null;
        GeographyPointValue point = null;
        GeographyValue polygon = null;

        // Determine which columns are non-null
        for (int i=0; i < numColumns; i++) {
            try {
                switch (columnNames[i].toUpperCase()) {
                case "TINY":
                case "TINYINT":
                    tiny = Byte.parseByte(columnValues[i]);
                    break;
                case "SMALL":
                case "SMALLINT":
                    small = Short.parseShort(columnValues[i]);
                    break;
                case "INT":
                case "INTEG":
                case "INTEGER":
                    integ = Integer.parseInt(columnValues[i]);
                    break;
                case "BIG":
                case "BIGINT":
                    big = Long.parseLong(columnValues[i]);
                    break;
                case "FLOT":
                case "FLOAT":
                    flot = Double.parseDouble(columnValues[i]);
                    break;
                case "DECML":
                case "DECIMAL":
                    dec = new BigDecimal(columnValues[i]);
                    break;
                case "TIME":
                case "TIMESTMP":
                case "TIMESTAMP":
                    time = columnValues[i];
                    break;
                case "VCHAR_INLINE":
                    vcharInline = columnValues[i];
                    break;
                case "VCHAR_INLINE_MAX":
                    vcharInlineMax = columnValues[i];
                    break;
                case "VCHAR_OUTLINE_MIN":
                    vcharOutlineMin = columnValues[i];
                    break;
                case "VCHAR_OUTLINE":
                    vcharOutline = columnValues[i];
                    break;
                case "VCHAR_DEFAULT":
                case "VARCHAR":
                    vcharDefault = columnValues[i];
                    break;
                case "VARBIN_INLINE":
                    varbarInline = columnValues[i].getBytes().toString();
                    break;
                case "VARBIN_INLINE_MAX":
                    varbarInlineMax = columnValues[i].getBytes().toString();
                    break;
                case "VARBIN_OUTLINE_MIN":
                    varbarOutlineMin = columnValues[i].getBytes().toString();
                    break;
                case "VARBIN_OUTLINE":
                    varbarOutline = columnValues[i].getBytes().toString();
                    break;
                case "VARBIN_DEFAULT":
                case "VARBINARY":
                    varbarDefault = columnValues[i].getBytes().toString();
                    break;
                case "POINT":
                case "GEOGRAPHY_POINT":
                    point = GeographyPointValue.fromWKT(columnValues[i]);
                    break;
                case "POLYGON":
                case "GEOGRAPHY":
                    polygon = new GeographyValue(columnValues[i]);
                    break;
                default:
                    throw new VoltTypeException("Unknown column name: '"+columnNames[i]+"'.");
                }
            } catch (IllegalArgumentException e) {
                throw new VoltTypeException("Unable to convert value '"+columnValues[i]
                        +"', for column name '"+columnNames[i]+"'.", e);
            } // end of try/catch

        } // end of for loop

        // Determine which SQLStmt to use
        SQLStmt sqlStatement = getSqlStatement(tableName);

        // TODO: debug print:
//        if (id == 0 || id == 500 || id == 999) {
//            System.out.println( "\nIn InsertOneRow:"
//                    + "\n  sqlStatement: "+sqlStatement
//                    + "\n  id   : "+id
//                    + "\n  tiny : "+tiny
//                    + "\n  small: "+small
//                    + "\n  integ: "+integ
//                    + "\n  big  : "+big
//                    + "\n  flot : "+flot
//                    + "\n  dec  : "+dec
//                    + "\n  time : "+time
//                    + "\n  vcharInline     : "+vcharInline
//                    + "\n  vcharInlineMax  : "+vcharInlineMax
//                    + "\n  vcharOutlineMin : "+vcharOutlineMin
//                    + "\n  vcharOutline    : "+vcharOutline
//                    + "\n  vcharDefault    : "+vcharDefault
//                    + "\n  varbarInline    : "+varbarInline
//                    + "\n  varbarInlineMax : "+varbarInlineMax
//                    + "\n  varbarOutlineMin: "+varbarOutlineMin
//                    + "\n  varbarOutline   : "+varbarOutline
//                    + "\n  varbarDefault   : "+varbarDefault
//                    + "\n  point  : "+point
//                    + "\n  polygon: "+polygon );
//        }

        // Queue the query
        voltQueueSQL(sqlStatement, id, id, tiny, small, integ, big, flot, dec, time,
                vcharInline,   vcharInlineMax,  vcharOutlineMin,  vcharOutline,  vcharDefault,
                varbarInline, varbarInlineMax, varbarOutlineMin, varbarOutline, varbarDefault,
                point, polygon);

        // Execute the query
        VoltTable[] vt = voltExecuteSQL(true);

        // TODO: debug print
//        if (id == 0 || id == 500 || id == 999) {
//            System.out.println( "\nvoltExecuteSQL result:"
//                               +"\n  vt: "+vt
//                               +"\n  length: "+vt.length
//                               +"\n  vt[0].getColumnCount    : "+vt[0].getColumnCount()
//                               +"\n  vt[0].getColumnName(0)  : "+vt[0].getColumnName(0)+", "
//                               +"\n  vt[0].getColumnType(0)  : "+vt[0].getColumnType(0)+", "
//                               +"\n  vt[0].toFormattedString :\n"+vt[0].toFormattedString()
//                               +"\n  ClientResponse.SUCCESS  : "+ClientResponse.SUCCESS
//                               +"\n  vt[0].getStatusCode     : "+vt[0].getStatusCode()
//                               );
//        }

        return vt;
    }


    // Determine which SQLStmt to use, based on tableName
    private SQLStmt getSqlStatement(String tableName) {
        SQLStmt sqlStatement = null;
        if (tableName == null) {
            throw new VoltAbortException("Illegal null table name ("+tableName+").");
        } else if ( "DUSB_R1".equals(tableName.toUpperCase()) ) {
            sqlStatement = INSERT_VALUES_DUSB_R1;
        } else if ( "DUSB_P1".equals(tableName.toUpperCase()) ) {
            sqlStatement = INSERT_VALUES_DUSB_P1;
        } else {
            throw new VoltAbortException("Unknown table name: '"+tableName+"'.");
        }
        return sqlStatement;
    }

}
