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


public class UpdateMultiValues extends VoltProcedure {

    // The SET clauses used in the Update Statements defined below
    // (with columns set near maximum values or at maximum lengths)
    public static final String SET_INLINE = "SET "
            +"TINY  = 99, "
            +"SMALL = 9999, "
            +"INTEG = 999999, "
            +"BIG   = 999999999999, "
            +"FLOT  = 999999.999999, "
            +"DECML = 999999999999999999999999.999999999999, "
            +"TIMESTMP = '1999-09-09 09:59:59.999999 ', "
            +"VCHAR_INLINE     = 'Updated Row 99', "
            +"VCHAR_INLINE_MAX = 'Updated Row 999'";
    public static final String SET_OUTLINE = SET_INLINE+", "
            +"VCHAR_OUTLINE_MIN = 'Outline update 9', "
            +"VCHAR_OUTLINE     = 'Outline col update 9', "
            +"VCHAR_DEFAULT     = 'Out-line (i.e., non-inline) columns updated; 999999' ";

    // Declare the (Update) SQL Statements to be used
    // (DUSB is short for delete-update-snapshot-benchmark)
    public static final SQLStmt UPDATE_ROWS_DUSB_R1_IN_BY_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_INLINE+" WHERE ID >= ? AND ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_R1_OUT_BY_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_OUTLINE+" WHERE ID >= ? AND ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_R1_IN_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_INLINE+" WHERE MOD_ID >= ? AND MOD_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_R1_OUT_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_OUTLINE+" WHERE MOD_ID >= ? AND MOD_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_R1_IN_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_INLINE+" WHERE BLOCK_ID >= ? AND BLOCK_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_R1_OUT_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_OUTLINE+" WHERE BLOCK_ID >= ? AND BLOCK_ID < ?;");

    public static final SQLStmt UPDATE_ROWS_DUSB_P1_IN_BY_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+SET_INLINE+" WHERE ID >= ? AND ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P1_OUT_BY_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+SET_OUTLINE+" WHERE ID >= ? AND ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P1_IN_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+SET_INLINE+" WHERE MOD_ID >= ? AND MOD_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P1_OUT_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+SET_OUTLINE+" WHERE MOD_ID >= ? AND MOD_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P1_IN_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+SET_INLINE+" WHERE BLOCK_ID >= ? AND BLOCK_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P1_OUT_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+SET_OUTLINE+" WHERE BLOCK_ID >= ? AND BLOCK_ID < ?;");

    public static final SQLStmt UPDATE_ROWS_DUSB_P2_IN_BY_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+SET_INLINE+" WHERE ID >= ? AND ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P2_OUT_BY_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+SET_OUTLINE+" WHERE ID >= ? AND ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P2_IN_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+SET_INLINE+" WHERE MOD_ID >= ? AND MOD_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P2_OUT_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+SET_OUTLINE+" WHERE MOD_ID >= ? AND MOD_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P2_IN_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+SET_INLINE+" WHERE BLOCK_ID >= ? AND BLOCK_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P2_OUT_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+SET_OUTLINE+" WHERE BLOCK_ID >= ? AND BLOCK_ID < ?;");

    public static final SQLStmt UPDATE_ROWS_DUSB_P3_IN_BY_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+SET_INLINE+" WHERE ID >= ? AND ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P3_OUT_BY_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+SET_OUTLINE+" WHERE ID >= ? AND ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P3_IN_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+SET_INLINE+" WHERE MOD_ID >= ? AND MOD_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P3_OUT_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+SET_OUTLINE+" WHERE MOD_ID >= ? AND MOD_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P3_IN_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+SET_INLINE+" WHERE BLOCK_ID >= ? AND BLOCK_ID < ?;");
    public static final SQLStmt UPDATE_ROWS_DUSB_P3_OUT_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+SET_OUTLINE+" WHERE BLOCK_ID >= ? AND BLOCK_ID < ?;");

    // The run() method, as required for each VoltProcedure
    public VoltTable[] run(String tableName, String columnName,
            long minValue, long maxValue, String inlineOrOutline)
            throws VoltAbortException
    {
        // Determine which SQLStmt to use
        SQLStmt sqlStatement = getSqlStatement(tableName, columnName, inlineOrOutline);

        // Queue the query
        voltQueueSQL(sqlStatement, minValue, maxValue);

        // Execute the query
        return voltExecuteSQL(true);
    }


    // Determine which SQLStmt to use, based on tableName, columnName,
    // and inlineOrOutline
    private SQLStmt getSqlStatement(String tableName, String columnName,
            String inlineOrOutline) {
        SQLStmt sqlStatement = null;

        // Check for null values
        if (tableName == null || columnName == null || inlineOrOutline == null) {
            throw new VoltAbortException("Illegal null table name ("+tableName+"), column"
                    +" name ("+columnName+") or 'inlineOrOutline' ("+inlineOrOutline+").");
        }

        String tableNameUpperCase  = tableName.toUpperCase();
        String columnNameUpperCase = columnName.toUpperCase();
        String inlineOrOutlineUpperCase = inlineOrOutline.toUpperCase();

        // Update the replicated table
        if ( "DUSB_R1".equals(tableNameUpperCase) ) {
            if ( "ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_ROWS_DUSB_R1_OUT_BY_ID;
                } else {
                    sqlStatement = UPDATE_ROWS_DUSB_R1_IN_BY_ID;
                }
            } else if ( "MOD_ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_ROWS_DUSB_R1_OUT_BY_MOD_ID;
                } else {
                    sqlStatement = UPDATE_ROWS_DUSB_R1_IN_BY_MOD_ID;
                }
            } else if ( "BLOCK_ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_ROWS_DUSB_R1_OUT_BY_BLOCK_ID;
                } else {
                    sqlStatement = UPDATE_ROWS_DUSB_R1_IN_BY_BLOCK_ID;
                }
            } else {
                throw new VoltAbortException("Unknown column name: '"+columnName
                        +"' (with table name '"+tableName+"').");
            }

        // Update the first partitioned table (partitioned on ID)
        } else if ( "DUSB_P1".equals(tableNameUpperCase) ) {
            if ( "ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_ROWS_DUSB_P1_OUT_BY_ID;
                    } else {
                        sqlStatement = UPDATE_ROWS_DUSB_P1_IN_BY_ID;
                    }
            } else if ( "MOD_ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_ROWS_DUSB_P1_OUT_BY_MOD_ID;
                    } else {
                        sqlStatement = UPDATE_ROWS_DUSB_P1_IN_BY_MOD_ID;
                    }
            } else if ( "BLOCK_ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_ROWS_DUSB_P1_OUT_BY_BLOCK_ID;
                    } else {
                        sqlStatement = UPDATE_ROWS_DUSB_P1_IN_BY_BLOCK_ID;
                    }
            } else {
                throw new VoltAbortException("Unknown column name: '"+columnName
                        +"' (with table name '"+tableName+"').");
            }

        // Update the second partitioned table (partitioned on MOD_ID)
        } else if ( "DUSB_P2".equals(tableNameUpperCase) ) {
            if ( "ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_ROWS_DUSB_P2_OUT_BY_ID;
                    } else {
                        sqlStatement = UPDATE_ROWS_DUSB_P2_IN_BY_ID;
                    }
            } else if ( "MOD_ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_ROWS_DUSB_P2_OUT_BY_MOD_ID;
                    } else {
                        sqlStatement = UPDATE_ROWS_DUSB_P2_IN_BY_MOD_ID;
                    }
            } else if ( "BLOCK_ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_ROWS_DUSB_P2_OUT_BY_BLOCK_ID;
                    } else {
                        sqlStatement = UPDATE_ROWS_DUSB_P2_IN_BY_BLOCK_ID;
                    }
            } else {
                throw new VoltAbortException("Unknown column name: '"+columnName
                        +"' (with table name '"+tableName+"').");
            }

        // Update the third partitioned table (partitioned on BLOCK_ID)
        } else if ( "DUSB_P3".equals(tableNameUpperCase) ) {
            if ( "ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_ROWS_DUSB_P3_OUT_BY_ID;
                    } else {
                        sqlStatement = UPDATE_ROWS_DUSB_P3_IN_BY_ID;
                    }
            } else if ( "MOD_ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_ROWS_DUSB_P3_OUT_BY_MOD_ID;
                    } else {
                        sqlStatement = UPDATE_ROWS_DUSB_P3_IN_BY_MOD_ID;
                    }
            } else if ( "BLOCK_ID".equals(columnNameUpperCase) ) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_ROWS_DUSB_P3_OUT_BY_BLOCK_ID;
                    } else {
                        sqlStatement = UPDATE_ROWS_DUSB_P3_IN_BY_BLOCK_ID;
                    }
            } else {
                throw new VoltAbortException("Unknown column name: '"+columnName
                        +"' (with table name '"+tableName+"').");
            }

        } else {
            throw new VoltAbortException("Unknown table name: '"+tableName
                    +"' (with column name '"+columnName+"').");
        }

        return sqlStatement;
    }

}
