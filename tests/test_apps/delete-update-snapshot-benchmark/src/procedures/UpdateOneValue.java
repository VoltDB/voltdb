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


public class UpdateOneValue extends VoltProcedure {

    // The SET clauses used in the Update Statements defined below
    // (with columns set near maximum values or at maximum lengths)
    static final String SET_INLINE = "SET "
            +"TINY  = 88, "
            +"SMALL = 8888, "
            +"INTEG = 888888, "
            +"BIG   = 888888888888, "
            +"FLOT  = 888888.888888, "
            +"DECML = 888888888888888888888888.888888888888, "
            +"TIMESTMP = '1888-08-08 08:58:58.888888 ', "
            +"VCHAR_INLINE     = 'Updated Row 88', "
            +"VCHAR_INLINE_MAX = 'Updated Row 888'";
    static final String SET_OUTLINE = SET_INLINE+", "
            +"VCHAR_OUTLINE_MIN = 'Outline update 8', "
            +"VCHAR_OUTLINE     = 'Outline col update 8', "
            +"VCHAR_DEFAULT     = 'Out-line (i.e., non-inline) columns updated; 888888' ";

    // Declare the (Update) SQL Statements to be used
    // (DUSB is short for delete-update-snapshot-benchmark)
    private static final SQLStmt UPDATE_1ROW_DUSB_R1_IN_BY_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_INLINE+" WHERE ID = ?;");
    private static final SQLStmt UPDATE_1ROW_DUSB_R1_OUT_BY_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_OUTLINE+" WHERE ID = ?;");
    private static final SQLStmt UPDATE_1ROW_DUSB_R1_IN_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_INLINE+" WHERE MOD_ID = ?;");
    private static final SQLStmt UPDATE_1ROW_DUSB_R1_OUT_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_OUTLINE+" WHERE MOD_ID = ?;");
    private static final SQLStmt UPDATE_1ROW_DUSB_R1_IN_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_INLINE+" WHERE BLOCK_ID = ?;");
    private static final SQLStmt UPDATE_1ROW_DUSB_R1_OUT_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_OUTLINE+" WHERE BLOCK_ID = ?;");

    static final SQLStmt UPDATE_1ROW_DUSB_P1_IN_BY_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+SET_INLINE+" WHERE ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P1_OUT_BY_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+SET_OUTLINE+" WHERE ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P1_IN_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+UpdateOneValue.SET_INLINE+" WHERE MOD_ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P1_OUT_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+UpdateOneValue.SET_OUTLINE+" WHERE MOD_ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P1_IN_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+UpdateOneValue.SET_INLINE+" WHERE BLOCK_ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P1_OUT_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P1 "+UpdateOneValue.SET_OUTLINE+" WHERE BLOCK_ID = ?;");

    static final SQLStmt UPDATE_1ROW_DUSB_P2_IN_BY_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+SET_INLINE+" WHERE ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P2_OUT_BY_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+SET_OUTLINE+" WHERE ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P2_IN_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+UpdateOneValue.SET_INLINE+" WHERE MOD_ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P2_OUT_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+UpdateOneValue.SET_OUTLINE+" WHERE MOD_ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P2_IN_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+UpdateOneValue.SET_INLINE+" WHERE BLOCK_ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P2_OUT_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P2 "+UpdateOneValue.SET_OUTLINE+" WHERE BLOCK_ID = ?;");

    static final SQLStmt UPDATE_1ROW_DUSB_P3_IN_BY_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+SET_INLINE+" WHERE ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P3_OUT_BY_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+SET_OUTLINE+" WHERE ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P3_IN_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+UpdateOneValue.SET_INLINE+" WHERE MOD_ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P3_OUT_BY_MOD_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+UpdateOneValue.SET_OUTLINE+" WHERE MOD_ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P3_IN_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+UpdateOneValue.SET_INLINE+" WHERE BLOCK_ID = ?;");
    static final SQLStmt UPDATE_1ROW_DUSB_P3_OUT_BY_BLOCK_ID = new SQLStmt(
            "UPDATE DUSB_P3 "+UpdateOneValue.SET_OUTLINE+" WHERE BLOCK_ID = ?;");


    // The run() method, as required for each VoltProcedure
    public VoltTable[] run(long idValue, String tableName, String columnName, String inlineOrOutline)
            throws VoltAbortException
    {
        // Determine which SQLStmt to use
        SQLStmt sqlStatement = getUpdateStatement(tableName, columnName, inlineOrOutline);

        // Queue the query
        voltQueueSQL(sqlStatement, idValue);

        // Execute the query
        return voltExecuteSQL(true);
    }


    // Determine which SQLStmt to use, based on tableName and inlineOrOutline
    SQLStmt getUpdateStatement(String tableName, String columnName, String inlineOrOutline) {
        SQLStmt sqlStatement = null;

        // Check for null values
        if (tableName == null || columnName == null || inlineOrOutline == null) {
            throw new VoltAbortException("Illegal null table name ("+tableName
                    +"), 'columnName' ("+columnName+"), or 'inlineOrOutline' ("
                    +inlineOrOutline+").");
        }

        String tableNameUpperCase = tableName.toUpperCase();
        String columnNameUpperCase = columnName.toUpperCase();
        String inlineOrOutlineUpperCase = inlineOrOutline.toUpperCase();

        // Update the replicated table
        if ( "DUSB_R1".equals(tableNameUpperCase) ) {
            if ("ID".equals(columnNameUpperCase)) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_1ROW_DUSB_R1_OUT_BY_ID;
                } else {
                    sqlStatement = UPDATE_1ROW_DUSB_R1_IN_BY_ID;
                }
            } else if ("MOD_ID".equals(columnNameUpperCase)) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_1ROW_DUSB_R1_OUT_BY_MOD_ID;
                } else {
                    sqlStatement = UPDATE_1ROW_DUSB_R1_IN_BY_MOD_ID;
                }
            } else if ("BLOCK_ID".equals(columnNameUpperCase)) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_1ROW_DUSB_R1_OUT_BY_BLOCK_ID;
                } else {
                    sqlStatement = UPDATE_1ROW_DUSB_R1_IN_BY_BLOCK_ID;
                }
            } else {
                throw new VoltAbortException("Unknown column name: '"+columnName+"'.");
            }

        // Update the first partitioned table (partitioned on ID)
        } else if ( "DUSB_P1".equals(tableNameUpperCase) ) {
            if ("ID".equals(columnNameUpperCase)) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_1ROW_DUSB_P1_OUT_BY_ID;
                } else {
                    sqlStatement = UPDATE_1ROW_DUSB_P1_IN_BY_ID;
                }
            } else if ("MOD_ID".equals(columnNameUpperCase)) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_1ROW_DUSB_P1_OUT_BY_MOD_ID;
                } else {
                    sqlStatement = UPDATE_1ROW_DUSB_P1_IN_BY_MOD_ID;
                }
            } else if ("BLOCK_ID".equals(columnNameUpperCase)) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_1ROW_DUSB_P1_OUT_BY_BLOCK_ID;
                } else {
                    sqlStatement = UPDATE_1ROW_DUSB_P1_IN_BY_BLOCK_ID;
                }
            } else {
                throw new VoltAbortException("Unknown column name: '"+columnName+"'.");
            }

        // second partitioned table (partitioned on MOD_ID)
        } else if ( "DUSB_P2".equals(tableNameUpperCase) ) {
            if ("MOD_ID".equals(columnNameUpperCase)) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_1ROW_DUSB_P2_OUT_BY_MOD_ID;
                } else {
                    sqlStatement = UPDATE_1ROW_DUSB_P2_IN_BY_MOD_ID;
                }
            } else if ("ID".equals(columnNameUpperCase)) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_1ROW_DUSB_P2_OUT_BY_ID;
                } else {
                    sqlStatement = UPDATE_1ROW_DUSB_P2_IN_BY_ID;
                }
            } else if ("BLOCK_ID".equals(columnNameUpperCase)) {
                if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                        inlineOrOutlineUpperCase.startsWith("NON")) {
                    sqlStatement = UPDATE_1ROW_DUSB_P2_OUT_BY_BLOCK_ID;
                } else {
                    sqlStatement = UPDATE_1ROW_DUSB_P2_IN_BY_BLOCK_ID;
                }
            } else {
                throw new VoltAbortException("Unknown column name: '"+columnName+"'.");
            }

            // third partitioned table (partitioned on BLOCK_ID)
            } else if ( "DUSB_P3".equals(tableNameUpperCase) ) {
                if ("BLOCK_ID".equals(columnNameUpperCase)) {
                    if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                            inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_1ROW_DUSB_P3_OUT_BY_BLOCK_ID;
                    } else {
                        sqlStatement = UPDATE_1ROW_DUSB_P3_IN_BY_BLOCK_ID;
                    }
                } else if ("ID".equals(columnNameUpperCase)) {
                    if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                            inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_1ROW_DUSB_P3_OUT_BY_ID;
                    } else {
                        sqlStatement = UPDATE_1ROW_DUSB_P3_IN_BY_ID;
                    }
                } else if ("MOD_ID".equals(columnNameUpperCase)) {
                    if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                            inlineOrOutlineUpperCase.startsWith("NON")) {
                        sqlStatement = UPDATE_1ROW_DUSB_P3_OUT_BY_MOD_ID;
                    } else {
                        sqlStatement = UPDATE_1ROW_DUSB_P3_IN_BY_MOD_ID;
                    }
                } else {
                    throw new VoltAbortException("Unknown column name: '"+columnName+"'.");
                }

        } else {
            throw new VoltAbortException("Unknown table name: '"+tableName+"'.");
        }

        return sqlStatement;
    }

}
