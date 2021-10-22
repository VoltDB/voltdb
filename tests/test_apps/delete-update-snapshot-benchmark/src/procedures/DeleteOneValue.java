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


public class DeleteOneValue extends VoltProcedure {

    // Declare the (Delete) SQL Statements to be used
    private static final SQLStmt DELETE_1ROW_DUSB_R1_BY_ID = new SQLStmt(
            "DELETE FROM DUSB_R1 WHERE ID = ?;");
    private static final SQLStmt DELETE_ROWS_DUSB_R1_BY_MOD_ID = new SQLStmt(
            "DELETE FROM DUSB_R1 WHERE MOD_ID = ?;");
    private static final SQLStmt DELETE_ROWS_DUSB_R1_BY_BLOCK_ID = new SQLStmt(
            "DELETE FROM DUSB_R1 WHERE MOD_ID = ?;");

    static final SQLStmt DELETE_1ROW_DUSB_P1_BY_ID = new SQLStmt(
            "DELETE FROM DUSB_P1 WHERE ID = ?;");
    static final SQLStmt DELETE_ROWS_DUSB_P1_BY_MOD_ID = new SQLStmt(
            "DELETE FROM DUSB_P1 WHERE MOD_ID = ?;");
    static final SQLStmt DELETE_ROWS_DUSB_P1_BY_BLOCK_ID = new SQLStmt(
            "DELETE FROM DUSB_P1 WHERE MOD_ID = ?;");

    static final SQLStmt DELETE_1ROW_DUSB_P2_BY_ID = new SQLStmt(
            "DELETE FROM DUSB_P2 WHERE ID = ?;");
    static final SQLStmt DELETE_ROWS_DUSB_P2_BY_MOD_ID = new SQLStmt(
            "DELETE FROM DUSB_P2 WHERE MOD_ID = ?;");
    static final SQLStmt DELETE_ROWS_DUSB_P2_BY_BLOCK_ID = new SQLStmt(
            "DELETE FROM DUSB_P2 WHERE BLOCK_ID = ?;");

    static final SQLStmt DELETE_1ROW_DUSB_P3_BY_ID = new SQLStmt(
            "DELETE FROM DUSB_P3 WHERE ID = ?;");
    static final SQLStmt DELETE_ROWS_DUSB_P3_BY_MOD_ID = new SQLStmt(
            "DELETE FROM DUSB_P3 WHERE MOD_ID = ?;");
    static final SQLStmt DELETE_ROWS_DUSB_P3_BY_BLOCK_ID = new SQLStmt(
            "DELETE FROM DUSB_P3 WHERE BLOCK_ID = ?;");


    // The run() method, as required for each VoltProcedure
    public VoltTable[] run(long idValue, String tableName, String columnName)
            throws VoltAbortException
    {
        // Determine which SQLStmt to use
        SQLStmt sqlStatement = getDeleteStatement(tableName, columnName);

        // Queue the query
        voltQueueSQL(sqlStatement, idValue);

        // Execute the query
        return voltExecuteSQL(true);
    }


    // Determine which SQLStmt to use, based on tableName
    SQLStmt getDeleteStatement(String tableName, String columnName) {
        SQLStmt sqlStatement = null;

        // Check for null values
        if (tableName == null || columnName == null) {
            throw new VoltAbortException("Illegal null table name ("
                    +tableName+") or column name ("+columnName+").");
        }

        String tableNameUpperCase = tableName.toUpperCase();
        String columnNameUpperCase = columnName.toUpperCase();

        // Delete from the first partitioned table (partitioned on ID)
        if ("DUSB_P1".equals(tableNameUpperCase)) {
            if ("ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_1ROW_DUSB_P1_BY_ID;
            } else if ("MOD_ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_ROWS_DUSB_P1_BY_MOD_ID;
            } else if ("BLOCK_ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_ROWS_DUSB_P1_BY_BLOCK_ID;
            } else {
                throw new VoltAbortException("Unknown column name: '"+columnName
                        +"' (with table name '"+tableName+"').");
            }

        // Delete from the second partitioned table (partitioned on MOD_ID)
        } else if ("DUSB_P2".equals(tableNameUpperCase)) {
            if ("MOD_ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_ROWS_DUSB_P2_BY_MOD_ID;
            } else if ("ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_1ROW_DUSB_P2_BY_ID;
            } else if ("BLOCK_ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_ROWS_DUSB_P2_BY_BLOCK_ID;
            } else {
                throw new VoltAbortException("Unknown column name: '"+columnName
                        +"' (with table name '"+tableName+"').");
            }

        // Delete from the second partitioned table (partitioned on MOD_ID)
        } else if ("DUSB_P3".equals(tableNameUpperCase)) {
            if ("BLOCK_ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_ROWS_DUSB_P3_BY_BLOCK_ID;
            } else if ("ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_1ROW_DUSB_P3_BY_ID;
            } else if ("MOD_ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_ROWS_DUSB_P3_BY_MOD_ID;
            } else {
                throw new VoltAbortException("Unknown column name: '"+columnName
                        +"' (with table name '"+tableName+"').");
            }

        // Delete from the replicated table
        } else if ("DUSB_R1".equals(tableNameUpperCase)) {
            if ("ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_1ROW_DUSB_R1_BY_ID;
            } else if ("MOD_ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_ROWS_DUSB_R1_BY_MOD_ID;
            } else if ("BLOCK_ID".equals(columnNameUpperCase)) {
                sqlStatement = DELETE_ROWS_DUSB_R1_BY_BLOCK_ID;
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
