/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

public class InsertFromSelect extends VoltProcedure {

    // Declare the SQL Statements to be used
    public final SQLStmt INSERT_SELECT_DUSB_R1 = new SQLStmt(
            "INSERT INTO DUSB_R1 SELECT ID + ?, CAST(? AS BIGINT), "+InsertOneRow.COLUMN_NAMES_NO_ID
                + " FROM DUSB_R1 WHERE ID >= ? AND ID < ? ORDER BY ID;");
    public final SQLStmt INSERT_SELECT_DUSB_P1 = new SQLStmt(
            "INSERT INTO DUSB_P1 SELECT ID + ?, CAST(? AS BIGINT), "+InsertOneRow.COLUMN_NAMES_NO_ID
                + " FROM DUSB_R1 WHERE ID >= ? AND ID < ? ORDER BY ID;");
    public final SQLStmt INSERT_SELECT_DUSB_P2 = new SQLStmt(
            "INSERT INTO DUSB_P2 SELECT ID + ?, CAST(? AS BIGINT), "+InsertOneRow.COLUMN_NAMES_NO_ID
                + " FROM DUSB_R1 WHERE ID >= ? AND ID < ? ORDER BY ID;");
    public final SQLStmt INSERT_SELECT_DUSB_P3 = new SQLStmt(
            "INSERT INTO DUSB_P3 SELECT ID + ?, CAST(? AS BIGINT), "+InsertOneRow.COLUMN_NAMES_NO_ID
                + " FROM DUSB_R1 WHERE ID >= ? AND ID < ? ORDER BY ID;");

    public VoltTable[] run(String tableName, long addToId, long blockId,
                           long minId, long maxId)
                     throws VoltAbortException {

        // Determine which SQLStmt to use
        SQLStmt sqlStatement = getSqlStatement(tableName);

        voltQueueSQL(sqlStatement, addToId, blockId, minId, maxId);

        // Execute the INSERT SELECT query
        return voltExecuteSQL(true);

    }   // end of run()


    // Determine which SQLStmt to use, based on tableName
    private SQLStmt getSqlStatement(String tableName) {
        SQLStmt sqlStatement = null;

        if (tableName == null) {
            throw new VoltAbortException("Illegal null table name ("+tableName+").");
        } else if ( "DUSB_P3".equals(tableName.toUpperCase()) ) {
            sqlStatement = INSERT_SELECT_DUSB_P3;
        } else if ( "DUSB_P2".equals(tableName.toUpperCase()) ) {
            sqlStatement = INSERT_SELECT_DUSB_P2;
        } else if ( "DUSB_P1".equals(tableName.toUpperCase()) ) {
            sqlStatement = INSERT_SELECT_DUSB_P1;
        } else if ( "DUSB_R1".equals(tableName.toUpperCase()) ) {
            sqlStatement = INSERT_SELECT_DUSB_R1;
        } else {
            throw new VoltAbortException("Unknown table name: '"+tableName+"'.");
        }

        return sqlStatement;

    }   // end of getSqlStatement()

}
