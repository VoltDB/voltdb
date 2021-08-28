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


public class DeleteRows extends VoltProcedure {

    // Declare the SQL Statements to be used
    public final static SQLStmt DELETE_FROM_DUSB_R1_BY_ID = new SQLStmt(
            "DELETE FROM DUSB_R1 WHERE ID >= ? AND ID < ?;");
    public final static SQLStmt DELETE_FROM_DUSB_R1_BY_MOD_ID = new SQLStmt(
            "DELETE FROM DUSB_R1 WHERE MOD_ID >= ? AND MOD_ID < ?;");
    public final static SQLStmt DELETE_FROM_DUSB_P1_BY_ID = new SQLStmt(
            "DELETE FROM DUSB_P1 WHERE ID >= ? AND ID < ?;");
    public final static SQLStmt DELETE_FROM_DUSB_P1_BY_MOD_ID = new SQLStmt(
            "DELETE FROM DUSB_P1 WHERE MOD_ID >= ? AND MOD_ID < ?;");


    // The run() method, as required for each VoltProcedure
    public VoltTable[] run(String tableName, String columnName,
            long minValue, long maxValue)
            throws VoltAbortException
    {
        // Determine which SQLStmt to use
        SQLStmt sqlStatement = getSqlStatement(tableName, columnName);

        // Queue the query
        voltQueueSQL(sqlStatement, minValue, maxValue);

        // Execute the query
        return voltExecuteSQL(true);
    }


    // Determine which SQLStmt to use, based on tableName and columnName
    private SQLStmt getSqlStatement(String tableName, String columnName) {
        SQLStmt sqlStatement = null;

        // Check for null values
        if (tableName == null || columnName == null) {
            throw new VoltAbortException("Illegal null table name ("
                    +tableName+") or column name ("+columnName+").");
        }

        // Delete from replicated table
        if ( "DUSB_R1".equals(tableName.toUpperCase()) ) {
            if ( "ID".equals(columnName.toUpperCase()) ) {
                sqlStatement = DELETE_FROM_DUSB_R1_BY_ID;
            } else if ( "MOD_ID".equals(columnName.toUpperCase()) ) {
                sqlStatement = DELETE_FROM_DUSB_R1_BY_MOD_ID;
            } else {
                throw new VoltAbortException("Unknown column name: '"+columnName
                        +"' (with table name '"+tableName+"').");
            }

        // Delete from partitioned table
        } else if ( "DUSB_P1".equals(tableName.toUpperCase()) ) {
            if ( "ID".equals(columnName.toUpperCase()) ) {
                sqlStatement = DELETE_FROM_DUSB_R1_BY_ID;
            } else if ( "MOD_ID".equals(columnName.toUpperCase()) ) {
                sqlStatement = DELETE_FROM_DUSB_P1_BY_MOD_ID;
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
