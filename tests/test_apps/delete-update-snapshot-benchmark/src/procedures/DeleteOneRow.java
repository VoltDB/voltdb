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


public class DeleteOneRow extends VoltProcedure {

    // Declare the SQL Statements to be used
    private final static SQLStmt DELETE_FROM_DUSB_R1 = new SQLStmt(
            "DELETE FROM DUSB_R1 WHERE ID = ?;");
    final static SQLStmt DELETE_FROM_DUSB_P1 = new SQLStmt(
            "DELETE FROM DUSB_P1 WHERE ID = ?;");


    // The run() method, as required for each VoltProcedure
    public VoltTable[] run(long idValue, String tableName)
            throws VoltAbortException
    {
        // Determine which SQLStmt to use
        SQLStmt sqlStatement = getDeleteStatement(tableName);

        // Queue the query
        voltQueueSQL(sqlStatement, idValue);

        // Execute the query
        return voltExecuteSQL(true);
    }


    // Determine which SQLStmt to use, based on tableName
    SQLStmt getDeleteStatement(String tableName) {
        SQLStmt sqlStatement = null;

        // Check for null values
        if (tableName == null) {
            throw new VoltAbortException("Illegal null table name ("
                    +tableName+") in DeleteOneRow.");
        }

        // Delete from replicated table
        if ("DUSB_R1".equals(tableName.toUpperCase())) {
            sqlStatement = DELETE_FROM_DUSB_R1;

        // Delete from partitioned table
        } else if ("DUSB_P1".equals(tableName.toUpperCase())) {
            sqlStatement = DELETE_FROM_DUSB_P1;

        } else {
            throw new VoltAbortException("Unknown table name: '"+tableName+"'.");
        }

        return sqlStatement;
    }

}
