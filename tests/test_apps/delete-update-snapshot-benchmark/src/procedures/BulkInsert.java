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
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;

import java.math.BigDecimal;

public class BulkInsert extends VoltProcedure {

    // Declare the SQL Statements to be used
    public final SQLStmt INSERT_SELECT_DUSB_R1 = new SQLStmt(
            "INSERT INTO DUSB_R1 SELECT ID + ?, "+InsertOneRow.COLUMN_NAMES_NO_ID
                + " FROM DUSB_R1 WHERE ID >= ? AND ID < ? ORDER BY ID;");
    public final SQLStmt INSERT_SELECT_DUSB_P1 = new SQLStmt(
            "INSERT INTO DUSB_P1 SELECT ID + ?, "+InsertOneRow.COLUMN_NAMES_NO_ID
                + " FROM DUSB_R1 WHERE ID >= ? AND ID < ? ORDER BY ID;");

    public VoltTable[] run(String tableName, String[] columnNames, String[] columnValues,
                           int minId, int blockSize, int maxId)
                     throws VoltAbortException {

        int numColumns = columnNames.length;
        if (numColumns != columnValues.length) {
            throw new VoltAbortException("Different lengths for columnNames ("+numColumns
                    + ") and columnValues ("+columnValues.length+") parameters.");
        }

        // Compute the number of blocks to insert, from the number of rows and
        // the block size: round to one decimal place, i.e., to the nearest 0.1;
        // then take the"ceiling" (nearest integer above or equal to)
        int numrows = maxId - minId;
        int numBlocks = (int) Math.ceil( Math.round( 10.0D *
                                         numrows / blockSize )
                                         / 10.0D );

        // Due to limits  on Partioned tables, they need to be populated by
        // INSERT FROM SELECT queries that select from a Replicated table; so
        // a Partioned table needs to copy one extra "block" at the beginning,
        // whereas a Replicated table is copying from itself, so the first
        // "block" has already been populated
        int minBlock = 0;
        if (tableName.contains("_R")) {
            minBlock = 1;
        }

        // Determine which SQLStmt to use
        SQLStmt sqlStatement = getSqlStatement(tableName);

       // Queue up some INSERT SELECT queries
        for (int block = minBlock; block < numBlocks; block++) {
            int addToId = block * blockSize;

            // Prevent inserting more than the total desired number of rows,
            // when the blockSize does not divide into it evenly
            if (minId + addToId + blockSize > maxId) {
                blockSize = maxId - addToId - minId;
            }

            voltQueueSQL(sqlStatement, addToId, minId, blockSize);
        }

        // Execute the INSERT SELECT queries
        return voltExecuteSQL(true);

    }   // end of run()


    // Determine which SQLStmt to use, based on tableName
    protected SQLStmt getSqlStatement(String tableName) {
        SQLStmt sqlStatement = null;
        if (tableName == null) {
            throw new VoltAbortException("Illegal null table name ("+tableName+").");
        } else if ( "DUSB_R1".equals(tableName.toUpperCase()) ) {
            sqlStatement = INSERT_SELECT_DUSB_R1;
        } else if ( "DUSB_P1".equals(tableName.toUpperCase()) ) {
            sqlStatement = INSERT_SELECT_DUSB_P1;
        } else {
            throw new VoltAbortException("Unknown table name: '"+tableName+"'.");
        }
        return sqlStatement;
   }


    // Inserts values into a single column of the specified table
    protected void insertValues(String tableName,
            long id, byte tiny, short small, int integ, long big,
            double flot, BigDecimal dec, String time,
            String vcharInline, String vcharInlineMax,
            String vcharOutlineMin, String vcharOutline, String vcharDefault,
            String varbarInline, String varbarInlineMax,
            String varbarOutlineMin, String varbarOutline, String varbarDefault,
            GeographyPointValue point, GeographyValue polygon)
    {
        // Determine which SQLStmt to use
        SQLStmt sqlStatement = getSqlStatement(tableName);

        // Queue the query; MOD_ID gets the same value as ID, for these initial columns
        voltQueueSQL(sqlStatement, id, id, tiny, small, integ, big, flot, dec, time,
                vcharInline,   vcharInlineMax,  vcharOutlineMin,  vcharOutline,  vcharDefault,
                varbarInline, varbarInlineMax, varbarOutlineMin, varbarOutline, varbarDefault,
                point, polygon);
    }

}
