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


public class UpdateOneRow extends VoltProcedure {

    // The SET clauses used in the Update Statements defined below
    // (with columns set near maximum values or at maximum lengths)
    private final static String SET_INLINE = "SET "
            +"TINY  = 88, "
            +"SMALL = 8888, "
            +"INTEG = 888888, "
            +"BIG   = 888888888888, "
            +"FLOT  = 888888.888888, "
            +"DECML = 888888888888888888888888.888888888888, "
            +"TIMESTMP = '1888-08-08 08:58:58.888888 ', "
            +"VCHAR_INLINE     = 'Updated Row 88', "
            +"VCHAR_INLINE_MAX = 'Updated Row 888'";
    private final static String SET_OUTLINE = SET_INLINE+", "
            +"VCHAR_OUTLINE_MIN = 'Outline update 8', "
            +"VCHAR_OUTLINE     = 'Outline col update 8', "
            +"VCHAR_DEFAULT     = 'Out-line (i.e., non-inline) columns updated; 888888' ";

    // Declare the (Update) SQL Statements to be used
    // (DUSB is short for delete-update-snapshot-benchmark)
    private final static SQLStmt UPDATE_DUSB_R1_IN = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_INLINE+" WHERE ID = ?;");
    private final static SQLStmt UPDATE_DUSB_R1_OUT = new SQLStmt(
            "UPDATE DUSB_R1 "+SET_OUTLINE+" WHERE ID = ?;");
    final static SQLStmt UPDATE_DUSB_P1_IN = new SQLStmt(
            "UPDATE DUSB_P1 "+SET_INLINE+" WHERE ID = ?;");
    final static SQLStmt UPDATE_DUSB_P1_OUT = new SQLStmt(
            "UPDATE DUSB_P1 "+SET_OUTLINE+" WHERE ID = ?;");


    // The run() method, as required for each VoltProcedure
    public VoltTable[] run(long idValue, String tableName, String inlineOrOutline)
            throws VoltAbortException
    {
        // Determine which SQLStmt to use
        SQLStmt sqlStatement = getUpdateStatement(tableName, inlineOrOutline);

        // Queue the query
        voltQueueSQL(sqlStatement, idValue);

        // Execute the query
        return voltExecuteSQL(true);
    }


    // Determine which SQLStmt to use, based on tableName and inlineOrOutline
    SQLStmt getUpdateStatement(String tableName, String inlineOrOutline) {
        SQLStmt sqlStatement = null;

        // Check for null values
        if (tableName == null || inlineOrOutline == null) {
            throw new VoltAbortException("Illegal null table name ("+tableName+") "
                    +"or 'inlineOrOutline' ("+inlineOrOutline+").");
        }

        String tableNameUpperCase = tableName.toUpperCase();
        String inlineOrOutlineUpperCase = inlineOrOutline.toUpperCase();

        // Update the replicated table
        if ( "DUSB_R1".equals(tableNameUpperCase) ) {
            if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                    inlineOrOutlineUpperCase.startsWith("NON")) {
                sqlStatement = UPDATE_DUSB_R1_OUT;
            } else {
                sqlStatement = UPDATE_DUSB_R1_IN;
            }

        // Update the partitioned table
        } else if ( "DUSB_P1".equals(tableNameUpperCase) ) {
            if (inlineOrOutlineUpperCase.startsWith("OUT") ||
                    inlineOrOutlineUpperCase.startsWith("NON")) {
                sqlStatement = UPDATE_DUSB_P1_OUT;
            } else {
                sqlStatement = UPDATE_DUSB_P1_IN;
            }

        } else {
            throw new VoltAbortException("Unknown table name: '"+tableName+"'.");
        }

        return sqlStatement;
    }

}
