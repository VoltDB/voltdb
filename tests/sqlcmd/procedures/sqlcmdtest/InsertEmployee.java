/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package sqlcmdtest;

import org.voltdb.*;

/**
 * Stored procedure that inserts a new row into the EMPLOYEE table, using the
 * next available ID, and the specified FIRST_NAME, LAST_NAME and DEPARTMENT.
 *
 * Used by sqlcmdtest to test using LOAD CLASSES and REMOVE CLASSES in sqlcmd.
 *
 * To build this into a .jar file, run voltdb/tests/sqlcmd/build_jar.sh, or
 * simply run 'ant sqlcmdtest', which does that automatically.
 */
public class InsertEmployee extends VoltProcedure {

    private final SQLStmt GetMaxId = new SQLStmt("SELECT MAX(ID) FROM EMPLOYEE");
    private final SQLStmt InsertEmpl = new SQLStmt("INSERT INTO EMPLOYEE VALUES (?, ?, ?, ?)");

    /**
    * Inserts a new row into the EMPLOYEE table, using the next available ID,
    * and the specified FIRST_NAME, LAST_NAME and DEPARTMENT.
    */
    public long run(String firstName, String lastName, String department) {

        // Get the maximum ID, of all current employees
        voltQueueSQL(GetMaxId);
        VoltTable[] queryResults = voltExecuteSQL();
        VoltTable result = queryResults[0];
        VoltTableRow row = result.fetchRow(0);
        long maxId = row.getLong(0);
        if (row.wasNull()) {  // there are no employees yet
            maxId = 0;
        }

        // Insert a new row into the EMPLOYEE table
        voltQueueSQL(InsertEmpl, (maxId + 1), firstName, lastName, department);
        voltExecuteSQL();
        return 1;  // 1 row inserted
    }

}
