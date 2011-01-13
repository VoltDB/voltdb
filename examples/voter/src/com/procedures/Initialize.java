/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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


// Initialize stored procedure
//
//   Initializes database.  If first time run, insert contestants.

package com.procedures;

import org.voltdb.*;

@ProcInfo(
    singlePartition = false
)

public class Initialize extends VoltProcedure {
    // check if the system has already been initialized
    public final SQLStmt checkContestant = new SQLStmt("select count(*) from contestants;");

    // insert a contestant
    public final SQLStmt insertContestant = new SQLStmt("insert into contestants (contestant_name, contestant_number) values (?, ?);");

    public VoltTable[] run(
        int maxContestant,
        String[] contestantArray
    ) {
        int numContestants = 1;

        voltQueueSQL(checkContestant);
        VoltTable results1[] = voltExecuteSQL();

        if (results1[0].fetchRow(0).getLong(0) == 0) {
            // insert contestants
            for (int i=0; i < maxContestant; i++) {
                voltQueueSQL(insertContestant, contestantArray[i], i+1);
            }

            voltExecuteSQL();

            numContestants = maxContestant;
        } else {
            // get number of contestants
            numContestants = (int) results1[0].fetchRow(0).getLong(0);
        }

        VoltTable vtContestants = new VoltTable(new VoltTable.ColumnInfo("numContestants",VoltType.INTEGER));
        Object row[] = new Object[1];
        row[0] = numContestants;
        vtContestants.addRow(row);

        final VoltTable[] vtReturn = {vtContestants};

        return vtReturn;
    }
}
