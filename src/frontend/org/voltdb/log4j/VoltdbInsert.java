/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.log4j;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class VoltdbInsert extends VoltProcedure {

    final String insertLog_sql = "INSERT INTO Logs (timestamp, level, message) values (?,?,?);";
    public final SQLStmt insertLog = new SQLStmt(insertLog_sql);

    public VoltTable[] run (long timestamp, String level, String message) throws VoltAbortException {
        voltQueueSQL(insertLog, timestamp, level, message);
        return voltExecuteSQL();
    }
}