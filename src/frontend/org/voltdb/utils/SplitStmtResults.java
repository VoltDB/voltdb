/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.utils;
import java.util.List;
import java.util.ArrayList;

/**
 * To store the statements which are parsed/split by the SQLLexer.splitStatements()
 * currently the incompleteStmt is used to store the incomplete multi statement procedure
 * and is used to return back to SQLCMD so that more user input can be appended at the end of it
 */
public class SplitStmtResults {
    public List<String> completelyParsedStmts;
    public String incompleteMuliStmtProc;

    public SplitStmtResults() {
        completelyParsedStmts = new ArrayList<String>();
        incompleteMuliStmtProc = null;
    }
}
