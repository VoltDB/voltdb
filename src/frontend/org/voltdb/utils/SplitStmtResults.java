/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

/**
 * To store the statements which are parsed/split by the SQLLexer.splitStatements().
 * Currently the incompleteStmt is used to store statements that are not complete,
 * so that more user input can be appended at the end of it.
 */
public class SplitStmtResults {
    private final List<String> m_completelyParsedStmts;
    private final String m_incompleteStmt;
    private final int m_incompleteStmtOffset;

    public SplitStmtResults(List<String> completelyParsedStmts, String incompleteStmt, int incompleteStmtOffset) {
        m_completelyParsedStmts = completelyParsedStmts;
        m_incompleteStmt = incompleteStmt;
        m_incompleteStmtOffset = incompleteStmtOffset;
    }

    public String getIncompleteStmt() {
        return m_incompleteStmt;
    }

    public List<String> getCompletelyParsedStmts() {
        return m_completelyParsedStmts;
    }

    public int getIncompleteStmtOffset() {
        return m_incompleteStmtOffset;
    }
}
