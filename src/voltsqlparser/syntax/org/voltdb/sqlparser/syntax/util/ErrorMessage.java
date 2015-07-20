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
 package org.voltdb.sqlparser.syntax.util;

/**
 * This contains one error message.  It has a line, a column
 * a severity and the message text.
 *
 * @author bwhite
 *
 */
public class ErrorMessage {
    public enum Severity {
        Info,
        Warning,
        Error,
        Fatal
    }
    int m_line;
    int m_col;
    String m_msg;
    Severity m_severity;

    public ErrorMessage(int aLine, int aCol, Severity aSeverity, String aMsg) {
        m_line = aLine;
        m_col = aCol;
        m_msg = aMsg;
        m_severity = aSeverity;
    }
    public final int getLine() {
        return m_line;
    }
    public final int getCol() {
        return m_col;
    }
    public final String getMsg() {
        return m_msg;
    }
    public final Severity getSeverity() {
        return m_severity;
    }

}
