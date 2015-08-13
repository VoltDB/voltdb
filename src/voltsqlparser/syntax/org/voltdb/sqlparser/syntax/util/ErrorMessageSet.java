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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.voltdb.sqlparser.syntax.util.ErrorMessage.Severity;

public class ErrorMessageSet implements Iterable<ErrorMessage> {
    List<ErrorMessage> m_errorMessages = new ArrayList<ErrorMessage>();
    int m_numberErrors = 0;
    int m_numberWarnings = 0;

    public void addError(int line,
                         int col,
                         String fmt,
                         Object ... args) {
        String msg = String.format(fmt, args);
        m_errorMessages.add(new ErrorMessage(line,
                                             col,
                                             Severity.Error,
                                             msg));
        m_numberErrors += 1;
    }

    public void addWarning(int line, int col, String errorMessageFormat,
            Object[] args) {
        String msg = String.format(errorMessageFormat, args);
        m_errorMessages.add(new ErrorMessage(line,
                                             col,
                                             Severity.Warning,
                                             msg));
        m_numberWarnings += 1;
    }

    public int size() {
        // TODO Auto-generated method stub
        return m_errorMessages.size();
    }

    @Override
    public Iterator<ErrorMessage> iterator() {
        return m_errorMessages.iterator();
    }

    public int numberErrors() {
        // TODO Auto-generated method stub
        return m_numberErrors;
    }

    public int numberWarnings() {
        // TODO Auto-generated method stub
        return m_numberWarnings;
    }
}
