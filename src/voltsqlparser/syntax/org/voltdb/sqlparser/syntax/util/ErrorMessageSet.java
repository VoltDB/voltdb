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
/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
package org.voltdb.sqlparser.syntax.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.voltdb.sqlparser.syntax.util.ErrorMessage.Severity;

public class ErrorMessageSet implements Iterable<ErrorMessage> {
    List<ErrorMessage> m_errorMessages = new ArrayList<ErrorMessage>();

    public void addError(int line,
                         int col,
                         String fmt,
                         Object ... args) {
        String msg = String.format(fmt, args);
        m_errorMessages.add(new ErrorMessage(line,
                                             col,
                                             Severity.Error,
                                             msg));
    }

    public void addWarning(int line, int col, String errorMessageFormat,
            Object[] args) {
        String msg = String.format(errorMessageFormat, args);
        m_errorMessages.add(new ErrorMessage(line,
                                             col,
                                             Severity.Warning,
                                             msg));
    }

    public int size() {
        // TODO Auto-generated method stub
        return m_errorMessages.size();
    }

    @Override
    public Iterator<ErrorMessage> iterator() {
        return m_errorMessages.iterator();
    }

}
