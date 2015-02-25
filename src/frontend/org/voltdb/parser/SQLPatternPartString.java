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

package org.voltdb.parser;

import java.util.regex.Pattern;

public class SQLPatternPartString extends SQLPatternPart
{
    private String m_str;

    SQLPatternPartString(String str)
    {
        m_str = str;
    }

    @Override
    public String generateExpression(int flagsAdd)
    {
        return m_str;
    }

    @Override
    void setCaptureLabel(String captureLabel)
    {
        // Only meaningful to capture-able elements, not raw strings.
        assert false;
    }

    @Override
    Pattern compile()
    {
        // Shouldn't really be used, but this is an obvious implementation.
        return Pattern.compile(m_str);
    }
}