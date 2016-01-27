/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

public abstract class SQLPatternPart
{
    int m_flags = 0;
    Integer m_minCount = null;
    Integer m_maxCount = null;

    abstract String generateExpression(int flagsAdd);
    abstract void setCaptureLabel(String captureLabel);
    abstract Pattern compile(String label);

    // Chainable methods for tweaking after construction.
    public SQLPatternPart withFlags(int flags)
    {
        m_flags |= flags;
        return this;
    }
}
