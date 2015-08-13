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
package org.voltdb.sqlparser.semantics.symtab;

import org.voltdb.sqlparser.syntax.IStringType;
import org.voltdb.sqlparser.syntax.symtab.ITop;

public class StringType extends Type implements ITop, IStringType {
    long m_maxSize = -1;
    public StringType(String aName, TypeKind aKind) {
        super(aName, aKind);
    }
    private StringType(String aName, TypeKind aKind, long aMaxSize) {
        super(aName, aKind);
        m_maxSize = aMaxSize;
    }
    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.semantics.symtab.IStringType#makeInstance(long)
     */
    @Override
    public StringType makeInstance(long aSize) {
        StringType answer = new StringType(getName(), getTypeKind(), aSize);
        return answer;
    }
    public final long getMaxSize() {
        return m_maxSize;
    }
}
