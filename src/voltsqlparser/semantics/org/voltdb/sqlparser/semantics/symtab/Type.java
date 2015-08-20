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

import org.voltdb.sqlparser.syntax.symtab.IType;
import org.voltdb.sqlparser.syntax.symtab.TypeKind;

/**
 * This is the base class of all types.  All types have a name,
 * a nominal and maximum size and a kind.
 *
 * @author bwhite
 *
 */
public class Type extends Top implements IType {
    public TypeKind m_kind;

    public Type(String aName, TypeKind aKind) {
        super(aName);
        m_kind = aKind;
    }

    public boolean equals(Type other) {
        return (this.getName().equals(other.getName()));
    }

    public final Type getType() {
        return this;
    }

    public String toString() {
        return this.getClass().toString();
    }

    public boolean isEqualType(Type rightType) {
        return m_kind == rightType.getTypeKind();
    }

    public final TypeKind getTypeKind() {
        return m_kind;
    }

    @Override
    public boolean isBooleanType() {
        return m_kind.isBoolean();
    }

    @Override
    public boolean isVoidType() {
    	return m_kind.isVoid();
    }
	@Override
	public boolean isErrorType() {
		return false;
	}
}
