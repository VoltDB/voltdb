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
package org.voltdb.sqlparser.syntax;

import org.voltdb.sqlparser.syntax.symtab.IType;

/**
 * An IStringType object is some kind of string type.  This could
 * be VARCHAR or VARBINARY.  This type is kind of odd in that we
 * have urtypes named VARCHAR and VARBINARY which have no max
 * size, and other types which do have max size.  The urtypes are
 * in the standard prelude, but they other types are in the ASTs.
 *
 * @author bwhite
 *
 */
public interface IStringType extends IType {

    public abstract IStringType makeInstance(long aSize);

}