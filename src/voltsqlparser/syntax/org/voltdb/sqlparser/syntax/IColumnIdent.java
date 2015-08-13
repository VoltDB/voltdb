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

/**
 * An IColumnIdent is a reference to a column in
 * a table definition.  It's very much like an IProjection,
 * but slightly more specialized, since there is no alias
 * and the table is implicit.  Maybe they should be be
 * merged.
 *
 * @author bwhite
 */
public interface IColumnIdent {

    public abstract String getColumnName();

    public abstract int getColLineNo();

    public abstract int getColColNo();

}