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
 package org.voltdb.sqlparser.syntax.symtab;

public interface ISymbolTable {

    /**
     * Define an entity in the Symbol Table.
     * @param aEntity
     */
    public void define(ITop aEntity);

    public int size();

    public boolean isEmpty();

    /**
     * Get the value of a string as anything.  See {@link Top} for
     * the type and value hierarchy.
     *
     * @param aName
     * @return
     */
    public ITop get(String aName);

    /**
     * Lookup a name and try to interpret it as a Type.
     * Return null if the name does not denote a type.
     *
     * @param aName The name.
     * @return The Type denoted by aName, or else null.
     */
    public IType getType(String aName);

    /**
     * Lookup a name and try to interpret it as a value.
     * Return null if the name does not denote a value.
     *
     * @param aName The name.
     * @return The value denoted by aName, or else null.
     */
    public IValue getValue(String aName);

}
