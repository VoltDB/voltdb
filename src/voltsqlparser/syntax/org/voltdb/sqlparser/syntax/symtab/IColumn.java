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

public interface IColumn extends ITop {

    public abstract IType getType();

    public String getName();

    /*
     * Set various attributes of the column.
     */
    public void setHasDefaultValue(boolean hasDefaultValue);
    public boolean hasDefaultValue();

    public void setDefaultValue(String defaultValue);
    public String getDefaultValue();

    public void setIsPrimaryKey(boolean isPrimaryKey);
    public boolean isPrimaryKey();

    public void setIsNullable(boolean value);
    public boolean isNullable();

    public void setIsNull(boolean value);
    public boolean isNull();

    public void setIsUniqueConstraint(boolean isUniqueConstraint);
    public boolean isUniqueConstraint();
}
