/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.planner;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.voltdb.expressions.AbstractExpression;

public class ParsedColInfo implements Cloneable {
    public String alias = null;
    public String columnName = null;
    public String tableName = null;
    public String tableAlias = null;
    public AbstractExpression expression = null;
    public int index = 0;

    // order by stuff
    public boolean orderBy = false;
    public boolean ascending = true;

    // group by
    public boolean groupBy = false;

    @Override
    public boolean equals (Object obj) {
        if (obj == null) return false;
        if (obj instanceof ParsedColInfo == false) return false;
        ParsedColInfo col = (ParsedColInfo) obj;
        if ( columnName != null && columnName.equals(col.columnName) &&
                tableName != null && tableName.equals(col.tableName) &&
                tableAlias != null && tableAlias.equals(col.tableAlias) &&
                expression != null && expression.equals(col.expression) )
            return true;
        return false;
    }

    // Based on implementation on equals().
    @Override
    public int hashCode() {
        int result = new HashCodeBuilder(17, 31).
                append(columnName).append(tableName).append(tableAlias).
                toHashCode();
        if (expression != null) {
            result += expression.hashCode();
        }
        return result;
    }

    @Override
    public ParsedColInfo clone() {
        ParsedColInfo col = null;
        try {
            col = (ParsedColInfo) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        col.expression = (AbstractExpression) expression.clone();
        return col;
    }
}
