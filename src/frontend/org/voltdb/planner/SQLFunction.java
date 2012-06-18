/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner;

import java.util.HashMap;
import java.util.Map;

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

public class SQLFunction {
    private final ExpressionType m_exprType;
    private final String m_sqlName;
    private final VoltType m_valueType;
    private final VoltType m_paramType;

    private SQLFunction(ExpressionType exprType, String sqlName, VoltType valueType, VoltType paramType) {
        m_exprType = exprType;
        m_sqlName = sqlName;
        m_valueType = valueType;
        m_paramType = paramType;
    }

    private static final Map<String, SQLFunction[]> name_lookup =
            new HashMap<String, SQLFunction[]>();

    public static SQLFunction[] functionsByName(String name) {
        return name_lookup.get(name.intern());
    }

    public static void nameFunctions(SQLFunction[] overloads) {
        name_lookup.put(overloads[0].m_sqlName.intern(), overloads);
    }

    static {
        // Initialize functions grouped by their SQL names to (someday) support type-specific overloads.
        final SQLFunction abs[] = {new SQLFunction(ExpressionType.FUNCTION_ABS, "abs", null, VoltType.NUMERIC),
                                  };
        nameFunctions(abs);
    }

    public VoltType getValueType() {
        return m_valueType;
    }

    public String getSqlName() {
        return m_sqlName;
    }

    public String getUniqueName() {
        return m_exprType.name().toLowerCase();
    }

    public boolean hasParameter() {
        return m_paramType != null;
    }

    public VoltType paramType() {
        return m_paramType;
    }

    public ExpressionType getExpressionType() {
        return m_exprType;
    }
}

