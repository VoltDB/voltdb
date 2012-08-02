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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

public class SQLFunction {
    private final ExpressionType m_exprType;
    private final String m_sqlName;
    private final int m_nArguments;
    private final VoltType m_valueType;
    private final VoltType m_paramType;

    private SQLFunction(ExpressionType exprType, String sqlName, int nArguments, VoltType valueType, VoltType paramType) {
        m_exprType = exprType;
        m_sqlName = sqlName;
        m_nArguments = nArguments;
        m_valueType = valueType;
        m_paramType = paramType;
    }

    private static final Map<String, SQLFunction[]> name_lookup =
            new HashMap<String, SQLFunction[]>();

    public static List<SQLFunction> functionsByNameAndArgumentCount(String name, int nArguments) {
        List<SQLFunction> result = new ArrayList<SQLFunction>();
        SQLFunction[] overloads = name_lookup.get(name.intern());
        if (overloads != null) {
            for (SQLFunction fn : overloads) {
                if (fn.m_nArguments != nArguments) {
                    continue;
                }
                result.add(fn);
            }
        }
        return result;
    }

    public static void nameFunctions(SQLFunction[] overloads) {
        name_lookup.put(overloads[0].m_sqlName.intern(), overloads);
    }

    static {
        // Initialize functions grouped by their SQL names to (someday) support type-specific overloads.
        final SQLFunction abs[] = {new SQLFunction(ExpressionType.FUNCTION_ABS, "abs", 1, null, VoltType.NUMERIC),
        };
        nameFunctions(abs);
        final SQLFunction substring[] = {new SQLFunction(ExpressionType.FUNCTION_SUBSTRING_FROM,     "substring", 2, VoltType.STRING, null),
                                         new SQLFunction(ExpressionType.FUNCTION_SUBSTRING_FROM_FOR, "substring", 3, VoltType.STRING, null),
        };
        nameFunctions(substring);
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

