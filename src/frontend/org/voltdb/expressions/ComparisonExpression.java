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

package org.voltdb.expressions;

import java.util.HashMap;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.types.ExpressionType;

/**
 *
 */
public class ComparisonExpression extends AbstractExpression {

    public ComparisonExpression(ExpressionType type) {
        super(type);
        setValueType(VoltType.BIGINT);
    }

    public ComparisonExpression(ExpressionType type, AbstractExpression left, AbstractExpression right) {
        super(type, left, right);
        setValueType(VoltType.BIGINT);
    }

    public ComparisonExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {}

    @Override
    public boolean needsRightExpression() {
        return true;
    }

    private static Map<ExpressionType,ExpressionType> reverses = new HashMap<ExpressionType, ExpressionType>();
    static {
        reverses.put(ExpressionType.COMPARE_EQUAL, ExpressionType.COMPARE_EQUAL);
        reverses.put(ExpressionType.COMPARE_NOTEQUAL, ExpressionType.COMPARE_NOTEQUAL);
        reverses.put(ExpressionType.COMPARE_LESSTHAN, ExpressionType.COMPARE_GREATERTHAN);
        reverses.put(ExpressionType.COMPARE_GREATERTHAN, ExpressionType.COMPARE_LESSTHAN);
        reverses.put(ExpressionType.COMPARE_LESSTHANOREQUALTO, ExpressionType.COMPARE_GREATERTHANOREQUALTO);
        reverses.put(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ExpressionType.COMPARE_LESSTHANOREQUALTO);
    }

    public ComparisonExpression reverseOperator() {
        ExpressionType reverseType = reverses.get(this.m_type);
        // Left and right exprs are reversed on purpose
        return new ComparisonExpression(reverseType, m_right, m_left);
    }

    @Override
    public void finalizeValueTypes()
    {
        finalizeChildValueTypes();
        //
        // IMPORTANT:
        // We are not handling the case where one of types is NULL. That is because we
        // are only dealing with what the *output* type should be, not what the actual
        // value is at execution time. There will need to be special handling code
        // over on the ExecutionEngine to handle special cases for conjunctions with NULLs
        // Therefore, it is safe to assume that the output is always going to be an
        // integer (for booleans)
        //
        m_valueType = VoltType.BIGINT;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }
}
