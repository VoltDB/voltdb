/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.expressions;

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

public class ConjunctionExpression extends AbstractExpression {
    public ConjunctionExpression(ExpressionType type) {
        super(type);
        setValueType(VoltType.BOOLEAN);
    }
    public ConjunctionExpression(ExpressionType type, AbstractExpression left, AbstractExpression right) {
        super(type, left, right);
        setValueType(VoltType.BOOLEAN);
    }
    public ConjunctionExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    @Override
    public boolean needsRightExpression() {
        return true;
    }

    @Override
    public void finalizeValueTypes() {
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
        m_valueType = VoltType.BOOLEAN;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    @Override
    public String explain(String impliedTableName) {
        ExpressionType type = getExpressionType();
        return "(" + m_left.explain(impliedTableName) +
            " " + type.symbol() + " " +
            m_right.explain(impliedTableName) + ")";
    }

    @Override
    public boolean isValueTypeIndexable(StringBuffer msg) {
        // Conjunction expression include and and or expression that results in boolean result
        // boolean result are not indexable
        msg.append("logical expression: '").append(getExpressionType().symbol()).append("'");
        return false;
    }

}
