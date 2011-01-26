/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.util.*;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.types.*;

/**
 *
 */
public class InComparisonExpression extends ComparisonExpression {

    public enum Members {
        VALUES;
    }

    protected List<AbstractExpression> m_values = new ArrayList<AbstractExpression>();

    public InComparisonExpression() {
        super(ExpressionType.COMPARE_IN);
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        //
        // We need at least one value defined
        //
        if (m_values.isEmpty()) {
            throw new Exception("ERROR: There we no values defined for '" + this + "'");
        }
        for (AbstractExpression exp : m_values) {
            exp.validate();
        }
        //
        // We always need a left node, but should never have a right node
        //
        if (m_left == null) {
            throw new Exception("ERROR: The left node for '" + this + "' is NULL");
        } else if (m_right != null) {
            throw new Exception("ERROR: The right node for '" + this + "' is '" + m_right + "', but we were expecting it to be NULL");
        }
    }

    /**
     * @return the values
     */
    public List<AbstractExpression> getValues() {
        return m_values;
    }
    /**
     * @param values the values to set
     */
    public void setValues(List<AbstractExpression> values) {
        m_values = values;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof InComparisonExpression == false) return false;
        InComparisonExpression expr = (InComparisonExpression) obj;

        // make sure the expressions in the list are the same
        for (int i = 0; i < m_values.size(); i++) {
            AbstractExpression left = m_values.get(i);
            AbstractExpression right = expr.m_values.get(i);
            if (left.equals(right) == false)
                return false;
        }

        // if all seems well, defer to the superclass, which checks kids
        return super.equals(obj);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        stringer.key(Members.VALUES.name()).array();
        for (AbstractExpression expr : m_values) {
            assert (expr instanceof JSONString);
            stringer.value(expr);
        }
        stringer.endArray();
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        super.loadFromJSONObject(obj, db);
        JSONArray valuesArray = obj.getJSONArray(Members.VALUES.name());
        for (int ii = 0; ii < valuesArray.length(); ii++) {
            if (valuesArray.isNull(ii)) {
                m_values.add(null);
            } else {
                m_values.add( AbstractExpression.fromJSONObject(valuesArray.getJSONObject(ii), db));
            }
        }
    }
}
