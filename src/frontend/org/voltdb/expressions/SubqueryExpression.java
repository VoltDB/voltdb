/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.expressions.TupleValueExpression.Members;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.types.ExpressionType;

/**
*
*/
public class SubqueryExpression extends AbstractExpression {

    public enum Members {
        SUBQUERY,
    }

    private AbstractParsedStmt m_subquery;
    // The list of TVE from the parent statement that are referenced from the subquery
    private List<TupleValueExpression> m_parentTves = new ArrayList<TupleValueExpression>();

    /**
     * Create a new SubqueryExpression
     * @param subquey The parsed statement
     */
    public SubqueryExpression(AbstractParsedStmt subquery) {
        super(ExpressionType.SUBQUERY);
        m_subquery = subquery;
        assert(m_subquery != null);
        AbstractExpression expr = m_subquery.joinTree.getAllFilters();
        List<TupleValueExpression> subqueryTves = ExpressionUtil.getTupleValueExpressions(expr);
        for (TupleValueExpression tve : subqueryTves) {
            // TODO Edge case of false positive - parent TVE incorrectly resolved at the child level
            if (tve.getParentTve() == true) {
                m_parentTves.add((TupleValueExpression)tve.clone());
            }
        }
    }

    public AbstractParsedStmt getSubquery() {
        return m_subquery;
    }

    public List<TupleValueExpression> getParentTves() {
        return m_parentTves;
    }

    @Override
    public Object clone() {
        return new SubqueryExpression(m_subquery);
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        if ((m_right != null) || (m_left != null))
            throw new Exception("ERROR: A subquery expression has child expressions for '" + this + "'");

    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj instanceof SubqueryExpression == false) {
            return false;
        }
        SubqueryExpression other = (SubqueryExpression) obj;
        // Expressions are equal if they refer to the same statement
        return m_subquery == other.m_subquery;
    }

    @Override
    public int hashCode() {
        int result = m_subquery.hashCode();
        // defer to the superclass, which factors in other attributes
        return result += super.hashCode();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        // TODO need JSON for the best plan here
        stringer.key(Members.SUBQUERY.name()).value(m_subquery);
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj) throws JSONException {
        // TODO
    }

    @Override
    public String explain(String impliedTableName) {
        // TODO Doesn't look too good
        return "(Subquery:" + this.m_subquery.toString() + ")";
    }

    @Override
    public void finalizeValueTypes() {
        // TODO
    }

}
