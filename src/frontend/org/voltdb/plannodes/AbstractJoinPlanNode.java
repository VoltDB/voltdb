/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.plannodes;

import org.json.JSONException;
import org.json.JSONStringer;
import org.voltdb.expressions.*;
import org.voltdb.planner.PlannerContext;
import org.voltdb.types.*;

/**
 *
 */
public abstract class AbstractJoinPlanNode extends AbstractPlanNode {

    public enum Members {
        JOIN_TYPE,
        PREDICATE;
    }

    protected JoinType m_joinType = JoinType.INNER;
    protected AbstractExpression m_predicate;

    /**
     * @param id
     */
    protected AbstractJoinPlanNode(PlannerContext context, Integer id) {
        super(context, id);
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        if (m_predicate != null) {
            m_predicate.validate();
        }
    }

    /**
     * @return the join_type
     */
    public JoinType getJoinType() {
        return m_joinType;
    }

    /**
     * @param join_type the join_type to set
     */
    public void setJoinType(JoinType join_type) {
        m_joinType = join_type;
    }

    /**
     * @return the predicate
     */
    public AbstractExpression getPredicate() {
        return m_predicate;
    }

    /**
     * @param predicate the predicate to set
     */
    public void setPredicate(AbstractExpression predicate) {
        m_predicate = predicate;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.JOIN_TYPE.name()).value(m_joinType.toString());
        stringer.key(Members.PREDICATE.name()).value(m_predicate);
    }
}
