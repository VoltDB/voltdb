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
import org.voltdb.planner.PlannerContext;
import org.voltdb.types.PlanNodeType;

public class DistinctPlanNode extends AbstractPlanNode {

    public enum Members {
        DISTINCT_COLUMN_GUID,
        DISTINCT_COLUMN_NAME;
    }

    //
    // TODO: This should really be an Expression that outputs some value?
    //       But how would that work for multi-column Distincts?
    //
    protected int m_distinctColumnGuid;
    protected String m_distinctColumnName;

    public DistinctPlanNode(PlannerContext context) {
        super(context);
    }

    /**
     * Create a DistinctPlanNode that clones the configuration information but
     * is not inserted in the plan graph and has a unique plan node id.
     * @return copy
     */
    public DistinctPlanNode produceCopyForTransformation() {
        DistinctPlanNode copy = new DistinctPlanNode(m_context);
        super.produceCopyForTransformation(copy);
        copy.m_distinctColumnGuid = m_distinctColumnGuid;
        copy.m_distinctColumnName = m_distinctColumnName;
        return copy;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.DISTINCT;
    }

    /**
     * @return the distinct_column GUID
     */
    public Integer getDistinctColumnGuid() {
        return m_distinctColumnGuid;
    }

    /**
     * @param distinctColumnGuid the distinct_column_guid to set
     */
    public void setDistinctColumnGuid(int distinctColumnGuid) {
        m_distinctColumnGuid = distinctColumnGuid;
    }

    /**
     * @return the distinct_column_name
     */
    public String getDistinctColumnName() {
        return m_distinctColumnName;
    }

    /**
     * @param distinct_column_name the distinct_column name to set
     */
    public void setDistinctColumnName(String distinct_column_name) {
        m_distinctColumnName = distinct_column_name;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.DISTINCT_COLUMN_GUID.name()).value(m_distinctColumnGuid);
        stringer.key(Members.DISTINCT_COLUMN_NAME.name()).value(m_distinctColumnName);
    }
}
