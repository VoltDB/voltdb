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

package org.voltdb.plannodes;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.types.PlanNodeType;

public class UpdatePlanNode extends AbstractOperationPlanNode {

    public enum Members {
        UPDATES_INDEXES;
    }

    boolean m_updatesIndexes = false;

    public UpdatePlanNode() {
        super();
    }

    public boolean doesUpdateIndexes() {
        return m_updatesIndexes;
    }

    public void setUpdateIndexes(boolean updateIndexes) {
        m_updatesIndexes = updateIndexes;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.UPDATE;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.UPDATES_INDEXES.name()).value(m_updatesIndexes);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "UPDATE";
    }
}
