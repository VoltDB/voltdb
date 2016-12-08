/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.plannodes;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SetOpType;

public class SetOpReceivePlanNode extends AbstractReceivePlanNode {

    private static class Members {
        static final String SETOP_TYPE = "SETOP_TYPE";
        static final String CHILDREN_CNT = "CHILDREN_CNT";
    }

    // SetOp Type
    private SetOpType m_setOpType;

    // Children count
    private int m_childrenCnt;

    public SetOpReceivePlanNode() {
        super();
        m_setOpType = SetOpType.NONE;
    }
    public SetOpReceivePlanNode(SetOpType setOpType, int childrenCnt) {
        super();
        m_setOpType = setOpType;
        m_childrenCnt = childrenCnt;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.SETOPRECEIVE;
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        super.generateOutputSchema(db);

        // except, while technically the resulting output schema is just a pass-through,
        // when the plan gets fragmented, this receive node will be at the bottom of the
        // fragment and will need its own serialized copy of its (former) child's output schema.
        m_hasSignificantOutputSchema = true;
    }

    @Override
    public void resolveColumnIndexes() {
        resolveColumnIndexes(m_outputSchema);
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        m_setOpType = SetOpType.valueOf(jobj.getString(Members.SETOP_TYPE));
        m_childrenCnt = jobj.getInt(Members.CHILDREN_CNT);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.SETOP_TYPE, m_setOpType.name());
        stringer.keySymbolValuePair(Members.CHILDREN_CNT, m_childrenCnt);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "RECEIVE SET OP " + m_setOpType.name();
    }

    public SetOpType getSetOpType() {
        return m_setOpType;
    }

}
