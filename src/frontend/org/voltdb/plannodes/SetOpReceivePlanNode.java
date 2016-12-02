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
    }

    // SetOp Type
    private SetOpType m_setOpType;

    // Cildren count
    private int m_chidlrednCnt;

    public SetOpReceivePlanNode() {
        super();
        m_setOpType = SetOpType.NONE;
    }
    public SetOpReceivePlanNode(SetOpType setOpType, int chidlrednCnt) {
        super();
        m_setOpType = setOpType;
        m_chidlrednCnt = chidlrednCnt;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.SETOPRECEIVE;
    }

    @Override
    public void resolveColumnIndexes()
    {
        resolveColumnIndexes(m_outputSchema);
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        m_setOpType = SetOpType.valueOf(jobj.getString(Members.SETOP_TYPE));
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.SETOP_TYPE, m_setOpType.name());
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "RECEIVE SET OP " + m_setOpType.name();
    }

    @Override
    public int getInputDependencyCount() {
        if (m_setOpType == SetOpType.EXCEPT || m_setOpType == SetOpType.EXCEPT_ALL) {
            // Partiton Set op result plus its children
            return m_chidlrednCnt + 1;
        } else if (m_setOpType == SetOpType.INTERSECT || m_setOpType == SetOpType.INTERSECT_ALL) {
            //
            return m_chidlrednCnt;
        } else {
            return 1;
        }
    }

    public SetOpType getSetOpType() {
        return m_setOpType;
    }


}
