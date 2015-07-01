/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

public class MaterializePlanNode extends ProjectionPlanNode {

    public enum Members {
        BATCHED;
    }

    protected boolean m_batched = false;

    public MaterializePlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.MATERIALIZE;
    }

    public void setBatched(boolean batched) {
        m_batched = batched;
    }

    public boolean isBatched() {
        return m_batched;
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        // MaterializePlanNodes have no children
        assert(m_children.size() == 0);
        // MaterializePlanNode's output schema is pre-determined, don't touch
        return;
    }

    @Override
    public void resolveColumnIndexes()
    {
        // MaterializePlanNodes have no children
        assert(m_children.size() == 0);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.BATCHED.name()).value(m_batched);
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        m_batched = jobj.getBoolean( Members.BATCHED.name() );
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "MATERIALIZE TUPLE from parameters and/or literals";
    }
}
