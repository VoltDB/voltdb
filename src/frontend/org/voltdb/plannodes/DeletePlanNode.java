/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

public class DeletePlanNode extends AbstractOperationPlanNode {

    public enum Members {
        TRUNCATE;
    }

    /** true if all tuples are deleted. */
    boolean m_truncate = false;

    public DeletePlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.DELETE;
    };

    public boolean isTruncate() {
        return m_truncate;
    }
    public void setTruncate(boolean truncate) {
        m_truncate = truncate;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.TRUNCATE.name()).value(m_truncate);
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        m_truncate = jobj.getBoolean( Members.TRUNCATE.name() );
    }

    @Override
    protected String explainPlanForNode(String indent) {
        if (m_truncate) {
            return "TRUNCATE TABLE " + m_targetTableName;
        }
        return "DELETE" + m_targetTableName;
    }
}
