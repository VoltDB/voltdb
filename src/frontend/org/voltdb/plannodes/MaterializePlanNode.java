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

package org.voltdb.plannodes;

import java.util.Collection;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.types.PlanNodeType;

public class MaterializePlanNode extends ProjectionPlanNode {

    public enum Members {
        BATCHED; // OBSOLETE
    }

    public MaterializePlanNode() {
        super();
    }

    public MaterializePlanNode(NodeSchema schema) {
        super();
        m_outputSchema = schema;
        m_hasSignificantOutputSchema = true;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.MATERIALIZE;
    }

    @Override
    public void generateOutputSchema(Database db) {
        // MaterializePlanNodes have no children
        assert(m_children.size() == 0);
        // MaterializePlanNode's output schema is pre-determined, don't touch
        // except when its output column(s) has a scalar subquery expression
        // Generate the output schema for subqueries if any
        Collection<AbstractExpression> exprs = findAllSubquerySubexpressions();
        for (AbstractExpression expr: exprs) {
            ((AbstractSubqueryExpression) expr).generateOutputSchema(db);
        }

        return;
    }

    @Override
    public void resolveColumnIndexes() {
        // MaterializePlanNodes have no children
        assert(m_children.size() == 0);
        resolveSubqueryColumnIndexes();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.BATCHED.name(), false);
    }

    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db)
            throws JSONException {
        super.loadFromJSONObject(jobj, db);
        assert( ! jobj.getBoolean(Members.BATCHED.name()));
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "MATERIALIZE TUPLE from parameters and/or literals";
    }
}
