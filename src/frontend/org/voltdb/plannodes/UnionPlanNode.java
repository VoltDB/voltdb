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

import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.ParsedUnionStmt;
import org.voltdb.planner.ParsedUnionStmt.UnionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class UnionPlanNode extends AbstractPlanNode {

    public enum Members {
        UNION_TYPE
    }

    // Union Type
    private ParsedUnionStmt.UnionType m_unionType;

    public UnionPlanNode() {
        super();
        m_unionType = ParsedUnionStmt.UnionType.NOUNION;
    }

    public UnionPlanNode(ParsedUnionStmt.UnionType unionType) {
        super();
        m_unionType = unionType;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.UNION;
    }

    @Override
    public void resolveColumnIndexes() {
        // Should be at least two children in a union
        assert(m_children.size() > 1);
        for (AbstractPlanNode child : m_children) {
            child.resolveColumnIndexes();
        }
    }

    public ParsedUnionStmt.UnionType getUnionType() {
        return m_unionType;
    }

    @Override
    public void generateOutputSchema(Database db) {
        // Should be at least two selects in a join
        assert(m_children.size() > 1);
        // The output schema for the union is the output schema from the first expression
        m_children.get(0).generateOutputSchema(db);
        m_outputSchema = m_children.get(0).getOutputSchema();

        // Then generate schemas for the remaining ones and make sure that they are identical
        for (int i = 1; i < m_children.size(); ++i) {
            AbstractPlanNode child = m_children.get(i);
            child.generateOutputSchema(db);
            NodeSchema schema = child.getOutputSchema();
            if (schema.size() != m_outputSchema.size()) {
                throw new RuntimeException("Column number mismatch detected in rows of UNION");
            }
            for (int j = 0; j < m_outputSchema.size(); ++j) {
                if (m_outputSchema.getColumn(j).getValueType() != schema.getColumn(j).getValueType()) {
                    throw new PlanningErrorException("Incompatible data types in UNION");
                }
            }
        }

        assert(! hasInlineVarcharOrVarbinary());

        m_outputSchema = m_children.get(0).getOutputSchema();
        m_hasSignificantOutputSchema = false; // It's just the first child's
        // Then check that they have the same types
   }

    private boolean hasInlineVarcharOrVarbinary() {
        for (AbstractPlanNode child : m_children) {
            for (SchemaColumn scol : child.getOutputSchema()) {
                if (AbstractExpression.hasInlineVarType(scol.getExpression())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.UNION_TYPE.name(), m_unionType.name());
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "UNION " + m_unionType.name();
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        m_unionType = UnionType.valueOf(jobj.getString(Members.UNION_TYPE.name()));
    }

    @Override
    public boolean isOutputOrdered (List<AbstractExpression> sortExpressions, List<SortDirectionType> sortDirections) {
        return false;
    }

    @Override
    public boolean isOrderDeterministic() {
        return false;
    }
}
