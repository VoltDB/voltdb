/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.util.List;
import java.util.ArrayList;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.types.PlanNodeType;
import org.voltdb.catalog.Database;
import org.voltdb.planner.ParsedUnionStmt;

public class UnionPlanNode extends AbstractPlanNode {

    public enum Members {
        UNION_TYPE
    }

    // Union Type
    private ParsedUnionStmt.UnionType m_unionType;

    public UnionPlanNode() {
        super();
        m_unionType = ParsedUnionStmt.UnionType.UNION;
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
    public void resolveColumnIndexes()
    {
        // Should be at least two selects in a join
        assert(m_children.size() > 1);
        for (AbstractPlanNode child : m_children)
        {
            child.resolveColumnIndexes();
        }
    }

    public ParsedUnionStmt.UnionType getUnionType() {
        return m_unionType;
    }

    /**
     * Create a DistinctPlanNode that clones the configuration information but
     * is not inserted in the plan graph and has a unique plan node id.
     * @return copy
     */
    public UnionPlanNode produceCopyForTransformation() {
        UnionPlanNode copy = new UnionPlanNode(m_unionType);
        super.produceCopyForTransformation(copy);
        copy.m_unionType = this.m_unionType;

        // Should be at least two selects in a join
        assert(m_children.size() > 1);
        // Better to have PlanNodeFactory
        for (AbstractPlanNode child : m_children)
        {
            AbstractPlanNode childCopy = null;
            if (child instanceof SeqScanPlanNode) {
                childCopy = new SeqScanPlanNode();
            } else if (child instanceof IndexScanPlanNode) {
                childCopy = new IndexScanPlanNode();
            } else if (child instanceof NestLoopPlanNode) {
                childCopy = new NestLoopPlanNode();
            } else if (child instanceof NestLoopIndexPlanNode) {
                childCopy = new NestLoopIndexPlanNode();
            } else if (child instanceof UnionPlanNode) {
                childCopy = new UnionPlanNode();
            } else {
                throw new RuntimeException("Unsupported statement type in UNION");
            }
            child.produceCopyForTransformation(childCopy);
        }
        return copy;
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        // Should be at least two selects in a join
        assert(m_children.size() > 1);
        for (AbstractPlanNode child : m_children)
        {
            child.generateOutputSchema(db);
        }
        // @TODO MIKE - what should be an output schema for union?
        m_outputSchema = m_children.get(0).getOutputSchema();
   }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.UNION_TYPE.name()).value(m_unionType.name());
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "UNION";
    }

}
