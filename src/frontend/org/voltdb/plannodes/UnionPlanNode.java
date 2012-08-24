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

import org.voltdb.types.PlanNodeType;
import org.voltdb.catalog.Database;
import org.voltdb.planner.ParsedUnionStmt;

public class UnionPlanNode extends AbstractPlanNode {
    // Union Type
    private ParsedUnionStmt.UnionType m_unionType;
    
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
        // This node doesn't actually exist
        assert(false);
    }
    
    public ParsedUnionStmt.UnionType getUnionType() {
        return m_unionType;
    }
    
    @Override
    public void generateOutputSchema(Database db)
    {

        // Should be at leats two selects in a join
        assert(m_children.size() > 1);
        for (AbstractPlanNode child : m_children)
        {
            child.generateOutputSchema(db);
        }
        // @TODO MIKE - what should be an output schema for union?
        m_outputSchema = m_children.get(0).getOutputSchema();
   }
    
    @Override
    protected String explainPlanForNode(String indent) {
        return "UNION";
    }
    
    
}
