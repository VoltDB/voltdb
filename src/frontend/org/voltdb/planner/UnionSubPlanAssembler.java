/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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


package org.voltdb.planner;

import org.voltdb.catalog.Database;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.UnionPlanNode;

/**
 * For a union plan, this class builds the part of the plan
 * which collects tuples from relations. Given the tables and the predicate
 * (and sometimes the output columns), this will build a plan that will output
 * matching tuples to a temp table. A delete, update or send plan node can then
 * be glued on top of it. In selects, aggregation and other projections are also
 * done on top of the result from this class.
 *
 */
public class UnionSubPlanAssembler extends SubPlanAssembler {
    /** Flag to signal the end of plan generation*/
    boolean m_done = false;
    /** Union type */
    ParsedUnionStmt.UnionType m_unionType = ParsedUnionStmt.UnionType.NOUNION;

    /**
    *
    * @param db The catalog's Database object.
    * @param parsedStmt The parsed and dissected statement object describing the sql to execute.
    * @param m_partitioning in/out param first element is partition key value, forcing a single-partition statement if non-null,
    * second may be an inferred partition key if no explicit single-partitioning was specified
    */
    UnionSubPlanAssembler(Database db, AbstractParsedStmt parsedStmt, PartitioningForStatement partitioning)
    {
       super(db, parsedStmt, partitioning);
       assert (parsedStmt instanceof ParsedUnionStmt);
       ParsedUnionStmt unionStmt = (ParsedUnionStmt)parsedStmt;
       m_unionType = unionStmt.m_unionType;
    }

    /**
     * So far, union plan is always represented by a single UnionPlanNode.
     */
    @Override
    AbstractPlanNode nextPlan() {
        // Since all plans are the same this method should be called only once
        if (m_done == true) {
            return null;
        }
        m_done = true;
        // Simply return an union plan node with a corresponding union type set
        return new UnionPlanNode(m_unionType);
    }

}
