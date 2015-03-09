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

package org.voltdb.planner;

import java.util.*;
import org.voltdb.expressions.AbstractExpression;

/**
 * The EEP examines all expressions in an AbstractParsedStmt and links
 * equivalence relations together. For example, if (A=B, and B=C), this
 * code can rewrite these expressions as (A=C and C=B) or (C=A and B=A).
 * The goal is to generate all possible ways to satisfy the expressions
 * and return a set of AbstractParsedStmts that all satisfy the expressions
 * in different ways.
 *
 */
public class ExpressionEquivalenceProcessor {

    /** The types of things that can be equivalent to each other */
    enum EEPValueType { COLUMN, PARAMETER, CONSTANT, OPAQUE_EXPRESSION }

    /** The core type in EEP represents a uniquely identifiable value */
    class EEPValue {
        EEPValueType type;
        AbstractExpression expr;
    }

    /**
     * @param stmt The original statement containing a valid set of expressions.
     * @return A set of logically equivalent stmt structures with differing
     * expressions.
     */
    static List<AbstractParsedStmt> getEquivalentStmts(AbstractParsedStmt stmt) {

        // The main data structure in the EEP. Maps values to the thing they are equal to.
        //Map<EEPValue, Set<EEPValue>> equivalenceMap = new HashMap<EEPValue, Set<EEPValue>>();

        //Map<AbstractExpression, EEPValue> values = new HashMap<AbstractExpression, EEPValue>();

        ArrayList<AbstractParsedStmt> retval = new ArrayList<AbstractParsedStmt>();
        retval.add(stmt);
        return retval;
    }

}
