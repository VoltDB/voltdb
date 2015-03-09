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

import java.util.ArrayList;

import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

public class AccessPath {
    Index index = null;
    IndexUseType use = IndexUseType.COVERING_UNIQUE_EQUALITY;
    boolean nestLoopIndexJoin = false;
    boolean requiresSendReceive = false;
    boolean keyIterate = false;
    IndexLookupType lookupType = IndexLookupType.EQ;
    SortDirectionType sortDirection = SortDirectionType.INVALID;
    // The initial expression is needed to adjust (forward) the start of the reverse
    // iteration when it had to initially settle for starting at
    // "greater than a prefix key".
    final ArrayList<AbstractExpression> initialExpr = new ArrayList<AbstractExpression>();
    final ArrayList<AbstractExpression> indexExprs = new ArrayList<AbstractExpression>();
    final ArrayList<AbstractExpression> endExprs = new ArrayList<AbstractExpression>();
    final ArrayList<AbstractExpression> otherExprs = new ArrayList<AbstractExpression>();
    final ArrayList<AbstractExpression> joinExprs = new ArrayList<AbstractExpression>();
    final ArrayList<AbstractExpression> bindings = new ArrayList<AbstractExpression>();

    @Override
    public String toString() {
        String retval = "";

        retval += "INDEX: " + ((index == null) ? "NULL" : (index.getParent().getTypeName() + "." + index.getTypeName())) + "\n";
        retval += "USE:   " + use.toString() + "\n";
        retval += "TYPE:  " + lookupType.toString() + "\n";
        retval += "DIR:   " + sortDirection.toString() + "\n";
        retval += "ITER?: " + String.valueOf(keyIterate) + "\n";
        retval += "NLIJ?: " + String.valueOf(nestLoopIndexJoin) + "\n";

        retval += "IDX EXPRS:\n";
        int i = 0;
        for (AbstractExpression expr : indexExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        retval += "END EXPRS:\n";
        i = 0;
        for (AbstractExpression expr : endExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        retval += "OTHER EXPRS:\n";
        i = 0;
        for (AbstractExpression expr : otherExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        retval += "JOIN EXPRS:\n";
        i = 0;
        for (AbstractExpression expr : joinExprs)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        return retval;
    }
}
