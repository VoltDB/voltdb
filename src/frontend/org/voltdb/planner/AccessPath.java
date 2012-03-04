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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Column;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

public class AccessPath {
    Index index = null;
    IndexUseType use = IndexUseType.COVERING_UNIQUE_EQUALITY;
    boolean nestLoopIndexJoin = false;
    boolean keyIterate = false;
    IndexLookupType lookupType = IndexLookupType.EQ;
    SortDirectionType sortDirection = SortDirectionType.INVALID;
    ArrayList<AbstractExpression> indexExprs = new ArrayList<AbstractExpression>();
    ArrayList<AbstractExpression> endExprs = new ArrayList<AbstractExpression>();
    ArrayList<AbstractExpression> otherExprs = new ArrayList<AbstractExpression>();
    ArrayList<AbstractExpression> joinExprs = new ArrayList<AbstractExpression>();

    /**
     * Given a specific join order and access path set for that join order, determine whether
     * all join expressions involving distributed tables are simple equality comparison between
     * partition columns. Example: select * from T1, T2 where T1.ID = T2.ID
     *
     * @param joinOrder An array of tables in a specific join order.
     * @param accessPath An array of access paths that match with the input tables.
     * @return true if all tables are joined on a respective partition key, false otherwise
     */
    static boolean isPartitionKeyEquality(Table[] joinOrder, AccessPath[] accessPath) {
        if (joinOrder.length < 2)
            // nothing to join
            return false;

        //Collect all index and join expressions from the accessPath for each table
        List<AbstractExpression> expressions = new ArrayList<AbstractExpression>();
        for (int i = 0; i < accessPath.length; ++i) {
            expressions.addAll(accessPath[i].joinExprs);
            expressions.addAll(accessPath[i].indexExprs);
        }
        // The total expression count should be one less than the tables count
        if (expressions.size() != joinOrder.length - 1)
            return false;

        //Iterate over the tables to collect partition columns
        Map<String, String> partitionMap = new HashMap<String, String>();
        for (Table t : joinOrder) {
            if (!t.getIsreplicated()) {
                Column partition = t.getPartitioncolumn();
                assert(partition != null);
                partitionMap.put(t.getTypeName(), partition.getTypeName());
            } else
                partitionMap.put(t.getTypeName(), null);
        }

        boolean joinOnPartition = true;
        Set<String> seenTables = new HashSet<String>();
        for (AbstractExpression expr : expressions) {
            //Expression must be of COMPARE_EQUAL type
            if (expr.getExpressionType() != ExpressionType.COMPARE_EQUAL) {
                joinOnPartition = false;
                break;
            }

            // Left and right subexpressions must be TVE based on the partition columns
            AbstractExpression[] subs = new AbstractExpression[2];
            subs[0] = expr.getLeft();
            subs[1] = expr.getRight();
            for (AbstractExpression sub : subs) {
                if (sub == null || !(sub instanceof TupleValueExpression)) {
                    joinOnPartition = false;
                    break;
                }
                TupleValueExpression tve = (TupleValueExpression) sub;
                // by now table must be in the map
                assert(partitionMap.containsKey(tve.getTableName()));
                String c = partitionMap.get(tve.getTableName());
                if (c != null && !tve.getColumnName().equals(c)) {
                    joinOnPartition = false;
                    break;
                }
                seenTables.add(tve.getTableName());
            }
            if (!joinOnPartition)
                break;
        }
        //Last check. All input tables must be part of some join expression
        if (seenTables.size() != joinOrder.length)
            joinOnPartition = false;

        return joinOnPartition;
    }

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
