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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.ExpressionType;
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
    ArrayList<AbstractExpression> indexExprs = new ArrayList<AbstractExpression>();
    ArrayList<AbstractExpression> endExprs = new ArrayList<AbstractExpression>();
    ArrayList<AbstractExpression> otherExprs = new ArrayList<AbstractExpression>();
    ArrayList<AbstractExpression> joinExprs = new ArrayList<AbstractExpression>();

    /**
     * Given a specific join order and access path set for that join order, determine whether
     * all join expressions involving distributed tables can be executed on a single partition.
     * This is only the case when they include equality comparisons between partition columns.
     * Example: select * from T1, T2 where T1.ID = T2.ID
     * Additionally, in this case, there may be a constant equality filter on any of the columns,
     * which we want to extract as our SP partitioning parameter.
     *
     * @param joinOrder An array of tables in a specific join order.
     * @param accessPath An array of access paths that match with the input tables.
     *        As a side effect, these get marked as requiring a Send/Receive if they use partitioned tables that
     *        aren't equality filtered on their partitioning column.
     * @param bindingOut - the second element of this array is set to the constant (if any) by which ALL partition
     *        keys are (at least transitively) equality filtered.
     * @return true if all partitioned tables are joined on a respective partition key, false otherwise
     */
    static int tagForMultiPartitionAccess(Table[] joinOrder, AccessPath[] accessPath, Object[] bindingOut) {
        //Collect all index and join expressions from the accessPath for each table
        TupleValueExpression firstPartitionKeyScanned = null;
        Set<TupleValueExpression> eqPartitionKeySet = new HashSet<TupleValueExpression>();
        int result = 0;
        Object bindingObject = null;

        // Work backwards through the join order list to simulate the scanning order
        for (int i = accessPath.length-1; i >=0; --i) {
            Table table = joinOrder[i];

            // Replicated tables don't need filter coverage.
            String partitionedTable = null;
            String columnNeedingCoverage = null;
            boolean columnCovered = true;
            boolean columnWasCovered = true;

            // Iterate over the tables to collect partition columns.
            if ( ! table.getIsreplicated()) {
                if (table.getMaterializer() != null) {
                    // TODO: Not yet supporting materialized views.
                    // This will give a false negative for a join against a materialized view
                    // whose underlying table is partitioned by (one of the) view's GROUP BY columns.
                    // It should not be that difficult to parse that info out of the view definition OR
                    // to tag the properly grouped view in the catalog as having that same partition column.
                    accessPath[i].requiresSendReceive = true;
                    result++;
                    continue;
                }
                Column partitionCol = table.getPartitioncolumn();
                if (partitionCol == null) {
                    // This is an obscure edge case exercised far too regularly by lazy unit tests.
                    // The table is declared non-replicated yet specifies no partitioning column.
                    // One interpretation of this edge case is that the table has "randomly distributed data".
                    // This is a failure case for the purposes of this function.
                    accessPath[i].requiresSendReceive = true;
                    result++;
                    continue;
                }
                partitionedTable = table.getTypeName();
                columnNeedingCoverage = partitionCol.getTypeName();
                columnCovered = false;
            }

            List<AbstractExpression> expressions = new ArrayList<AbstractExpression>();
            expressions.addAll(accessPath[i].joinExprs);
            expressions.addAll(accessPath[i].otherExprs);

            for (AbstractExpression expr : expressions) {
                // Ignore expressions that are not of COMPARE_EQUAL type
                if (expr.getExpressionType() != ExpressionType.COMPARE_EQUAL) {
                    continue;
                }

                // Left and right subexpressions must be TVE based on the partition columns or
                // a unique constant-based or parameter-based expression.
                TupleValueExpression tveExpr = null;
                AbstractExpression leftExpr = expr.getLeft();
                AbstractExpression constExpr = null;
                boolean needsRelevanceCheck = false;
                columnWasCovered = columnCovered;
                if (leftExpr instanceof TupleValueExpression) {
                    tveExpr = (TupleValueExpression) leftExpr;
                    if (partitionedTable != null &&
                        tveExpr.getTableName().equals(partitionedTable) &&
                        tveExpr.getColumnName().equals(columnNeedingCoverage)) {
                        columnCovered = true;
                        if (firstPartitionKeyScanned == null) {
                            firstPartitionKeyScanned = tveExpr;
                            // Start the collection of columns that are equal to each other (at least one of which is a partition key column).
                            eqPartitionKeySet.add(tveExpr);
                        } else {
                            needsRelevanceCheck = true;
                        }
                    }
                    constExpr = expr.getRight();
                    if (constExpr instanceof TupleValueExpression) {
                        // Actually, RHS is not a constant at all -- "column1 = column2".
                        // XXX: For the case of self-joins, there MUST be some way (missed here) to distinguish a TVE based on the
                        // current (Access Path's) occurrence of (i.e. "range over") the table and a TVE based on another
                        // (Access Path's) use of that same table.
                        // As a result of this miss, we may report a false positive for odd edge cases like:
                        // "select A.*, B.* from SameTable A, SameTable B where A.partitionColumn = B.nonPartitionColumn".
                        // Here the A.partitionKey could get mistaken for a filter on B.partitionKey because all that we are testing for
                        // is whether any filter on (Access Path) B references the partition column of B's underlying table.
                        TupleValueExpression tve2 = (TupleValueExpression) constExpr;
                        if (partitionedTable != null &&
                            tve2.getTableName().equals(partitionedTable) &&
                            tve2.getColumnName().equals(columnNeedingCoverage)) {
                            columnCovered = true;
                            if (firstPartitionKeyScanned == null) {
                                firstPartitionKeyScanned = tve2;
                                // Start the collection of columns that are equal to each other (at least one of which is a partition key column).
                                eqPartitionKeySet.add(tve2);
                                eqPartitionKeySet.add(tveExpr);
                                continue;
                            }
                            needsRelevanceCheck = true;
                        } else if (firstPartitionKeyScanned == tveExpr) {
                            // Add to the collection of columns that are equal to each other (but possibly not equal to other sets of columns).
                            eqPartitionKeySet.add(tve2);
                            continue;
                        } else if (needsRelevanceCheck) {
                            // one TVE is the partition key for this table, but DOES this equality link it in with other partition keys?
                            if (eqPartitionKeySet.contains(tveExpr)) {
                                // Yes, another equivalent column.
                                eqPartitionKeySet.add(tve2);
                                continue;
                            }
                            if (eqPartitionKeySet.contains(tve2)) {
                                // Yes, another equivalent column.
                                eqPartitionKeySet.add(tveExpr);
                                continue;
                            }
                            // Neither of the equivalent columns, one of which is a partition key,
                            // has any connection (YET!) to a previously scanned partition key.
                            // This is probably going to require a send-receive on this scan, so don't consider the column now "covered".
                            columnCovered = columnWasCovered;
                        }
                        // XXX: We completely ignore TVE to TVE equalities that seem irrelevant (so far) at the risk of not recognizing their
                        // contribution in strange edge cases like the contribution of "A.x = A.partitionKey"
                        // in strange edge cases like "A.x = A.partitionKey and A.x = B.partitionKey"
                        // Or the contribution of "A.x = A.y" in strange edge cases like
                        // "A.x = A.y and A.y = A.partitionKey and A.x = B.partitionKey"
                        // The alternative would be to maintain equivalence sets of the table's TVEs.
                        // Similarly, we could track all constant-filtered TVEs in case a later TVE to TVE equality made them relevant
                        // to the partition key, and possibly to the first partition key.
                        continue;
                    }
                } else {
                    // Not "column = constant" -- try "constant = column"
                    AbstractExpression rightExpr = expr.getRight();
                    if ( ! (rightExpr instanceof TupleValueExpression)) {
                        continue; // ... no, neither side is a column.
                    }
                    // fall through with the LHS and RHS roles reversed
                    constExpr = tveExpr;
                    tveExpr = (TupleValueExpression) rightExpr;
                    if (partitionedTable != null &&
                        tveExpr.getTableName().equals(partitionedTable) &&
                        tveExpr.getColumnName().equals(columnNeedingCoverage)) {
                        columnCovered = true;
                    }
                    if (partitionedTable != null &&
                        tveExpr.getTableName().equals(partitionedTable) &&
                        tveExpr.getColumnName().equals(columnNeedingCoverage)) {
                        columnCovered = true;
                        if (firstPartitionKeyScanned == null) {
                            firstPartitionKeyScanned = tveExpr;
                            // Start the collection of columns that are equal to each other (at least one of which is a partition key column).
                            eqPartitionKeySet.add(tveExpr);
                        } else {
                            needsRelevanceCheck = true;
                        }
                    }
                }

                if (constExpr.hasAnySubexpressionOfClass(TupleValueExpression.class)) {
                    columnCovered = columnWasCovered;
                    continue; // expression is based on column values, so is not a suitable constant.
                }

                if (eqPartitionKeySet.contains(tveExpr)) {
                    // the TVE is equal to a constant AND to the first partition key.
                    // By implication, the partition key must have a constant value.
                    if (bindingObject == null) {
                        bindingObject = extractPartitioningValue(tveExpr, constExpr);
                    }
                    continue;
                } else if (needsRelevanceCheck) {
                    if (bindingObject != null) {
                        Object altBinding = extractPartitioningValue(tveExpr, constExpr);
                        if (altBinding.equals(bindingObject)) {
                            // Equality to a common constant works as TVE equality.
                            eqPartitionKeySet.add(tveExpr);
                            continue;
                        }
                    }
                }
                // Apparently an irrelevant filter.
                columnCovered = columnWasCovered;
            }

            // All expressions for this access path have been considered.
            // Any partition column better have been filtered by an equality by now.

            if ( ! columnCovered) {
                accessPath[i].requiresSendReceive = true;
                result++;
            }
        }
        bindingOut[1] = bindingObject;
        // It's only a little strange to sometimes be set bindingOut[1] to a non-null value even when returning true.
        // What it means in that case is that the query could IN THEORY distribute the results of a multi-partition
        // scan to the single partition designated by the bindingObject for a final join with that partition's local
        // data. This currently unsupported edge case is currently promptly detected and kept out of the candidate plans.
        // But THIS code, anyway, stands ready.
        return result;
    }

    public static Object extractPartitioningValue(TupleValueExpression tveExpr, AbstractExpression constExpr) {
        // TODO: There is currently no way to pass back as a partition key value
        // the constant value resulting from a general constant expression such as
        // "WHERE a.pk = b.pk AND b.pk = SQRT(3*3+4*4)" because the planner has no expression evaluation capabilities.
        if (constExpr instanceof ConstantValueExpression) {
            // ConstantValueExpression exports its value as a string, which is handy for serialization,
            // but the hashinator wants a partition-key-column-type-appropriate value.
            // For safety, don't trust the constant's type
            // -- it's apparently comparable to the column, but may not be an exact match(?).
            // XXX: Actually, there may need to be additional filtering in the code above to not accept
            // constant equality filters that would require the COLUMN type to be non-trivially converted (?)
            // -- it MAY not be safe to limit execution of such a filter on any single partition.
            // For now, for partitioning purposes, leave constants for string columns as they are,
            // and process matches for integral columns via constant-to-string-to-bigInt conversion.
            String stringValue = ((ConstantValueExpression) constExpr).getValue();
            if (tveExpr.getValueType().isInteger()) {
                try {
                    return new Long(stringValue);
                } catch (NumberFormatException nfe) {
                    // Disqualify this constant by leaving objValue null -- probably should have caught this earlier?
                    // This causes the statement to fall back to being identified as multi-partition.
                }
            } else {
                return stringValue;
            }
        }
        return null;
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
