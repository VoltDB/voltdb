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

package org.voltdb.planner;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.SchemaColumn;

/**
 * Represents the partitioning of the data underlying a statement.
 * In the simplest case, this is pre-determined by the single-partition context of the statement
 * from a stored procedure annotation or a single-statement procedure attribute.
 * In the more interesting ad hoc case, a user can specify that a statement be run on all partitions,
 * but the semantics of the statement may indicate that the same result could be produced more optimally
 * by running it on a single partition selected based on the hash of some partition key value,
 * whether a statement parameter or a constant in the text of the statement.
 * These cases arise both in queries and in (partitioned table) DML.
 * As a multi-partition statement is analyzed in the planner, this object is filled in with details
 * regarding its suitability for running correctly on a single partition.
 *
 * For a multi-fragment plan that contains a join,
 * is it better to send partitioned tuples and join them on the coordinator
 * or is it better to join them before sending?
 * If bandwidth (or capacity of the receiving temp table) were the primary concern,
 * a decision could be based on
 * A) how much wider the joined rows are than the pre-joined rows.
 * B) the expected yield of the join filtering -- does each pre-joined row typically
 *    match and get joined with multiple partner rows or does it typically fail to match
 *    any row.
 * The statistics required to determine "B" are not generally available.
 * In any case, there are two over-arching concerns.
 * One is the correct handling of a special case
 * -- a join of partitioned tables on their partition keys.
 * In this case, the join MUST happen on each partition prior to sending any tuples.
 * This restriction stems directly from the limitation that there can only be two fragments in a plan,
 * and that a fragment produces a single (intermediate or final) result table.
 * The "coordinator" receives the (one) intermediate result table and produces
 * the final result table. It can not receive tuples from two different partitioned tables.
 * The second over-arching consideration is that there is an optimization available to the
 * transaction processor for the special case in which a coordinator fragment does not need to
 * access any persistent local data (I learned this second hand from Izzy. --paul).
 * This provides further motivation to do all scanning and joining in the collector fragment
 * prior to sending tuples.
 *
 * These two considerations normally override all others,
 * so that all multi-partition plans only "send after all joins", regardless of bandwidth/capacity
 * considerations, but there remains some edge cases in which the decision MUST go the other way,
 * that is, sending tuples prior to joining on the coordinator.
 * This occurs for some OUTER JOINS between a replicated OUTER table and a partitioned INNER table as in:
 *
 * SELECT * FROM replicated R LEFT JOIN partitioned P ON ...;
 *
 * See the comment in SelectSubPlanAssembler.getSelectSubPlanForJoin
 */
public class StatementPartitioning implements Cloneable{
    /**
     * This value is only meaningful if m_inferPartitioning is false.
     * It can be set true to force single-partition statement planning and
     * to forbid single-partition planning/execution of replicated table DML.
     * Since that would corrupt the replication, it is flagged as an error.
     * Otherwise, no attempt is made to validate that a single partition statement would
     * have the same result as the same query run on all partitions.
     * It is up to the user to decide whether that is an issue.
     * It can be set to false to force multi-partition statement planning.
     * This MAY involve sub-optimal dispatch of fragments to partitions with no matching data.
     * Currently, even inserts into partitioned tables are allowed to successfully execute
     * on "wrong" partitions, but they are prevented at the lowest level from taking effect there.
     */
    private final boolean m_forceSP;

    /**
     * Enables inference of single partitioning from statement.
     */
    private final boolean m_inferPartitioning;
    /*
     * For partitioned table DML, caches the partitioning column for later matching with its prospective value.
     * If that value is constant or a parameter, SP is an option.
     */
    private Column m_partitionColForDML; // Not used in SELECT plans.
    /*
     * For a multi-partition statement that can definitely be run SP, this is a constant partitioning key value
     * inferred from the analysis (suitable for hashinating).
     * If null, SP may not be safe, or the partitioning may be based on something less obvious like a parameter or constant expression.
     */
    private Object m_inferredValue = null;
    private int m_inferredParameterIndex = -1;
    /*
     * Any constant/parameter-based expressions found to be equality-filtering partitioning columns.
     */
    private final Set<AbstractExpression> m_inferredExpression = new HashSet<AbstractExpression>();
    /*
     * The actual number of partitioned table scans in the query (when supported, self-joins should count as multiple).
     */
    private int m_countOfPartitionedTables = -1;
    /*
     * The number of independently partitioned table scans in the query. This is initially the same as
     * m_countOfPartitionedTables, but gets reduced by 1 each time a partitioned table (scan)'s partitioning column
     * is seen to be filtered by equality to a constant value or to a previously scanned partition column.
     * When the count is 0, the statement can be executed single-partition.
     * When the count is 1, multi-partition execution can join any number of tables in the collector plan fragment.
     * When the count is 2 or greater, the statement would require three or more fragments to execute, so is disallowed.
     */
    private int m_countOfIndependentlyPartitionedTables = -1;
    /*
     * If true, and the target table it replicated,
     * SP execution is strictly forbidden, even if requested.
     */
    private boolean m_isDML = false;
    /*
     * The table and column name of a partitioning column, typically the first scanned, if there are more than one,
     * proposed in feedback messages for possible use in single-partitioning annotations and attributes.
     */
    private String m_fullColumnName;

    /**
     * @param specifiedValue non-null if only SP plans are to be assumed
     * @param lockInInferredPartitioningConstant true if MP plans should be automatically optimized for SP where possible
     */
    private StatementPartitioning(boolean inferPartitioning, boolean forceSP) {
        m_inferPartitioning = inferPartitioning;
        m_forceSP = forceSP;
    }

    public static StatementPartitioning forceSP() {
        return new StatementPartitioning(false, true);
    }

    public static StatementPartitioning forceMP() {
        return new StatementPartitioning(false, false);
    }

    public static StatementPartitioning inferPartitioning() {
        return new StatementPartitioning(true, /* default to MP */ false);
    }


    public boolean isInferred() {
        return m_inferPartitioning;
    }

    /**
     * @return A new PartitioningForStatement
     */
    @Override
    public Object clone() {
        return new StatementPartitioning(m_inferPartitioning, m_forceSP);
    }

    /**
     * accessor
     */
    public boolean wasSpecifiedAsSingle() {
        return m_forceSP && ! m_inferPartitioning;
    }

    /**
     * @param string table.column name of a(nother) equality-filtered partitioning column
     * @param constExpr -- a constant/parameter-based expression that equality-filters the partitioning column
     */
    public void addPartitioningExpression(String fullColumnName, AbstractExpression constExpr,
            VoltType valueType) {
        if (m_fullColumnName == null) {
            m_fullColumnName = fullColumnName;
        }
        m_inferredExpression.add(constExpr);
        if (constExpr instanceof ParameterValueExpression) {
            ParameterValueExpression pve = (ParameterValueExpression)constExpr;
            m_inferredParameterIndex = pve.getParameterIndex();
        } else {
            m_inferredValue = ConstantValueExpression.extractPartitioningValue(valueType, constExpr);
        }
    }

    public Object getInferredPartitioningValue() {
        return m_inferredValue;
    }

    public int getInferredParameterIndex() {
        return m_inferredParameterIndex;
    }


    /**
     * accessor
     */
    public int getCountOfPartitionedTables() {
        // Should always have been set, early on.
        assert(m_countOfPartitionedTables != -1);
        return m_countOfPartitionedTables;
    }

    /**
     * accessor
     */
    public int getCountOfIndependentlyPartitionedTables() {
        return m_countOfIndependentlyPartitionedTables;

    }

    /**
     * Returns true if there exists a single partition expression
     */
    public boolean isInferredSingle() {
        return m_inferPartitioning &&
                (((m_countOfIndependentlyPartitionedTables == 0) && ! m_isDML)  ||
                        (m_inferredExpression.size() == 1));
    }

    /**
     * Returns true if the statement will require two fragments.
     */
    public boolean requiresTwoFragments() {
        if (m_inferPartitioning) {
            if (isInferredSingle()) {
                return false;
            }
        } else {
            if (m_forceSP || (m_countOfPartitionedTables == 0)) {
                return false;
            }
        }
        return true;
    }

    /**
     * smart accessor - only returns a value if it was unique
     * @return
     */
    public AbstractExpression singlePartitioningExpression() {
        if (m_inferredExpression.size() == 1) {
            return m_inferredExpression.iterator().next();
        }
        return null;
    }

    /**
     * accessor
     */
    public boolean getIsReplicatedTableDML() {
        return m_isDML && (m_countOfIndependentlyPartitionedTables == 0);
    }

    /**
     * @param parameter potentially enabling replicatedTableDML check
     */
    public void setIsDML() { m_isDML = true; }

    /**
     * accessor
     * @return
     */
    public String getFullColumnName() {
        return m_fullColumnName;
    }

    /**
     * accessor
     * @param partitioncolumn
     */
    public void setPartitioningColumnForDML(Column partitioncolumn) {
        if (m_inferPartitioning) {
            m_partitionColForDML = partitioncolumn; // Not used in SELECT plans.
        }
    }

    /**
     * @return
     */
    public Column getPartitionColForDML() {
        return m_partitionColForDML;
    }

    /**
     * Given the query's list of tables and its collection(s) of equality-filtered columns and their equivalents,
     * determine whether all joins involving partitioned tables can be executed locally on a single partition.
     * This is only the case when they include equality comparisons between partition key columns.
     * VoltDB will reject joins of multiple partitioned tables unless all their partition keys are
     * constrained to be equal to each other.
     * Example: select * from T1, T2 where T1.ID = T2.ID
     * Additionally, in this case, there may be a constant equality filter on any of the columns,
     * which we want to extract as our SP partitioning parameter.
     *
     * @param tableAliasList The tables.
     * @param valueEquivalence Their column equality filters
     * @return the number of independently partitioned tables
     *         -- partitioned tables that aren't joined or filtered by the same value.
     *         The caller can raise an alarm if there is more than one.
     */
    public int analyzeForMultiPartitionAccess(Collection<StmtTableScan> collection,
            HashMap<AbstractExpression, Set<AbstractExpression>> valueEquivalence)
    {
        TupleValueExpression tokenPartitionKey = null;
        Set< Set<AbstractExpression> > eqSets = new HashSet< Set<AbstractExpression> >();
        int unfilteredPartitionKeyCount = 0;

        // Iterate over the tables to collect partition columns.
        for (StmtTableScan tableScan : collection) {
            // Replicated tables don't need filter coverage.
            if (tableScan.getIsReplicated()) {
                continue;
            }

            // The partition column can be null in an obscure edge case.
            // The table is declared non-replicated yet specifies no partitioning column.
            // This can occur legitimately when views based on partitioned tables neglect to group by the partition column.
            // The interpretation of this edge case is that the table has "randomly distributed data".
            // In such a case, the table is valid for use by MP queries only and can only be joined with replicated tables
            // because it has no recognized partitioning join key.
            List<SchemaColumn> columnsNeedingCoverage = tableScan.getPartitioningColumns();

            if (tableScan instanceof StmtSubqueryScan) {
                StmtSubqueryScan subScan = (StmtSubqueryScan) tableScan;
                subScan.promoteSinglePartitionInfo(valueEquivalence, eqSets);
            }

            boolean unfiltered = true;
            for (AbstractExpression candidateColumn : valueEquivalence.keySet()) {
                if ( ! (candidateColumn instanceof TupleValueExpression)) {
                    continue;
                }
                TupleValueExpression candidatePartitionKey = (TupleValueExpression) candidateColumn;
                if (! canCoverPartitioningColumn(candidatePartitionKey, columnsNeedingCoverage)) {
                    continue;
                }
                unfiltered = false;
                if (tokenPartitionKey == null) {
                    tokenPartitionKey = candidatePartitionKey;
                }
                eqSets.add(valueEquivalence.get(candidatePartitionKey));
            }

            if (unfiltered) {
                ++unfilteredPartitionKeyCount;
            }
        }

        m_countOfIndependentlyPartitionedTables = eqSets.size() + unfilteredPartitionKeyCount;
        if ((unfilteredPartitionKeyCount == 0) && (eqSets.size() == 1)) {
            for (Set<AbstractExpression> partitioningValues : eqSets) {
                for (AbstractExpression constExpr : partitioningValues) {
                    if (constExpr instanceof TupleValueExpression) {
                        continue;
                    }
                    VoltType valueType = tokenPartitionKey.getValueType();
                    addPartitioningExpression(tokenPartitionKey.getTableName() +
                            '.' + tokenPartitionKey.getColumnName(), constExpr, valueType);
                    // Only need one constant value.
                    break;
                }
            }
        }

        return m_countOfIndependentlyPartitionedTables;
    }

    private static boolean canCoverPartitioningColumn(TupleValueExpression candidatePartitionKey,
            List<SchemaColumn> columnsNeedingCoverage) {
        if (columnsNeedingCoverage == null)
            return false;

        for (SchemaColumn col: columnsNeedingCoverage) {
            String partitionedTableAlias = col.getTableAlias();
            String columnNeedingCoverage = col.getColumnAlias();

            assert(candidatePartitionKey.getTableAlias() != null);
            if ( ! candidatePartitionKey.getTableAlias().equals(partitionedTableAlias)) {
                continue;
            }
            String candidateColumnName = candidatePartitionKey.getColumnName();
            if ( ! candidateColumnName.equals(columnNeedingCoverage)) {
                continue;
            }

            // Maybe need more checkings
            return true;
        }

        return false;
    }

    /**
     * @param tableCacheList
     * @throws PlanningErrorException
     */
    void analyzeTablePartitioning(Collection<StmtTableScan> collection)
            throws PlanningErrorException
    {
        m_countOfPartitionedTables = 0;
        // Do we have a need for a distributed scan at all?
        // Iterate over the tables to collect partition columns.
        for (StmtTableScan tableScan : collection) {
            if ( ! tableScan.getIsReplicated()) {
                ++m_countOfPartitionedTables;
            }
        }
        // Initial guess -- as if no equality filters.
        m_countOfIndependentlyPartitionedTables = m_countOfPartitionedTables;
    }

}
