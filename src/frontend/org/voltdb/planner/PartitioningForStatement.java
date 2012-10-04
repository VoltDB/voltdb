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
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.TupleValueExpression;

/**
 * Represents the partitioning of the data underlying a statement.
 * In the simplest case, this is pre-determined by the single-partition context of the statement
 * from a stored procedure annotation or a single-statement procedure attribute,
 * or implied by the presence of a partition key value for an ad hoc query.
 * In the more interesting case, a user can specify that a statement be run on all partitions,
 * but the semantics of the statement may indicate that the same result could be produced more optimally
 * by running it on a single partition based on some partition key, whether a statement parameter or a constant in the
 * text of the statement.
 * These cases arise both in queries and in (partitioned table) DML.
 * As a multi-partition statement is analyzed in the planner, this object is filled in with details regarding its
 * suitability for running correctly on a single partition.
 */
public class PartitioningForStatement {

    /**
     * This value can be provided any non-null value to force single-partition statement planning and
     * (at least currently) disabling any kind of analysis of single-partition suitability
     * (except to forbid single-partition execution of replicated table DML.
     * Currently most of these cases are taken at face value -- the only case where such single partitioning is second-guessed
     * is in DML for replicated tables where a single-partition write would corrupt the replication.
     * This is flagged as an error.  Otherwise, no attempt is made to validate that a single partition statement would
     * have the same result as the same query run on all partitions. That is for the caller to decide.
     */
    private final Object m_specifiedValue;
    /**
     * This value can be initialized to true to give the planner permission
     * to not only analyze whether the statement CAN be run single-partition,
     * but also to decide that it WILL and to alter the plan accordingly.
     * If initialized to false, the analysis is considered to be for advisory purposes only,
     * so the planner may not commit to plan changes specific to single-partition execution.
     */
    private final boolean m_lockIn;
    /**
     * Enables inference of single partitioning from statement.
     */
    private final boolean m_inferSP;
    /*
     * For partitioned table DML, caches the partitioning column for later matching with its prospective value.
     * If that value is constant or a parameter, SP is an option.
     */
    private Column m_partitionCol; // Not used in SELECT plans.
    /*
     * For a multi-partition statement that can definitely be run SP, this is a constant partitioning key value
     * inferred from the analysis (suitable for hashinating).
     * If null, SP may not be safe, or the partitioning may be based on something less obvious like a parameter or constant expression.
     */
    private Object m_inferredValue = null;
    /*
     * Any constant/parameter-based expressions found to be equality-filtering partitioning columns.
     */
    private final Set<AbstractExpression> m_inferredExpression = new HashSet<AbstractExpression>();
    /*
     * The actual number of partitioned table scans in the query (when supported, self-joins should count as multiple).
     */
    private int m_countOfPartitionedTables = -1;
    private Map<String, String> m_partitionColumnByTable = null;
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
     * If true, SP execution is strictly forbidden, even if requested.
     */
    private boolean m_replicatedTableDML = false;
    /*
     * The table and column name of a partitioning column, typically the first scanned, if there are more than one,
     * proposed in feedback messages for possible use in single-partitioning annotations and attributes.
     */
    private String m_fullColumnName;

    /**
     * @param specifiedValue non-null if only SP plans are to be assumed
     * @param lockInInferredPartitioningConstant true if MP plans should be automatically optimized for SP where possible
     */
    public PartitioningForStatement(Object specifiedValue, boolean lockInInferredPartitioningConstant, boolean inferSP) {
        m_specifiedValue = specifiedValue;
        m_lockIn = lockInInferredPartitioningConstant;
        m_inferSP = inferSP;
    }

    /**
     * accessor
     */
    public boolean wasSpecifiedAsSingle() {
        return m_specifiedValue != null;
    }


    /**
     * smart accessor that doesn't allow contradiction of an original non-null value setting.
     */
    public void setInferredValue(Object best) {
        if (m_specifiedValue != null) {
            // The only correct value is the one that was specified.
            // TODO: A later implementation may support gentler "validation" of a specified partitioning value
            assert(m_specifiedValue.equals(best));
            return;
        }
        m_inferredValue = best;
    }

    /**
     * @param string table.column name of a(nother) equality-filtered partitioning column
     * @param constExpr -- a constant/parameter-based expression that equality-filters the partitioning column
     */
    public void addPartitioningExpression(String fullColumnName, AbstractExpression constExpr) {
        if (m_fullColumnName == null) {
            m_fullColumnName = fullColumnName;
        }
        m_inferredExpression.add(constExpr);
    }

    /**
     * smart accessor giving precedence to the constructor-specified value over any inferred value
     * @return
     */
    public Object effectivePartitioningValue() {
        if (m_specifiedValue != null) {
            // For now, the only correct value is the one that was specified.
            // TODO: A later implementation may support gentler "validation" of a specified partitioning value
            assert(m_inferredValue == null);
            return m_specifiedValue;
        }
        if (m_lockIn) {
            return m_inferredValue;
        }
        return null;
    }

    /**
     * accessor
     * @return
     */
    public Object inferredPartitioningValue() {
        return m_inferredValue;
    }

    /**
     * @param countOfPartitionedTables actually, the number of table scans, if there are self-joins
     * @param countOfPartitionedTables2
     */
    public void setPartitionedTables(Map<String,String> partitionColumnByTable, int countOfPartitionedTables) {
        // Should only be set once, early on.
        assert(m_countOfPartitionedTables == -1);
        m_partitionColumnByTable = partitionColumnByTable;
        m_countOfPartitionedTables = countOfPartitionedTables;
        m_countOfIndependentlyPartitionedTables = countOfPartitionedTables; // Initial guess -- as if no equality filters.

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
        return m_replicatedTableDML;
    }

    /**
     * @param replicatedTableDML
     */
    public void setIsReplicatedTableDML(boolean replicatedTableDML) {
        m_replicatedTableDML = replicatedTableDML;
    }

    /**
     * accessor
     * @return
     */
    public String getFullColumnName() {
        return m_fullColumnName;
    }

    /**
     * smart accessor
     */
    public boolean hasPartitioningConstantLockedIn() {
        return m_lockIn && (m_inferredValue != null);
    }

    /**
     * accessor
     * @param partitioncolumn
     */
    public void setPartitioningColumn(Column partitioncolumn) {
        if (m_inferSP) {
            m_partitionCol = partitioncolumn; // Not used in SELECT plans.
        }

    }

    /**
     * @return
     */
    public Column getColumn() {
        return m_partitionCol;
    }

    /**
     * Given the query's list of tables and its collection(s) of equality-filtered columns and their equivalents,
     * determine whether all joins involving partitioned tables can be executed locally on a single partition.
     * This is only the case when they include equality comparisons between partition key columns.
     * VoltDb will reject joins of multiple partitioned tables unless all their partition keys are
     * constrained to be equal to each other.
     * Example: select * from T1, T2 where T1.ID = T2.ID
     * Additionally, in this case, there may be a constant equality filter on any of the columns,
     * which we want to extract as our SP partitioning parameter.
     *
     * @param tableList The tables.
     * @param valueEquivalence Their column equality filters
     */
    public void analyzeForMultiPartitionAccess(ArrayList<Table> tableList, HashMap<AbstractExpression, Set<AbstractExpression>> valueEquivalence) {
        TupleValueExpression tokenPartitionKey = null;
        Set< Set<AbstractExpression> > eqSets = new HashSet< Set<AbstractExpression> >();
        int unfilteredPartitionKeyCount = 0;

        // Iterate over the tables to collect partition columns.
        for (Table table : tableList) {
            // Replicated tables don't need filter coverage.
            if (table.getIsreplicated()) {
                continue;
            }

            String partitionedTable = table.getTypeName();
            String columnNeedingCoverage = m_partitionColumnByTable.get(partitionedTable);
            boolean unfiltered = true;

            for (AbstractExpression candidateColumn : valueEquivalence.keySet()) {
                if ( ! (candidateColumn instanceof TupleValueExpression)) {
                    continue;
                }
                TupleValueExpression candidatePartitionKey = (TupleValueExpression) candidateColumn;
                if ( ! candidatePartitionKey.getTableName().equals(partitionedTable)) {
                    continue;
                }
                if ( ! candidatePartitionKey.getColumnName().equals(columnNeedingCoverage)) {
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
                    addPartitioningExpression(tokenPartitionKey.getTableName() + '.' + tokenPartitionKey.getColumnName(), constExpr);
                    Object partitioningObject = ConstantValueExpression.extractPartitioningValue(tokenPartitionKey.getValueType(), constExpr);
                    setInferredValue(partitioningObject);
                    // Only need one constant value.
                    break;
                }
            }
        }
    }
}
