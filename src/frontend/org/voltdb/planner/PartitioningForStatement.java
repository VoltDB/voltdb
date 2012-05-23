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

import java.util.HashSet;
import java.util.Set;

import org.voltdb.catalog.Column;
import org.voltdb.expressions.AbstractExpression;

/**
 *
 */
public class PartitioningForStatement {

    private final Object m_specifiedValue;
    private final boolean m_lockIn;
    private Column m_partitionCol; // Not used in SELECT plans.
    private Object m_inferredValue = null;
    // Any constant/parameter-based expressions found to be equality-filtering partitioning columns.
    // In the best case scenario (single partition statement),
    // this is a singleton identifying a simple constant or parameter.
    private final Set<AbstractExpression> m_inferredExpression = new HashSet<AbstractExpression>();
    private int m_countOfPartitionedTables = -1;
    private int m_countOfIndependentlyPartitionedTables = -1;
    private boolean m_replicatedTableDML = false;
    private String m_fullColumnName;

    /**
     * @param lockInInferredPartitioningConstant
     * @param m_specifiedValue
     */
    public PartitioningForStatement(Object specifiedValue, boolean lockInInferredPartitioningConstant) {
        m_specifiedValue = specifiedValue;
        m_lockIn = lockInInferredPartitioningConstant;
    }

    /**
     *
     */
    public boolean wasSpecifiedAsSingle() {
        return m_specifiedValue != null;
    }


    /**
     *
     */
    public void setEffectiveValue(Object best) {
        if (m_specifiedValue != null) {
            // The only correct value is the one that was specified.
            // TODO: A later implementation may support gentler "validation" of a specified partitioning value
            assert(m_specifiedValue.equals(best));
            return;
        }
        m_inferredValue = best;
    }

    /**
     * @param string
     * @param constExpr -- a constant/parameter-based expression that equality-filters any partitioning column
     */
    public void addPartitioningExpression(String fullColumnName, AbstractExpression constExpr) {
        if (m_fullColumnName == null) {
            m_fullColumnName = fullColumnName;
        }

        m_inferredExpression.add(constExpr);
    }

    /**
     * @return
     */
    public Object effectivePartitioningValue() {
        if (m_specifiedValue != null) {
            // For now, the only correct value is the one that was specified.
            // TODO: A later implementation may support gentler "validation" of a specified partitioning value
            assert(m_inferredValue == null);
            return m_specifiedValue;
        }
        return m_inferredValue;
    }

    /**
     * @param countOfPartitionedTables
     */
    public void setCountOfPartitionedTables(int countOfPartitionedTables) {
        // Should only be set once, early on.
        assert(m_countOfPartitionedTables == -1);
        m_countOfPartitionedTables = countOfPartitionedTables;
        m_countOfIndependentlyPartitionedTables = countOfPartitionedTables; // Initial guess -- as if no equality filters.

    }

    /**
     */
    public int getCountOfPartitionedTables() {
        // Should always have been set, early on.
        assert(m_countOfPartitionedTables != -1);
        return m_countOfPartitionedTables;
    }

    /**
     * @param countOfPartitionedTables
     */
    public void setCountOfIndependentlyPartitionedTables(int countOfPartitionedTables) {
        m_countOfIndependentlyPartitionedTables = countOfPartitionedTables;
    }

    /**
     */
    public int getCountOfIndependentlyPartitionedTables() {
        return m_countOfIndependentlyPartitionedTables;

    }

    /**
     * @return
     */
    public AbstractExpression singlePartitioningExpression() {
        if (m_inferredExpression.size() == 1) {
            return m_inferredExpression.iterator().next();
        }
        return null;
    }

    /**
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
     * @return
     */
    public String getFullColumnName() {
        return m_fullColumnName;
    }

    public boolean hasPartitioningConstantLockedIn() {
        return m_lockIn && (m_inferredValue != null);
    }

    /**
     * @param partitioncolumn
     */
    public void setPartitioningColumn(Column partitioncolumn) {
        m_partitionCol = partitioncolumn; // Not used in SELECT plans.

    }

    /**
     * @return
     */
    public Column getColumn() {
        // TODO Auto-generated method stub
        return m_partitionCol;
    }
}
