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

import org.voltdb.expressions.AbstractExpression;

/**
 *
 */
public class PartitioningForStatement {

    private final Object m_specifiedValue;
    private Object m_inferredValue = null;
    // Any constant/parameter-based expressions found to be equality-filtering partitioning columns.
    // In the best case scenario (single partition statement),
    // this is a singleton identifying a simple constant or parameter.
    private final Set<AbstractExpression> m_inferredExpression = new HashSet<AbstractExpression>();


    /**
     * @param m_specifiedValue
     */
    public PartitioningForStatement(Object specifiedValue) {
        m_specifiedValue = specifiedValue;
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
     * @param constExpr -- a constant/parameter-based expression that equality-filters any partitioning column
     */
    public void addPartitioningExpression(AbstractExpression constExpr) {
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

}
