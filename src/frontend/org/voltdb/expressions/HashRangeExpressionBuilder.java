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

package org.voltdb.expressions;

import java.util.Map;

import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.Ordering;

/**
 * Convenience class to generate a HashRangeExpression.
 */
public class HashRangeExpressionBuilder {

    /**
     * Constructor.
     */
    public HashRangeExpressionBuilder() {
    }

    /**
     * Add a value pair.
     * @param value1
     * @param value2
     */
    public HashRangeExpressionBuilder put(Integer value1, Integer value2) {
        m_builder.put(value1, value2);
        return this;
    }

    /**
     * Generate a hash range expression.
     * @return  hash range expression
     */
    public HashRangeExpression build(Integer hashColumnIndex) {
        Map<Integer, Integer> ranges = m_builder.build();
        HashRangeExpression predicate = new HashRangeExpression();
        predicate.setRanges(ranges);
        predicate.setHashColumnIndex(hashColumnIndex);
        return predicate;
    }

    /// Builder object that produces immutable maps.
    private ImmutableSortedMap.Builder<Integer, Integer> m_builder = new ImmutableSortedMap.Builder<Integer, Integer>(Ordering.natural());
}
