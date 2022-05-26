/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.exceptions.ValidationError;
import org.voltdb.types.ExpressionType;

import com.google_voltpatches.common.collect.ImmutableSortedMap;

/**
 *
 */
public class HashRangeExpression extends AbstractValueExpression {

    public enum Members {
        HASH_COLUMN,
        RANGES,
        RANGE_START,
        RANGE_END
    }

    protected ImmutableSortedMap<Integer, Integer> m_ranges;
    protected int m_hashColumn = Integer.MIN_VALUE;

    public HashRangeExpression() {
        super(ExpressionType.HASH_RANGE);
        //See the comment in ConjunctionExpression
        setValueType(VoltType.BOOLEAN);
    }

    @Override
    public void validate() {
        super.validate();

        if ((m_right != null) || (m_left != null)) {
            throw new ValidationError("A Hash Range expression has child expressions for '%s'", toString());
        } else if (m_hashColumn == Integer.MIN_VALUE) {
            throw new ValidationError("A Hash Range has no hash column set for '%s'", toString());
        } else if (m_ranges == null) {
           throw new ValidationError("A Hash Range has no ranges set for '%s'", toString());
       }
    }

    /**
     * @return the column index to hash
     */
    public Integer getHashColumnIndex() {
        return m_hashColumn;
    }

    /**
     * @param columnIndex The index of the column to set
     */
    public void setHashColumnIndex(Integer hashColumnIndex) {
        m_hashColumn = hashColumnIndex;
    }

    /**
     * @return the ranges
     */
    public Map<Integer, Integer> getRanges() {
        return m_ranges;
    }

    /**
     * @param ranges the column_alias to set
     */
    public void setRanges(Map<Integer, Integer> ranges) {
        m_ranges = ImmutableSortedMap.copyOf(ranges);
    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof HashRangeExpression)) {
            return false;
        }
        HashRangeExpression expr = (HashRangeExpression) obj;

        if ((m_ranges == null) != (expr.m_ranges == null)) {
            return false;
        }
        if ((m_hashColumn == Integer.MIN_VALUE) != (expr.m_hashColumn == Integer.MIN_VALUE)) {
            return false;
        }
        if (m_ranges != null) { // Implying both sides non-null
            if (! m_ranges.equals(expr.m_ranges)) {
                return false;
            }
        }
        if (m_hashColumn != Integer.MIN_VALUE) { // Implying both sides non-null
            return m_hashColumn == expr.m_hashColumn;
        }
        return true;
    }

    @Override
    public int hashCode() {
        // based on implementation of equals
        int result = 0;
        if (m_ranges != null) {
            result += m_ranges.hashCode();
        }
        if (m_hashColumn != Integer.MIN_VALUE) {
            result += m_hashColumn;
        }
        // defer to the superclass, which factors in other attributes
        return result += super.hashCode();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.HASH_COLUMN.name(), m_hashColumn);
        stringer.key(Members.RANGES.name()).array();
        for (Map.Entry<Integer, Integer> e : m_ranges.entrySet()) {
            stringer.object();
            stringer.keySymbolValuePair(Members.RANGE_START.name(), e.getKey());
            stringer.keySymbolValuePair(Members.RANGE_END.name(), e.getValue());
            stringer.endObject();
        }
        stringer.endArray();
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj) throws JSONException {
        m_hashColumn = obj.getInt(Members.HASH_COLUMN.name());
        JSONArray array = obj.getJSONArray(Members.RANGES.name());
        ImmutableSortedMap.Builder<Integer, Integer> b = ImmutableSortedMap.naturalOrder();
        for (int ii = 0; ii < array.length(); ii++) {
            JSONObject range = array.getJSONObject(ii);
            b.put(range.getInt(Members.RANGE_START.name()), range.getInt(Members.RANGE_END.name()));
        }
        m_ranges = b.build();
    }

    @Override
    public String explain(String impliedTableName) {
        return "hash range";
    }
}
