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

package org.voltdb.planner;

import java.util.Arrays;


/**
 * BoundPlan is an immutable representation of a SQL execution plan.
 * It refers to a parameterized statement, and so it could apply to
 * multiple SQL literal strings before constants have been pulled out.
 * It also has an array of constant values for any bound parameters,
 * referenced positionally, that are required by the plan.
 * It is an element of the parameterized plan cache.
 */
public class BoundPlan {

    /** The plan itself. */
    public final CorePlan m_core;
    /** The parameter bindings required to apply the plan to the matching query. */
    public final String[] m_constants;

    public BoundPlan(CorePlan corePlan, String[] constantBindings)
    {
        m_core = corePlan;
        m_constants = constantBindings;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BOUND PLAN {");
        if (m_core != null) {
            sb.append("\n   CORE: ").append(m_core.toString());
        }
        if (m_constants != null) {
            sb.append("\n   BINDINGS: ").append(m_constants.toString());
        }
        sb.append("\n}");
        return sb.toString();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     *
     * Used mainly for debugging and for assertions.
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BoundPlan)) {
            return false;
        }
        BoundPlan other = (BoundPlan) obj;

        if ( ! m_core.equals(other.m_core)) {
            return false;
        }
        if (m_constants == null) {
            if (other.m_constants != null) {
                return false;
            }
        }
        else if ( ! Arrays.equals(m_constants, other.m_constants)) {
            return false;
        }
        return true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        assert false : "hashCode not designed";
        return 42; // any arbitrary constant will do
    }

    public boolean allowsParams(Object[] paramArray) {
        // A cached plan with no parameter binding requirements is an automatic match.
        if (m_constants == null) {
            return true;
        }
        if (m_constants.length > paramArray.length) {
            return false;
        }
        for (int ii = 0; ii < m_constants.length; ++ii) {
            // Only need to check "bound" parameters.
            if (m_constants[ii] != null) {
                Object param = paramArray[ii];
                if (param == null) {
                    return false;
                }
                if ( ! m_constants[ii].equals(param.toString())) {
                    return false;
                }
            }
        }
        // All bound parameters matched
        return true;
    }
}
