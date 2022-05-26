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

package org.voltdb.plannerv2;

import org.apache.calcite.sql.validate.SqlAbstractConformance;

public class VoltSqlConformance extends SqlAbstractConformance {

    public static final VoltSqlConformance INSTANCE = new VoltSqlConformance();

    private VoltSqlConformance() {
    }

    /**
     * Whether to allow aliases from the {@code SELECT} clause to be used as
     * column names in the {@code GROUP BY} clause.
     */
    @Override public boolean isGroupByAlias() {
        return true;
    }

    /**
     * Whether to allow aliases from the {@code SELECT} clause to be used as
     * column names in the {@code HAVING} clause.
     */
    @Override public boolean isHavingAlias() {
        return true;
    }

    @Override public boolean isBangEqualAllowed() {
        return true;
    }

    /**
     * Whether to allow parentheses to be specified in calls to niladic functions
     * and procedures (that is, functions and procedures with no parameters).
     *
     * <p>For example, {@code CURRENT_DATE} is a niladic system function. In
     * standard SQL it must be invoked without parentheses:
     *
     * <blockquote><code>VALUES CURRENT_DATE</code></blockquote>
     *
     * <p>If {@code allowNiladicParentheses}, the following syntax is also valid:
     *
     * <blockquote><code>VALUES CURRENT_DATE()</code></blockquote>
     */
    @Override public boolean allowNiladicParentheses() {
        return true;
    }
}
