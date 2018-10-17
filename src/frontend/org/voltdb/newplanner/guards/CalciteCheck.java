/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.newplanner.guards;

/**
 * Check if the SQL statement should be passed to Calcite.
 * This is a temporary check before we can make Calcite support all the VoltDB syntaxes.
 * @author Yiqun Zhang
 * @since 8.4
 */
public abstract class CalciteCheck {

    /**
     * The next check in the chain.
     */
    CalciteCheck m_next;

    /**
     * All the {@code CalciteCheck} subclasses should implement this method to do
     * their own checks and return a result.
     * @param sql the SQL statement to check.
     * @return the check result.
     */
    protected abstract boolean doCheck(String sql);

    /**
     * Indicates whether this check is a negative check.
     * A positive check should proceed to its next chained check on check failure.
     * A negative check should proceed to its next chained check on check success.
     * @return true if the check is a negative one.
     */
    protected abstract boolean isNegativeCheck();

    /**
     * Start the chained check to see if the SQL statement should be passed to Calcite.
     * @param sql the SQL statement to check.
     * @return true if this statement should be directed to Calcite.
     */
    public final boolean check(String sql) {
        boolean pass = doCheck(sql);
        if (doCheck(sql) == isNegativeCheck()) {
            if (m_next != null) {
                return m_next.check(sql);
            }
        }
        return pass;
    }

    /**
     * Add another {@code CalcitePass} instance to the end of the chain and return the
     * newly appended {@code CalcitePass} instance.
     * @param next the instance to append.
     * @return the instance appended.
     */
    public final CalciteCheck addNext(CalciteCheck next) {
        m_next = next;
        return m_next;
    }
}
