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

package org.voltdb.planner.optimizer;

import org.voltdb.expressions.AbstractExpression;

/**
 * A wrapper of AbstractExpression on logical equivalence relation instead of structural equality.
 * Note that equivalence takes longer to check and thus this class should only be used minimally
 * when needed.
 */
public final class EquivalentExpression implements Comparable<EquivalentExpression> {
    private final AbstractExpression m_expr;
    public EquivalentExpression(AbstractExpression e) {
        assert (e != null);
        m_expr = e;
    }
    public AbstractExpression get() {
        return m_expr;
    }
    @Override
    public int compareTo(EquivalentExpression other) {
        return get().compareTo(other.get());
    }
    @Override
    public boolean equals(Object other) {       // replace equality with equivalence
        return other instanceof EquivalentExpression && compareTo((EquivalentExpression) other) == 0;
    }
    @Override
    public int hashCode() {
        return get().hashCode();
    }
}
