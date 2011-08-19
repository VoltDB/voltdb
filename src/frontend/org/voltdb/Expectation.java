/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb;

import org.voltdb.VoltProcedure.VoltAbortException;

public class Expectation {
    static enum Type {
        EXPECT_EMPTY,
        EXPECT_ONE_ROW,
        EXPECT_ZERO_OR_ONE_ROW,
        EXPECT_NON_EMPTY,
        EXPECT_SCALAR,
        EXPECT_SCALAR_LONG,
        EXPECT_SCALAR_MATCH,
    }

    final private Type m_type;
    final private long m_scalar;

    Expectation(Type t) {
        assert (t != null);
        m_type = t;
        m_scalar = 0;
    }

    Expectation(Type t, long scalar) {
        m_type = t;
        m_scalar = scalar;
    }

    static void check(Expectation expectation, VoltTable table) throws VoltAbortException {
        if (expectation == null)
            return;

        switch (expectation.m_type) {
        case EXPECT_EMPTY:
            return;
        case EXPECT_ONE_ROW:
            return;
        case EXPECT_ZERO_OR_ONE_ROW:
            return;
        case EXPECT_NON_EMPTY:
            return;
        case EXPECT_SCALAR:
            return;
        case EXPECT_SCALAR_LONG:
            return;
        case EXPECT_SCALAR_MATCH:
            return;
        }
    }
}
