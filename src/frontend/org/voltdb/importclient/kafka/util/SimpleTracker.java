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
package org.voltdb.importclient.kafka.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;

import org.voltdb.importer.CommitTracker;

//Simple tracker used for timed based commit.
public class SimpleTracker implements CommitTracker {
    private final AtomicLong m_commitPoint = new AtomicLong(-1);
    @Override
    public void submit(long offset) {
        //NoOp
    }

    @Override
    public long commit(long commit) {
        return m_commitPoint.accumulateAndGet(commit, new LongBinaryOperator() {
            @Override
            public long applyAsLong(long orig, long newval) {
                return (orig > newval) ? orig : newval;
            }
        });
    }

    @Override
    public void resetTo(long offset) {
        m_commitPoint.set(offset);
    }

    @Override
    public long getSafe() {
        return 0;
    }
}
