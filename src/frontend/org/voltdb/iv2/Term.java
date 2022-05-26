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

package org.voltdb.iv2;

import com.google_voltpatches.common.base.Supplier;

import java.util.List;

// Some comments on threading and organization.
//   replica change handler runs in the zookeeper watch thread context.
//   replica change handler invokes a method in init.mbox that also
//   takes the init.mbox deliver lock
//
//   it is important that repair work happens with the deliver lock held
//   and that updatereplicas also holds this lock -- replica failure during
//   repair must happen unambigously before or after each local repair action.

/**
 * Term encapsulates the leader's watch responsibilities.
 */
public interface Term
{
    /**
     * Start a new Term. Returns a future that is done when the leadership has
     * been fully assumed and all surviving replicas have been repaired.
     */
    public void start();

    public void shutdown();

    /** Get the current set of HSIds which we're monitoring */
    public Supplier<List<Long>> getInterestingHSIds();
}
