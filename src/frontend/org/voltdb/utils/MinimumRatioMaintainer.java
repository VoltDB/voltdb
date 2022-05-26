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

package org.voltdb.utils;


import com.google_voltpatches.common.base.Preconditions;

/*
 *
 * Maintain a minimum ratio between two counts. unrestricted is always counted.
 * restricted is counted, but may not be allowed if doing a restricted increment would break the ratio restriction.
 *
 * This is useful for maintaining a best possible latency for tasks of the unrestricted type by allow them to burst,
 * but still ensuring that the restricted tasks are done at a rate that maintains the ratio.
 *
 */
public class MinimumRatioMaintainer {
    private final double ratio;
    private long unrestrictedCount = 1;
    private long restrictedCount = 1;

    public MinimumRatioMaintainer(double ratio) {
        Preconditions.checkArgument(ratio > 0.0);
        Preconditions.checkArgument( ratio < 1.0);
        this.ratio = ratio;
    }

    public void didUnrestricted() {
        unrestrictedCount++;
    }

    public void didRestricted() {
        restrictedCount++;
    }

    public boolean canDoRestricted() {
        return (unrestrictedCount / (double)(unrestrictedCount + restrictedCount)) > ratio;
    }
}
