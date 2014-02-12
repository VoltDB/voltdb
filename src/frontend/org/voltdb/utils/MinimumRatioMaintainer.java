/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
 * Maintain a minimum ratio between two counts. A is always and counted.
 * B is counted, but may not be allowed if doing a B would break the ratio restriction.
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
