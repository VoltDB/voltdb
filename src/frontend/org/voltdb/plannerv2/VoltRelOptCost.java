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

import java.util.Comparator;
import java.util.Objects;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

public class VoltRelOptCost implements RelOptCost, Comparator<VoltRelOptCost> {

    /** Implementation of {@link org.apache.calcite.plan.RelOptCostFactory}
     * that creates {@link org.voltdb.plannerv2.VoltRelOptCost}s. */
    static final RelOptCostFactory FACTORY = new RelOptCostFactory() {
        @Override public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
            return new VoltRelOptCost(dRows, dCpu, dIo);
        }
        @Override public RelOptCost makeHugeCost() {
            return VoltRelOptCost.HUGE;
        }
        @Override public RelOptCost makeInfiniteCost() {
            return VoltRelOptCost.INFINITY;
        }
        @Override public RelOptCost makeTinyCost() {
            return VoltRelOptCost.TINY;
        }
        @Override public RelOptCost makeZeroCost() {
            return VoltRelOptCost.ZERO;
        }
    };

    private static final VoltRelOptCost INFINITY = new VoltRelOptCost(
            Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY) {
        public String toString() {
            return "{inf}";
        }
    };

    private static final VoltRelOptCost HUGE = new VoltRelOptCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
        public String toString() {
            return "{huge}";
        }
    };

    private static final VoltRelOptCost ZERO = new VoltRelOptCost(0.0, 0.0, 0.0) {
        public String toString() {
            return "{0}";
        }
    };

    private static final VoltRelOptCost TINY = new VoltRelOptCost(1.0, 1.0, 0.0) {
        public String toString() {
            return "{tiny}";
        }
    };

    // ~ Instance fields
    // --------------------------------------------------------

    private final double cpu;
    private final double io;
    private final double rowCount;

    // ~ Constructors
    // -----------------------------------------------------------

    VoltRelOptCost(double rowCount, double cpu, double io) {
        this.rowCount = Math.max(0.0, rowCount);
        this.cpu = Math.max(0.0, cpu);
        this.io = Math.max(0.0, io);
    }

    // ~ Methods
    // ----------------------------------------------------------------

    @Override public double getCpu() {
        return cpu;
    }

    @Override public double getIo() {
        return io;
    }

    @Override public double getRows() {
        return rowCount;
    }

    @Override public boolean isInfinite() {
        return this == INFINITY || rowCount == Double.POSITIVE_INFINITY
                || cpu == Double.POSITIVE_INFINITY || io == Double.POSITIVE_INFINITY;
    }

    @Override public int compare(VoltRelOptCost lhs, VoltRelOptCost rhs) {
        return Comparator
                .comparingDouble(VoltRelOptCost::getCpu)
                .thenComparingDouble(VoltRelOptCost::getRows)
                .thenComparingDouble(VoltRelOptCost::getIo)
                .compare(lhs, rhs);
    }

    @Override public boolean isLe(RelOptCost other) {
        return compare(this, (VoltRelOptCost) other) <= 0;
    }

    @Override public boolean isLt(RelOptCost other) {
        return compare(this, (VoltRelOptCost) other) < 0;
    }

    @Override public boolean equals(RelOptCost other) {
        return compare(this, (VoltRelOptCost) other) == 0;
    }

    @Override public int hashCode() {
        return Objects.hash(rowCount, cpu, io);
    }

    @Override public boolean isEqWithEpsilon(RelOptCost other) {
        final VoltRelOptCost that = (VoltRelOptCost) other;
        if (compare(this, that) == 0) {
            return true;
        } else {
            final VoltRelOptCost delta = (VoltRelOptCost) minus(other);
            return Math.max(Math.abs(delta.getCpu()), Math.max(Math.abs(delta.getRows()), Math.abs(delta.getIo()))) <
                    RelOptUtil.EPSILON;
        }
    }

    @Override public RelOptCost minus(RelOptCost other) {
        if (this == INFINITY) {
            return this;
        } else {
            final VoltRelOptCost that = (VoltRelOptCost) other;
            return new VoltRelOptCost(rowCount - that.rowCount, cpu - that.cpu, io - that.io);
        }
    }

    @Override public RelOptCost multiplyBy(double factor) {
        if (this == INFINITY) {
            return this;
        } else {
            return new VoltRelOptCost(rowCount * factor, cpu * factor, io * factor);
        }
    }

    @Override public double divideBy(RelOptCost cost) {
        // Compute the geometric average of the ratios of all of the factors
        // which are non-zero and finite.
        final VoltRelOptCost that = (VoltRelOptCost) cost;
        double d = 1;
        double n = 0;
        if (rowCount != 0 && !Double.isInfinite(rowCount)
                && that.rowCount != 0 && !Double.isInfinite(that.rowCount)) {
            d *= rowCount / that.rowCount;
            ++n;
        }
        if (cpu != 0 && !Double.isInfinite(cpu) && that.cpu != 0 && !Double.isInfinite(that.cpu)) {
            d *= cpu / that.cpu;
            ++n;
        }
        if (io != 0 && !Double.isInfinite(io) && that.io != 0 && !Double.isInfinite(that.io)) {
            d *= io / that.io;
            ++n;
        }
        if (n == 0) {
            return 1.0;
        } else {
            return Math.pow(d, 1 / n);
        }
    }

    @Override public RelOptCost plus(RelOptCost other) {
        assert other instanceof VoltRelOptCost : "plus(): other is not an instance of VoltRelOptCost";
        final VoltRelOptCost that = (VoltRelOptCost) other;
        return new VoltRelOptCost(rowCount + that.rowCount, cpu + that.cpu, io + that.io);
    }

    @Override public String toString() {
        return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
    }
}
