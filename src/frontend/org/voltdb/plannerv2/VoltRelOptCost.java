/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.util.Objects;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.voltdb.VoltOverflowException;

public class VoltRelOptCost implements RelOptCost {

    public static final RelOptCostFactory FACTORY = new Factory();

    static final VoltRelOptCost INFINITY = new VoltRelOptCost(
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY) {
        public String toString() {
            return "{inf}";
        }
    };

    static final VoltRelOptCost HUGE = new VoltRelOptCost(
            Double.MAX_VALUE,
            Double.MAX_VALUE,
            Double.MAX_VALUE) {
        public String toString() {
            return "{huge}";
        }
    };

    static final VoltRelOptCost ZERO = new VoltRelOptCost(0.0, 0.0, 0.0) {
        public String toString() {
            return "{0}";
        }
    };

    static final VoltRelOptCost TINY = new VoltRelOptCost(1.0, 1.0, 0.0) {
        public String toString() {
            return "{tiny}";
        }
    };

    // ~ Instance fields
    // --------------------------------------------------------

    final double cpu;
    final double io;
    final double rowCount;

    // ~ Constructors
    // -----------------------------------------------------------

    VoltRelOptCost(double rowCount, double cpu, double io) {
        if (rowCount < 0) {
            rowCount = 0.0;
        }
        if (cpu < 0) {
            cpu = 0.0;
        }
        if (io < 0) {
            io = 0.0;
        }
        this.rowCount = rowCount;
        this.cpu = cpu;
        this.io = io;
    }

    // ~ Methods
    // ----------------------------------------------------------------

    public double getCpu() {
        return cpu;
    }

    public boolean isInfinite() {
        return (this == INFINITY) || (this.rowCount == Double.POSITIVE_INFINITY)
                || (this.cpu == Double.POSITIVE_INFINITY)
                || (this.io == Double.POSITIVE_INFINITY);
    }

    public double getIo() {
        return io;
    }

    public boolean isLe(RelOptCost other) {
        VoltRelOptCost that = (VoltRelOptCost) other;
        int cpuCompare = Double.compare(this.cpu, that.cpu);
        return this == that ||
                ((cpuCompare < 0) || (cpuCompare == 0 && Double.compare(this.rowCount, that.rowCount) <= 0));
    }

    public boolean isLt(RelOptCost other) {
        VoltRelOptCost that = (VoltRelOptCost) other;
        int cpuCompare = Double.compare(this.cpu, that.cpu);
        return (cpuCompare < 0) ||
                (cpuCompare == 0 && Double.compare(this.rowCount, that.rowCount) < 0);
    }

    public double getRows() {
        return rowCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowCount, cpu, io);
    }

    public boolean equals(RelOptCost other) {
        return this == other || other instanceof VoltRelOptCost
                && (Double.compare(this.rowCount, ((VoltRelOptCost) other).rowCount) == 0)
                && (Double.compare(this.cpu, ((VoltRelOptCost) other).cpu) == 0)
                && (Double.compare(this.io, ((VoltRelOptCost) other).io) == 0);
    }

    public boolean isEqWithEpsilon(RelOptCost other) {
        if (!(other instanceof VoltRelOptCost)) {
            return false;
        }
        VoltRelOptCost that = (VoltRelOptCost) other;
        return (this == that) || ((Math
                .abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
                && (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
                && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON));
    }

    public RelOptCost minus(RelOptCost other) {
        if (this == INFINITY) {
            return this;
        }
        VoltRelOptCost that = (VoltRelOptCost) other;
        return new VoltRelOptCost(this.rowCount - that.rowCount,
                this.cpu - that.cpu, this.io - that.io);
    }

    public RelOptCost multiplyBy(double factor) {
        if (this == INFINITY) {
            return this;
        }
        if (Double.isInfinite(rowCount * factor)) {
            throw new VoltOverflowException("VoltRelOptCost rowCount is infinite");
        }
        if (Double.isInfinite(cpu * factor)) {
            throw new VoltOverflowException("VoltRelOptCost cpu is infinite");
        }
        if (Double.isInfinite(io * factor)) {
            throw new VoltOverflowException("VoltRelOptCost io is infinite");
        }

        return new VoltRelOptCost(rowCount * factor, cpu * factor, io * factor);
    }

    public double divideBy(RelOptCost cost) {
        // Compute the geometric average of the ratios of all of the factors
        // which are non-zero and finite.
        VoltRelOptCost that = (VoltRelOptCost) cost;
        double d = 1;
        double n = 0;
        if ((this.rowCount != 0) && !Double.isInfinite(this.rowCount)
                && (that.rowCount != 0) && !Double.isInfinite(that.rowCount)) {
            d *= this.rowCount / that.rowCount;
            ++n;
        }
        if ((this.cpu != 0) && !Double.isInfinite(this.cpu) && (that.cpu != 0)
                && !Double.isInfinite(that.cpu)) {
            d *= this.cpu / that.cpu;
            ++n;
        }
        if ((this.io != 0) && !Double.isInfinite(this.io) && (that.io != 0)
                && !Double.isInfinite(that.io)) {
            d *= this.io / that.io;
            ++n;
        }
        if (n == 0) {
            return 1.0;
        }
        return Math.pow(d, 1 / n);
    }

    public RelOptCost plus(RelOptCost other) {
        VoltRelOptCost that = (VoltRelOptCost) other;
        if ((this == INFINITY) || (that == INFINITY)) {
            return INFINITY;
        }
        if (Double.isInfinite(rowCount + that.rowCount)) {
            throw new VoltOverflowException("VoltRelOptCost rowCount is infinite");
        }
        if (Double.isInfinite(cpu + that.cpu)) {
            throw new VoltOverflowException("VoltRelOptCost cpu is infinite");
        }
        if (Double.isInfinite(io + that.io)) {
            throw new VoltOverflowException("VoltRelOptCost io is infinite");
        }

        return new VoltRelOptCost(this.rowCount + that.rowCount,
                this.cpu + that.cpu, this.io + that.io);
    }

    public String toString() {
        return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
    }

    /** Implementation of {@link org.apache.calcite.plan.RelOptCostFactory}
     * that creates {@link org.voltdb.plannerv2.VoltRelOptCost}s. */
    private static class Factory implements RelOptCostFactory {
      public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
        return new VoltRelOptCost(dRows, dCpu, dIo);
      }

      public RelOptCost makeHugeCost() {
        return VoltRelOptCost.HUGE;
      }

      public RelOptCost makeInfiniteCost() {
        return VoltRelOptCost.INFINITY;
      }

      public RelOptCost makeTinyCost() {
        return VoltRelOptCost.TINY;
      }

      public RelOptCost makeZeroCost() {
        return VoltRelOptCost.ZERO;
      }
    }
}
