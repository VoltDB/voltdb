/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.planner;

/**
 * Abstract base class for the code that computes plan cost given
 * a set of statistics for a plan.
 *
 */
public abstract class AbstractCostModel {

    /**
     * Computes the cost of a VoltDB plan given a PlanStatistics object which describes
     * the work done by the Plan.
     *
     * @param stats The statistics describing the work for a given plan.
     * @return The computed cost of the plan.
     */
    public abstract double getPlanCost(PlanStatistics stats);
}
