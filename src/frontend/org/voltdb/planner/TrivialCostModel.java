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

package org.voltdb.planner;

import java.util.HashMap;

/**
 * The trivial cost model returns one for any plan. In theory this will
 * always select the first plan given to it, but it doesn't really matter.
 *
 */
public class TrivialCostModel extends AbstractCostModel {

    @Override
    public double getPlanCost(PlanStatistics stats) {
        double cost = 0;

        // just add up the total tuples read for the plan
        for (int i = 0; i < stats.getLevelCount(); i++) {
            HashMap<StatsField, Long> level = stats.getStatisticsForLevel(i);
            Long levelValueObj = level.get(StatsField.TUPLES_READ);
            long levelValue = (levelValueObj == null) ? 0 : levelValueObj.longValue();
            cost += levelValue;
        }

        return cost;
    }

}
