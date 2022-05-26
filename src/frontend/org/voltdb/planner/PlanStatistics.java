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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * <p>Describes the resource usage of a complete query plan. It is comprised of a set
 * of levels, each containing a vector of resource usage. The levels correspond to
 * the number of network hops that must be made to get to the root node of the plan.
 * These network hops correspond to the division between PlanFragments.</p>
 *
 * <p>This over-complicated structure is the input to the Cost Model object, which will
 * output a single double value representing the cost of the plan. Plans with lower
 * costs will be selected over others.</p>
 *
 */
public class PlanStatistics {

    final int MAX_LEVELS = 20;

    int m_levelCount;
    ArrayList<HashMap<StatsField, Long>> m_levels;

    /**
     * Initialize an empty map for each possible level.
     */
    PlanStatistics() {
        m_levelCount = 0;
        m_levels = new ArrayList<HashMap<StatsField, Long>>();
        for (int i = 0; i < MAX_LEVELS; i++)
            m_levels.add(new HashMap<StatsField, Long>());
    }

    /**
     * Add a specific value to a scalar statistic, identified by a level and
     * a field name. Note, incrementing by zero is allowed and will cause
     * hasStatistic to return true, rather than false, and getStatistic to
     * return 0 rather than -1.
     *
     * @param level The number of network hops from the root plannonde.
     * @param field The name of the statistic in question.
     * @param incrementValue The value to increment the statistic by.
     */
    public void incrementStatistic(int level, StatsField field, long incrementValue) {
        if (level >= MAX_LEVELS)
            throw new RuntimeException("Plan containts too many levels");

        // get the existing value
        Long existing = m_levels.get(level).get(field);
        if (existing == null)
            existing = 0L;

        // increment the existing value
        existing = existing.longValue() + incrementValue;

        // set the new value
        m_levels.get(level).put(field, existing);

        // if this is the highest level, memoize it
        if (level >= m_levelCount)
            m_levelCount = level + 1;
    }

    /**
     * Check if a particular statistic has a non-null (inc. zero) value.
     *
     * @param level The number of network hops from the root plannonde.
     * @param field The name of the statistic in question.
     * @return True if the statistic has been set, false otherwise.
     */
    public boolean hasStatistic(int level, StatsField field) {
        if (level >= m_levelCount) return false;
        return m_levels.get(level).containsKey(field);
    }

    /**
     * Get the value fo the statistic with a given name and level.
     *
     * @param level The number of network hops from the root plannonde.
     * @param field The name of the statistic in question.
     * @return The value of the statistic, or -1 if not set.
     */
    public long getStatistic(int level, StatsField field) {
        if (level >= m_levelCount) return -1L;
        Long stat = m_levels.get(level).get(field);
        if (stat != null) return stat.longValue();
        else return -1L;
    }

    /**
     * Get the map field names to values for a particular level.
     *
     * @param level The number of network hops from the root plannonde.
     * @return The map of field names to values for a particular level.
     */
    public HashMap<StatsField, Long> getStatisticsForLevel(int level) {
        if (level >= m_levelCount) return null;
        return m_levels.get(level);
    }

    /**
     * Get the number of levels with non-empty statistic sets.
     *
     * @return The number of levels with non-empty statistic sets.
     */
    public int getLevelCount() {
        return m_levelCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int level = 0; level < m_levels.size(); level++) {
            sb.append("LEVEL ").append(level).append(":\n");
            HashMap<StatsField, Long> levelMap = m_levels.get(level);
            for (Entry<StatsField, Long> entry : levelMap.entrySet()) {
                sb.append("  ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        }
        return sb.toString();
    }

}
