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

#ifndef STATSAGENT_H_
#define STATSAGENT_H_

#include "common/ids.h"
#include "common/types.h"

#include <vector>
#include <map>

namespace voltdb {
class StatsSource;
class TempTable;
/**
 * StatsAgent serves as a central registrar for all sources of statistical runtime information in an EE. In the future this could perform
 * further aggregation and processing on the collected statistics. Right now statistics are only collected on persistent tables but that
 * could be extended to include stats about plan fragments and the temp tables connecting them.
 */
class StatsAgent {
public:
    /**
     * Do nothing constructor
     */
    StatsAgent();

    /**
     * Associate the specified StatsSource with the specified CatalogId under the specified StatsSelector
     * @param sst Type of statistic being registered
     * @param catalogId CatalogId of the resource
     * @param statsSource statsSource containing statistics for the resource
     */
    void registerStatsSource(voltdb::StatisticsSelectorType sst, voltdb::CatalogId catalogId, voltdb::StatsSource* statsSource);

    /**
     * Unassociate all instances of this selector type
     */
    void unregisterStatsSource(voltdb::StatisticsSelectorType sst, int32_t relativeIndexOfTable = -1);

    /**
     * Get statistics for the specified resources
     * @param sst StatisticsSelectorType of the resources
     * @param catalogIds CatalogIds of the resources statistics should be retrieved for
     * @param interval Return counters since the beginning or since this method was last invoked
     * @param now Timestamp to return with each row
     */
    TempTable* getStats(
            voltdb::StatisticsSelectorType sst,
            int64_t m_siteId, int32_t m_partitionId,
            std::vector<voltdb::CatalogId> catalogIds,
            bool interval,
            int64_t now);

    ~StatsAgent();

private:
    /**
     * Map from a statistics selector to a map of CatalogIds to StatsSources.
     */
    std::map<voltdb::StatisticsSelectorType, std::multimap<voltdb::CatalogId, voltdb::StatsSource*> > m_statsCategoryByStatsSelector;

    /**
     * Temporary tables for aggregating the results of table statistics keyed by type of statistic
     */
    std::map<voltdb::StatisticsSelectorType, voltdb::TempTable*> m_statsTablesByStatsSelector;
};

}

#endif /* STATSAGENT_H_ */
