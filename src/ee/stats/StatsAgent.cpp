/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "stats/StatsAgent.h"
#include "stats/StatsSource.h"
#include "common/ids.h"
#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "storage/PersistentTableStats.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include <cassert>
#include <string>
#include <vector>

namespace voltdb {

StatsAgent::StatsAgent() {}

/**
 * Associate the specified StatsSource with the specified CatalogId under the specified StatsSelector
 * @param sst Type of statistic being registered
 * @param catalogId CatalogId of the resource
 * @param statsSource statsSource containing statistics for the resource
 */
void StatsAgent::registerStatsSource(voltdb::StatisticsSelectorType sst, voltdb::CatalogId catalogId, voltdb::StatsSource* statsSource)
{
    m_statsCategoryByStatsSelector[sst][catalogId] = statsSource;
}

void StatsAgent::unregisterStatsSource(voltdb::StatisticsSelectorType sst)
{
    // get the map of id-to-source
    std::map<voltdb::StatisticsSelectorType,
      std::map<voltdb::CatalogId, voltdb::StatsSource*> >::iterator it1 =
      m_statsCategoryByStatsSelector.find(sst);

    if (it1 == m_statsCategoryByStatsSelector.end()) {
        return;
    }
    it1->second.clear();
}

/**
 * Get statistics for the specified resources
 * @param sst StatisticsSelectorType of the resources
 * @param catalogIds CatalogIds of the resources statistics should be retrieved for
 * @param interval Whether to return counters since the beginning or since the last time this was called
 * @param Timestamp to embed in each row
 */
Table* StatsAgent::getStats(voltdb::StatisticsSelectorType sst,
                            std::vector<voltdb::CatalogId> catalogIds,
                            bool interval, int64_t now)
{
    assert (catalogIds.size() > 0);
    if (catalogIds.size() < 1) {
        return NULL;
    }
    std::map<voltdb::CatalogId, voltdb::StatsSource*> *statsSources = &m_statsCategoryByStatsSelector[sst];
    Table *statsTable = m_statsTablesByStatsSelector[sst];
    if (statsTable == NULL) {
        /*
         * Initialize the output table the first time.
         */
        voltdb::StatsSource *ss = (*statsSources)[catalogIds[0]];
        voltdb::Table *table = ss->getStatsTable(interval, now);
        statsTable = reinterpret_cast<Table*>(
            voltdb::TableFactory::getTempTable(
                table->databaseId(),
                std::string("Persistent Table aggregated stats temp table"),
                TupleSchema::createTupleSchema(table->schema()),
                table->columnNames(),
                NULL));
        m_statsTablesByStatsSelector[sst] = statsTable;
    }

    statsTable->deleteAllTuples(false);

    for (int ii = 0; ii < catalogIds.size(); ii++) {
        voltdb::StatsSource *ss = (*statsSources)[catalogIds[ii]];
        assert (ss != NULL);
        if (ss == NULL) {
            continue;
        }

        voltdb::TableTuple *statsTuple = ss->getStatsTuple(interval, now);
        statsTable->insertTuple(*statsTuple);
    }
    return statsTable;
}

StatsAgent::~StatsAgent()
{
    for (std::map<voltdb::StatisticsSelectorType, voltdb::Table*>::iterator i = m_statsTablesByStatsSelector.begin();
        i != m_statsTablesByStatsSelector.end(); i++) {
        delete i->second;
    }
}

}
