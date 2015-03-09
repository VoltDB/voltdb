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

using namespace voltdb;
using namespace std;

namespace
{
    // Super-ghetto empty stats table "factory".  Should be able to
    // just replace the call to this in getStats() with a call to an
    // auto-generated stats table factory based on XML or JSON input
    // in some future happy world.  Go blow up the static methods
    // returning schema/table parts in StatsSource and it's subclasses
    // when this happens, too.
    Table* getEmptyStatsTable(StatisticsSelectorType sst)
    {
        switch (sst)
        {
        case STATISTICS_SELECTOR_TYPE_TABLE:
            {
                return TableStats::generateEmptyTableStatsTable();
            }
        case STATISTICS_SELECTOR_TYPE_INDEX:
            {
                return IndexStats::generateEmptyIndexStatsTable();
            }
        default:
            {
                throwFatalException("Attempted to get unsupported stats type");
            }
        }
    }
}

StatsAgent::StatsAgent() {}

/**
 * Associate the specified StatsSource with the specified CatalogId under the specified StatsSelector
 * @param sst Type of statistic being registered
 * @param catalogId CatalogId of the resource
 * @param statsSource statsSource containing statistics for the resource
 */
void StatsAgent::registerStatsSource(StatisticsSelectorType sst, CatalogId catalogId, StatsSource* statsSource)
{
    m_statsCategoryByStatsSelector[sst].insert(
        pair<CatalogId, StatsSource*>(catalogId, statsSource));
}

void StatsAgent::unregisterStatsSource(StatisticsSelectorType sst)
{
    // get the map of id-to-source
    map<StatisticsSelectorType,
      multimap<CatalogId, StatsSource*> >::iterator it1 =
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
Table* StatsAgent::getStats(StatisticsSelectorType sst,
                            vector<CatalogId> catalogIds,
                            bool interval, int64_t now)
{
    if (catalogIds.size() < 1) {
        return NULL;
    }

    multimap<CatalogId, StatsSource*> *statsSources = &m_statsCategoryByStatsSelector[sst];

    Table *statsTable = m_statsTablesByStatsSelector[sst];
    if (statsTable == NULL) {
        statsTable = getEmptyStatsTable(sst);
        m_statsTablesByStatsSelector[sst] = statsTable;
    }

    statsTable->deleteAllTuples(false);

    for (int ii = 0; ii < catalogIds.size(); ii++) {
        multimap<CatalogId, StatsSource*>::const_iterator iter;
        for (iter = statsSources->find(catalogIds[ii]);
             (iter != statsSources->end()) && (iter->first == catalogIds[ii]);
             iter++) {

            StatsSource *ss = iter->second;
            assert (ss != NULL);
            if (ss == NULL) {
                continue;
            }

            TableTuple *statsTuple = ss->getStatsTuple(interval, now);
            statsTable->insertTuple(*statsTuple);
        }
    }
    return statsTable;
}

StatsAgent::~StatsAgent()
{
    for (map<StatisticsSelectorType, Table*>::iterator i = m_statsTablesByStatsSelector.begin();
        i != m_statsTablesByStatsSelector.end(); i++) {
        delete i->second;
    }
}
