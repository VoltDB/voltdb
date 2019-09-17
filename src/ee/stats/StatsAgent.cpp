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
#include "StatsAgent.h"

#include "StatsSource.h"
#include "indexes/IndexStats.h"
#include "storage/TableStats.h"
#include "storage/temptable.h"

using namespace voltdb;
using namespace std;

namespace {
    // Super-ghetto empty stats table "factory".  Should be able to
    // just replace the call to this in getStats() with a call to an
    // auto-generated stats table factory based on XML or JSON input
    // in some future happy world.  Go blow up the static methods
    // returning schema/table parts in StatsSource and it's subclasses
    // when this happens, too.
    TempTable* getEmptyStatsTable(StatisticsSelectorType sst) {
        switch (sst) {
            case STATISTICS_SELECTOR_TYPE_TABLE:
                return TableStats::generateEmptyTableStatsTable();
            case STATISTICS_SELECTOR_TYPE_INDEX:
                return IndexStats::generateEmptyIndexStatsTable();
            default:
                throwFatalException("Attempted to get unsupported stats type");
        }
    }
}

/**
 * Associate the specified StatsSource with the specified CatalogId under the specified StatsSelector
 * @param sst Type of statistic being registered
 * @param catalogId CatalogId of the resource
 * @param statsSource statsSource containing statistics for the resource
 */
void StatsAgent::registerStatsSource(StatisticsSelectorType sst,
                                     CatalogId catalogId,
                                     StatsSource* statsSource) {
    vassert(statsSource != nullptr);
    m_statsCategoryByStatsSelector[sst].insert(make_pair(catalogId, statsSource));
    VOLT_DEBUG("Partition %d registered %s stats source (%p) for table %s at index %d.",
               ThreadLocalPool::getEnginePartitionId(),
               sst == StatisticsSelectorType::STATISTICS_SELECTOR_TYPE_TABLE ? "a table" : "an index",
               statsSource, statsSource->getTableName().c_str(), catalogId);
}

void StatsAgent::unregisterStatsSource(StatisticsSelectorType sst, int32_t relativeIndexOfTable) {
    // get the map of id-to-source
    map<StatisticsSelectorType,
      multimap<CatalogId, StatsSource*> >::iterator it1 =
      m_statsCategoryByStatsSelector.find(sst);

    if (it1 == m_statsCategoryByStatsSelector.end()) {
        return;
    }
    if (relativeIndexOfTable == -1) {
        it1->second.clear();
        VOLT_DEBUG("Partition %d unregistered all %s stats sources.",
                   ThreadLocalPool::getEnginePartitionId(),
                   sst == StatisticsSelectorType::STATISTICS_SELECTOR_TYPE_TABLE ? "table" : "index");
    } else {
        it1->second.erase(relativeIndexOfTable);
        VOLT_DEBUG("Partition %d unregistered %s stats source for table at index %d.",
                   ThreadLocalPool::getEnginePartitionId(),
                   sst == StatisticsSelectorType::STATISTICS_SELECTOR_TYPE_TABLE ? "a table" : "an index",
                   relativeIndexOfTable);
    }
}

/**
 * Get statistics for the specified resources
 * @param sst StatisticsSelectorType of the resources
 * @param catalogIds CatalogIds of the resources statistics should be retrieved for
 * @param interval Whether to return counters since the beginning or since the last time this was called
 * @param Timestamp to embed in each row
 */
TempTable* StatsAgent::getStats(StatisticsSelectorType sst,
                                int64_t siteId, int32_t partitionId,
                                vector<CatalogId> catalogIds,
                                bool interval, int64_t now) {
    if (catalogIds.size() < 1) {
        return nullptr;
    }

    multimap<CatalogId, StatsSource*> *statsSources = &m_statsCategoryByStatsSelector[sst];

    TempTable *statsTable = m_statsTablesByStatsSelector[sst];
    if (statsTable == nullptr) {
        statsTable = getEmptyStatsTable(sst);
        m_statsTablesByStatsSelector[sst] = statsTable;
    }

    statsTable->deleteAllTempTuples();

    for (auto const id : catalogIds) {
        for (auto iter = statsSources->find(id);
             iter != statsSources->end() && iter->first == id;
             iter++) {
            StatsSource *ss = iter->second;
            vassert(ss != nullptr);
            if (ss == nullptr) {           // TODO: WTF??
                continue;
            }
            TableTuple *statsTuple = ss->getStatsTuple(siteId, partitionId, interval, now);
            statsTable->insertTuple(*statsTuple);
        }
    }
    return statsTable;
}

StatsAgent::~StatsAgent() {
    for (auto i : m_statsTablesByStatsSelector) {
        delete i.second;
    }
}
