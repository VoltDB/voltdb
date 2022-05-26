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

#ifndef MATERIALIZEDVIEWTRIGGERFORWRITE_H_
#define MATERIALIZEDVIEWTRIGGERFORWRITE_H_

#include "MaterializedViewTriggerForInsert.h"

namespace voltdb {

/**
 * Manage the inserts, deletes and updates for a materialized view table based
 * on inserts, deletes and updates to a source table. An instance sits between
 * two tables translasting changes in one table into changes in the other table.
 * The factory method, build, uses information parsed from the catalog to configure
 * initializers for the private constructor.
 */
class MaterializedViewTriggerForWrite : public MaterializedViewTriggerForInsert {
public:
    static void build(PersistentTable *srcTable,
                      PersistentTable *destTable,
                      catalog::MaterializedViewInfo *mvInfo);
    ~MaterializedViewTriggerForWrite();

    /**
     * This updates the materialized view desitnation table to reflect
     * write operations to the source table.
     * Called when the source table is deleting a tuple,
     * OR as a first step when the source table is updating a tuple
     * -- followed by a compensating call to processTupleInsert.
     */
    void processTupleDelete(const TableTuple &oldTuple, bool fallible);

    void updateDefinition(PersistentTable *destTable,
                          catalog::MaterializedViewInfo *mvInfo) {
        MaterializedViewTriggerForInsert::updateDefinition(destTable, mvInfo);
        setupMinMaxRecalculation(mvInfo->indexForMinMax(),
                                 mvInfo->fallbackQueryStmts());
    }


private:
    MaterializedViewTriggerForWrite(PersistentTable *srcTable,
                                    PersistentTable *destTable,
                                    catalog::MaterializedViewInfo *mvInfo);

    void setupMinMaxRecalculation(const catalog::CatalogMap<catalog::IndexRef> &indexForMinOrMax,
                                  const catalog::CatalogMap<catalog::Statement> &fallbackQueryStmts);

    void allocateMinMaxSearchKeyTuple();

    NValue findMinMaxFallbackValueIndexed(const TableTuple& oldTuple,
                                          const NValue &existingValue,
                                          const NValue &initialNull,
                                          int negate_for_min,
                                          int aggIndex,
                                          int minMaxAggIdx,
                                          int aggExprOffset);

    NValue findMinMaxFallbackValueSequential(const TableTuple& oldTuple,
                                             const NValue &existingValue,
                                             const NValue &initialNull,
                                             int negate_for_min,
                                             int aggIndex,
                                             int aggExprOffset);

    NValue findFallbackValueUsingPlan(const TableTuple& oldTuple,
                                      const NValue &initialNull,
                                      int aggIndex,
                                      int minMaxAggIdx,
                                      int aggExprOffset);

    // the source persistent table
    PersistentTable *m_srcPersistentTable;
    TableTuple m_minMaxSearchKeyTuple;
    boost::shared_array<char> m_minMaxSearchKeyBackingStore;
    size_t m_minMaxSearchKeyBackingStoreSize;
    // the index on srcTable which can be used to find each fallback min or max column.
    std::vector<TableIndex *> m_indexForMinMax;
    // Executor vectors to be executed when fallback on min/max value is needed (ENG-8641).
    std::vector<boost::shared_ptr<ExecutorVector> > m_fallbackExecutorVectors;
    std::vector<bool> m_usePlanForAgg;

};

} // namespace voltdb

#endif // MATERIALIZEDVIEWTRIGGERFORWRITE_H_
