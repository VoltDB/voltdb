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

#include "TableCatalogDelegate.hpp"

#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/table.h"
#include "catalog/index.h"
#include "catalog/column.h"
#include "catalog/columnref.h"
#include "catalog/constraint.h"
#include "catalog/materializedviewinfo.h"

#include "common/CatalogUtil.h"
#include "common/ExecuteWithMpMemory.h"
#include "common/types.h"
#include "common/TupleSchemaBuilder.h"
#include "common/ValueFactory.hpp"
#include "common/StackTrace.h"

#include "expressions/expressionutil.h"
#include "expressions/functionexpression.h"

#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"

#include "storage/AbstractDRTupleStream.h"
#include "storage/constraintutil.h"
#include "storage/MaterializedViewTriggerForWrite.h"
#include "storage/persistenttable.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "execution/VoltDBEngine.h"
#include "sha1/sha1.h"

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>

#include <string>
#include <vector>
#include <map>

namespace voltdb {

TableCatalogDelegate::~TableCatalogDelegate() {
    if (m_table) {
        m_table->decrementRefcount();
    }
}

Table* TableCatalogDelegate::getTable() const {
    // If a persistent table has an active delta table, return the delta table instead of the whole table.
    auto persistentTable = dynamic_cast<PersistentTable*>(m_table);
    if (persistentTable && persistentTable->isDeltaTableActive()) {
        return persistentTable->deltaTable();
    }

    return m_table;
}

TupleSchema* TableCatalogDelegate::createTupleSchema(catalog::Table const& catalogTable,
                                                     bool isXDCR) {
    // Columns:
    // Column is stored as map<std::string, Column*> in Catalog. We have to
    // sort it by Column index to preserve column order.
    auto numColumns = catalogTable.columns().size();
    bool needsDRTimestamp = isXDCR && catalogTable.isDRed();
    bool needsHiddenCountForView = false;
    bool needsHiddenCloumnTableWithMigrate = isTableWithMigrate(static_cast<TableType>(catalogTable.tableType()));
    std::map<std::string, catalog::Column*>::const_iterator colIterator;

    // only looking for potential existing table count(*) when this is a Materialized view table
    if (isTableMaterialized(catalogTable)) {
      for (colIterator = catalogTable.columns().begin();
           colIterator != catalogTable.columns().end(); colIterator++) {
          auto catalogColumn = colIterator->second;
          if (catalogColumn->aggregatetype() == EXPRESSION_TYPE_AGGREGATE_COUNT_STAR) {
            // there exists count(*), directly quit
            break;
          }
      }
      if (colIterator == catalogTable.columns().end()) {
        // iterator meets the end, meaning no count(*) column
        needsHiddenCountForView = true;
      }
    }

    // DR timestamp and hidden COUNT(*) should not appear at the same time
    vassert(!(needsDRTimestamp && needsHiddenCountForView));
    int numHiddenColumns = (needsDRTimestamp || needsHiddenCountForView) ? 1 : 0;
    if (needsHiddenCloumnTableWithMigrate) {
         numHiddenColumns++;
    }
    TupleSchemaBuilder schemaBuilder(numColumns, numHiddenColumns);

    for (colIterator = catalogTable.columns().begin();
         colIterator != catalogTable.columns().end(); colIterator++) {

        auto catalogColumn = colIterator->second;
        schemaBuilder.setColumnAtIndex(catalogColumn->index(),
                                       static_cast<ValueType>(catalogColumn->type()),
                                       static_cast<int32_t>(catalogColumn->size()),
                                       catalogColumn->nullable(),
                                       catalogColumn->inbytes());
    }
    int hiddenIndex = 0;
    if (needsDRTimestamp) {
        // Create a hidden timestamp column for a DRed table in an
        // active-active context.
        //
        // Column will be marked as not nullable in TupleSchema,
        // because we never expect a null value here, but this is not
        // actually enforced at runtime.
        schemaBuilder.setHiddenColumnAtIndex(hiddenIndex++, HiddenColumn::XDCR_TIMESTAMP);
        VOLT_DEBUG("Adding hidden column for dr table %s index %d", catalogTable.name().c_str(), hiddenIndex);
    }

    if (needsHiddenCountForView) {
        schemaBuilder.setHiddenColumnAtIndex(hiddenIndex++, HiddenColumn::VIEW_COUNT);
        VOLT_DEBUG("Adding hidden column for mv %s index %d", catalogTable.name().c_str(), hiddenIndex);
    }

    // Always create the hidden column for migrate last so the hidden columns can be handled correctly on java side
    // for snapshot write plans.
    if (needsHiddenCloumnTableWithMigrate) {
        VOLT_DEBUG("Adding hidden column for migrate table %s index %d", catalogTable.name().c_str(), hiddenIndex);
        schemaBuilder.setHiddenColumnAtIndex(hiddenIndex++, HiddenColumn::MIGRATE_TXN);
    }
    vassert(numHiddenColumns == hiddenIndex);
    return schemaBuilder.build();
}

bool TableCatalogDelegate::getIndexScheme(catalog::Table const& catalogTable,
                                          catalog::Index const& catalogIndex,
                                          TupleSchema const* schema,
                                          TableIndexScheme* scheme) {
    std::vector<int> index_columns;

    // The catalog::Index object now has a list of columns that are to be
    // used
    if (catalogIndex.columns().size() == 0) {
       VOLT_ERROR("Index '%s' in table '%s' does not declare any columns to use",
             catalogIndex.name().c_str(), catalogTable.name().c_str());
       return false;
    }

    auto indexedExpressions = TableIndex::simplyIndexColumns();
    std::string const& expressionsAsText = catalogIndex.expressionsjson();
    if (expressionsAsText.length() != 0) {
        ExpressionUtil::loadIndexedExprsFromJson(indexedExpressions, expressionsAsText);
    }

    // Since the columns are not going to come back in the proper order from
    // the catalogs, we'll use the index attribute to make sure we put them
    // in the right order
    index_columns.resize(catalogIndex.columns().size());
    for (auto const& col : catalogIndex.columns()) {
        auto const* catalogColref = col.second;
        vassert(catalogColref->index() >= 0);
        index_columns[catalogColref->index()] = catalogColref->column()->index();
    }
    // partial index predicate
    std::string const& predicateAsText  = catalogIndex.predicatejson();
    AbstractExpression* predicate = NULL;
    if (!predicateAsText.empty()) {
        predicate = ExpressionUtil::loadExpressionFromJson(predicateAsText);
    }
    *scheme = TableIndexScheme(catalogIndex.name(),
                               (TableIndexType)catalogIndex.type(),
                               index_columns,
                               indexedExpressions,
                               predicate,
                               catalogIndex.unique(),
                               catalogIndex.countable(),
                               catalogIndex.migrating(),
                               expressionsAsText,
                               predicateAsText,
                               schema);
    return true;
}

/**
 * Locally defined function to make a string from an index schema
 */
static std::string
getIndexIdFromMap(TableIndexType type, bool countable, bool isUnique,
                  std::string const& expressionsAsText, std::vector<int32_t> columnIndexes,
                  std::string const& predicateAsText) {
    // add the uniqueness of the index
    std::string retval = isUnique ? "U" : "M";

    // add the type of the index
    switch (type) {
    case BALANCED_TREE_INDEX:
        retval += "B";
        break;
    case HASH_TABLE_INDEX:
        retval += "H";
        break;
    case COVERING_CELL_INDEX:
        retval += "G"; // C is taken
        break;
    default:
        // this would need to change if we added index types
        vassert(false);
        break;
    }

    // add whether it's counting or not
    retval += countable ? "C" : "N"; // (N)ot countable?

    // concat the target table column indexes into a unique string
    // using the order they appear in the index
    for (size_t i = 0; i < columnIndexes.size(); i++) {
        char buf[128];
        snprintf(buf, 128, "-%d", columnIndexes[i]);
        buf[sizeof buf - 1] = '\0';
        retval += buf;
    }

    // Expression indexes need to have IDs that stand out as unique from each other and from colunn indexes
    // that may reference the exact same set of columns.
    if ( ! expressionsAsText.empty()) {
        retval += expressionsAsText;
    }
    // Add partial index predicate if any
    if ( ! predicateAsText.empty()) {
        retval += predicateAsText;
    }
    return retval;
}

std::string
TableCatalogDelegate::getIndexIdString(const catalog::Index& catalogIndex) {
    std::vector<int32_t> columnIndexes(catalogIndex.columns().size());

    // get the list of column indexes in the target table
    // in the order they appear in the index
    std::map<std::string, catalog::ColumnRef*>::const_iterator colIterator;
    for (colIterator = catalogIndex.columns().begin();
         colIterator != catalogIndex.columns().end();
         colIterator++) {
        int32_t index = colIterator->second->index();
        auto catalogColumn = colIterator->second->column();
        columnIndexes[index] = catalogColumn->index();
    }

    const std::string expressionsAsText = catalogIndex.expressionsjson();

    const std::string predicateAsText = catalogIndex.predicatejson();

    return getIndexIdFromMap((TableIndexType)catalogIndex.type(),
                             true, //catalogIndex.countable(), // always counting for now
                             catalogIndex.unique(),
                             expressionsAsText,
                             columnIndexes,
                             predicateAsText);
}

std::string
TableCatalogDelegate::getIndexIdString(const TableIndexScheme& indexScheme) {
    std::vector<int32_t> columnIndexes(indexScheme.columnIndices.size());

    // get the list of column indexes in the target table
    // in the order they appear in the index
    for (int i = 0; i < indexScheme.columnIndices.size(); i++) {
        columnIndexes[i] = indexScheme.columnIndices[i];
    }

    return getIndexIdFromMap(indexScheme.type,
                             true, // indexScheme.countable, // // always counting for now
                             indexScheme.unique,
                             indexScheme.expressionsAsText,
                             columnIndexes,
                             indexScheme.predicateAsText);
}


Table* TableCatalogDelegate::constructTableFromCatalog(catalog::Database const& catalogDatabase,
                                                       catalog::Table const& catalogTable,
                                                       bool isXDCR,
                                                       int tableAllocationTargetSize,
                                                       bool forceNoDR) {
    // get an array of table column names
    const int numColumns = static_cast<int>(catalogTable.columns().size());
    std::map<std::string, catalog::Column*>::const_iterator colIterator;
    std::vector<std::string> columnNames(numColumns);
    for (colIterator = catalogTable.columns().begin();
         colIterator != catalogTable.columns().end();
         colIterator++) {
        auto catalogColumn = colIterator->second;
        columnNames[catalogColumn->index()] = catalogColumn->name();
    }

    // get the schema for the table
    TupleSchema* schema = createTupleSchema(catalogTable, isXDCR);

    // Indexes
    std::map<std::string, TableIndexScheme> index_map;
    std::map<std::string, catalog::Index*>::const_iterator idxIterator;
    for (idxIterator = catalogTable.indexes().begin();
         idxIterator != catalogTable.indexes().end(); idxIterator++) {
        auto catalogIndex = idxIterator->second;

        TableIndexScheme index_scheme;
        if (getIndexScheme(catalogTable, *catalogIndex, schema, &index_scheme)) {
            index_map[catalogIndex->name()] = index_scheme;
        }
    }

    // Constraints
    std::string pkeyIndexId;
    std::map<std::string, catalog::Constraint*>::const_iterator constraintIterator;
    for (constraintIterator = catalogTable.constraints().begin();
         constraintIterator != catalogTable.constraints().end();
         constraintIterator++) {
        auto catalogConstraint = constraintIterator->second;

        // Constraint Type
        ConstraintType type = (ConstraintType) catalogConstraint->type();
        switch (type) {
        case CONSTRAINT_TYPE_PRIMARY_KEY:
            // Make sure we have an index to use
            vassert(catalogConstraint->index());
            // Make sure they didn't declare more than one primary key index
            vassert(pkeyIndexId.empty());
            pkeyIndexId = catalogConstraint->index()->name();
            break;
        case CONSTRAINT_TYPE_UNIQUE:
            // Make sure we have an index to use
            // TODO: In the future I would like bring back my Constraint
            //       object so that we can keep track of everything that a
            //       table has...
            vassert(catalogConstraint->index());
            break;
        // Unsupported
        case CONSTRAINT_TYPE_CHECK:
        case CONSTRAINT_TYPE_FOREIGN_KEY:
        case CONSTRAINT_TYPE_MAIN:
            VOLT_WARN("Unsupported type '%s' for constraint '%s'",
                      constraintutil::getTypeName(type).c_str(),
                      catalogConstraint->name().c_str());
            break;
        // Unknown
        default:
            VOLT_ERROR("Invalid constraint type '%s' for '%s'",
                       constraintutil::getTypeName(type).c_str(),
                       catalogConstraint->name().c_str());
            vassert(false);
            return NULL;
        }
    }

    // Build the index array
    // Please note the index array should follow the order of primary key first,
    // all unique indices afterwards, and all the non-unique indices at the end.
    std::deque<TableIndexScheme> indexes;
    TableIndexScheme pkeyIndex_scheme;
    for (auto const& indexIterator : index_map) {
       auto const& indexScheme = indexIterator.second;
       // Exclude the primary key
       if (indexScheme.name.compare(pkeyIndexId) == 0) {
          pkeyIndex_scheme = indexScheme;
       } else if (indexScheme.unique) {
          indexes.push_front(indexScheme);
       } else {
          indexes.push_back(indexScheme);
       }
    }

    // partition column:
    catalog::Column const* partitionColumn = catalogTable.partitioncolumn();
    int partitionColumnIndex = -1;
    if (partitionColumn != NULL) {
        partitionColumnIndex = partitionColumn->index();
    }

    m_tableType = static_cast<TableType>(catalogTable.tableType());
    bool drEnabled = !forceNoDR && catalogTable.isDRed();
    bool isReplicated = catalogTable.isreplicated();
    m_materialized = isTableMaterialized(catalogTable);
    std::string const& tableName = catalogTable.name();
    int32_t databaseId = catalogDatabase.relativeIndex();
    SHA1_CTX shaCTX;
    SHA1Init(&shaCTX);
    SHA1Update(&shaCTX, reinterpret_cast<const uint8_t*>(catalogTable.signature().c_str()), (uint32_t )::strlen(catalogTable.signature().c_str()));
    SHA1Final(reinterpret_cast<unsigned char*>(m_signatureHash), &shaCTX);
    // Persistent table will use default size (2MB) if tableAllocationTargetSize is zero.
    if (m_materialized) {
      catalog::MaterializedViewInfo* mvInfo = catalogTable.materializer()->views().get(catalogTable.name());
      if (mvInfo && mvInfo->groupbycols().size() == 0) {
        // ENG-8490: If the materialized view came with no group by, set table block size to 64KB
        // to achieve better space efficiency.
        // FYI: maximum column count = 1024, largest fixed length data type is short varchars (64 bytes)
        tableAllocationTargetSize = 1024 * 64;
      }
    }
    VOLT_DEBUG("Creating %s %s as %s, type: %d", m_materialized?"VIEW":"TABLE",
               tableName.c_str(), isReplicated?"REPLICATED":"PARTITIONED", catalogTable.tableType());
    Table* table = TableFactory::getPersistentTable(
            databaseId, tableName.c_str(), schema, columnNames, m_signatureHash,
            m_materialized, partitionColumnIndex, m_tableType, tableAllocationTargetSize,
            m_compactionThreshold, drEnabled, isReplicated);
    PersistentTable* persistentTable = dynamic_cast<PersistentTable*>(table);
    if ( ! persistentTable) {
        vassert(pkeyIndexId.empty());
        vassert(indexes.empty());
        return table;
    }

    // add a pkey index if one exists
    if ( ! pkeyIndexId.empty()) {
        TableIndex* pkeyIndex = TableIndexFactory::getInstance(pkeyIndex_scheme);
        vassert(pkeyIndex);
        persistentTable->addIndex(pkeyIndex);
        persistentTable->setPrimaryKeyIndex(pkeyIndex);
    }

    // add other indexes
    BOOST_FOREACH(TableIndexScheme& scheme, indexes) {
        TableIndex* index = TableIndexFactory::getInstance(scheme);
        vassert(index);
        persistentTable->addIndex(index);
    }

    return table;
}

void TableCatalogDelegate::init(catalog::Database const& catalogDatabase,
                                catalog::Table const& catalogTable,
                                bool isXDCR) {
    m_table = constructTableFromCatalog(catalogDatabase,
                                        catalogTable,
                                        isXDCR);
    if ( ! m_table) {
        return;
    }

    // configure for stats tables
    PersistentTable* persistenttable = dynamic_cast<PersistentTable*>(m_table);
    if (persistenttable) {
        persistenttable->configureIndexStats();
    }
    m_table->incrementRefcount();
}

PersistentTable* TableCatalogDelegate::createDeltaTable(catalog::Database const& catalogDatabase,
        catalog::Table const& catalogTable) {
    bool isXDCR = ExecutorContext::getEngine()->getIsActiveActiveDREnabled();
    // Delta table will only have one row (currently).
    // Set the table block size to 64KB to achieve better space efficiency.
    // FYI: maximum column count = 1024, largest fixed length data type is short varchars (64 bytes)
    // Delta table must be forced to have DR disabled even if the source table is DRed,
    // therefore true is passed in for the forceNoDR parameter
    Table* deltaTable = constructTableFromCatalog(catalogDatabase, catalogTable, isXDCR, 1024 * 64, true);
    deltaTable->incrementRefcount();
    // We have the restriction that view on joined table cannot have non-persistent table source.
    // So here we could use static_cast. But if we in the future want to lift this limitation,
    // we will have to put more thoughts on this.
    return static_cast<PersistentTable*>(deltaTable);
}

static void migrateChangedTuples(catalog::Table const& catalogTable,
        PersistentTable* existingTable, PersistentTable* newTable) {
    int64_t existingTupleCount = existingTable->activeTupleCount();

    // remove all indexes from the existing table
    const std::vector<TableIndex*> currentIndexes = existingTable->allIndexes();
    for (int i = 0; i < currentIndexes.size(); i++) {
        existingTable->removeIndex(currentIndexes[i]);
    }

    // All the (surviving) materialized views depending on the existing table will need to be "transfered"
    // to the new table -- BUT there's no rush.
    // The "deleteTupleForSchemaChange" variant of deleteTuple used here on the existing table
    // leaves any dependent materialized view tables untouched/intact
    // (technically, temporarily out of synch with the shrinking table).
    // But the normal "insertPersistentTuple" used here on the new table tries to populate any dependent
    // materialized views.
    // Rather than empty the surviving view tables, and transfer them to the new table to be re-populated "retail",
    // transfer them "wholesale" post-migration.

    // figure out what goes in each columns of the new table

    // set default values once in the temp tuple
    int columnCount = newTable->columnCount();

    // default values
    boost::scoped_array<NValue> defaults_array(new NValue[columnCount]);
    NValue* defaults = defaults_array.get();

    // map from existing table
    int columnSourceMap[columnCount];

    // Indicator that object allocation is required in the column assignment,
    // to cover an explosion from an inline-sized to an out-of-line-sized string.
    bool columnExploded[columnCount];

    std::vector<std::string> oldColumnNames = existingTable->getColumnNames();

    catalog::CatalogMap<catalog::Column>::field_map_iter colIter;
    for (colIter = catalogTable.columns().begin();
         colIter != catalogTable.columns().end();
         colIter++) {
        auto column = colIter->second;
        std::string colName = column->name();
        int newIndex = column->index();

        // assign a default value, if one exists
        ValueType defaultColType = static_cast<ValueType>(column->defaulttype());
        if (defaultColType == ValueType::tINVALID) {
            defaults[newIndex] = ValueFactory::getNullValue();
        }
        else {
            std::string defaultValue = column->defaultvalue();
            // this could probably use the temporary string pool instead?
            // (Instead of passing NULL to use persistant storage)
            defaults[newIndex] = ValueFactory::nvalueFromSQLDefaultType(defaultColType, defaultValue, NULL);
        }

        // find a source column in the existing table, if one exists
        columnSourceMap[newIndex] = -1; // -1 is code for not found, use defaults
        for (int oldIndex = 0; oldIndex < oldColumnNames.size(); oldIndex++) {
            if (oldColumnNames[oldIndex].compare(colName) == 0) {
                columnSourceMap[newIndex] = oldIndex;
                columnExploded[newIndex] = (existingTable->schema()->columnIsInlined(oldIndex) &&
                                            ! newTable->schema()->columnIsInlined(newIndex));
                break;
            }
        }
    }

    TableTuple scannedTuple(existingTable->schema());

    int64_t tuplesMigrated = 0;

    // going to run until the source table has no allocated blocks
    size_t blocksLeft = existingTable->allocatedBlockCount();
    while (blocksLeft) {

        TableIterator iterator(existingTable->iterator());
        TableTuple& tupleToInsert = newTable->tempTuple();

        while (iterator.next(scannedTuple)) {

            //printf("tuple: %s\n", scannedTuple.debug(existingTable->name()).c_str());

            // set the values from the old table or from defaults
            for (int i = 0; i < columnCount; i++) {
                if (columnSourceMap[i] >= 0) {
                    NValue value = scannedTuple.getNValue(columnSourceMap[i]);
                    if (columnExploded[i]) {
                        value.allocateObjectFromPool();
                    }
                    tupleToInsert.setNValue(i, value);
                }
                else {
                    tupleToInsert.setNValue(i, defaults[i]);
                }
            }

            // insert into the new table
            newTable->insertPersistentTuple(tupleToInsert, false);

            // delete from the old table
            existingTable->deleteTupleForSchemaChange(scannedTuple);

            // note one tuple moved
            ++tuplesMigrated;

            // if a block was just deleted, start the iterator again on the next block
            // this avoids using the block iterator over a changing set of blocks
            size_t prevBlocksLeft = blocksLeft;
            blocksLeft = existingTable->allocatedBlockCount();
            if (blocksLeft < prevBlocksLeft) {
                break;
            }
        }
    }

    // release any memory held by the default values --
    // normally you'd want this in a finally block, but since this code failing
    // implies serious problems, we'll not worry our pretty little heads
    for (int i = 0; i < columnCount; i++) {
        defaults[i].free();
    }

    // check tuple counts are sane
    vassert(newTable->activeTupleCount() == existingTupleCount);
    // dumb way to structure an assert avoids unused variable warning (lame)
    if (tuplesMigrated != existingTupleCount) {
        vassert(tuplesMigrated == existingTupleCount);
    }
}

static void migrateViews(const catalog::CatalogMap<catalog::MaterializedViewInfo>& views,
                         PersistentTable* existingTable, PersistentTable* newTable,
                         std::map<std::string, TableCatalogDelegate*> const& delegatesByName) {
    std::vector<catalog::MaterializedViewInfo*> survivingInfos;
    std::vector<MaterializedViewTriggerForWrite*> survivingViews;
    std::vector<MaterializedViewTriggerForWrite*> obsoleteViews;

    // Now, it's safe to transfer the wholesale state of the surviving
    // dependent materialized views.
    MaterializedViewTriggerForWrite::segregateMaterializedViews(existingTable->views(),
            views.begin(), views.end(),
            survivingInfos, survivingViews, obsoleteViews);

    // This process temporarily duplicates the materialized view definitions and their
    // target table reference counts for all the right materialized view tables,
    // leaving the others to go away with the existingTable.
    // Since this is happening "mid-stream" in the redefinition of all of the source and target tables,
    // there needs to be a way to handle cases where the target table HAS been redefined already and
    // cases where it HAS NOT YET been redefined (and cases where it just survives intact).
    // At this point, the materialized view makes a best effort to use the
    // current/latest version of the table -- particularly, because it will have made off with the
    // "old" version's primary key index, which is used in the MaterializedViewTriggerForInsert constructor.
    // Once ALL tables have been added/(re)defined, any materialized view definitions that still use
    // an obsolete target table needs to be brought forward to reference the replacement table.
    // See initMaterializedViews

    for (int ii = 0; ii < survivingInfos.size(); ++ii) {
        catalog::MaterializedViewInfo* currInfo = survivingInfos[ii];
        PersistentTable* oldDestTable = survivingViews[ii]->destTable();
        // Use the now-current definiton of the target table, to be updated later, if needed.
        TableCatalogDelegate* targetDelegate = findInMapOrNull(oldDestTable->name(),
                                                               delegatesByName);
        PersistentTable* destTable = oldDestTable; // fallback value if not (yet) redefined.
        if (targetDelegate) {
            PersistentTable* newDestTable = targetDelegate->getPersistentTable();
            if (newDestTable) {
                destTable = newDestTable;
            }
        }
        // This guards its destTable from accidental deletion with a refcount bump.
        MaterializedViewTriggerForWrite::build(newTable, destTable, currInfo);
    }
}

static void migrateExportViews(catalog::CatalogMap<catalog::MaterializedViewInfo> const& views,
                               StreamedTable* existingTable, StreamedTable* newTable,
                  std::map<std::string, TableCatalogDelegate*> const& delegatesByName) {
    std::vector<catalog::MaterializedViewInfo*> survivingInfos;
    std::vector<MaterializedViewTriggerForStreamInsert*> survivingViews;
    std::vector<MaterializedViewTriggerForStreamInsert*> obsoleteViews;

    // Now, it's safe to transfer the wholesale state of the surviving
    // dependent materialized views.
    MaterializedViewTriggerForStreamInsert::segregateMaterializedViews(existingTable->views(),
            views.begin(), views.end(),
            survivingInfos, survivingViews, obsoleteViews);
    vassert(obsoleteViews.size() == 0);

    // This process temporarily duplicates the materialized view definitions and their
    // target table reference counts for all the right materialized view tables,
    // leaving the others to go away with the existingTable.
    // Since this is happening "mid-stream" in the redefinition of all of the source and target tables,
    // there needs to be a way to handle cases where the target table HAS been redefined already and
    // cases where it HAS NOT YET been redefined (and cases where it just survives intact).
    // At this point, the materialized view makes a best effort to use the
    // current/latest version of the table -- particularly, because it will have made off with the
    // "old" version's primary key index, which is used in the MaterializedViewInsertTrigger constructor.
    // Once ALL tables have been added/(re)defined, any materialized view definitions that still use
    // an obsolete target table need to be brought forward to reference the replacement table.
    // See initMaterializedViews

    for (int ii = 0; ii < survivingInfos.size(); ++ii) {
        catalog::MaterializedViewInfo* currInfo = survivingInfos[ii];
        PersistentTable* oldDestTable = survivingViews[ii]->destTable();
        // Use the now-current definiton of the target table, to be updated later, if needed.
        TableCatalogDelegate* targetDelegate = findInMapOrNull(oldDestTable->name(),
                                                               delegatesByName);
        PersistentTable* destTable = oldDestTable; // fallback value if not (yet) redefined.
        if (targetDelegate) {
            PersistentTable* newDestTable = targetDelegate->getPersistentTable();
            if (newDestTable) {
                destTable = newDestTable;
            }
        }
        // This guards its destTable from accidental deletion with a refcount bump.
        MaterializedViewTriggerForStreamInsert::build(newTable, destTable, currInfo);
    }
}

void
TableCatalogDelegate::processSchemaChanges(catalog::Database const& catalogDatabase,
                                           catalog::Table const& catalogTable,
                                           std::map<std::string, TableCatalogDelegate*> const& delegatesByName,
                                           bool isXDCR) {
    DRTupleStreamDisableGuard guard(ExecutorContext::getExecutorContext());

    ///////////////////////////////////////////////
    // Create a new table so two tables exist.
    // Make this delegate point to the new table,
    // so we can migrate views below, which may
    // contains plans that reference this table
    ///////////////////////////////////////////////

    Table* existingTable = m_table;
    m_table = constructTableFromCatalog(catalogDatabase, catalogTable, isXDCR);
    vassert(m_table);
    m_table->incrementRefcount();
    PersistentTable* newPersistentTable = dynamic_cast<PersistentTable*>(m_table);
    PersistentTable* existingPersistentTable = dynamic_cast<PersistentTable*>(existingTable);
    StreamedTable* newStreamedTable = dynamic_cast<StreamedTable*>(m_table);
    StreamedTable* existingStreamedTable = dynamic_cast<StreamedTable*>(existingTable);

    ///////////////////////////////////////////////
    // Move tuples from one table to the other
    ///////////////////////////////////////////////
    if (existingPersistentTable && newPersistentTable) {
        migrateChangedTuples(catalogTable, existingPersistentTable, newPersistentTable);
        migrateViews(catalogTable.views(), existingPersistentTable, newPersistentTable, delegatesByName);
        existingStreamedTable = existingPersistentTable->getStreamedTable();
        newStreamedTable = newPersistentTable->getStreamedTable();
    }
    if (existingStreamedTable && newStreamedTable) {
        ExportTupleStream* wrapper = existingStreamedTable->getWrapper();
        // There should be no pending buffer at the time of UAC
        vassert(wrapper != NULL &&
                (wrapper->getCurrBlock() == NULL ||
                 wrapper->getCurrBlock()->getRowCount() == 0));
        existingStreamedTable->setWrapper(NULL);
        newStreamedTable->setWrapper(wrapper);
        migrateExportViews(catalogTable.views(), existingStreamedTable, newStreamedTable, delegatesByName);
    }

    ///////////////////////////////////////////////
    // Drop the old table
    ///////////////////////////////////////////////
    if (existingPersistentTable && newPersistentTable &&
            newPersistentTable->isReplicatedTable() != existingPersistentTable->isReplicatedTable()) {
        // A table can only be modified from replicated to partitioned
        vassert(newPersistentTable->isReplicatedTable());
        // Assume the MP memory context before starting the deallocate
        ExecuteWithMpMemory useMpMemory;
        existingTable->decrementRefcount();
    }
    else {
        existingTable->decrementRefcount();
    }

    ///////////////////////////////////////////////
    // Patch up the new table as a replacement
    ///////////////////////////////////////////////

    // configure for stats tables
    if (newPersistentTable) {
        newPersistentTable->configureIndexStats();
    }
}

void TableCatalogDelegate::deleteCommand() {
    if (m_table) {
        m_table->decrementRefcount();
        m_table = NULL;
    }
}

static bool isDefaultNow(std::string const& defaultValue) {
    std::vector<std::string> tokens;
    boost::split(tokens, defaultValue, boost::is_any_of(":"));
    if (tokens.size() != 2) {
        return false;
    }

    int funcId = boost::lexical_cast<int>(tokens[1]);
    return funcId == FUNC_CURRENT_TIMESTAMP;
}

// This method produces a row containing all the default values for
// the table, skipping over fields explictly set, and adding "default
// now" fields to nowFields.
void TableCatalogDelegate::initTupleWithDefaultValues(Pool* pool,
                                                      catalog::Table const* catalogTable,
                                                      std::set<int> const& fieldsExplicitlySet,
                                                      TableTuple& tbTuple,
                                                      std::vector<int>& nowFields) {
    catalog::CatalogMap<catalog::Column>::field_map_iter colIter;
    for (colIter = catalogTable->columns().begin();
         colIter != catalogTable->columns().end();
         colIter++) {
        auto col = colIter->second;
        if (fieldsExplicitlySet.find(col->index()) != fieldsExplicitlySet.end()) {
            // this field will be set explicitly so no need to
            // serialize the default value
            continue;
        }

        ValueType defaultColType = static_cast<ValueType>(col->defaulttype());

        switch (defaultColType) {
            case ValueType::tINVALID:
                tbTuple.setNValue(col->index(), ValueFactory::getNullValue());
                break;

            case ValueType::tTIMESTAMP:
                if (isDefaultNow(col->defaultvalue())) {
                    // Caller will need to set this to the current
                    // timestamp at the appropriate time
                    nowFields.push_back(col->index());
                    break;
                }
                /* fall through */ // gcc-7 needs this comment.
                // else, fall through to default case
            default:
                NValue defaultValue = ValueFactory::nvalueFromSQLDefaultType(defaultColType,
                        col->defaultvalue(),
                        pool);
                tbTuple.setNValue(col->index(), defaultValue);
                break;
        }
    }
}

// only allowed on streams to handle topic transitions (STREAM <--> CONNECTOR_LESS_STREAM)
void TableCatalogDelegate::setTableType(TableType tableType) {
    vassert(tableTypeIsStream(tableType));
    vassert(tableTypeIsStream(m_tableType));
    m_tableType = tableType;
}

}
