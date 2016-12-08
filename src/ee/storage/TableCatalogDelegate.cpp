/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include "common/types.h"
#include "common/TupleSchemaBuilder.h"
#include "common/ValueFactory.hpp"
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
#include "sha1/sha1.h"

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>

#include <string>
#include <vector>
#include <map>

using namespace std;

namespace voltdb {

TableCatalogDelegate::~TableCatalogDelegate()
{
    if (m_table) {
        m_table->decrementRefcount();
    }
}

Table *TableCatalogDelegate::getTable() const {
    // If a persistent table has an active delta table, return the delta table instead of the whole table.
    PersistentTable *persistentTable = dynamic_cast<PersistentTable*>(m_table);
    if (persistentTable && persistentTable->isDeltaTableActive()) {
        return persistentTable->deltaTable();
    }
    return m_table;
}

TupleSchema *TableCatalogDelegate::createTupleSchema(catalog::Database const &catalogDatabase,
                                                     catalog::Table const &catalogTable) {
    // Columns:
    // Column is stored as map<String, Column*> in Catalog. We have to
    // sort it by Column index to preserve column order.
    const int numColumns = static_cast<int>(catalogTable.columns().size());
    bool needsDRTimestamp = catalogDatabase.isActiveActiveDRed() && catalogTable.isDRed();
    TupleSchemaBuilder schemaBuilder(numColumns,
                                     needsDRTimestamp ? 1 : 0); // number of hidden columns

    map<string, catalog::Column*>::const_iterator col_iterator;
    for (col_iterator = catalogTable.columns().begin();
         col_iterator != catalogTable.columns().end(); col_iterator++) {

        const catalog::Column *catalog_column = col_iterator->second;
        schemaBuilder.setColumnAtIndex(catalog_column->index(),
                                       static_cast<ValueType>(catalog_column->type()),
                                       static_cast<int32_t>(catalog_column->size()),
                                       catalog_column->nullable(),
                                       catalog_column->inbytes());
    }

    if (needsDRTimestamp) {
        // Create a hidden timestamp column for a DRed table in an
        // active-active context.
        //
        // Column will be marked as not nullable in TupleSchema,
        // because we never expect a null value here, but this is not
        // actually enforced at runtime.
        schemaBuilder.setHiddenColumnAtIndex(0,
                                             VALUE_TYPE_BIGINT,
                                             8,      // field size in bytes
                                             false); // nulls not allowed
    }

    return schemaBuilder.build();
}

bool TableCatalogDelegate::getIndexScheme(catalog::Table const &catalogTable,
                                          catalog::Index const &catalogIndex,
                                          const TupleSchema *schema,
                                          TableIndexScheme *scheme)
{
    vector<int> index_columns;
    vector<ValueType> column_types;

    // The catalog::Index object now has a list of columns that are to be
    // used
    if (catalogIndex.columns().size() == (size_t)0) {
        VOLT_ERROR("Index '%s' in table '%s' does not declare any columns"
                   " to use",
                   catalogIndex.name().c_str(),
                   catalogTable.name().c_str());
        return false;
    }

    vector<AbstractExpression*> indexedExpressions = TableIndex::simplyIndexColumns();
    const std::string expressionsAsText = catalogIndex.expressionsjson();
    if (expressionsAsText.length() != 0) {
        ExpressionUtil::loadIndexedExprsFromJson(indexedExpressions, expressionsAsText);
    }

    // Since the columns are not going to come back in the proper order from
    // the catalogs, we'll use the index attribute to make sure we put them
    // in the right order
    index_columns.resize(catalogIndex.columns().size());
    map<string, catalog::ColumnRef*>::const_iterator colref_iterator;
    for (colref_iterator = catalogIndex.columns().begin();
         colref_iterator != catalogIndex.columns().end();
         colref_iterator++) {
        catalog::ColumnRef *catalog_colref = colref_iterator->second;
        if (catalog_colref->index() < 0) {
            VOLT_ERROR("Invalid column '%d' for index '%s' in table '%s'",
                       catalog_colref->index(),
                       catalogIndex.name().c_str(),
                       catalogTable.name().c_str());
            return false;
        }
        index_columns[catalog_colref->index()] = catalog_colref->column()->index();
    }
    // partial index predicate
    const std::string predicateAsText = catalogIndex.predicatejson();
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
                  const std::string& expressionsAsText, vector<int32_t> columnIndexes,
                  const std::string& predicateAsText) {
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
            assert(false);
            break;
    }

    // add whether it's counting or not
    if (countable) {
        retval += "C";
    }
    else {
        retval += "N"; // (N)ot countable?
    }

    // concat the target table column indexes into a unique string
    // using the order they appear in the index
    for (size_t i = 0; i < columnIndexes.size(); i++) {
        char buf[128];
        snprintf(buf, 128, "-%d", columnIndexes[i]);
        retval += buf;
    }

    // Expression indexes need to have IDs that stand out as unique from each other and from colunn indexes
    // that may reference the exact same set of columns.
    if (expressionsAsText.length() != 0) {
        retval += expressionsAsText;
    }
    // Add partial index predicate if any
    if (!predicateAsText.empty()) {
        retval += predicateAsText;
    }
    return retval;
}

std::string
TableCatalogDelegate::getIndexIdString(const catalog::Index &catalogIndex)
{
    vector<int32_t> columnIndexes(catalogIndex.columns().size());

    // get the list of column indexes in the target table
    // in the order they appear in the index
    map<string, catalog::ColumnRef*>::const_iterator col_iterator;
    for (col_iterator = catalogIndex.columns().begin();
         col_iterator != catalogIndex.columns().end();
         col_iterator++)
    {
        int32_t index = col_iterator->second->index();
        const catalog::Column *catalogColumn = col_iterator->second->column();
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
TableCatalogDelegate::getIndexIdString(const TableIndexScheme &indexScheme)
{
    vector<int32_t> columnIndexes(indexScheme.columnIndices.size());

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


Table *TableCatalogDelegate::constructTableFromCatalog(catalog::Database const &catalogDatabase,
                                                       catalog::Table const &catalogTable,
                                                       int tableAllocationTargetSize)
{
    // Create a persistent table for this table in our catalog
    int32_t table_id = catalogTable.relativeIndex();

    // get an array of table column names
    const int numColumns = static_cast<int>(catalogTable.columns().size());
    map<string, catalog::Column*>::const_iterator col_iterator;
    vector<string> columnNames(numColumns);
    for (col_iterator = catalogTable.columns().begin();
         col_iterator != catalogTable.columns().end();
         col_iterator++)
    {
        const catalog::Column *catalog_column = col_iterator->second;
        columnNames[catalog_column->index()] = catalog_column->name();
    }

    // get the schema for the table
    TupleSchema *schema = createTupleSchema(catalogDatabase, catalogTable);

    // Indexes
    map<string, TableIndexScheme> index_map;
    map<string, catalog::Index*>::const_iterator idx_iterator;
    for (idx_iterator = catalogTable.indexes().begin();
         idx_iterator != catalogTable.indexes().end(); idx_iterator++) {
        catalog::Index *catalog_index = idx_iterator->second;

        TableIndexScheme index_scheme;
        if (getIndexScheme(catalogTable, *catalog_index, schema, &index_scheme)) {
            index_map[catalog_index->name()] = index_scheme;
        }
    }

    // Constraints
    string pkey_index_id;
    map<string, catalog::Constraint*>::const_iterator constraint_iterator;
    for (constraint_iterator = catalogTable.constraints().begin();
         constraint_iterator != catalogTable.constraints().end();
         constraint_iterator++) {
        catalog::Constraint *catalog_constraint = constraint_iterator->second;

        // Constraint Type
        ConstraintType type = (ConstraintType)catalog_constraint->type();
        switch (type) {
            case CONSTRAINT_TYPE_PRIMARY_KEY:
                // Make sure we have an index to use
                if (catalog_constraint->index() == NULL) {
                    VOLT_ERROR("The '%s' constraint '%s' on table '%s' does"
                               " not specify an index",
                               constraintutil::getTypeName(type).c_str(),
                               catalog_constraint->name().c_str(),
                               catalogTable.name().c_str());
                    return NULL;
                }
                // Make sure they didn't declare more than one primary key index
                else if (pkey_index_id.size() > 0) {
                    VOLT_ERROR("Trying to declare a primary key on table '%s'"
                               "using index '%s' but '%s' was already set as"
                               " the primary key",
                               catalogTable.name().c_str(),
                               catalog_constraint->index()->name().c_str(),
                               pkey_index_id.c_str());
                    return NULL;
                }
                pkey_index_id = catalog_constraint->index()->name();
                break;
            case CONSTRAINT_TYPE_UNIQUE:
                // Make sure we have an index to use
                // TODO: In the future I would like bring back my Constraint
                //       object so that we can keep track of everything that a
                //       table has...
                if (catalog_constraint->index() == NULL) {
                    VOLT_ERROR("The '%s' constraint '%s' on table '%s' does"
                               " not specify an index",
                               constraintutil::getTypeName(type).c_str(),
                               catalog_constraint->name().c_str(),
                               catalogTable.name().c_str());
                    return NULL;
                }
                break;
            // Unsupported
            case CONSTRAINT_TYPE_CHECK:
            case CONSTRAINT_TYPE_FOREIGN_KEY:
            case CONSTRAINT_TYPE_MAIN:
                VOLT_WARN("Unsupported type '%s' for constraint '%s'",
                          constraintutil::getTypeName(type).c_str(),
                          catalog_constraint->name().c_str());
                break;
            // Unknown
            default:
                VOLT_ERROR("Invalid constraint type '%s' for '%s'",
                           constraintutil::getTypeName(type).c_str(),
                           catalog_constraint->name().c_str());
                return NULL;
        }
    }

    // Build the index array
    // Please note the index array should follow the order of primary key first,
    // all unique indices afterwards, and all the non-unique indices at the end.
    deque<TableIndexScheme> indexes;
    TableIndexScheme pkey_index_scheme;
    map<string, TableIndexScheme>::const_iterator index_iterator;
    for (index_iterator = index_map.begin(); index_iterator != index_map.end();
         index_iterator++) {
        // Exclude the primary key
        if (index_iterator->second.name.compare(pkey_index_id) == 0) {
            pkey_index_scheme = index_iterator->second;
        // Just add it to the list
        } else {
            if (index_iterator->second.unique) {
                indexes.push_front(index_iterator->second);
            } else {
                indexes.push_back(index_iterator->second);
            }
        }
    }

    // partition column:
    const catalog::Column* partitionColumn = catalogTable.partitioncolumn();
    int partitionColumnIndex = -1;
    if (partitionColumn != NULL) {
        partitionColumnIndex = partitionColumn->index();
    }

    bool exportEnabled = isExportEnabledForTable(catalogDatabase, table_id);
    bool tableIsExportOnly = isTableExportOnly(catalogDatabase, table_id);
    bool drEnabled = catalogTable.isDRed();
    m_materialized = isTableMaterialized(catalogTable);
    const string& tableName = catalogTable.name();
    int32_t databaseId = catalogDatabase.relativeIndex();
    SHA1_CTX shaCTX;
    SHA1Init(&shaCTX);
    SHA1Update(&shaCTX, reinterpret_cast<const uint8_t *>(catalogTable.signature().c_str()), (uint32_t )::strlen(catalogTable.signature().c_str()));
    SHA1Final(reinterpret_cast<unsigned char *>(m_signatureHash), &shaCTX);
    // Persistent table will use default size (2MB) if tableAllocationTargetSize is zero.
    if (m_materialized) {
      catalog::MaterializedViewInfo *mvInfo = catalogTable.materializer()->views().get(catalogTable.name());
      if (mvInfo && mvInfo->groupbycols().size() == 0) {
        // ENG-8490: If the materialized view came with no group by, set table block size to 64KB
        // to achieve better space efficiency.
        // FYI: maximum column count = 1024, largest fixed length data type is short varchars (64 bytes)
        tableAllocationTargetSize = 1024 * 64;
      }
    }
    Table *table = TableFactory::getPersistentTable(databaseId, tableName,
                                                    schema, columnNames, m_signatureHash,
                                                    m_materialized,
                                                    partitionColumnIndex, exportEnabled,
                                                    tableIsExportOnly,
                                                    tableAllocationTargetSize,
                                                    catalogTable.tuplelimit(),
                                                    m_compactionThreshold,
                                                    drEnabled);
    PersistentTable* persistentTable = dynamic_cast<PersistentTable*>(table);
    if ( ! persistentTable) {
        return table;
    }

    // add a pkey index if one exists
    if (pkey_index_id.size() != 0) {
        TableIndex *pkeyIndex = TableIndexFactory::getInstance(pkey_index_scheme);
        assert(pkeyIndex);
        persistentTable->addIndex(pkeyIndex);
        persistentTable->setPrimaryKeyIndex(pkeyIndex);
    }

    // add other indexes
    BOOST_FOREACH(TableIndexScheme &scheme, indexes) {
        TableIndex *index = TableIndexFactory::getInstance(scheme);
        assert(index);
        persistentTable->addIndex(index);
    }

    return table;
}

void TableCatalogDelegate::init(catalog::Database const &catalogDatabase,
        catalog::Table const &catalogTable)
{
    m_table = constructTableFromCatalog(catalogDatabase,
                                        catalogTable);
    if (!m_table) {
        return;
    }

    evaluateExport(catalogDatabase, catalogTable);

    // configure for stats tables
    PersistentTable* persistenttable = dynamic_cast<PersistentTable*>(m_table);
    if (persistenttable) {
        persistenttable->configureIndexStats();
    }
    m_table->incrementRefcount();
}

PersistentTable* TableCatalogDelegate::createDeltaTable(catalog::Database const &catalogDatabase,
        catalog::Table const &catalogTable)
{
    // Delta table will only have one row (currently).
    // Set the table block size to 64KB to achieve better space efficiency.
    // FYI: maximum column count = 1024, largest fixed length data type is short varchars (64 bytes)
    Table *deltaTable = constructTableFromCatalog(catalogDatabase, catalogTable, 1024 * 64);
    deltaTable->incrementRefcount();
    // We have the restriction that view on joined table cannot have non-persistent table source.
    // So here we could use static_cast. But if we in the future want to lift this limitation,
    // we will have to put more thoughts on this.
    return static_cast<PersistentTable*>(deltaTable);
}

//After catalog is updated call this to ensure your export tables are connected correctly.
void TableCatalogDelegate::evaluateExport(catalog::Database const &catalogDatabase,
                           catalog::Table const &catalogTable)
{
    m_exportEnabled = isExportEnabledForTable(catalogDatabase, catalogTable.relativeIndex());
}

static void migrateChangedTuples(catalog::Table const &catalogTable,
        PersistentTable* existingTable, PersistentTable* newTable)
{
    int64_t existingTupleCount = existingTable->activeTupleCount();

    // remove all indexes from the existing table
    const vector<TableIndex*> currentIndexes = existingTable->allIndexes();
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
    NValue *defaults = defaults_array.get();

    // map from existing table
    int columnSourceMap[columnCount];

    // Indicator that object allocation is required in the column assignment,
    // to cover an explosion from an inline-sized to an out-of-line-sized string.
    bool columnExploded[columnCount];

    vector<std::string> oldColumnNames = existingTable->getColumnNames();

    catalog::CatalogMap<catalog::Column>::field_map_iter colIter;
    for (colIter = catalogTable.columns().begin();
         colIter != catalogTable.columns().end();
         colIter++)
    {
        std::string colName = colIter->second->name();
        catalog::Column *column = colIter->second;
        int newIndex = column->index();

        // assign a default value, if one exists
        ValueType defaultColType = static_cast<ValueType>(column->defaulttype());
        if (defaultColType == VALUE_TYPE_INVALID) {
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

        TableIterator &iterator = existingTable->iterator();
        TableTuple &tupleToInsert = newTable->tempTuple();

        while (iterator.next(scannedTuple)) {

            //printf("tuple: %s\n", scannedTuple.debug(existingTable->name()).c_str());

            // set the values from the old table or from defaults
            for (int i = 0; i < columnCount; i++) {
                if (columnSourceMap[i] >= 0) {
                    NValue value = scannedTuple.getNValue(columnSourceMap[i]);
                    if (columnExploded[i]) {
                        value.allocateObjectFromInlinedValue(NULL);
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
    assert(newTable->activeTupleCount() == existingTupleCount);
    // dumb way to structure an assert avoids unused variable warning (lame)
    if (tuplesMigrated != existingTupleCount) {
        assert(tuplesMigrated == existingTupleCount);
    }
}

static void migrateViews(const catalog::CatalogMap<catalog::MaterializedViewInfo>& views,
                         PersistentTable* existingTable, PersistentTable* newTable,
                         std::map<std::string, TableCatalogDelegate*> const& delegatesByName)
{
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
        catalog::MaterializedViewInfo * currInfo = survivingInfos[ii];
        PersistentTable* oldTargetTable = survivingViews[ii]->targetTable();
        // Use the now-current definiton of the target table, to be updated later, if needed.
        TableCatalogDelegate* targetDelegate = findInMapOrNull(oldTargetTable->name(),
                                                               delegatesByName);
        PersistentTable* targetTable = oldTargetTable; // fallback value if not (yet) redefined.
        if (targetDelegate) {
            PersistentTable* newTargetTable =
                dynamic_cast<PersistentTable*>(targetDelegate->getTable());
            if (newTargetTable) {
                targetTable = newTargetTable;
            }
        }
        // This guards its targetTable from accidental deletion with a refcount bump.
        MaterializedViewTriggerForWrite::build(newTable, targetTable, currInfo);
    }
}

static void migrateExportViews(const catalog::CatalogMap<catalog::MaterializedViewInfo>& views,
                  StreamedTable* existingTable, StreamedTable* newTable,
                  std::map<std::string, TableCatalogDelegate*> const& delegatesByName)
{
    std::vector<catalog::MaterializedViewInfo*> survivingInfos;
    std::vector<MaterializedViewTriggerForStreamInsert*> survivingViews;
    std::vector<MaterializedViewTriggerForStreamInsert*> obsoleteViews;

    // Now, it's safe to transfer the wholesale state of the surviving
    // dependent materialized views.
    MaterializedViewTriggerForStreamInsert::segregateMaterializedViews(existingTable->views(),
            views.begin(), views.end(),
            survivingInfos, survivingViews, obsoleteViews);
    assert(obsoleteViews.size() == 0);

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
        catalog::MaterializedViewInfo * currInfo = survivingInfos[ii];
        PersistentTable* oldTargetTable = survivingViews[ii]->targetTable();
        // Use the now-current definiton of the target table, to be updated later, if needed.
        TableCatalogDelegate* targetDelegate = findInMapOrNull(oldTargetTable->name(),
                                                               delegatesByName);
        PersistentTable* targetTable = oldTargetTable; // fallback value if not (yet) redefined.
        if (targetDelegate) {
            PersistentTable* newTargetTable =
                dynamic_cast<PersistentTable*>(targetDelegate->getTable());
            if (newTargetTable) {
                targetTable = newTargetTable;
            }
        }
        // This guards its targetTable from accidental deletion with a refcount bump.
        MaterializedViewTriggerForStreamInsert::build(newTable, targetTable, currInfo);
    }
}

void
TableCatalogDelegate::processSchemaChanges(catalog::Database const &catalogDatabase,
                                           catalog::Table const &catalogTable,
                                           std::map<std::string, TableCatalogDelegate*> const &delegatesByName)
{
    DRTupleStreamDisableGuard guard(ExecutorContext::getExecutorContext());

    ///////////////////////////////////////////////
    // Create a new table so two tables exist.
    // Make this delegate point to the new table,
    // so we can migrate views below, which may
    // contains plans that reference this table
    ///////////////////////////////////////////////

    Table* existingTable = m_table;
    m_table = constructTableFromCatalog(catalogDatabase, catalogTable);
    assert(m_table);
    m_table->incrementRefcount();
    PersistentTable* newPersistentTable = dynamic_cast<PersistentTable*>(m_table);
    PersistentTable* existingPersistentTable = dynamic_cast<PersistentTable*>(existingTable);

    ///////////////////////////////////////////////
    // Move tuples from one table to the other
    ///////////////////////////////////////////////
    if (existingPersistentTable && newPersistentTable) {
        migrateChangedTuples(catalogTable, existingPersistentTable, newPersistentTable);
        migrateViews(catalogTable.views(), existingPersistentTable, newPersistentTable, delegatesByName);
    }
    else {
        StreamedTable* newStreamedTable = dynamic_cast<StreamedTable*>(m_table);
        StreamedTable *existingStreamedTable = dynamic_cast<StreamedTable*>(existingTable);
        if (existingStreamedTable && newStreamedTable) {
            migrateExportViews(catalogTable.views(), existingStreamedTable, newStreamedTable, delegatesByName);
        }
    }

    ///////////////////////////////////////////////
    // Drop the old table
    ///////////////////////////////////////////////
    existingTable->decrementRefcount();

    ///////////////////////////////////////////////
    // Patch up the new table as a replacement
    ///////////////////////////////////////////////

    // configure for stats tables
    if (newPersistentTable) {
        newPersistentTable->configureIndexStats();
    }
}

void TableCatalogDelegate::deleteCommand()
{
    if (m_table) {
        m_table->decrementRefcount();
        m_table = NULL;
    }
}

static bool isDefaultNow(const std::string& defaultValue) {
    std::vector<std::string> tokens;
    boost::split(tokens, defaultValue, boost::is_any_of(":"));
    if (tokens.size() != 2) {
        return false;
    }

    int funcId = boost::lexical_cast<int>(tokens[1]);
    if (funcId == FUNC_CURRENT_TIMESTAMP) {
        return true;
    }

    return false;
}

// This method produces a row containing all the default values for
// the table, skipping over fields explictly set, and adding "default
// now" fields to nowFields.
void TableCatalogDelegate::initTupleWithDefaultValues(Pool* pool,
                                                      catalog::Table const *catalogTable,
                                                      const std::set<int>& fieldsExplicitlySet,
                                                      TableTuple& tbTuple,
                                                      std::vector<int>& nowFields) {
    catalog::CatalogMap<catalog::Column>::field_map_iter colIter;
    for (colIter = catalogTable->columns().begin();
         colIter != catalogTable->columns().end();
         colIter++) {

        catalog::Column *col = colIter->second;
        if (fieldsExplicitlySet.find(col->index()) != fieldsExplicitlySet.end()) {
            // this field will be set explicitly so no need to
            // serialize the default value
            continue;
        }

        ValueType defaultColType = static_cast<ValueType>(col->defaulttype());

        switch (defaultColType) {
        case VALUE_TYPE_INVALID:
            tbTuple.setNValue(col->index(), ValueFactory::getNullValue());
            break;

        case VALUE_TYPE_TIMESTAMP:
            if (isDefaultNow(col->defaultvalue())) {
                // Caller will need to set this to the current
                // timestamp at the appropriate time
                nowFields.push_back(col->index());
                break;
            }
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

}
