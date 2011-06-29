/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
#include "indexes/tableindex.h"
#include "storage/constraintutil.h"
#include "storage/MaterializedViewMetadata.h"
#include "storage/persistenttable.h"
#include "storage/StreamBlock.h"
#include "storage/table.h"
#include "storage/tablefactory.h"

#include <vector>
#include <map>

using namespace std;
namespace voltdb {

TableCatalogDelegate::TableCatalogDelegate(int32_t catalogId, string path, string signature) :
    CatalogDelegate(catalogId, path), m_table(NULL), m_exportEnabled(false),
    m_signature(signature)
{
}

TableCatalogDelegate::~TableCatalogDelegate()
{
    if (m_table) {
        m_table->decrementRefcount();
    }
}

int
TableCatalogDelegate::init(ExecutorContext *executorContext,
                           catalog::Database &catalogDatabase,
                           catalog::Table &catalogTable)
{
    // Create a persistent table for this table in our catalog
    int32_t table_id = catalogTable.relativeIndex();

    // Columns:
    // Column is stored as map<String, Column*> in Catalog. We have to
    // sort it by Column index to preserve column order.
    const int numColumns = static_cast<int>(catalogTable.columns().size());
    vector<ValueType> columnTypes(numColumns);
    vector<int32_t> columnLengths(numColumns);
    vector<bool> columnAllowNull(numColumns);
    map<string, catalog::Column*>::const_iterator col_iterator;
    string *columnNames = new string[numColumns];
    for (col_iterator = catalogTable.columns().begin();
         col_iterator != catalogTable.columns().end(); col_iterator++) {
        const catalog::Column *catalog_column = col_iterator->second;
        const int columnIndex = catalog_column->index();
        const ValueType type = static_cast<ValueType>(catalog_column->type());
        columnTypes[columnIndex] = type;
        const int32_t size = static_cast<int32_t>(catalog_column->size());
        //Strings length is provided, other lengths are derived from type
        bool varlength = (type == VALUE_TYPE_VARCHAR) || (type == VALUE_TYPE_VARBINARY);
        const int32_t length = varlength ? size : static_cast<int32_t>(NValue::getTupleStorageSize(type));
        columnLengths[columnIndex] = length;
        columnAllowNull[columnIndex] = catalog_column->nullable();
        columnNames[catalog_column->index()] = catalog_column->name();
    }

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes,
                                                         columnLengths,
                                                         columnAllowNull, true);

    // Indexes
    map<string, TableIndexScheme> index_map;
    map<string, catalog::Index*>::const_iterator idx_iterator;
    for (idx_iterator = catalogTable.indexes().begin();
         idx_iterator != catalogTable.indexes().end(); idx_iterator++) {
        catalog::Index *catalog_index = idx_iterator->second;
        vector<int> index_columns;
        vector<ValueType> column_types;

        // The catalog::Index object now has a list of columns that are to be
        // used
        if (catalog_index->columns().size() == (size_t)0) {
            VOLT_ERROR("Index '%s' in table '%s' does not declare any columns"
                       " to use",
                       catalog_index->name().c_str(),
                       catalogTable.name().c_str());
            delete [] columnNames;
            return false;
        }

        // Since the columns are not going to come back in the proper order from
        // the catalogs, we'll use the index attribute to make sure we put them
        // in the right order
        index_columns.resize(catalog_index->columns().size());
        column_types.resize(catalog_index->columns().size());
        bool isIntsOnly = true;
        map<string, catalog::ColumnRef*>::const_iterator colref_iterator;
        for (colref_iterator = catalog_index->columns().begin();
             colref_iterator != catalog_index->columns().end();
             colref_iterator++) {
            catalog::ColumnRef *catalog_colref = colref_iterator->second;
            if (catalog_colref->index() < 0) {
                VOLT_ERROR("Invalid column '%d' for index '%s' in table '%s'",
                           catalog_colref->index(),
                           catalog_index->name().c_str(),
                           catalogTable.name().c_str());
                delete [] columnNames;
                return false;
            }
            // check if the column does not have an int type
            if ((catalog_colref->column()->type() != VALUE_TYPE_TINYINT) &&
                (catalog_colref->column()->type() != VALUE_TYPE_SMALLINT) &&
                (catalog_colref->column()->type() != VALUE_TYPE_INTEGER) &&
                (catalog_colref->column()->type() != VALUE_TYPE_BIGINT)) {
                isIntsOnly = false;
            }
            index_columns[catalog_colref->index()] = catalog_colref->column()->index();
            column_types[catalog_colref->index()] = (ValueType) catalog_colref->column()->type();
        }

        TableIndexScheme index_scheme(catalog_index->name(),
                                      (TableIndexType)catalog_index->type(),
                                      index_columns,
                                      column_types,
                                      catalog_index->unique(),
                                      isIntsOnly,
                                      schema);
        index_map[catalog_index->name()] = index_scheme;
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
                    delete [] columnNames;
                    return false;
                }
                // Make sure they didn't declare more than one primary key index
                else if (pkey_index_id.size() > 0) {
                    VOLT_ERROR("Trying to declare a primary key on table '%s'"
                               "using index '%s' but '%s' was already set as"
                               " the primary key",
                               catalogTable.name().c_str(),
                               catalog_constraint->index()->name().c_str(),
                               pkey_index_id.c_str());
                    delete [] columnNames;
                    return false;
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
                    delete [] columnNames;
                    return false;
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
                delete [] columnNames;
                return false;
        }
    }

    // Build the index array
    vector<TableIndexScheme> indexes;
    TableIndexScheme pkey_index;
    map<string, TableIndexScheme>::const_iterator index_iterator;
    for (index_iterator = index_map.begin(); index_iterator != index_map.end();
         index_iterator++) {
        // Exclude the primary key
        if (index_iterator->first.compare(pkey_index_id) == 0) {
            pkey_index = index_iterator->second;
        // Just add it to the list
        } else {
            indexes.push_back(index_iterator->second);
        }
    }

    // partition column:
    const catalog::Column* partitionColumn = catalogTable.partitioncolumn();
    int partitionColumnIndex = -1;
    if (partitionColumn != NULL) {
        partitionColumnIndex = partitionColumn->index();
    }

    if (pkey_index_id.size() == 0) {
        int32_t databaseId = catalogDatabase.relativeIndex();
        m_table = TableFactory::getPersistentTable(databaseId, executorContext,
                                                 catalogTable.name(), schema, columnNames,
                                                 indexes, partitionColumnIndex,
                                                 isExportEnabledForTable(catalogDatabase, table_id),
                                                 isTableExportOnly(catalogDatabase, table_id));
    } else {
        int32_t databaseId = catalogDatabase.relativeIndex();
        m_table = TableFactory::getPersistentTable(databaseId, executorContext,
                                                 catalogTable.name(), schema, columnNames,
                                                 pkey_index, indexes, partitionColumnIndex,
                                                 isExportEnabledForTable(catalogDatabase, table_id),
                                                 isTableExportOnly(catalogDatabase, table_id));
    }
    delete[] columnNames;

    m_exportEnabled = isExportEnabledForTable(catalogDatabase, table_id);
    m_table->incrementRefcount();
    return 0;
}


void TableCatalogDelegate::deleteCommand()
{
    if (m_table) {
        m_table->decrementRefcount();
        m_table = NULL;
    }
}


}
