/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#ifndef TABLECATALOGDELEGATE_HPP
#define TABLECATALOGDELEGATE_HPP

#include "common/CatalogDelegate.hpp"
#include "indexes/tableindex.h"
#include "catalog/table.h"
#include "catalog/index.h"

namespace catalog {
class Database;
}

namespace voltdb {
class Table;
class ExecutorContext;
class TupleSchema;
struct TableIndexScheme;

/*
 * Implementation of CatalogDelgate for Table
 */

class TableCatalogDelegate : public CatalogDelegate {
  public:
    TableCatalogDelegate(int32_t catalogId, std::string path, std::string signature);
    virtual ~TableCatalogDelegate();


    // Delegate interface
    virtual void deleteCommand();


    // table specific
    int init(catalog::Database &catalogDatabase,
             catalog::Table &catalogTable);

    int processSchemaChanges(catalog::Database &catalogDatabase,
                             catalog::Table &catalogTable);

    static TupleSchema *createTupleSchema(catalog::Table &catalogTable);

    static bool getIndexScheme(catalog::Table &catalogTable,
                               catalog::Index &catalogIndex,
                               const TupleSchema *schema,
                               TableIndexScheme *scheme);

    /**
     * Return a string that identifies this index by table name and schema,
     * rather than by given/assigned name.
     */
    static std::string getIndexIdString(const catalog::Index &catalogIndex);
    static std::string getIndexIdString(const TableIndexScheme &indexScheme);

    // ADXXX: should be const
    Table *getTable() {
        return m_table;
    }

    bool exportEnabled() {
        return m_exportEnabled;
    }

    std::string signature() {
        return m_signature;
    }

  private:
    static Table *constructTableFromCatalog(catalog::Database &catalogDatabase,
                                            catalog::Table &catalogTable);

    voltdb::Table *m_table;
    bool m_exportEnabled;
    std::string m_signature;
};

}

#endif
