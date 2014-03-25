/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
#include "catalog/table.h"
#include "catalog/index.h"
#include "storage/persistenttable.h"

namespace catalog {
class Database;
}

namespace voltdb {
class Table;
class PersistentTable;
class ExecutorContext;
class TupleSchema;
struct TableIndexScheme;

// There might be a better place for this, but current callers happen to have this header in common.
template<typename K, typename V> V findInMapOrNull(const K& key, std::map<K, V> const &the_map)
{
    typename std::map<K, V>::const_iterator lookup = the_map.find(key);
    if (lookup != the_map.end()) {
        return lookup->second;
    }
    return (V)NULL;
}

/*
 * Implementation of CatalogDelgate for Table
 */

class TableCatalogDelegate : public CatalogDelegate {
  public:
    TableCatalogDelegate(int32_t catalogId, std::string path, std::string signature, int32_t compactionThreshold);
    virtual ~TableCatalogDelegate();


    // Delegate interface
    virtual void deleteCommand();

    // table specific
    int init(catalog::Database const &catalogDatabase,
             catalog::Table const &catalogTable);

    void processSchemaChanges(catalog::Database const &catalogDatabase,
                             catalog::Table const &catalogTable,
                             std::map<std::string, CatalogDelegate*> const &tablesByName);

    static void migrateChangedTuples(catalog::Table const &catalogTable,
                                     voltdb::PersistentTable* existingTable,
                                     voltdb::PersistentTable* newTable);

    static TupleSchema *createTupleSchema(catalog::Table const &catalogTable);

    static bool getIndexScheme(catalog::Table const &catalogTable,
                               catalog::Index const &catalogIndex,
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

    PersistentTable *getPersistentTable() {
        return dynamic_cast<PersistentTable *> (m_table);
    }

    void setTable(Table * tb) {
        m_table = tb;
    }

    bool exportEnabled() {
        return m_exportEnabled;
    }

    std::string signature() {
        return m_signature;
    }

  private:
    static Table *constructTableFromCatalog(catalog::Database const &catalogDatabase,
                                            catalog::Table const &catalogTable,
                                            const int32_t compactionThreshold);

    voltdb::Table *m_table;
    bool m_exportEnabled;
    std::string m_signature;
    const int32_t m_compactionThreshold;
};

}

#endif
