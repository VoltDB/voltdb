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

#ifndef TABLECATALOGDELEGATE_HPP
#define TABLECATALOGDELEGATE_HPP

#include "catalog/table.h"
#include "catalog/index.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"

namespace catalog {
class Database;
}

namespace voltdb {
class Table;
class PersistentTable;
class StreamedTable;
class Pool;
class ExecutorContext;
class TupleSchema;
struct TableIndexScheme;
class DRTupleStream;
class VoltDBEngine;

// There might be a better place for this, but current callers happen to have this header in common.
template<typename K, typename V> V findInMapOrNull(const K& key, std::map<K, V> const &the_map)
{
    typename std::map<K, V>::const_iterator lookup = the_map.find(key);
    if (lookup != the_map.end()) {
        return lookup->second;
    }
    return (V)NULL;
}

template<typename K, typename V> V findInMapOrNull(const K& key, std::unordered_map<K, V> const &the_map)
{
    typename std::unordered_map<K, V>::const_iterator lookup = the_map.find(key);
    if (lookup != the_map.end()) {
        return lookup->second;
    }
    return (V)NULL;
}

/*
 * Implementation of CatalogDelgate for Table
 */

class TableCatalogDelegate {
  public:
    TableCatalogDelegate(const std::string& signature, int32_t compactionThreshold, VoltDBEngine* engine)
        : m_table(NULL)
        , m_tableType(PERSISTENT)
        , m_materialized(false)
        , m_signature(signature)
        , m_compactionThreshold(compactionThreshold)
    {}

    ~TableCatalogDelegate();

    void deleteCommand();

    void init(catalog::Database const &catalogDatabase,
              catalog::Table const &catalogTable,
              bool isXDCR);
    PersistentTable *createDeltaTable(catalog::Database const &catalogDatabase,
            catalog::Table const &catalogTable);

    void processSchemaChanges(catalog::Database const &catalogDatabase,
                             catalog::Table const &catalogTable,
                              std::map<std::string, TableCatalogDelegate*> const &tablesByName,
                              bool isXDCR);

    static TupleSchema *createTupleSchema(catalog::Table const &catalogTable,
                                          bool isXDCR);

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

    /**
     * Sets each field in the tuple to the default value for the
     * table.  Schema is assumed to be the same as the target table.
     * 1. This method will skip over the fields whose indices appear in
     *    parameter fieldsExplicitlySet.
     * 2. If any timestamp columns with default of NOW are found,
     *    their indices will be appended to nowFields.  It's up to the
     *    caller to set these to the appropriate time.
     */
    void initTupleWithDefaultValues(Pool* pool,
                                    catalog::Table const *catalogTable,
                                    const std::set<int>& fieldsExplicitlySet,
                                    TableTuple& tbTuple,
                                    std::vector<int>& nowFields);

    Table *getTable() const;

    TableType getTableType() const {
        return m_tableType;
    }

    void setTableType(TableType tableType);

    PersistentTable *getPersistentTable() {
        return dynamic_cast<PersistentTable*>(m_table);
    }

    StreamedTable *getStreamedTable() {
        return dynamic_cast<StreamedTable *> (m_table);
    }

    void setTable(Table * tb) {
        m_table = tb;
    }

    const std::string& signature() const { return m_signature; }

    const char* signatureHash() const { return m_signatureHash; }

    int64_t signatureHashAsLong() const { return *reinterpret_cast<const int64_t*>(signatureHash()); }

    /*
     * Returns true if this table is a materialized view
     */
    bool materialized() { return m_materialized; }
  private:
    Table *constructTableFromCatalog(catalog::Database const &catalogDatabase,
                                     catalog::Table const &catalogTable,
                                     bool isXDCR,
                                     int tableAllocationTargetSize = 0,
                                     /* indicates whether the constructed table should inherit isDRed attributed from
                                      * the provided catalog table or set isDRed to false forcefully. Currently, only
                                      * delta tables for joins in materialized views use the second option */
                                     bool forceNoDR = false);

    voltdb::Table *m_table;
    TableType m_tableType;
    bool m_materialized;
    const std::string m_signature;
    const int32_t m_compactionThreshold;
    char m_signatureHash[20];
};

}

#endif
