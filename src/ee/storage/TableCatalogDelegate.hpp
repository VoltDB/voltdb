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

#ifndef TABLECATALOGDELEGATE_HPP
#define TABLECATALOGDELEGATE_HPP

#include "common/CatalogDelegate.hpp"

namespace catalog {
class Table;
class Database;
}

namespace voltdb {
class Table;
class ExecutorContext;

/*
 * Implementation of CatalogDelgate for Table
 */

class TableCatalogDelegate : public CatalogDelegate {
  public:
    TableCatalogDelegate(int32_t catalogVersion, int32_t catalogId, std::string path);
    virtual ~TableCatalogDelegate();


    // Delegate interface
    virtual void deleteCommand();


    // table specific
    int init(ExecutorContext *executorContext,
             catalog::Database &catalogDatabase,
             catalog::Table &catalogTable);

    // ADXXX: should be const
    Table *getTable() {
        return m_table;
    }

    bool exportEnabled() {
        return m_exportEnabled;
    }

  private:
    voltdb::Table *m_table;
    bool m_exportEnabled;
};

}

#endif
