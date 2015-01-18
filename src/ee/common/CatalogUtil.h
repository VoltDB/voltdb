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

#ifndef CATALOGUTIL_H
#define CATALOGUTIL_H

#include "catalog/connector.h"
#include "catalog/connectortableinfo.h"
#include "catalog/database.h"
#include "catalog/table.h"

/*
  Global helper functions for extracting more complex
  information from the catalog.
*/


/**
 * A table is export only if any connector's table list marks it as
 * such. Search through the connector's table lists accordingly.
 */
bool isTableExportOnly(catalog::Database const & database, int32_t tableIndex) {

    // no export, no export only tables
    if (database.connectors().size() == 0) {
        return false;
    }

    // there is one well-known-named connector
    catalog::Connector *connector = database.connectors().get("0");

    // iterate the connector tableinfo list looking for tableindex
    std::map<std::string, catalog::ConnectorTableInfo*>::const_iterator it;
    for (it = connector->tableInfo().begin();
         it != connector->tableInfo().end();
         it++)
    {
        if (it->second->table()->relativeIndex() == tableIndex) {
            return it->second->appendOnly();
        }
    }

    return false;
}


/**
 * a table is only enable for export if explicitly listed in
 * a connector's table list and if export is enabled for the
 * database as a whole
 */
bool isExportEnabledForTable(catalog::Database const & database, int32_t tableIndex) {

    // export is disabled unless a connector exists
    if (database.connectors().size() == 0) {
        return false;
    }

    // there is one well-known-named connector
    catalog::Connector *connector = database.connectors().get("0");

    // export is disabled if the connector is disabled
    if (!(connector->enabled())) {
        return false;
    }

    // iterate the connector tableinfo list looking for tableIndex
    std::map<std::string, catalog::ConnectorTableInfo*>::const_iterator it;
    for (it = connector->tableInfo().begin();
         it != connector->tableInfo().end();
         it++)
    {
        if (it->second->table()->relativeIndex() == tableIndex) {
            return true;
        }
    }

    return false;
}

bool isTableMaterialized(const catalog::Table &table) {
    return table.materializer() != NULL;
}


#endif

