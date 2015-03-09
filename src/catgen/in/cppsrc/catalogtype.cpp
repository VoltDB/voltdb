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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

#include <cctype>
#include <cstdlib>
#include "catalogtype.h"
#include "catalogmap.h"
#include "catalog.h"
#include "common/SerializableEEException.h"

using namespace voltdb;
using namespace std;
using namespace catalog;

CatalogType::CatalogType(Catalog * catalog, CatalogType * parent, const string &path, const string & name) {
    m_catalog = catalog;
    m_parent = parent;
    m_name = name;
    m_path = path;
    m_relativeIndex = -1;

    if (this != m_catalog) {
        m_catalog->registerGlobally(this);
    }
}

CatalogType::~CatalogType() {
    if (this != m_catalog) {
        m_catalog->unregisterGlobally(this);
    }
}

void CatalogType::set(const string &field, const string &value) {
    CatalogValue val;
    int32_t indicator = tolower(value[0]);
    // paths
    if (indicator == '/') {
        //printf("Adding a path ref for %s[%s:\n    %s\n", path().c_str(), field.c_str(), value.c_str());
        //fflush(stdout);
        CatalogType *type = m_catalog->itemForRef(value);
        if (!type) {
            //printf("Adding unresolved info for path:\n    %s\n", value.c_str());
            //fflush(stdout);
            m_catalog->addUnresolvedInfo(value, this, field);
            update();
            return;
        }
        val.typeValue = type;
    }
    // null paths
    else if (indicator == 'n')
        val.intValue = 0;
    // strings
    else if (indicator == '\"')
        val.strValue = value.substr(1, value.length() - 2);
    // boolean (true)
    else if (indicator == 't')
        val.intValue = 1;
    // boolean (false)
    else if (indicator == 'f')
        val.intValue = 0;
    // Integers (including negatives)
    else if (isdigit(indicator) || (indicator == '-' && value.length() > 1 && isdigit(value[1])))
        val.intValue = atoi(value.c_str());
    else {
        string msg = "Invalid value '" + value + "' for field '" + field + "'";
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      msg.c_str());
    }

    m_fields[field] = val;
    update();
}

string CatalogType::name() const {
    return m_name;
}

string CatalogType::path() const {
    return m_path;
}

CatalogType * CatalogType::parent() const {
    return m_parent;
}

Catalog *CatalogType::catalog() const {
    return m_catalog;
}

int32_t CatalogType::relativeIndex() const {
    return m_relativeIndex;
}
