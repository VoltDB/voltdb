/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

#include "constraint.h"
#include "catalog.h"
#include "index.h"
#include "table.h"
#include "columnref.h"

using namespace catalog;
using namespace std;

Constraint::Constraint(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_foreignkeycols(catalog, this, path + "/" + "foreignkeycols")
{
    CatalogValue value;
    m_fields["type"] = value;
    m_fields["oncommit"] = value;
    m_fields["index"] = value;
    m_fields["foreignkeytable"] = value;
    m_childCollections["foreignkeycols"] = &m_foreignkeycols;
}

void Constraint::update() {
    m_type = m_fields["type"].intValue;
    m_oncommit = m_fields["oncommit"].strValue.c_str();
    m_index = m_fields["index"].typeValue;
    m_foreignkeytable = m_fields["foreignkeytable"].typeValue;
}

CatalogType * Constraint::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("foreignkeycols") == 0) {
        CatalogType *exists = m_foreignkeycols.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_foreignkeycols.add(childName);
    }
    throw std::string("Trying to add to an unknown child collection.");
    return NULL;
}

CatalogType * Constraint::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("foreignkeycols") == 0)
        return m_foreignkeycols.get(childName);
    return NULL;
}

int32_t Constraint::type() const {
    return m_type;
}

const string & Constraint::oncommit() const {
    return m_oncommit;
}

const Index * Constraint::index() const {
    return dynamic_cast<Index*>(m_index);
}

const Table * Constraint::foreignkeytable() const {
    return dynamic_cast<Table*>(m_foreignkeytable);
}

const CatalogMap<ColumnRef> & Constraint::foreignkeycols() const {
    return m_foreignkeycols;
}

