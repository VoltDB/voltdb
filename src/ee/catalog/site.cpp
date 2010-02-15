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

#include "site.h"
#include "catalog.h"
#include "host.h"
#include "partition.h"

using namespace catalog;
using namespace std;

Site::Site(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["isexec"] = value;
    m_fields["host"] = value;
    m_fields["partition"] = value;
    m_fields["initiatorid"] = value;
}

void Site::update() {
    m_isexec = m_fields["isexec"].intValue;
    m_host = m_fields["host"].typeValue;
    m_partition = m_fields["partition"].typeValue;
    m_initiatorid = m_fields["initiatorid"].intValue;
}

CatalogType * Site::addChild(const std::string &collectionName, const std::string &childName) {
    throw std::string("Trying to add to an unknown child collection.");
    return NULL;
}

CatalogType * Site::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool Site::isexec() const {
    return m_isexec;
}

const Host * Site::host() const {
    return dynamic_cast<Host*>(m_host);
}

const Partition * Site::partition() const {
    return dynamic_cast<Partition*>(m_partition);
}

int32_t Site::initiatorid() const {
    return m_initiatorid;
}

