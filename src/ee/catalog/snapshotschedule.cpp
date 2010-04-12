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

#include <cassert>
#include "snapshotschedule.h"
#include "catalog.h"

using namespace catalog;
using namespace std;

SnapshotSchedule::SnapshotSchedule(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["frequencyUnit"] = value;
    m_fields["frequencyValue"] = value;
    m_fields["retain"] = value;
    m_fields["path"] = value;
    m_fields["prefix"] = value;
}

void SnapshotSchedule::update() {
    m_frequencyUnit = m_fields["frequencyUnit"].strValue.c_str();
    m_frequencyValue = m_fields["frequencyValue"].intValue;
    m_retain = m_fields["retain"].intValue;
    m_path = m_fields["path"].strValue.c_str();
    m_prefix = m_fields["prefix"].strValue.c_str();
}

CatalogType * SnapshotSchedule::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * SnapshotSchedule::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

void SnapshotSchedule::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
}

const string & SnapshotSchedule::frequencyUnit() const {
    return m_frequencyUnit;
}

int32_t SnapshotSchedule::frequencyValue() const {
    return m_frequencyValue;
}

int32_t SnapshotSchedule::retain() const {
    return m_retain;
}

const string & SnapshotSchedule::path() const {
    return m_path;
}

const string & SnapshotSchedule::prefix() const {
    return m_prefix;
}

