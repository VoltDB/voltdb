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

#include "stmtparameter.h"
#include "catalog.h"
#include "procparameter.h"

using namespace catalog;
using namespace std;

StmtParameter::StmtParameter(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["sqltype"] = value;
    m_fields["javatype"] = value;
    m_fields["index"] = value;
    m_fields["procparameter"] = value;
}

void StmtParameter::update() {
    m_sqltype = m_fields["sqltype"].intValue;
    m_javatype = m_fields["javatype"].intValue;
    m_index = m_fields["index"].intValue;
    m_procparameter = m_fields["procparameter"].typeValue;
}

CatalogType * StmtParameter::addChild(const std::string &collectionName, const std::string &childName) {
    throw std::string("Trying to add to an unknown child collection.");
    return NULL;
}

CatalogType * StmtParameter::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

int32_t StmtParameter::sqltype() const {
    return m_sqltype;
}

int32_t StmtParameter::javatype() const {
    return m_javatype;
}

int32_t StmtParameter::index() const {
    return m_index;
}

const ProcParameter * StmtParameter::procparameter() const {
    return dynamic_cast<ProcParameter*>(m_procparameter);
}

