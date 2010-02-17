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
#include "planfragment.h"
#include "catalog.h"

using namespace catalog;
using namespace std;

PlanFragment::PlanFragment(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["hasdependencies"] = value;
    m_fields["multipartition"] = value;
    m_fields["plannodetree"] = value;
    m_fields["nontransactional"] = value;
}

void PlanFragment::update() {
    m_hasdependencies = m_fields["hasdependencies"].intValue;
    m_multipartition = m_fields["multipartition"].intValue;
    m_plannodetree = m_fields["plannodetree"].strValue.c_str();
    m_nontransactional = m_fields["nontransactional"].intValue;
}

CatalogType * PlanFragment::addChild(const std::string &collectionName, const std::string &childName) {
    throw std::string("Trying to add to an unknown child collection.");
    return NULL;
}

CatalogType * PlanFragment::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

void PlanFragment::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
}

bool PlanFragment::hasdependencies() const {
    return m_hasdependencies;
}

bool PlanFragment::multipartition() const {
    return m_multipartition;
}

const string & PlanFragment::plannodetree() const {
    return m_plannodetree;
}

bool PlanFragment::nontransactional() const {
    return m_nontransactional;
}

