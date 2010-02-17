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
#include "statement.h"
#include "catalog.h"
#include "stmtparameter.h"
#include "planfragment.h"
#include "column.h"

using namespace catalog;
using namespace std;

Statement::Statement(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_parameters(catalog, this, path + "/" + "parameters"), m_fragments(catalog, this, path + "/" + "fragments"), m_output_columns(catalog, this, path + "/" + "output_columns")
{
    CatalogValue value;
    m_fields["sqltext"] = value;
    m_fields["querytype"] = value;
    m_fields["readonly"] = value;
    m_fields["singlepartition"] = value;
    m_fields["replicatedtabledml"] = value;
    m_fields["batched"] = value;
    m_fields["paramnum"] = value;
    m_childCollections["parameters"] = &m_parameters;
    m_childCollections["fragments"] = &m_fragments;
    m_childCollections["output_columns"] = &m_output_columns;
    m_fields["exptree"] = value;
    m_fields["fullplan"] = value;
    m_fields["cost"] = value;
}

void Statement::update() {
    m_sqltext = m_fields["sqltext"].strValue.c_str();
    m_querytype = m_fields["querytype"].intValue;
    m_readonly = m_fields["readonly"].intValue;
    m_singlepartition = m_fields["singlepartition"].intValue;
    m_replicatedtabledml = m_fields["replicatedtabledml"].intValue;
    m_batched = m_fields["batched"].intValue;
    m_paramnum = m_fields["paramnum"].intValue;
    m_exptree = m_fields["exptree"].strValue.c_str();
    m_fullplan = m_fields["fullplan"].strValue.c_str();
    m_cost = m_fields["cost"].intValue;
}

CatalogType * Statement::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("parameters") == 0) {
        CatalogType *exists = m_parameters.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_parameters.add(childName);
    }
    if (collectionName.compare("fragments") == 0) {
        CatalogType *exists = m_fragments.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_fragments.add(childName);
    }
    if (collectionName.compare("output_columns") == 0) {
        CatalogType *exists = m_output_columns.get(childName);
        if (exists)
            throw std::string("trying to add a duplicate value.");
        return m_output_columns.add(childName);
    }
    throw std::string("Trying to add to an unknown child collection.");
    return NULL;
}

CatalogType * Statement::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("parameters") == 0)
        return m_parameters.get(childName);
    if (collectionName.compare("fragments") == 0)
        return m_fragments.get(childName);
    if (collectionName.compare("output_columns") == 0)
        return m_output_columns.get(childName);
    return NULL;
}

void Statement::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("parameters") == 0)
        return m_parameters.remove(childName);
    if (collectionName.compare("fragments") == 0)
        return m_fragments.remove(childName);
    if (collectionName.compare("output_columns") == 0)
        return m_output_columns.remove(childName);
}

const string & Statement::sqltext() const {
    return m_sqltext;
}

int32_t Statement::querytype() const {
    return m_querytype;
}

bool Statement::readonly() const {
    return m_readonly;
}

bool Statement::singlepartition() const {
    return m_singlepartition;
}

bool Statement::replicatedtabledml() const {
    return m_replicatedtabledml;
}

bool Statement::batched() const {
    return m_batched;
}

int32_t Statement::paramnum() const {
    return m_paramnum;
}

const CatalogMap<StmtParameter> & Statement::parameters() const {
    return m_parameters;
}

const CatalogMap<PlanFragment> & Statement::fragments() const {
    return m_fragments;
}

const CatalogMap<Column> & Statement::output_columns() const {
    return m_output_columns;
}

const string & Statement::exptree() const {
    return m_exptree;
}

const string & Statement::fullplan() const {
    return m_fullplan;
}

int32_t Statement::cost() const {
    return m_cost;
}

