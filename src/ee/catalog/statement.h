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

#ifndef  CATALOG_STATEMENT_H_
#define  CATALOG_STATEMENT_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class StmtParameter;
class PlanFragment;
class Column;
/**
 * A parameterized SQL statement embedded in a stored procedure
 */
class Statement : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Statement>;

protected:
    Statement(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    std::string m_sqltext;
    int32_t m_querytype;
    bool m_readonly;
    bool m_singlepartition;
    bool m_replicatedtabledml;
    bool m_batched;
    int32_t m_paramnum;
    CatalogMap<StmtParameter> m_parameters;
    CatalogMap<PlanFragment> m_fragments;
    CatalogMap<Column> m_output_columns;
    std::string m_exptree;
    std::string m_fullplan;
    int32_t m_cost;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;

public:
    /** GETTER: The text of the sql statement */
    const std::string & sqltext() const;
    int32_t querytype() const;
    /** GETTER: Can the statement modify any data? */
    bool readonly() const;
    /** GETTER: Does the statement only use data on one partition? */
    bool singlepartition() const;
    /** GETTER: Should the result of this statememt be divided by partition count before returned */
    bool replicatedtabledml() const;
    bool batched() const;
    int32_t paramnum() const;
    /** GETTER: The set of parameters to this SQL statement */
    const CatalogMap<StmtParameter> & parameters() const;
    /** GETTER: The set of plan fragments used to execute this statement */
    const CatalogMap<PlanFragment> & fragments() const;
    /** GETTER: The set of columns in the output table */
    const CatalogMap<Column> & output_columns() const;
    /** GETTER: A serialized representation of the original expression tree */
    const std::string & exptree() const;
    /** GETTER: A serialized representation of the un-fragmented plan */
    const std::string & fullplan() const;
    /** GETTER: The cost of this plan measured in arbitrary units */
    int32_t cost() const;
};

} // namespace catalog

#endif //  CATALOG_STATEMENT_H_
