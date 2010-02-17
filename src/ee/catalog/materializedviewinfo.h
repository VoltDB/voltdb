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

#ifndef CATALOG_MATERIALIZEDVIEWINFO_H_
#define CATALOG_MATERIALIZEDVIEWINFO_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class Table;
class ColumnRef;
/**
 * Information used to build and update a materialized view
 */
class MaterializedViewInfo : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<MaterializedViewInfo>;

protected:
    MaterializedViewInfo(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    CatalogType* m_dest;
    CatalogMap<ColumnRef> m_groupbycols;
    std::string m_predicate;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual void removeChild(const std::string &collectionName, const std::string &childName);

public:
    /** GETTER: The table which will be updated when the source table is updated */
    const Table * dest() const;
    /** GETTER: The columns involved in the group by of the aggregation */
    const CatalogMap<ColumnRef> & groupbycols() const;
    /** GETTER: A filtering predicate */
    const std::string & predicate() const;
};

} // namespace catalog

#endif //  CATALOG_MATERIALIZEDVIEWINFO_H_
