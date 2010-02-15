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

#ifndef  CATALOG_COLUMN_H_
#define  CATALOG_COLUMN_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class ConstraintRef;
class MaterializedViewInfo;
/**
 * A table column
 */
class Column : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Column>;

protected:
    Column(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    int32_t m_index;
    int32_t m_type;
    int32_t m_size;
    bool m_nullable;
    std::string m_name;
    std::string m_defaultvalue;
    int32_t m_defaulttype;
    CatalogMap<ConstraintRef> m_constraints;
    CatalogType* m_matview;
    int32_t m_aggregatetype;
    CatalogType* m_matviewsource;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;

public:
    /** GETTER: The column's order in the table */
    int32_t index() const;
    /** GETTER: The type of the column (int/double/date/etc) */
    int32_t type() const;
    /** GETTER: (currently unused) */
    int32_t size() const;
    /** GETTER: Is the column nullable? */
    bool nullable() const;
    /** GETTER: Name of column */
    const std::string & name() const;
    /** GETTER: Default value of the column */
    const std::string & defaultvalue() const;
    /** GETTER: Type of the default value of the column */
    int32_t defaulttype() const;
    /** GETTER: Constraints that use this column */
    const CatalogMap<ConstraintRef> & constraints() const;
    /** GETTER: If part of a materialized view, ref of view info */
    const MaterializedViewInfo * matview() const;
    /** GETTER: If part of a materialized view, represents aggregate type */
    int32_t aggregatetype() const;
    /** GETTER: If part of a materialized view, represents source column */
    const Column * matviewsource() const;
};

} // namespace catalog

#endif //  CATALOG_COLUMN_H_
