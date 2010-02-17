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

#ifndef CATALOG_PROCPARAMETER_H_
#define CATALOG_PROCPARAMETER_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

/**
 * Metadata for a parameter to a stored procedure
 */
class ProcParameter : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<ProcParameter>;

protected:
    ProcParameter(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    int32_t m_type;
    bool m_isarray;
    int32_t m_index;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual void removeChild(const std::string &collectionName, const std::string &childName);

public:
    /** GETTER: The data type for the parameter (int/float/date/etc) */
    int32_t type() const;
    /** GETTER: Is the parameter an array of value */
    bool isarray() const;
    /** GETTER: The index of the parameter within the list of parameters for the stored procedure */
    int32_t index() const;
};

} // namespace catalog

#endif //  CATALOG_PROCPARAMETER_H_
