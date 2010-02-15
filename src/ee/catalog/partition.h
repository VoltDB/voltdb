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

#ifndef  CATALOG_PARTITION_H_
#define  CATALOG_PARTITION_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

/**
 * A logical, replicate-able partition
 */
class Partition : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Partition>;

protected:
    Partition(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);


    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;

public:
};

} // namespace catalog

#endif //  CATALOG_PARTITION_H_
