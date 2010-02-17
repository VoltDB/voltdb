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

#ifndef CATALOG_SITE_H_
#define CATALOG_SITE_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class Host;
class Partition;
/**
 * A physical execution context for the system
 */
class Site : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Site>;

protected:
    Site(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    bool m_isexec;
    CatalogType* m_host;
    CatalogType* m_partition;
    int32_t m_initiatorid;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual void removeChild(const std::string &collectionName, const std::string &childName);

public:
    /** GETTER: Does the site execute workunits? */
    bool isexec() const;
    /** GETTER: Which host does the site belong to? */
    const Host * host() const;
    /** GETTER: Which logical data partition does this host process? */
    const Partition * partition() const;
    /** GETTER: If the site is an initiator, this is its tightly packed id */
    int32_t initiatorid() const;
};

} // namespace catalog

#endif //  CATALOG_SITE_H_
