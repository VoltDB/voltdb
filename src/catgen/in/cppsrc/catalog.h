/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

#ifndef CATALOG_CATALOG_H_
#define CATALOG_CATALOG_H_

#include <vector>
#include <map>
#include <string>
#include <list>
#include "boost/unordered_map.hpp"
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class Cluster;

/**
 * The root class in the Catalog hierarchy, which is essentially a tree of
 * instances of CatalogType objects, accessed by guids globally, paths
 * globally, and child names when given a parent.
 *
 */
class Catalog : public CatalogType {
    friend class CatalogType;

protected:
    struct UnresolvedInfo {
        CatalogType * type;
        std::string field;
    };

    std::map<std::string, std::list<UnresolvedInfo> > m_unresolved;

    // it would be nice if this code was generated
    CatalogMap<Cluster> m_clusters;

    // for memory cleanup and fast-lookup purposes
    boost::unordered_map<std::string, CatalogType*> m_allCatalogObjects;

    // last use path
    CatalogType* m_lastUsedPath;

    //  paths of objects recently deleted from the catalog.
    std::vector<std::string> m_deletions;

    void executeOne(const std::string &stmt);
    CatalogType * itemForRef(const std::string &ref);
    CatalogType * itemForPath(const CatalogType *parent, const std::string &path);
    CatalogType * itemForPathPart(const CatalogType *parent, const std::string &pathPart) const;

    virtual void update();
    virtual CatalogType *addChild(const std::string &collectionName, const std::string &childName);
    virtual CatalogType *getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

    void registerGlobally(CatalogType *catObj);
    void unregisterGlobally(CatalogType *catObj);

    void addUnresolvedInfo(std::string path, CatalogType *type, std::string fieldName);
private:
    void resolveUnresolvedInfo(std::string path);
    void cleanupExecutionBookkeeping();

public:
    void purgeDeletions();

    /**
     * Create a new Catalog hierarchy.
     */
    Catalog();
    virtual ~Catalog();

    /**
     * Run one or more single-line catalog commands separated by newlines.
     * See the docs for more info on catalog statements.
     * @param stmts A string containing one or more catalog commands separated by
     * newlines
     */
    void execute(const std::string &stmts);

    /** GETTER: The set of the clusters in this catalog */
    const CatalogMap<Cluster> & clusters() const;

    /** pass in a buffer at least half as long as the string */
    static void hexDecodeString(const std::string &hexString, char *buffer);

    /** pass in a buffer at twice as long as the string */
    static void hexEncodeString(const char *string, char *buffer);

    /** return by out-param a copy of the recently deleted paths. */
    void getDeletedPaths(std::vector<std::string> &deletions);
};

}

#endif // CATALOG_CATALOG
