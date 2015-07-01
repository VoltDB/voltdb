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

#ifndef CATALOG_CATALOG_TYPE_H_
#define CATALOG_CATALOG_TYPE_H_

#include <map>
#include <string>
#include <stdint.h>
#include "catalogmap.h"

namespace catalog {

class Catalog;
class CatalogType;

struct CatalogValue {
    std::string strValue;
    int32_t intValue;
    CatalogType* typeValue;

    CatalogValue() : intValue(0), typeValue(NULL) {}
};

/**
 * The base class for all objects in the Catalog. CatalogType instances all
 * have a name, a guid and a path (from the root). They have fields and children.
 * All fields are simple types. All children are CatalogType instances.
 *
 */
class CatalogType {
    friend class Catalog;

  private:
    void clearUpdateStatus() {
        m_wasAdded = false;
        m_wasUpdated = false;
    }
    void added() {
        m_wasAdded = true;
    }

    void updated() {
        m_wasUpdated = true;
    }

    bool m_wasAdded;     // victim of 'add' command in catalog update
    bool m_wasUpdated;   // target of 'set' command in catalog update

  protected:
    std::map<std::string, CatalogValue> m_fields;
    // the void* is a giant hack, solutions welcome
    std::map<std::string, void*> m_childCollections;

    std::string m_name;
    std::string m_path;
    CatalogType *m_parent;
    Catalog *m_catalog;
    int32_t m_relativeIndex;

    CatalogType(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    virtual ~CatalogType();

    void set(const std::string &field, const std::string &value);
    virtual void update() = 0;
    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name) = 0;
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const = 0;
    // returns true if a child was deleted
    virtual bool removeChild(const std::string &collectionName, const std::string &childName) = 0;

public:

    /**
     * Get the parent of this CatalogType instance
     * @return The name of this CatalogType instance
     */
    std::string name() const;

    /**
     * Get the parent of this CatalogType instance
     * @return The parent of this CatalogType instance
     */
    std::string path() const;

    /**
     * Get the parent of this CatalogType instance
     * @return The parent of this CatalogType instance
     */
    CatalogType * parent() const;

    /**
     * Get a pointer to the root Catalog instance for this CatalogType
     * instance
     * @return A pointer to the root Catalog instance
     */
    Catalog * catalog() const;

    /**
     * Get the index of this node within its parent collection
     * @return The index of this node amongst its sibling nodes
     */
    int32_t relativeIndex() const;

    bool wasAdded() const {
        return m_wasAdded;
    }

    bool wasUpdated() const {
        return m_wasUpdated;
    }
};

}

#endif // CATALOG_CATALOG_TYPE_H_
