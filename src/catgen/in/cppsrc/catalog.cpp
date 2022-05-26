/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include <cstdlib>
#include <cassert>
#include <functional>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include "catalog.h"
#include "catalogtype.h"
#include "cluster.h"
#include "common/SerializableEEException.h"
#include "common/MiscUtil.h"
#include "common/debuglog.h"

using namespace voltdb;
using namespace catalog;
using namespace std;

Catalog::Catalog()
: CatalogType(this, NULL, "/", "catalog"), m_clusters(this, this, "/clusters") {
    m_allCatalogObjects["/"] = this;
    m_childCollections["clusters"] = &m_clusters;
    m_relativeIndex = 1;
    m_lastUsedPath = NULL;
}

Catalog::~Catalog() {
    m_lastUsedPath = NULL;
    std::map<std::string, Cluster*>::const_iterator cluster_iter = m_clusters.begin();
    while (cluster_iter != m_clusters.end()) {
        delete cluster_iter->second;
        cluster_iter++;
    }
    m_clusters.clear();
}

/*
 * Clear the wasAdded/wasUpdated and deletion path lists.
 */
void Catalog::cleanupExecutionBookkeeping() {
    // sad there isn't clean syntax to for_each a map's pair->second
    boost::unordered_map<std::string, CatalogType*>::iterator iter;
    for (iter = m_allCatalogObjects.begin(); iter != m_allCatalogObjects.end(); iter++) {
        CatalogType *ct = iter->second;
        ct->clearUpdateStatus();
    }
    m_deletions.clear();
}

void Catalog::purgeDeletions() {
    for (std::vector<std::string>::iterator i = m_deletions.begin();
         i != m_deletions.end();
         i++)
    {
        boost::unordered_map<std::string, CatalogType*>::iterator object = m_allCatalogObjects.find(*i);
        if (object == m_allCatalogObjects.end()) {
            std::string errmsg = "Catalog reference for " + (*i) + " not found.";
            throw SerializableEEException(
                    VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                    errmsg);
        }
        delete object->second;
    }
    m_deletions.clear();
}

void Catalog::execute(const string &stmts) {
    cleanupExecutionBookkeeping();

    vector<string> lines = MiscUtil::splitString(stmts, '\n');
    for (int32_t i = 0; i < lines.size(); ++i) {
        executeOne(lines[i]);
    }

    if (m_unresolved.size() > 0) {
        throw SerializableEEException(
                VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                "failed to execute catalog");
    }
}

/*
 * Produce constituent elements of catalog command.
 */
static void parse(const string &stmt,
                  string &command,
                  string &ref,
                  string &coll,
                  string &child)
{
    // stmt is formatted as one of:
    // add ref collection name
    // set ref fieldname value
    // ref = path | guid
    // parsed into strings: command ref coll child

    size_t pos = 0;
    size_t end = stmt.find(' ', pos);
    command = stmt.substr(pos, end - pos);
    pos = end + 1;
    end = stmt.find(' ', pos);
    ref = stmt.substr(pos, end - pos);
    pos = end + 1;
    end = stmt.find(' ', pos);
    coll = stmt.substr(pos, end - pos);
    pos = end + 1;
    end = stmt.length() + 1;
    child = stmt.substr(pos, end - pos);

    // cout << std::endl << "Configuring catalog: " << std::endl;
    // cout << "Command: " << command << endl;
    // cout << "Ref: " << ref << endl;
    // cout << "A: " << coll << endl;
    // cout << "B: " << child << endl;
}

/*
 * Run one catalog command.
 */
void Catalog::executeOne(const string &stmt) {
    string command, ref, coll, child;
    parse(stmt, command, ref, coll, child);

    CatalogType *item = NULL;
    if (ref.compare("$PREV") == 0) {
        if (!m_lastUsedPath) {
            // Silently ignore failures -- these are indicative of commands for types
            // that the EE doesn't need/support (hopefully).
            // Trust java code to send us the right thing. Trade sanity check here
            // for memory usage and simpler code on the java side.
            return;
        }
        item = m_lastUsedPath;
    }
    else {
        item = itemForRef(ref);
        if (item == NULL) {
            // Silently ignore failures -- these are indicative of commands for types
            // that the EE doesn't need/support (hopefully).
            // Trust java code to send us the right thing. Trade sanity check here
            // for memory usage and simpler code on the java side.
            m_lastUsedPath = NULL;
            return;
        }
        m_lastUsedPath = item;
    }

    // execute
    if (command.compare("add") == 0) {
        CatalogType *type = item->addChild(coll, child);
        if (type == NULL) {
            // Silently ignore failures -- these are indicative of commands for types
            // that the EE doesn't need/support (hopefully).
            // Trust java code to send us the right thing. Trade sanity check here
            // for memory usage and simpler code on the java side.
            return;
        }
        type->added();
        resolveUnresolvedInfo(type->path());
    }
    else if (command.compare("set") == 0) {
        item->set(coll, child);
        item->updated();
    }
    else if (command.compare("delete") == 0) {
        // remove from collection and hash path to the deletion tracker
        // throw if nothing was removed.
        if(item->removeChild(coll, child)) {
            m_deletions.push_back(ref + "/" + coll + MAP_SEPARATOR + child);
        }
        else {
            // Silently ignore failures -- these are indicative of commands for types
            // that the EE doesn't need/support (hopefully).
            // Trust java code to send us the right thing. Trade sanity check here
            // for memory usage and simpler code on the java side.
            return;
        }
    }
    else {
        throw SerializableEEException(
                VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                "Invalid catalog command.");
    }
}

const CatalogMap<Cluster> & Catalog::clusters() const {
    return m_clusters;
}

CatalogType *Catalog::itemForRef(const string &ref) {
    // if it's a path
    boost::unordered_map<std::string, CatalogType*>::const_iterator iter;
    iter = m_allCatalogObjects.find(ref);
    if (iter != m_allCatalogObjects.end()) {
        return iter->second;
    }
    else {
        return NULL;
    }
}

CatalogType *Catalog::itemForPath(const CatalogType *parent, const string &path) {
    string realpath = path;
    if (path.at(0) == '/') {
        realpath = realpath.substr(1);
    }

    // root case
    if (realpath.length() == 0) {
        return this;
    }

    vector<string> parts = MiscUtil::splitToTwoString(realpath, '/');

    // child of root
    if (parts.size() <= 1) {
        return itemForPathPart(parent, parts[0]);
    }

    CatalogType *nextParent = itemForPathPart(parent, parts[0]);
    if (nextParent == NULL) {
        return NULL;
    }
    return itemForPath(nextParent, parts[1]);
}

CatalogType *Catalog::itemForPathPart(const CatalogType *parent, const string &pathPart) const {
    vector<string> parts = MiscUtil::splitToTwoString(pathPart, MAP_SEPARATOR);
    if (parts.size() <= 1) {
        return NULL;
    }
    return parent->getChild(parts[0], parts[1]);
}

void Catalog::registerGlobally(CatalogType *catObj) {
    boost::unordered_map<std::string, CatalogType*>::iterator iter
      = m_allCatalogObjects.find(catObj->path());
    if (iter != m_allCatalogObjects.end() && iter->second != catObj ) {
        // this is a defect if it happens
        printf("Replacing object at path: %s (%p,%p)\n",
               catObj->path().c_str(), iter->second, catObj);
        vassert(false);
    }
    m_allCatalogObjects[catObj->path()] = catObj;
}

void Catalog::unregisterGlobally(CatalogType *catObj) {
    boost::unordered_map<std::string, CatalogType*>::iterator iter
      = m_allCatalogObjects.find(catObj->path());
    if (iter != m_allCatalogObjects.end()) {
        m_allCatalogObjects.erase(iter);
    }
}

void Catalog::update() {
    // nothing to do
}

/*
 * Add a path to the unresolved list to be processed when
 * the referenced value appears
 */
void Catalog::addUnresolvedInfo(std::string path, CatalogType *type, std::string fieldName) {
    vassert(type != NULL);

    UnresolvedInfo ui;
    ui.field = fieldName;
    ui.type = type;

    std::list<UnresolvedInfo> lui = m_unresolved[path];
    lui.push_back(ui);
    m_unresolved[path] = lui;
}

/*
 * Clean up any resolved binding for path.
 */
void Catalog::resolveUnresolvedInfo(string path) {
    if (m_unresolved.count(path) != 0) {
        std::list<UnresolvedInfo> lui = m_unresolved[path];
        m_unresolved.erase(path);
        std::list<UnresolvedInfo>::const_iterator iter;
        for (iter = lui.begin(); iter != lui.end(); iter++) {
            UnresolvedInfo ui = *iter;
            std::string path2 = "set " + ui.type->path() + " "
              + ui.field + " " + path;
            executeOne(path2);
        }
    }
}

CatalogType *
Catalog::addChild(const string &collectionName, const string &childName) {
    if (collectionName.compare("clusters") == 0) {
        CatalogType *exists = m_clusters.get(childName);
        if (exists) {
            throw SerializableEEException(
                    VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                    "trying to add a duplicate value.");
        }
        return m_clusters.add(childName);
    }

    return NULL;
}

CatalogType *
Catalog::getChild(const string &collectionName, const string &childName) const {
    if (collectionName.compare("clusters") == 0) {
        return m_clusters.get(childName);
    }
    return NULL;
}

bool
Catalog::removeChild(const std::string &collectionName, const std::string &childName) {
    vassert(m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("clusters") == 0) {
        return m_clusters.remove(childName);
    }
    return false;
}

/** takes in 0-F, returns 0-15 */
int32_t hexCharToInt(char c) {
    c = static_cast<char>(toupper(c));
    vassert((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F'));
    int32_t retval;
    if (c >= 'A') {
        retval = c - 'A' + 10;
    }
    else {
        retval = c - '0';
    }
    vassert(retval >=0 && retval < 16);
    return retval;
}

/** pass in a buffer at least (len(hexString)/2+1) */
void Catalog::hexDecodeString(const string &hexString, char *buffer) {
    vassert(buffer);
    int32_t i;
    for (i = 0; i < hexString.length() / 2; i++) {
        int32_t high = hexCharToInt(hexString[i * 2]);

        int32_t low = hexCharToInt(hexString[i * 2 + 1]);
        int32_t result = high * 16 + low;
        vassert(result >= 0 && result < 256);
        buffer[i] = static_cast<char>(result);
    }
    buffer[i] = '\0';
}

/** pass in a buffer at least (2*len+1) encode to uppercase hex as expected by hexCharToInt() */
void Catalog::hexEncodeString(const char *string, char *buffer, size_t len) {
    vassert(buffer);
    int32_t i = 0;
    for (; i < len; i++) {
        char ch[2];
        snprintf(ch, 2, "%X", (string[i] >> 4) & 0xF);
        buffer[i * 2] = ch[0];
        snprintf(ch, 2, "%X", string[i] & 0xF);
        buffer[(i * 2) + 1] = ch[0];
    }
    buffer[i*2] = '\0';
}

void Catalog::getDeletedPaths(vector<std::string> &deletions) {
    copy(m_deletions.begin(), m_deletions.end(), back_inserter(deletions));
}
