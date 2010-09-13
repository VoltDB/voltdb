/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

#ifndef CATALOGDELEGATE_HPP
#define CATALOGDELEGATE_HPP

#include <stdint.h>
#include <string>

namespace voltdb {

/*
 * Interface that bridges catalog textual changes to changes
 * made to actual EE objects. That is, bridge "set /cluster/..
 * tables[A]" to a change to the actual voltdb::Table instance.
 */

class CatalogDelegate {
  public:
    CatalogDelegate(int32_t catalogVersion, int32_t catalogId, std::string path) :
        m_catalogVersion(catalogVersion), m_catalogId(catalogId), m_path(path) {
    }

    virtual ~CatalogDelegate() {
    }


    /* Deleted from the catalog */
    virtual void deleteCommand() = 0;

    /* Read the path */
    const std::string& path() {
        return m_path;
    }

    /* Return the global delegate id (catalog version | catalog id) */
    int64_t delegateId() const {
        int64_t shiftedVersion = m_catalogVersion;
        shiftedVersion = shiftedVersion << 32;
        return (shiftedVersion + m_catalogId);
    }

  private:
    /* The catalog version when this delegate was created */
    int32_t m_catalogVersion;

    /* The catalog id when this delegate was created */
    int32_t m_catalogId;

    /* The delegate owns this path and all sub-trees of this path */
    const std::string m_path;

};

}

#endif
