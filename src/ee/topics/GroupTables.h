/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

#pragma once

#include "storage/persistenttable.h"

namespace voltdb {
namespace topics {

/**
 * Simple interface for a class which has getters for all of group tables
 */
class GroupTables {
public:
    virtual ~GroupTables() = default;

    /**
     * Return a pointer to the table which holds all of the groups
     */
    virtual PersistentTable* getGroupTable() const = 0;

    /**
     * Return a pointer to the table which holds all of the group members
     */
    virtual PersistentTable* getGroupMemberTable() const = 0;

    /**
     * Return a pointer to the table which holds all of the committed group offsets
     */
    virtual PersistentTable* getGroupOffsetTable() const = 0;
};

}
}
