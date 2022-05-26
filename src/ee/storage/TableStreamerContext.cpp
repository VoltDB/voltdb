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

#include "common/TupleOutputStreamProcessor.h"
#include "common/TupleOutputStream.h"
#include "storage/TableStreamerContext.h"
#include "storage/persistenttable.h"

namespace voltdb
{

/**
 * Constructor with predicates.
 */
TableStreamerContext::TableStreamerContext(
        PersistentTable &table,
        PersistentTableSurgeon &surgeon,
        int32_t partitionId,
        const std::vector<std::string> &predicateStrings) :
    m_surgeon(surgeon),
    m_table(table),
    m_maxTupleLength(table.schema()->getMaxSerializedTupleSize(true)),
    m_partitionId(partitionId)
{
    TableStreamerContext::updatePredicates(predicateStrings);
}

/**
 * Constructor without predicates.
 */
TableStreamerContext::TableStreamerContext(
        PersistentTable &table,
        PersistentTableSurgeon &surgeon,
        int32_t partitionId) :
    m_surgeon(surgeon),
    m_table(table),
    m_maxTupleLength(table.schema()->getMaxSerializedTupleSize(true)),
    m_partitionId(partitionId)
{}

/**
 * Parse and save predicates.
 */
void TableStreamerContext::updatePredicates(const std::vector<std::string> &predicateStrings)
{
    // Parse predicate strings. The factory type determines the kind of
    // predicates that get generated.
    // Throws an exception to be handled by caller on errors.
    std::ostringstream errmsg;
    m_predicates.clear();
    if (!m_predicates.parseStrings(predicateStrings, errmsg, m_predicateDeleteFlags)) {
        throwFatalException("TableStreamerContext() failed to parse predicate strings: %s", errmsg.str().c_str());
    }
}

}
