/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
#include "storage/TableStreamerHelper.h"
#include "storage/persistenttable.h"

namespace voltdb
{

/**
 * TableStreamerHelper constructor.
 */
TableStreamerHelper::TableStreamerHelper(TableStreamerContext &context,
                                         TupleOutputStreamProcessor &outputStreams,
                                         std::vector<int> &retPositions) :
    m_context(context),
    m_outputStreams(outputStreams),
    m_retPositions(retPositions)
{
    // Need to initialize the output stream list.
    if (m_outputStreams.empty()) {
        throwFatalException("TableStreamerHelper: at least one output stream is expected.");
    }
}

/**
 * TableStreamerHelper destructor.
 */
TableStreamerHelper::~TableStreamerHelper()
{}

/**
 * Open the output stream(s).
 */
void TableStreamerHelper::open()
{
    m_outputStreams.open(m_context.getTable(),
                         m_context.getMaxTupleLength(),
                         m_context.getPartitionId(),
                         m_context.getPredicates(),
                         m_context.getPredicateDeleteFlags());
}

/**
 * Close the output stream(s) and update the position vector.
 */
void TableStreamerHelper::close()
{
    m_outputStreams.close();

    // If more was streamed copy current positions for return.
    // Can this copy be avoided?
    for (size_t i = 0; i < m_outputStreams.size(); i++) {
        m_retPositions.push_back((int)m_outputStreams.at(i).position());
    }
}

/**
 * Write a row to the output stream(s).
 */
bool TableStreamerHelper::write(TableTuple &tuple, bool &deleteTuple)
{
    return m_outputStreams.writeRow(m_context.getSerializer(), tuple, deleteTuple);
}

}
