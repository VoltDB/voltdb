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

#ifndef TABLE_STREAMER_HELPER_H
#define TABLE_STREAMER_HELPER_H

#include <vector>

namespace voltdb
{

class TableStreamerContext;
class TupleOutputStreamProcessor;
class TableTuple;

/**
 * Helper class that makes it more convenient to implement contexts that
 * perform tuple streaming.
 */
class TableStreamerHelper
{
    friend class TableStreamerContext;

public:

    /**
     * Open the output stream(s).
     */
    void open();

    /**
     * Close the output stream(s) and update the position vector.
     */
    void close();

    /**
     * Write a row to the output stream(s).
     */
    bool write(TableTuple &tuple, bool &deleteTuple);

    /**
     * Destructor.
     */
    virtual ~TableStreamerHelper();

private:

    /**
     * TableStreamerHelper constructor.
     */
    TableStreamerHelper(TableStreamerContext &context,
                        TupleOutputStreamProcessor &outputStreams,
                        std::vector<int> &retPositions);

    TableStreamerContext &m_context;
    TupleOutputStreamProcessor &m_outputStreams;
    std::vector<int> &m_retPositions;
};

} // namespace voltdb

#endif // TABLE_STREAMER_HELPER_H
