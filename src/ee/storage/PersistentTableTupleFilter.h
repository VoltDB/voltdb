/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

#include "common/tabletuple.h"
#include "table.h"
#include "tabletuplefilter.h"

#include <vector>
#include <string>
#include <boost/bimap.hpp>


namespace voltdb {


class PersistentTableTupleFilter : public TableTupleFilter {

private:
    boost::bimap<uint64_t, uint64_t> m_tupleIndexes{};
public:
    PersistentTableTupleFilter() = default;
    virtual ~PersistentTableTupleFilter() {}

    virtual void init(Table* table);

    virtual uint64_t getTupleIndex(const TableTuple& tuple);

    virtual uint64_t getTupleAddress(size_t tupleIdx);
};
}
