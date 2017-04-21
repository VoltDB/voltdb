/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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

#ifndef VOLTDB_LARGETEMPTABLEBLOCK_HPP
#define VOLTDB_LARGETEMPTABLEBLOCK_HPP

#include <utility>

#include "common/Pool.hpp"

#include "storage/TupleBlock.h"

namespace voltdb {

    class LargeTempTable;

    class LargeTempTableBlock {
    public:
        LargeTempTableBlock(LargeTempTable* ltt);

        bool hasFreeTuples() const;
        std::pair<char*, int> nextFreeTuple();

        const Pool* getPool() const {
            return m_pool.get();
        }

        Pool* getPool() {
            return m_pool.get();
        }

    private:

        std::unique_ptr<Pool> m_pool;
        TBPtr m_tupleBlockPointer;
    };
} // end namespace voltdb

#endif // VOLTDB_LARGETEMPTABLEBLOCK_HPP
