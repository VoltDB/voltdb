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

#ifndef _EE_COMMON_COMPACTINGSTRINGSTORAGE_H_
#define _EE_COMMON_COMPACTINGSTRINGSTORAGE_H_

#include "CompactingStringPool.h"
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

namespace voltdb {

    class CompactingStringStorage {
    public:
        CompactingStringStorage();
        ~CompactingStringStorage();

        boost::shared_ptr<CompactingStringPool> get(size_t size);

        boost::shared_ptr<CompactingStringPool> getExact(size_t size);

        std::size_t getPoolAllocationSize();

    private:
        boost::unordered_map<size_t,
                             boost::shared_ptr<CompactingStringPool> > m_poolMap;
    };
}

#endif /* COMPACTINGSTRINGSTORAGE_H_ */
