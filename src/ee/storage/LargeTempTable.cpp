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

#include "common/LargeTempTableBlockCache.h"
#include "storage/LargeTableIterator.h"
#include "storage/LargeTempTable.h"

namespace voltdb {

bool LargeTempTable::insertTuple(TableTuple& tuple) {
    ReferenceSerializeOutput output;

    size_t neededBytes = tuple.serializationSize();

    if (m_blockForWriting == nullptr || neededBytes > m_blockForWriting->getRemainingBytes()) {
        m_blockForWriting = new LargeTempTableBlock();
        m_blocks.push_back(m_blockForWriting);
        assert(neededBytes <= m_blockForWriting->getRemainingBytes());
    }

    size_t startPos = m_blockForWriting->getUsedBytes();
    output.initializeWithPosition(m_blockForWriting->getData(), LargeTempTableBlock::getBlocksize(), startPos);
    tuple.serializeTo(output);

    size_t usedBytes = output.position() - startPos;
    m_blockForWriting->incrementUsedBytes(usedBytes);

    ++m_numTuples;

    return true;
}

LargeTableIterator LargeTempTable::largeIterator() const {
    return LargeTableIterator(schema(), &m_blocks);
}

} // namespace voltdb
