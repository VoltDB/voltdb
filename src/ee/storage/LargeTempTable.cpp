/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
#include "storage/LargeTempTable.h"
#include "storage/LargeTempTableBlock.h"

namespace voltdb {

LargeTempTable::LargeTempTable()
    : AbstractTempTable(LargeTempTableBlock::BLOCK_SIZE_IN_BYTES)
    , m_blockIds()
    , m_iter(this, m_blockIds.begin())
    , m_blockForWriting(NULL)
{
}

void LargeTempTable::getEmptyBlock() {
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    // Mark the current block we're writing to as unpinned so it can
    // be stored if needed to make space for the next block.
    if (m_blockForWriting != NULL) {
        m_blockForWriting->unpin();
    }

    // Try to get an empty block (this will invoke I/O via topend, and
    // could throw for any number of reasons)
    LargeTempTableBlock* newBlock = lttBlockCache->getEmptyBlock(m_schema);

    m_blockForWriting = newBlock;
    m_blockIds.push_back(m_blockForWriting->id());
}

bool LargeTempTable::insertTuple(TableTuple& source) {

    if (m_blockForWriting == NULL) {
        if (! m_blockIds.empty()) {
            throwSerializableEEException("Attempt to insert after finishInserts() called");
        }

        getEmptyBlock();
    }

    bool success = m_blockForWriting->insertTuple(source);
    if (! success) {
        if (m_blockForWriting->activeTupleCount() == 0) {
            throwSerializableEEException("Failed to insert tuple into empty LTT block");
        }

        // Try again, maybe there will be enough space with an empty block.
        getEmptyBlock();
        success = m_blockForWriting->insertTuple(source);
        if (! success) {
            throwSerializableEEException("Failed to insert tuple into empty LTT block");
        }
    }

    ++m_tupleCount;

    return true;
}

void LargeTempTable::finishInserts() {
    if (m_blockForWriting) {
        assert (m_blockIds.size() > 0 && m_blockIds.back() == m_blockForWriting->id());
        if (m_blockForWriting->isPinned()) {
            // In general, if m_blockForWriting is not null, then the
            // block it points to will be pinned.  The only case where
            // this is not true is when we throw an exception
            // attempting to fetch a new empty block.
            m_blockForWriting->unpin();
        }
        m_blockForWriting = NULL;
    }
}

TableIterator LargeTempTable::iterator() {
    if (m_blockForWriting != NULL) {
        throwSerializableEEException("Attempt to iterate over large temp table before finishInserts() is called");
    }

    m_iter.reset(m_blockIds.begin());
    return m_iter;
}


void LargeTempTable::deleteAllTempTuples() {
    finishInserts();

    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    BOOST_FOREACH(int64_t blockId, m_blockIds) {
        lttBlockCache->releaseBlock(blockId);
    }

    m_blockIds.clear();
    m_tupleCount = 0;
}

std::vector<int64_t>::iterator LargeTempTable::releaseBlock(std::vector<int64_t>::iterator it) {
    if (it == m_blockIds.end()) {
        // block may have already been deleted
        return it;
    }

    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    m_tupleCount -= lttBlockCache->getBlockTupleCount(*it);
    lttBlockCache->releaseBlock(*it);

    return m_blockIds.erase(it);
}

LargeTempTable::~LargeTempTable() {
    deleteAllTempTuples();
}

void LargeTempTable::nextFreeTuple(TableTuple*) {
    throwSerializableEEException("nextFreeTuple not implemented");
}

std::string LargeTempTable::debug(const std::string& spacer) const {
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    std::ostringstream oss;
    oss << Table::debug(spacer);
    std::string infoSpacer = spacer + "  |";
    oss << infoSpacer << "\tLTT BLOCK IDS (" << m_blockIds.size() << " blocks):\n";
    if (m_blockIds.size() > 0) {
        BOOST_FOREACH(auto id, m_blockIds) {
            oss << infoSpacer;
            LargeTempTableBlock *block = lttBlockCache->getBlockForDebug(id);
            if (block != NULL) {
                oss << "   " << block->debug();
            }
            else {
                oss << "   block " << id << " is not in LTT block cache?!";
            }

            oss << "\n";
        }
    }
    else {
        oss << infoSpacer << "  <no blocks>\n";
    }

    return oss.str();
}

} // namespace voltdb
