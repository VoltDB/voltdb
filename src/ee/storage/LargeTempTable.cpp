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
    , m_insertsFinished(false)
    , m_iter(this, m_blockIds.begin())
    , m_blockForWriting(NULL)
{
}

void LargeTempTable::getEmptyBlock() {
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    if (m_blockForWriting != NULL) {
        lttBlockCache->unpinBlock(m_blockForWriting->id());
    }

    int64_t nextBlockId;
    std::tie(nextBlockId, m_blockForWriting) = lttBlockCache->getEmptyBlock(this);
    m_blockIds.push_back(nextBlockId);
}

bool LargeTempTable::insertTuple(TableTuple& source) {
    TableTuple target(m_schema);
    assert(! m_insertsFinished);

    if (m_blockForWriting == NULL) {
        getEmptyBlock();
    }

    bool success = m_blockForWriting->insertTuple(source);
    if (! success) {
        getEmptyBlock();
        success = m_blockForWriting->insertTuple(source);
        if (! success) {
            throwDynamicSQLException("Failed to insert tuple after allocating new LTT block");
        }
    }

    ++m_tupleCount;

    return true;
}

void LargeTempTable::finishInserts() {
    assert(! m_insertsFinished);
    m_insertsFinished = true;

    if (m_blockIds.empty()) {
        return;
    }

    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    int64_t id = m_blockIds.back();
    if (lttBlockCache->blockIsPinned(id)) {
        lttBlockCache->unpinBlock(id);
    }
}

void LargeTempTable::deleteAllTempTuples() {
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    if (! m_insertsFinished) {
        finishInserts();
    }

    BOOST_FOREACH(int64_t blockId, m_blockIds) {
        lttBlockCache->releaseBlock(blockId);
    }

    m_blockIds.clear();
    m_tupleCount = 0;

    // Mark this table as again ready for inserts
    m_insertsFinished = false;
    m_blockForWriting = NULL;
}

LargeTempTable::~LargeTempTable() {
    deleteAllTempTuples();
}

void LargeTempTable::nextFreeTuple(TableTuple*) {
    throwDynamicSQLException("nextFreeTuple not yet implemented");
}

std::string LargeTempTable::debug(const std::string& spacer) const {
    std::ostringstream oss;
    //oss << Table::debug(spacer);
    std::string infoSpacer = spacer + "  |";
    oss << infoSpacer << "\tLTT BLOCK IDS:\n";
    if (m_blockIds.size() > 0) {
        BOOST_FOREACH(auto id, m_blockIds) {
            oss << infoSpacer << "  " << id << "\n";
        }
    }
    else {
        oss << infoSpacer << "  <no blocks>\n";
    }

    return oss.str();
}

} // namespace voltdb
