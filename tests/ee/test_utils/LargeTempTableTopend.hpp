/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef LARGE_TEMP_TABLE_TOPEND_HPP
#define LARGE_TEMP_TABLE_TOPEND_HPP

#include <cstring>
#include <map>

#include "harness.h"

#include "common/tabletuple.h"
#include "common/Topend.h"

#include "storage/LargeTempTableBlock.h"

namespace voltdb {
class TupleSchema;
}

/**
 * A topend that can be used in unit tests that test large queries.
 * This topend provides methods to store, load and release methods to
 * manipulate large temp tables blocks, which are managed in an
 * in-memory std::map that mocks the filesystem.
 */
class LargeTempTableTopend : public voltdb::DummyTopend {
private:

    class Block {
    public:
        Block(char* data, int64_t activeTupleCount, const voltdb::TupleSchema *schema)
            : m_origAddress(data)
            , m_data(new char[voltdb::LargeTempTableBlock::BLOCK_SIZE_IN_BYTES])
            , m_activeTupleCount(activeTupleCount)
            , m_schema(schema)
        {
            ::memcpy(m_data.get(), data, voltdb::LargeTempTableBlock::BLOCK_SIZE_IN_BYTES);
        }

        Block()
            : m_origAddress(NULL)
            , m_data()
            , m_activeTupleCount(0)
            , m_schema(NULL)
        {
        }

        char* data() {
            return m_data.get();
        }

        char* origAddress() {
            return m_origAddress;
        }

        int64_t activeTupleCount() {
            return m_activeTupleCount;
        }

        std::string debug() const {
            std::ostringstream oss;
            voltdb::TableTuple tuple{m_data.get(), m_schema};
            oss << "First tuple: " << tuple.debugSkipNonInlineData() << "\n";
            return oss.str();
        }

    private:
        char* m_origAddress;
        std::unique_ptr<char[]> m_data;
        int64_t m_activeTupleCount;
        const voltdb::TupleSchema* m_schema;
    };

public:

    bool storeLargeTempTableBlock(voltdb::LargeTempTableBlock* block) {
        assert (m_map.count(block->id()) == 0);

        std::unique_ptr<char[]> storage = block->releaseData();
        Block *newBlock = new Block{storage.get(), block->activeTupleCount(), block->schema()};
        m_map[block->id()] = newBlock;

        return true;
    }

    bool loadLargeTempTableBlock(voltdb::LargeTempTableBlock* block) {
        auto it = m_map.find(block->id());
        assert (it != m_map.end());
        Block *storedBlock = it->second;

        std::unique_ptr<char[]> storage{new char[voltdb::LargeTempTableBlock::BLOCK_SIZE_IN_BYTES]};
        ::memcpy(storage.get(), storedBlock->data(), voltdb::LargeTempTableBlock::BLOCK_SIZE_IN_BYTES);
        block->setData(storedBlock->origAddress(), std::move(storage));
        assert(block->activeTupleCount() == storedBlock->activeTupleCount());

        return true;
    }

    bool releaseLargeTempTableBlock(int64_t blockId) {
        auto it = m_map.find(blockId);
        if (it == m_map.end()) {
            assert(false);
            return false;
        }

        Block* storedBlock = it->second;
        m_map.erase(blockId);
        delete storedBlock;

        return true;
    }

    size_t storedBlockCount() const {
        return m_map.size();
    }

    ~LargeTempTableTopend() {
        assert(m_map.size() == 0);
    }

    std::string debug() const {
        std::ostringstream oss;
        oss << "LTTTopend: (" << m_map.size() << " blocks)\n";
        BOOST_FOREACH(auto &entry, m_map) {
            oss << "  Block " << entry.first << ": " << entry.second->debug() << "\n";
        }

        return oss.str();
    }

private:

    std::map<int64_t, Block*> m_map;
};

#endif // LARGE_TEMP_TABLE_TOPEND_HPP
