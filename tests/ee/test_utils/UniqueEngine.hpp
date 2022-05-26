/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#ifndef TESTS_EE_TEST_UTILS_UNIQUEENGINE_HPP
#define TESTS_EE_TEST_UTILS_UNIQUEENGINE_HPP

#include "execution/VoltDBEngine.h"
#include "common/SynchronizedThreadLock.h"
#include "common/Topend.h"

/**
 * A helper class to create an instance of VoltDBEngine that will
 * destroy itself when it goes out of scope.  This class has
 * unique_ptr-like semantics.
 *
 * Create one of these using UniqueEngineBuilder, defined below.
 */
class UniqueEngine {
    friend class UniqueEngineBuilder;

public:
    const voltdb::VoltDBEngine* operator->() const {
        return m_engine.get();
    }

    voltdb::VoltDBEngine* operator->() {
        return m_engine.get();
    }

    voltdb::VoltDBEngine* get() {
        return m_engine.get();
    }

    const voltdb::VoltDBEngine* get() const {
        return m_engine.get();
    }

    // This move constructor is not necessary on modern compilers
    // (probably since all members have move constructors defined),
    // but C6 still requires it.
    UniqueEngine(UniqueEngine&& that)
        : m_topend(that.m_topend.release())
        , m_engine(that.m_engine.release())
    {
    }

private:

    UniqueEngine(std::unique_ptr<voltdb::Topend> topend,
                 int64_t tempTableMemoryLimitInBytes)
        : m_topend(topend.release())
        , m_engine(new voltdb::VoltDBEngine(m_topend.get()))
    {
        m_engine->initialize(1,     // clusterIndex
                             1,     // siteId
                             0,     // partitionId
                             1,     // sitesPerHost
                             0,     // hostId
                             "",    // hostname
                             0,     // drClusterId
                             1024,  // defaultDrBufferSize
                             false,
                             -1,
                             false,
                             tempTableMemoryLimitInBytes,
                             true, // this is lowest site/engineId
                             95);   // compaction threshold
        m_engine->setUndoToken(0);
    }
    UniqueEngine()
    {
        if (m_engine.get() != NULL) {
            m_engine.reset();
            voltdb::SynchronizedThreadLock::destroy();
        }
    }

    std::unique_ptr<voltdb::Topend> m_topend;
    std::unique_ptr<voltdb::VoltDBEngine> m_engine;
};

/**
 * Use this class to create an instance of UniqueEngine.
 *
 * Options:
 *   setTempTableMemoryLimit (default is same as product default, 50MB)
 *   setTopend               (DummyTopend is used by default)
 */
class UniqueEngineBuilder {
public:
    /** Instantiate a builder */
    UniqueEngineBuilder()
        : m_tempTableMemoryLimit(voltdb::DEFAULT_TEMP_TABLE_MEMORY)
        , m_topend(new voltdb::DummyTopend())
    {
    }

    /** Set a non-default limit for temp table memory */
    UniqueEngineBuilder& setTempTableMemoryLimit(int64_t ttMemLimitInBytes) {
        m_tempTableMemoryLimit = ttMemLimitInBytes;
        return *this;
    }

    /** Provide a custom top end for the engine */
    UniqueEngineBuilder& setTopend(std::unique_ptr<voltdb::Topend> topend) {
        m_topend.swap(topend);
        return *this;
    }

    /** Create an engine */
    UniqueEngine build() {
        assert(m_topend.get() != NULL);
        return UniqueEngine(std::move(m_topend), m_tempTableMemoryLimit);
    }

private:
    int64_t m_tempTableMemoryLimit;
    std::unique_ptr<voltdb::Topend> m_topend;
};

#endif // EE_TESTS_TEST_UTILS_UNIQUEENGINE_HPP
