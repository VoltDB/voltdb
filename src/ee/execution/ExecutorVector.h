/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef EXECUTORVECTOR_H
#define EXECUTORVECTOR_H

#include "storage/TempTableLimits.h"
#include "plannodes/plannodefragment.h"
#include "boost/scoped_ptr.hpp"
#include "boost/shared_ptr.hpp"
#include <vector>
#include <map>

namespace catalog {
class Statement;
}

namespace voltdb {

class VoltDBEngine;
class AbstractPlanNode;
class AbstractExecutor;
class ExecutorContext;

/**
 * A list of executors for runtime.
 */
class ExecutorVector {
public:
    /**
     * This is the static factory method for creating instances of
     * this class from a plan serialized to JSON.
     */
    static boost::shared_ptr<ExecutorVector> fromJsonPlan(
            VoltDBEngine* engine, const std::string& jsonPlan, int64_t fragId);
    static boost::shared_ptr<ExecutorVector> fromCatalogStatement(
            VoltDBEngine* engine, catalog::Statement *stmt);

    /** Accessor function to satisfy boost::multi_index::const_mem_fun template. */
    int64_t getFragId() const { return m_fragId; }

    TempTableLimits const* limits() const {
        return &m_limits;
    }

    bool isLargeQuery() const {
        return m_fragment->isLargeQuery();
    }

    /** Return a std::string with helpful info about this object. */
    std::string debug() const;

    void setupContext(ExecutorContext* executorContext);

    void resetLimitStats();

    // Get the executors list for a given subplan. The default plan id = 0
    // represents the top level parent plan
    const std::vector<AbstractExecutor*>& getExecutorList(int planId = 0);

    void getRidOfSendExecutor(int planId = 0);
private:

    /**
     * This method is private.  Please use static factory method
     * fromJsonPlan to construct an instance of ExecutorVector.
     *
     * Construct an ExecutorVector instance.  Object will not be
     * initialized until its init method is called.  (Initialization
     * has been placed there to avoid throwing an exception in the
     * constructor.)
     *
     * Note: This constructed instance of ExecutorVector takes
     * ownership of the PlanNodeFragment here; it will be released
     * (automatically via boost::scoped_ptr) when this instance goes
     * away.
     */
    ExecutorVector(int64_t fragmentId, int64_t logThreshold, int64_t memoryLimit, PlanNodeFragment* fragment)
        : m_fragId(fragmentId) , m_limits(memoryLimit, logThreshold) , m_fragment(fragment) { }

    /** Build the list of executors from its plan node fragment */
    void init(VoltDBEngine* engine);

    void initPlanNode(VoltDBEngine* engine, AbstractPlanNode* node);

    const int64_t m_fragId;
    std::map<int, std::vector<AbstractExecutor*>> m_subplanExecListMap;
    TempTableLimits m_limits;
    boost::scoped_ptr<PlanNodeFragment> m_fragment;
};

} // namespace voltdb

#endif // EXECUTORVECTOR_H
