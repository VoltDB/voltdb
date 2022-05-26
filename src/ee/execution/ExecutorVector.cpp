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

#include "ExecutorVector.h"
#include "VoltDBEngine.h"
#include "catalog/planfragment.h"
#include "catalog/statement.h"
#include "executors/abstractexecutor.h"
#include "executors/executorfactory.h"

namespace voltdb {

boost::shared_ptr<ExecutorVector> ExecutorVector::fromCatalogStatement(
        VoltDBEngine* engine, catalog::Statement *stmt) {
    const string& b64plan = stmt->fragments().begin()->second->plannodetree();
    const string jsonPlan = engine->getTopend()->decodeBase64AndDecompress(b64plan);
    return fromJsonPlan(engine, jsonPlan, -1);
}

boost::shared_ptr<ExecutorVector> ExecutorVector::fromJsonPlan(
      VoltDBEngine* engine, const std::string& jsonPlan, int64_t fragId) {
    std::unique_ptr<PlanNodeFragment> pnf;
    try {
        pnf.reset(PlanNodeFragment::createFromCatalog(jsonPlan.c_str()));
    } catch (SerializableEEException&) {
        throw;
    } catch (std::exception const& e) {
        throwSerializableEEException(
                "Unable to initialize PlanNodeFragment for PlanFragment '%jd' with plan:\n%s: what(): %s",
                (intmax_t)fragId, jsonPlan.c_str(), e.what());
    }
    VOLT_TRACE("\n%s\n", pnf->debug().c_str());
    vassert(pnf->getRootNode());

    if (!pnf->getRootNode()) {
        throwSerializableEEException(
                "Deserialized PlanNodeFragment for PlanFragment '%jd' does not have a root PlanNode",
                (intmax_t)fragId);
    }

    int64_t tempTableLogLimit = engine->tempTableLogLimit();
    int64_t tempTableMemoryLimit = engine->tempTableMemoryLimit();

    // ENG-1333 HACK.  If the plan node fragment has a delete node,
    // then turn off the governors
    if (pnf->hasDelete()) {
        tempTableLogLimit = DEFAULT_TEMP_TABLE_MEMORY;
        tempTableMemoryLimit = -1;
    }

    // Note: the executor vector takes ownership of the plan node
    // fragment here.
    boost::shared_ptr<ExecutorVector> ev(new ExecutorVector(
                fragId, tempTableLogLimit, tempTableMemoryLimit, pnf.release()));
    ev->init(engine);
    return ev;
}

void ExecutorVector::init(VoltDBEngine* engine) {
    // Initialize each node!
    for (auto it = m_fragment->executeListBegin(); it != m_fragment->executeListEnd(); ++it) {
        auto const& planNodeList = it->second;
        std::vector<AbstractExecutor*> executorList;
        for(AbstractPlanNode* planNode : planNodeList) {
            initPlanNode(engine, planNode);
            executorList.emplace_back(planNode->getExecutor());
        }
        m_subplanExecListMap.emplace(it->first, executorList);
    }
}

std::string ExecutorVector::debug() const {
    std::ostringstream oss;
    oss << "Fragment ID: " << m_fragId << ", ";
    oss << "Temp table memory in bytes: " << m_limits.getAllocated() << std::endl;
    for (auto const& it : m_subplanExecListMap) {
       auto const& executorList = it.second;
       oss << "Statement id:" << it.first << ", list size: " << executorList.size() << ", ";
        for(AbstractExecutor* ae : executorList) {
            oss << ae->getPlanNode()->debug(" ") << "\n";
        }
    }
    return oss.str();
}

void ExecutorVector::initPlanNode(VoltDBEngine* engine, AbstractPlanNode* node) {
    vassert(node);
    vassert(node->getExecutor() == NULL);

    // Executor is created here. An executor is *devoted* to this
    // plannode so that it can cache anything for the plannode
    AbstractExecutor* executor = getNewExecutor(engine, node, isLargeQuery());
    if (executor == NULL) {
        throwSerializableEEException(
                "Unexpected error. Invalid statement plan. A fragment (%jd) has an unknown plan node type (%d)",
                (intmax_t)m_fragId, (int)node->getPlanNodeType());
    }
    node->setExecutor(executor);

    // If this PlanNode has an internal PlanNode (e.g.,
    // AbstractScanPlanNode can have internal Projections), set
    // that internal node's executor as well.
    for (auto iter : node->getInlinePlanNodes()) {
        initPlanNode(engine, iter.second);
    }

    // Now use the plannode to initialize the executor for execution later on
    if (! executor->init(engine, *this)) {
        throwSerializableEEException(
                "The executor failed to initialize for PlanNode '%s' for PlanFragment '%jd'",
                node->debug().c_str(), (intmax_t)m_fragId);
    }
}

void ExecutorVector::setupContext(ExecutorContext* executorContext) {
    executorContext->setupForExecutors(&m_subplanExecListMap);
}

void ExecutorVector::resetLimitStats() { m_limits.resetPeakMemory(); }

const std::vector<AbstractExecutor*>& ExecutorVector::getExecutorList(int planId) {
    vassert(m_subplanExecListMap.find(planId) != m_subplanExecListMap.end());
    return m_subplanExecListMap.find(planId)->second;
}

void ExecutorVector::getRidOfSendExecutor(int planId) {
    auto iter = m_subplanExecListMap.find(planId);
    vassert(iter != m_subplanExecListMap.end());
    std::vector<AbstractExecutor*> const executorList = iter->second;
    iter->second.clear();
    for(AbstractExecutor* executor : executorList) {
        if (executor->getPlanNode()->getPlanNodeType() != PlanNodeType::Send) {
            iter->second.emplace_back(executor);
        }
    }
}

} // namespace voltdb
