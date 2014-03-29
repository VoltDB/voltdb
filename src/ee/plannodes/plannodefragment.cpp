/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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


#include <stdexcept>
#include <sstream>
#include "common/FatalException.hpp"
#include "plannodefragment.h"
#include "catalog/catalog.h"
#include "abstractplannode.h"

using namespace std;

using namespace voltdb;

PlanNodeFragment::PlanNodeFragment(int stmtCnt) :
    m_serializedType("org.voltdb.plannodes.PlanNodeList"),
    m_stmtCnt(stmtCnt),
    m_idToNodeMap(),
    m_stmtExecutionListArray(new std::vector<AbstractPlanNode*>[m_stmtCnt]),
    m_stmtPlanNodesArray(new std::vector<AbstractPlanNode*>[m_stmtCnt]),
    m_parameters()
{}

PlanNodeFragment::PlanNodeFragment(AbstractPlanNode *root_node) :
    m_serializedType("org.voltdb.plannodes.PlanNodeList"),
    m_stmtCnt(1),
    m_idToNodeMap(),
    m_stmtExecutionListArray(new std::vector<AbstractPlanNode*>[m_stmtCnt]),
    m_stmtPlanNodesArray(new std::vector<AbstractPlanNode*>[m_stmtCnt]),
    m_parameters()
{
    if (constructTree(root_node) != true) {
        throwFatalException("Failed to construct plan fragment");
    }
}

bool PlanNodeFragment::constructTree(AbstractPlanNode* node) {
    if (m_idToNodeMap.find(node->getPlanNodeId()) == m_idToNodeMap.end()) {
        m_stmtExecutionListArray[0].push_back(node);
        m_idToNodeMap[node->getPlanNodeId()] = node;
        std::vector<AbstractPlanNode*> children = node->getChildren();
        for (int ii = 0; ii < children.size(); ++ii) {
            if (!constructTree(children[ii])) {
                return false;
            }
        }
    }
    return true;
}

PlanNodeFragment::~PlanNodeFragment() {
    for (int ii = 0; ii < m_stmtCnt; ii++) {
        for (std::vector<AbstractPlanNode*>::iterator it = m_stmtPlanNodesArray[ii].begin();
             it != m_stmtPlanNodesArray[ii].end(); ++it) {
                delete *it;
        }
    }
}

PlanNodeFragment *
PlanNodeFragment::createFromCatalog(const string value)
{
    //cout << "DEBUG PlanNodeFragment::createFromCatalog: value.size() == " << value.size() << endl;
    //cout << "DEBUG PlanNodeFragment::createFromCatalog: value == " << value << endl;

    PlannerDomRoot domRoot(value.c_str());

    PlanNodeFragment *retval = PlanNodeFragment::fromJSONObject(domRoot.rootObject());
    return retval;
}

PlanNodeFragment *
PlanNodeFragment::fromJSONObject(PlannerDomValue obj)
{
    // read and construct plannodes from json object
    int stmtCnt = 1;
    auto_ptr<PlanNodeFragment> pnf;
    if (obj.hasNonNullKey("PLAN_NODES_LISTS")) {
        PlannerDomValue planNodesListArray = obj.valueForKey("PLAN_NODES_LISTS");
        if (!obj.hasNonNullKey("EXECUTE_LISTS")) {
            throwFatalException("Failed to construct plan fragment. Missing EXECUTE_LISTS key");
        }
        PlannerDomValue executeListArray = obj.valueForKey("EXECUTE_LISTS");
        if (planNodesListArray.arrayLen() != executeListArray.arrayLen()) {
            throwFatalException("Failed to construct plan fragment. EXECUTE_LISTS and PLAN_NODES_LISTS do not match");
        }
        stmtCnt = planNodesListArray.arrayLen();
        pnf.reset(new PlanNodeFragment(stmtCnt));
        for (int i = 0; i < stmtCnt; i++) {
            PlannerDomValue planNodesList = planNodesListArray.valueAtIndex(i).valueForKey("PLAN_NODES");
            PlannerDomValue executeList = executeListArray.valueAtIndex(i).valueForKey("EXECUTE_LIST");
            PlanNodeFragment::nodeListFromJSONObject(pnf.get(), planNodesList, executeList, i);
        }
    } else {
        pnf.reset(new PlanNodeFragment(stmtCnt));
        PlanNodeFragment::nodeListFromJSONObject(pnf.get(), obj.valueForKey("PLAN_NODES"), obj.valueForKey("EXECUTE_LIST"), 0);
    }
    PlanNodeFragment::loadParamsFromJSONObject(pnf.get(), obj);

    PlanNodeFragment *retval = pnf.get();
    pnf.release();
    assert(retval);
    return retval;
}

void
PlanNodeFragment::nodeListFromJSONObject(PlanNodeFragment *pnf, PlannerDomValue planNodesList, PlannerDomValue executeList, int stmtId)
{
    // NODE_LIST
    std::vector<AbstractPlanNode*>& planNodes = pnf->m_stmtPlanNodesArray[stmtId];
    for (int i = 0; i < planNodesList.arrayLen(); i++) {
        AbstractPlanNode *node = NULL;
        node = AbstractPlanNode::fromJSONObject(planNodesList.valueAtIndex(i));
        assert(node);

        planNodes.push_back(node);
        pnf->m_idToNodeMap[node->getPlanNodeId()] = node;
    }

    // walk the plannodes and complete each plannode's id-to-node maps
    for (std::vector< AbstractPlanNode* >::const_iterator node = planNodes.begin();
         node != planNodes.end(); ++node) {
        const std::vector<CatalogId> childIds = (*node)->getChildIds();
        std::vector<AbstractPlanNode*> &children = (*node)->getChildren();
        for (int zz = 0; zz < childIds.size(); zz++) {
            children.push_back(pnf->m_idToNodeMap[childIds[zz]]);
        }
    }

    // EXECUTE_LIST
    std::vector<AbstractPlanNode*>& executeNodeList = pnf->m_stmtExecutionListArray[stmtId];
    for (int i = 0; i < executeList.arrayLen(); i++) {
        executeNodeList.push_back(pnf->m_idToNodeMap[executeList.valueAtIndex(i).asInt()]);
    }

}

void
PlanNodeFragment::loadParamsFromJSONObject(PlanNodeFragment *pnf, PlannerDomValue obj)
{
    PlannerDomValue parametersArray = obj.valueForKey("PARAMETERS");
    for (int i = 0; i < parametersArray.arrayLen(); i++) {
        PlannerDomValue parameterArray = parametersArray.valueAtIndex(i);
        int index = parameterArray.valueAtIndex(0).asInt();
        std::string typeString = parameterArray.valueAtIndex(1).asStr();
        pnf->m_parameters.push_back(std::pair< int, voltdb::ValueType>(index, stringToValue(typeString)));
    }
}

bool PlanNodeFragment::hasDelete() const
{
    bool has_delete = false;
    // delete node can be only in the parent statement
    std::vector<AbstractPlanNode*>& planNodes = m_stmtPlanNodesArray[0];
    for (int ii = 0; ii < planNodes.size(); ii++)
    {
        if (planNodes[ii]->getPlanNodeType() == PLAN_NODE_TYPE_DELETE)
        {
            has_delete = true;
            break;
        }
        if (planNodes[ii]->getInlinePlanNode(PLAN_NODE_TYPE_DELETE) != NULL)
        {
            has_delete = true;
            break;
        }
    }
    return has_delete;
}

std::string PlanNodeFragment::debug() {
    std::ostringstream buffer;
    for (int i = 0; i < m_stmtCnt; ++i) {
        buffer << "Execute List " << i << ":\n";
        std::vector<AbstractPlanNode*>& executeList = m_stmtExecutionListArray[i];
        for (int ctr = 0, cnt = (int)executeList.size(); ctr < cnt; ctr++) {
            buffer << "   [" << ctr << "]: " << executeList[ctr]->debug() << "\n";
        }
        buffer << "Execute Tree " << i << ":\n";
        buffer << getRootNode(i)->debug(true);
    }
    return (buffer.str());
}
