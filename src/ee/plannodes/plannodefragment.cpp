/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include <memory>

#include <boost/foreach.hpp>

#include "common/FatalException.hpp"
#include "plannodefragment.h"
#include "catalog/catalog.h"
#include "abstractplannode.h"

using namespace std;

using namespace voltdb;

PlanNodeFragment::PlanNodeFragment() :
    m_serializedType("org.voltdb.plannodes.PlanNodeList"),
    m_idToNodeMap(),
    m_stmtExecutionListMap(),
    m_parameters()
{}

PlanNodeFragment::PlanNodeFragment(AbstractPlanNode *root_node) :
    m_serializedType("org.voltdb.plannodes.PlanNodeList"),
    m_idToNodeMap(),
    m_stmtExecutionListMap(),
    m_parameters()
{
    std::auto_ptr<std::vector<AbstractPlanNode*> > executeNodeList(new std::vector<AbstractPlanNode*>());
    m_stmtExecutionListMap.insert(std::make_pair(0, executeNodeList.get()));
    executeNodeList.release();

    constructTree(root_node);
}

void PlanNodeFragment::constructTree(AbstractPlanNode* node)
{
    if (m_idToNodeMap.find(node->getPlanNodeId()) == m_idToNodeMap.end()) {
        m_stmtExecutionListMap[0]->push_back(node);
        m_idToNodeMap[node->getPlanNodeId()] = node;
        std::vector<AbstractPlanNode*> children = node->getChildren();
        for (int ii = 0; ii < children.size(); ++ii) {
            constructTree(children[ii]);
        }
    }
}

PlanNodeFragment::~PlanNodeFragment()
{
    typedef  std::map<int, std::vector<AbstractPlanNode*>* >::value_type MapEntry;
    BOOST_FOREACH(MapEntry& entry, m_stmtExecutionListMap) {
        delete entry.second;
    }

    std::map<CatalogId, AbstractPlanNode*>::iterator it = m_idToNodeMap.begin();
    for (; it != m_idToNodeMap.end(); ++it) {
        delete it->second;
    }
}

PlanNodeFragment *
PlanNodeFragment::createFromCatalog(const string value)
{
    //cout << "DEBUG PlanNodeFragment::createFromCatalog: value.size() == " << value.size() << endl;
    //cout << "DEBUG PlanNodeFragment::createFromCatalog: value == " << value << endl;

    PlannerDomRoot domRoot(value.c_str());
    try {
        PlanNodeFragment *retval = PlanNodeFragment::fromJSONObject(domRoot.rootObject());
        return retval;
    }
    catch (UnexpectedEEException& ue) {
        string prefix("\ncreateFromCatalog:\n");
        ue.appendContextToMessage(prefix + value);
        throw;
    }
}

PlanNodeFragment *
PlanNodeFragment::fromJSONObject(PlannerDomValue obj)
{
    // read and construct plannodes from json object
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
        int stmtCnt = planNodesListArray.arrayLen();
        pnf.reset(new PlanNodeFragment());
        for (int i = 0; i < stmtCnt; i++) {
            int stmtId = planNodesListArray.valueAtIndex(i).valueForKey("STATEMENT_ID").asInt();
            PlannerDomValue planNodesList = planNodesListArray.valueAtIndex(i).valueForKey("PLAN_NODES");
            PlannerDomValue executeList = executeListArray.valueAtIndex(i).valueForKey("EXECUTE_LIST");
            PlanNodeFragment::nodeListFromJSONObject(pnf.get(), planNodesList, executeList, stmtId);
        }
    } else {
        pnf.reset(new PlanNodeFragment());
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
    assert(pnf->m_stmtExecutionListMap.find(stmtId) == pnf->m_stmtExecutionListMap.end());
    // NODE_LIST
    std::vector<AbstractPlanNode*> planNodes;
    for (int i = 0; i < planNodesList.arrayLen(); i++) {
        AbstractPlanNode *node = AbstractPlanNode::fromJSONObject(planNodesList.valueAtIndex(i));
        assert(node);
        assert(pnf->m_idToNodeMap.find(node->getPlanNodeId()) == pnf->m_idToNodeMap.end());
        pnf->m_idToNodeMap[node->getPlanNodeId()] = node;
        planNodes.push_back(node);
    }

    // walk the plannodes and complete each plannode's id-to-node maps
    for (std::vector< AbstractPlanNode* >::const_iterator node = planNodes.begin();
         node != planNodes.end(); ++node) {
        const std::vector<CatalogId>& childIds = (*node)->getChildIds();

        for (int zz = 0; zz < childIds.size(); zz++) {
            (*node)->addChild(pnf->m_idToNodeMap[childIds[zz]]);
        }
    }

    // EXECUTE_LIST
    std::auto_ptr<std::vector<AbstractPlanNode*> > executeNodeList(new std::vector<AbstractPlanNode*>());
    for (int i = 0; i < executeList.arrayLen(); i++) {
        executeNodeList->push_back(pnf->m_idToNodeMap[executeList.valueAtIndex(i).asInt()]);
    }
    pnf->m_stmtExecutionListMap.insert(std::make_pair(stmtId, executeNodeList.get()));
    executeNodeList.release();

}

void
PlanNodeFragment::loadParamsFromJSONObject(PlanNodeFragment *pnf, PlannerDomValue obj)
{
    if (obj.hasKey("PARAMETERS")) {
        PlannerDomValue parametersArray = obj.valueForKey("PARAMETERS");
        for (int i = 0; i < parametersArray.arrayLen(); i++) {
            PlannerDomValue parameterArray = parametersArray.valueAtIndex(i);
            int index = parameterArray.valueAtIndex(0).asInt();
            std::string typeString = parameterArray.valueAtIndex(1).asStr();
            pnf->m_parameters.push_back(std::pair< int, voltdb::ValueType>(index, stringToValue(typeString)));
        }
    }
}

bool PlanNodeFragment::hasDelete() const
{
    bool has_delete = false;
    // delete node can be only in the parent statement
    assert(m_stmtExecutionListMap.find(0) != m_stmtExecutionListMap.end());
    std::vector<AbstractPlanNode*>* planNodes = m_stmtExecutionListMap.find(0)->second;
    for (int ii = 0; ii < planNodes->size(); ii++)
    {
        if ((*planNodes)[ii]->getPlanNodeType() == PLAN_NODE_TYPE_DELETE)
        {
            has_delete = true;
            break;
        }
        if ((*planNodes)[ii]->getInlinePlanNode(PLAN_NODE_TYPE_DELETE) != NULL)
        {
            has_delete = true;
            break;
        }
    }
    return has_delete;
}

std::string PlanNodeFragment::debug()
{
    std::ostringstream buffer;
    for (PlanNodeMapIterator mapIt = m_stmtExecutionListMap.begin(); mapIt != m_stmtExecutionListMap.end(); ++mapIt) {
        buffer << "Execute List " << mapIt->first << ":\n";
        std::vector<AbstractPlanNode*>* executeList = mapIt->second;
        for (int ctr = 0, cnt = (int)executeList->size(); ctr < cnt; ctr++) {
            buffer << "   [" << ctr << "]: " << (*executeList)[ctr]->debug() << "\n";
        }
        buffer << "Execute Tree " << mapIt->first << ":\n";
        static const std::string no_spacer("");
        buffer << getRootNode()->debug(no_spacer);
    }

    return (buffer.str());
}
