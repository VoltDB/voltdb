/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

PlanNodeFragment::PlanNodeFragment()
{
    m_serializedType = "org.voltdb.plannodes.PlanNodeList";
}

PlanNodeFragment::PlanNodeFragment(AbstractPlanNode *root_node)
{
    m_serializedType = "org.voltdb.plannodes.PlanNodeList";
    constructTree(root_node);
}

void PlanNodeFragment::constructTree(AbstractPlanNode* node)
{
    if (m_idToNodeMap.find(node->getPlanNodeId()) == m_idToNodeMap.end()) {
        m_planNodes.push_back(node);
        m_idToNodeMap[node->getPlanNodeId()] = node;
        std::vector<AbstractPlanNode*> children = node->getChildren();
        for (int ii = 0; ii < children.size(); ++ii) {
            constructTree(children[ii]);
        }
    }
}

PlanNodeFragment::~PlanNodeFragment()
{
    for (int ii = 0; ii < m_planNodes.size(); ii++) {
        delete m_planNodes[ii];
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
    auto_ptr<PlanNodeFragment> pnf(new PlanNodeFragment());

    // read and construct plannodes from json object
    PlannerDomValue planNodesArray = obj.valueForKey("PLAN_NODES");

    for (int i = 0; i < planNodesArray.arrayLen(); i++) {
        AbstractPlanNode *node = NULL;
        node = AbstractPlanNode::fromJSONObject(planNodesArray.valueAtIndex(i));
        assert(node);

        pnf->m_planNodes.push_back(node);
        pnf->m_idToNodeMap[node->getPlanNodeId()] = node;
    }

    // walk the plannodes and complete each plannode's id-to-node maps
    for (std::vector< AbstractPlanNode* >::const_iterator node = pnf->m_planNodes.begin();
         node != pnf->m_planNodes.end(); ++node) {
        const std::vector<CatalogId> childIds = (*node)->getChildIds();
        for (int zz = 0; zz < childIds.size(); zz++) {
            (*node)->addChild(pnf->m_idToNodeMap[childIds[zz]]);
        }
    }
    pnf->loadFromJSONObject(obj);

    PlanNodeFragment *retval = pnf.get();
    pnf.release();
    assert(retval);
    return retval;
}

void
PlanNodeFragment::loadFromJSONObject(PlannerDomValue obj)
{
    PlannerDomValue executeListArray = obj.valueForKey("EXECUTE_LIST");
    for (int i = 0; i < executeListArray.arrayLen(); i++) {
        m_executionList.push_back(m_idToNodeMap[executeListArray.valueAtIndex(i).asInt()]);
    }

    if (obj.hasKey("PARAMETERS")) {
        PlannerDomValue parametersArray = obj.valueForKey("PARAMETERS");
        for (int i = 0; i < parametersArray.arrayLen(); i++) {
            PlannerDomValue parameterArray = parametersArray.valueAtIndex(i);
            int index = parameterArray.valueAtIndex(0).asInt();
            std::string typeString = parameterArray.valueAtIndex(1).asStr();
            parameters.push_back(std::pair< int, voltdb::ValueType>(index, stringToValue(typeString)));
        }
    }
}

bool PlanNodeFragment::hasDelete() const
{
    bool has_delete = false;
    for (int ii = 0; ii < m_planNodes.size(); ii++) {
        if (m_planNodes[ii]->getPlanNodeType() == PLAN_NODE_TYPE_DELETE) {
            has_delete = true;
            break;
        }
        if (m_planNodes[ii]->getInlinePlanNode(PLAN_NODE_TYPE_DELETE) != NULL) {
            has_delete = true;
            break;
        }
    }
    return has_delete;
}

std::string PlanNodeFragment::debug()
{
    std::ostringstream buffer;
    buffer << "Execute List:\n";
    for (int ctr = 0, cnt = (int)m_executionList.size(); ctr < cnt; ctr++) {
        buffer << "   [" << ctr << "]: " << m_executionList[ctr]->debug() << "\n";
    }
    buffer << "Execute Tree:\n";
    static const std::string no_spacer("");
    buffer << getRootNode()->debug(no_spacer);
    return (buffer.str());
}
