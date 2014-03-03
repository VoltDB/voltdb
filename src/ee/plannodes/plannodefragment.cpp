/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
#include "common/FatalException.hpp"
#include "plannodefragment.h"
#include "catalog/catalog.h"
#include "abstractplannode.h"

using namespace std;

using namespace voltdb;

PlanNodeFragment::PlanNodeFragment() :
        m_serializedType("org.voltdb.plannodes.PlanNodeList"),
        m_idToNodeMap(),
        m_stmtExecutionListArray(),
        m_stmtPlanNodesArray(),
        m_parameters(),
        m_subqueryParameters() 
{
    // Create an entry for the parent statement. It always exists
    m_stmtPlanNodesArray.push_back(new std::vector<AbstractPlanNode*>());
    m_stmtExecutionListArray.push_back(new std::vector<AbstractPlanNode*>());
}

PlanNodeFragment::PlanNodeFragment(AbstractPlanNode *root_node) :
        m_serializedType("org.voltdb.plannodes.PlanNodeList"),
        m_idToNodeMap(),
        m_stmtExecutionListArray(),
        m_stmtPlanNodesArray(),
        m_parameters(),
        m_subqueryParameters()
{
    // Create an entry for the parent statement. It always exists
    m_stmtPlanNodesArray.push_back(new std::vector<AbstractPlanNode*>());
    m_stmtExecutionListArray.push_back(new std::vector<AbstractPlanNode*>());
    if (constructTree(root_node) != true) {
        throwFatalException("Failed to construct plan fragment");
    }
}

bool PlanNodeFragment::constructTree(AbstractPlanNode* node) {
    if (m_idToNodeMap.find(node->getPlanNodeId()) == m_idToNodeMap.end()) {
        m_stmtPlanNodesArray[0]->push_back(node);
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
    for (int ii = 0; ii < m_stmtExecutionListArray.size(); ii++) {
        delete m_stmtExecutionListArray[ii];
    }

    for (int ii = 0; ii < m_stmtPlanNodesArray.size(); ii++) {
        std::vector<AbstractPlanNode*>* planNodes = m_stmtPlanNodesArray[ii];
        for (int jj = 0; jj < planNodes->size(); jj++) {
            delete (*planNodes)[jj];
        }
        delete m_stmtPlanNodesArray[ii];
    }

    for (int kk = 0; kk < m_subqueryParameters.size(); ++kk) {
        std::vector<std::pair< int, voltdb::AbstractExpression*> >* params = m_subqueryParameters[kk];
        for (int mm = 0; mm < params->size(); ++mm) {
            delete (*params)[mm].second;
        }
        delete m_subqueryParameters[kk];
    }
}

PlanNodeFragment *
PlanNodeFragment::createFromCatalog(const string value)
{
    //cout << "DEBUG PlanNodeFragment::createFromCatalog: value.size() == " << value.size() << endl;
    //cout << "DEBUG PlanNodeFragment::createFromCatalog: value == " << value << endl;

    PlannerDomRoot domRoot(value.c_str());
    PlannerDomValue rootObj = domRoot.rootObject();

    std::auto_ptr<PlanNodeFragment> pnf(new PlanNodeFragment());

    // load nodes from the parent statement
    PlanNodeFragment::fromJSONObject(rootObj.valueForKey("PLAN_NODES"), *pnf->m_stmtPlanNodesArray[0], pnf->m_idToNodeMap);
    PlannerDomValue executeListArray = rootObj.valueForKey("EXECUTE_LIST");
    for (int i = 0; i < executeListArray.arrayLen(); i++) {
        pnf->m_stmtExecutionListArray[0]->push_back(pnf->m_idToNodeMap[executeListArray.valueAtIndex(i).asInt()]);
    }

    // load parameters
    PlanNodeFragment::loadParametersFromJSONObject(rootObj.valueForKey("PARAMETERS"), pnf->m_parameters);

    // load subqueries
    if (rootObj.hasKey("SUBQUERIES_PLAN_NODES")) {
        PlanNodeFragment::loadSubqueriesFromJSONObject(rootObj, pnf.get());
    }

    PlanNodeFragment *retval = pnf.release();
    assert(retval);
    return retval;
}

void
PlanNodeFragment::loadSubqueriesFromJSONObject(PlannerDomValue rootObj, PlanNodeFragment* pnf) {
    PlannerDomValue planNodesArray = rootObj.valueForKey("SUBQUERIES_PLAN_NODES");
    PlannerDomValue executionListArray = rootObj.valueForKey("SUBQUERIES_EXECUTE_LISTS");
    if (planNodesArray.arrayLen() != executionListArray.arrayLen()) {
        throwFatalException("Failed to load subqueries.");
    }
    int size = planNodesArray.arrayLen();
    pnf->m_stmtPlanNodesArray.reserve(size);
    pnf->m_stmtExecutionListArray.reserve(size);
    for (int i = 0; i < size; ++i) {
        PlannerDomValue executionListObj = executionListArray.valueAtIndex(i);
        int id = executionListObj.valueForKey("SUBQUERY_ID").asInt();
        if (id != i + 1) {
            throwFatalException("Failed to load subqueries: subquery ids are out of order.");
        }
        std::auto_ptr<std::vector<AbstractPlanNode*> > executionList(new std::vector<AbstractPlanNode*>());
        PlannerDomValue subExecuteListArray = executionListObj.valueForKey("EXECUTE_LIST");
        for (int j = 0; i < subExecuteListArray.arrayLen(); j++) {
            executionList->push_back(pnf->m_idToNodeMap[subExecuteListArray.valueAtIndex(j).asInt()]);
        }
        pnf->m_stmtExecutionListArray.push_back(executionList.release());

        PlannerDomValue planNodesObj = planNodesArray.valueAtIndex(i);
        id = planNodesObj.valueForKey("SUBQUERY_ID").asInt();
        if (id != i + 1) {
            throwFatalException("Failed to load subqueries: subquery ids are out of order.");
        }
        std::auto_ptr<std::vector<AbstractPlanNode*> > planNodes(new std::vector<AbstractPlanNode*>());
        PlanNodeFragment::fromJSONObject(planNodesObj.valueForKey("PLAN_NODES"), *planNodes.get(), pnf->m_idToNodeMap);
        pnf->m_stmtPlanNodesArray.push_back(planNodes.release());
    }
    // load subquery parameters
    if (rootObj.hasKey("SUBQUERIES_PARAMETERS")) {
        PlannerDomValue parametersArray = rootObj.valueForKey("SUBQUERIES_PARAMETERS");
        int paramSize = parametersArray.arrayLen();
        pnf->m_subqueryParameters.reserve(paramSize);
        for (int i = 0; i < paramSize; ++i) {
            PlannerDomValue subqueryParamsObj = parametersArray.valueAtIndex(i);
            PlanNodeFragment::loadSubqueryParametersFromJSONObject(subqueryParamsObj, pnf->m_subqueryParameters);
        }
    }

// @TODO ENG-451-exists - final pass through to distribute params and executors vectors

    return;
}

void
PlanNodeFragment::fromJSONObject(PlannerDomValue planNodesArray, std::vector<AbstractPlanNode*>& planNodes,
    std::map<CatalogId, AbstractPlanNode*>& idToNodeMap)
{
    for (int i = 0; i < planNodesArray.arrayLen(); i++) {
        AbstractPlanNode *node = NULL;
        node = AbstractPlanNode::fromJSONObject(planNodesArray.valueAtIndex(i));
        assert(node);

        planNodes.push_back(node);
        if (idToNodeMap.find(node->getPlanNodeId()) != idToNodeMap.end()) {
            throwFatalException("Failed to load subqueries: node ids are not unique.");
        }
        idToNodeMap[node->getPlanNodeId()] = node;
    }

    // walk the plannodes and complete each plannode's id-to-node maps
    for (std::vector< AbstractPlanNode* >::const_iterator node = planNodes.begin();
         node != planNodes.end(); ++node) {
        const std::vector<CatalogId> childIds = (*node)->getChildIds();
        std::vector<AbstractPlanNode*> &children = (*node)->getChildren();
        for (int zz = 0; zz < childIds.size(); zz++) {
            children.push_back(idToNodeMap[childIds[zz]]);
        }

        const std::vector<CatalogId> parentIds = (*node)->getParentIds();
        std::vector<AbstractPlanNode*> &parents = (*node)->getParents();
        for (int zz = 0; zz < parentIds.size(); zz++) {
            parents.push_back(idToNodeMap[parentIds[zz]]);
        }
    }
}

void
PlanNodeFragment::loadParametersFromJSONObject(PlannerDomValue parametersArray,
        std::vector<std::pair< int, voltdb::ValueType> >& parameters)
{
    for (int i = 0; i < parametersArray.arrayLen(); i++) {
        PlannerDomValue parameterArray = parametersArray.valueAtIndex(i);
        int index = parameterArray.valueAtIndex(0).asInt();
        std::string typeString = parameterArray.valueAtIndex(1).asStr();
        parameters.push_back(std::pair< int, voltdb::ValueType>(index, stringToValue(typeString)));
    }
}

void
PlanNodeFragment::loadSubqueryParametersFromJSONObject(PlannerDomValue subqueryParamsObj,
        std::vector<std::vector<std::pair< int, voltdb::AbstractExpression*> >*>& subqueryParameters)
{
    int subqueryId = subqueryParamsObj.valueForKey("SUBQUERY_ID").asInt();
    if (subqueryId != subqueryParameters.size() + 1) {
        throwFatalException("Failed to load subqueries params: subquery ids are out of order.");
    }
    PlannerDomValue parametersArray = subqueryParamsObj.valueForKey("PARAMETERS");
    int size = parametersArray.arrayLen();
    std::auto_ptr<std::vector<std::pair< int, voltdb::AbstractExpression*> > > parameters(
        new std::vector<std::pair< int, voltdb::AbstractExpression*> >(size));
    for (int i = 0; i < size; i++) {
        PlannerDomValue parameterObj = parametersArray.valueAtIndex(i);
        int index = parameterObj.valueForKey("PARAMETER_IDX").asInt();
        AbstractExpression* tve = AbstractExpression::buildExpressionTree(parameterObj.valueForKey("PARAMETER_EXPR"));
        parameters->push_back(std::pair< int, AbstractExpression*>(index, tve));
    }
    subqueryParameters.push_back(parameters.release());
}


bool PlanNodeFragment::hasDelete() const
{
    bool has_delete = false;
    assert(!m_stmtPlanNodesArray.empty());
    // Child statements can not have a delete node. Check only the parent statement
    std::vector<AbstractPlanNode*> planNodes = *m_stmtPlanNodesArray[0];
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
// TODO ENG_451-inexists add substatements
    std::ostringstream buffer;
    buffer << "Execute List:\n";
    for (int ctr = 0, cnt = (int)m_stmtExecutionListArray[0]->size(); ctr < cnt; ctr++) {
        buffer << "   [" << ctr << "]: " << (*m_stmtExecutionListArray[0])[ctr]->debug() << "\n";
    }
    buffer << "Execute Tree:\n";
    buffer << getRootNode()->debug(true);
    return (buffer.str());
}
