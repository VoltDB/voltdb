/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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

namespace voltdb {

PlanNodeFragment::PlanNodeFragment()
{
    m_serializedType = "org.voltdb.plannodes.PlanNodeList";
}

PlanNodeFragment::PlanNodeFragment(AbstractPlanNode *root_node)
{
    m_serializedType = "org.voltdb.plannodes.PlanNodeList";
    if (constructTree(root_node) != true) {
        throwFatalException("Failed to construct plan fragment");
    }
}

bool PlanNodeFragment::constructTree(AbstractPlanNode* node) {
    if (m_idToNodeMap.find(node->getPlanNodeId()) == m_idToNodeMap.end()) {
        m_planNodes.push_back(node);
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
    for (int ii = 0; ii < m_planNodes.size(); ii++) {
        delete m_planNodes[ii];
    }
}

PlanNodeFragment *
PlanNodeFragment::createFromCatalog(const string hex_string,
                                    const catalog::Database *catalog_db)
{
    //cout << "PlanNodeFragment::createFromCatalog: hex_string.size() == " << hex_string.size() << endl;
    assert (hex_string.size() % 2 == 0);
    int buffer_length = (int)hex_string.size() / 2 + 1;
    boost::shared_array<char> buffer(new char[buffer_length]);
    catalog::Catalog::hexDecodeString(hex_string, buffer.get());
    std::string bufferString( buffer.get() );
    json_spirit::Value value;
    json_spirit::read( bufferString, value );

    return PlanNodeFragment::fromJSONObject(value.get_obj(), catalog_db);
}

PlanNodeFragment *
PlanNodeFragment::fromJSONObject(json_spirit::Object &obj,
                                 const catalog::Database *catalog_db)
{
    json_spirit::Value planNodesValue = json_spirit::find_value( obj, "PLAN_NODES");
    if (planNodesValue == json_spirit::Value::null) {
        throwFatalException("Failure attempting to load plan a plan node fragment from a "
                                 "json_spirit::Object. There was no value \"PLAN_NODES\"");
    }

    PlanNodeFragment * pnf = new PlanNodeFragment();
    // read and construct plannodes from json object
    json_spirit::Array planNodesArray = planNodesValue.get_array();
    for (int ii = 0; ii < planNodesArray.size(); ii++) {
        AbstractPlanNode *node = NULL;
        try {
            node = AbstractPlanNode::fromJSONObject(planNodesArray[ii].get_obj(), catalog_db);
        }
        catch (SerializableEEException &ex) {
            delete pnf;
            throw;
        }
        pnf->m_planNodes.push_back(node);
        pnf->m_idToNodeMap[node->getPlanNodeId()] = node;
    }

    // walk the plannodes and complete each plannode's id-to-node maps
    for (std::vector< AbstractPlanNode* >::const_iterator node = pnf->m_planNodes.begin();
         node != pnf->m_planNodes.end(); ++node) {
        const std::vector<CatalogId> childIds = (*node)->getChildIds();
        std::vector<AbstractPlanNode*> &children = (*node)->getChildren();
        for (int zz = 0; zz < childIds.size(); zz++) {
            children.push_back(pnf->m_idToNodeMap[childIds[zz]]);
        }

        const std::vector<CatalogId> parentIds = (*node)->getParentIds();
        std::vector<AbstractPlanNode*> &parents = (*node)->getParents();
        for (int zz = 0; zz < parentIds.size(); zz++) {
            parents.push_back(pnf->m_idToNodeMap[parentIds[zz]]);
        }
    }
    try {
        pnf->loadFromJSONObject(obj);
    }
    catch (SerializableEEException &eeEx) {
        delete pnf;
        throw;
    }
    return pnf;
}

void
PlanNodeFragment::loadFromJSONObject(json_spirit::Object &obj)
{
    json_spirit::Value executeListValue = json_spirit::find_value( obj, "EXECUTE_LIST");
    if (executeListValue == json_spirit::Value::null) {
        // throw if list arrived without an execution ordering
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "Failure while loading a PlanNodeList. "
                                      "Couldn't find value \"EXECUTE_LIST\"");
    }
    else {
        json_spirit::Array executeListArray = executeListValue.get_array();
        for (int ii = 0; ii < executeListArray.size(); ii++) {
            m_executionList.push_back(m_idToNodeMap[executeListArray[ii].get_int()]);
        }
    }

    json_spirit::Value parametersArrayValue = json_spirit::find_value( obj, "PARAMETERS");
    if (parametersArrayValue == json_spirit::Value::null) {
        //throw if list arrived without a parameter mapping
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "Failure while loading a PlanNodeList. "
                                      "Couldn't find value \"PARAMETERS\"");
    }
    else {
        json_spirit::Array parametersArray = parametersArrayValue.get_array();
        for (int ii = 0; ii < parametersArray.size(); ii++) {
            json_spirit::Array parameterArray = parametersArray[ii].get_array();
            int index = parameterArray[0].get_int();
            std::string typeString = parameterArray[1].get_str();
            parameters.push_back(std::pair< int, voltdb::ValueType>(index, stringToValue(typeString)));
        }
    }
}


std::string PlanNodeFragment::debug() {
    std::ostringstream buffer;
    buffer << "Execute List:\n";
    for (int ctr = 0, cnt = (int)m_executionList.size(); ctr < cnt; ctr++) {
        buffer << "   [" << ctr << "]: " << m_executionList[ctr]->debug() << "\n";
    }
    buffer << "Execute Tree:\n";
    buffer << getRootNode()->debug(true);
    return (buffer.str());
}



}

