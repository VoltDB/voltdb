/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

#include "abstractplannode.h"

#include "common/TupleSchema.h"
#include "executors/abstractexecutor.h"
#include "plannodeutil.h"

#include <sstream>
#include <stdexcept>
#include <string>

using namespace json_spirit;
using namespace std;
using namespace voltdb;

AbstractPlanNode::AbstractPlanNode(int32_t plannode_id)
    : m_planNodeId(plannode_id), m_outputTable(NULL), m_executor(NULL),
      m_isInline(false)
{
}

AbstractPlanNode::AbstractPlanNode()
    : m_planNodeId(-1), m_outputTable(NULL), m_executor(NULL),
      m_isInline(false)
{
}

AbstractPlanNode::~AbstractPlanNode()
{
    delete m_executor;
    map<PlanNodeType, AbstractPlanNode*>::iterator iter;
    for (iter = m_inlineNodes.begin(); iter != m_inlineNodes.end(); iter++)
    {
        delete (*iter).second;
    }
    for (int i = 0; i < m_outputSchema.size(); i++)
    {
        delete m_outputSchema[i];
    }
}

// ------------------------------------------------------------------
// CHILDREN + PARENTS METHODS
// ------------------------------------------------------------------
void
AbstractPlanNode::addChild(AbstractPlanNode* child)
{
    m_children.push_back(child);
}

vector<AbstractPlanNode*>&
AbstractPlanNode::getChildren()
{
    return m_children;
}

vector<int32_t>&
AbstractPlanNode::getChildIds()
{
    return m_childIds;
}

const vector<AbstractPlanNode*>&
AbstractPlanNode::getChildren() const
{
    return m_children;
}

void
AbstractPlanNode::addParent(AbstractPlanNode* parent)
{
    m_parents.push_back(parent);
}

vector<AbstractPlanNode*>&
AbstractPlanNode::getParents()
{
    return m_parents;
}

vector<int32_t>&
AbstractPlanNode::getParentIds()
{
    return m_parentIds;
}

const vector<AbstractPlanNode*>&
AbstractPlanNode::getParents() const
{
    return m_parents;
}

// ------------------------------------------------------------------
// INLINE PLANNODE METHODS
// ------------------------------------------------------------------
void
AbstractPlanNode::addInlinePlanNode(AbstractPlanNode* inline_node)
{
    m_inlineNodes[inline_node->getPlanNodeType()] = inline_node;
    inline_node->m_isInline = true;
}

AbstractPlanNode*
AbstractPlanNode::getInlinePlanNode(PlanNodeType type) const
{
    map<PlanNodeType, AbstractPlanNode*>::const_iterator lookup =
        m_inlineNodes.find(type);
    AbstractPlanNode* ret = NULL;
    if (lookup != m_inlineNodes.end())
    {
        ret = lookup->second;
    }
    else
    {
        VOLT_TRACE("No internal PlanNode with type '%s' is available for '%s'",
                   plannodeutil::getTypeName(type).c_str(),
                   this->debug().c_str());
    }
    return ret;
}

map<PlanNodeType, AbstractPlanNode*>&
AbstractPlanNode::getInlinePlanNodes()
{
    return m_inlineNodes;
}

const map<PlanNodeType, AbstractPlanNode*>&
AbstractPlanNode::getInlinePlanNodes() const
{
    return m_inlineNodes;
}

bool
AbstractPlanNode::isInline() const
{
    return m_isInline;
}

// ------------------------------------------------------------------
// DATA MEMBER METHODS
// ------------------------------------------------------------------
void
AbstractPlanNode::setPlanNodeId(int32_t plannode_id)
{
    m_planNodeId = plannode_id;
}

int32_t
AbstractPlanNode::getPlanNodeId() const
{
    return m_planNodeId;
}

void
AbstractPlanNode::setExecutor(AbstractExecutor* executor)
{
    m_executor = executor;
}

void
AbstractPlanNode::setInputTables(const vector<Table*>& val)
{
    m_inputTables = val;
}

vector<Table*>&
AbstractPlanNode::getInputTables()
{
    return m_inputTables;
}

void
AbstractPlanNode::setOutputTable(Table* table)
{
    m_outputTable = table;
}

Table*
AbstractPlanNode::getOutputTable() const
{
    return m_outputTable;
}

const vector<SchemaColumn*>&
AbstractPlanNode::getOutputSchema() const
{
    return m_outputSchema;
}

TupleSchema*
AbstractPlanNode::generateTupleSchema(bool allowNulls)
{
    int schema_size = static_cast<int>(m_outputSchema.size());
    vector<voltdb::ValueType> columnTypes;
    vector<int32_t> columnSizes;
    vector<bool> columnAllowNull(schema_size, allowNulls);

    for (int i = 0; i < schema_size; i++)
    {
        SchemaColumn* col = m_outputSchema[i];
        columnTypes.push_back(col->getExpression()->getValueType());
        columnSizes.push_back(col->getExpression()->getValueSize());
    }

    TupleSchema* schema =
        TupleSchema::createTupleSchema(columnTypes, columnSizes,
                                       columnAllowNull, true);
    return schema;
}

// ----------------------------------------------------
//  Serialization Functions
// ----------------------------------------------------
AbstractPlanNode*
AbstractPlanNode::fromJSONObject(Object &obj) {

    Value typeValue = find_value(obj, "PLAN_NODE_TYPE");
    if (typeValue == Value::null)
    {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractPlanNode::fromJSONObject:"
                                      " PLAN_NODE_TYPE value is null");
    }
    string typeString = typeValue.get_str();
    AbstractPlanNode* node =
        plannodeutil::getEmptyPlanNode(stringToPlanNode(typeString));

    Value idValue = find_value(obj, "ID");
    if (idValue == Value::null)
    {
        delete node;
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractPlanNode::fromJSONObject:"
                                      " ID value is null");
    }
    node->m_planNodeId = (int32_t) idValue.get_int();

    Value inlineNodesValue = find_value(obj,"INLINE_NODES");
    if (inlineNodesValue == Value::null)
    {
        delete node;
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractPlanNode::fromJSONObject:"
                                      " INLINE_NODES value is null");
    }

    Array inlineNodes = inlineNodesValue.get_array();
    for (int ii = 0; ii < inlineNodes.size(); ii++)
    {
        AbstractPlanNode* newNode = NULL;
        try {
            Object obj = inlineNodes[ii].get_obj();
            newNode = AbstractPlanNode::fromJSONObject(obj);
        }
        catch (SerializableEEException &ex) {
            delete newNode;
            delete node;
            throw;
        }

        // todo: if this throws, new Node can be leaked.
        // As long as newNode is not NULL, this will not throw.
        node->addInlinePlanNode(newNode);
    }

    Value parentNodeIdsValue = find_value(obj, "PARENT_IDS");
    if (parentNodeIdsValue == Value::null)
    {
        delete node;
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractPlanNode::fromJSONObject:"
                                      " PARENT_IDS value is null");
    }

    Array parentNodeIdsArray = parentNodeIdsValue.get_array();
    for (int ii = 0; ii < parentNodeIdsArray.size(); ii++)
    {
        int32_t parentNodeId = (int32_t) parentNodeIdsArray[ii].get_int();
        node->m_parentIds.push_back(parentNodeId);
    }

    Value childNodeIdsValue = find_value(obj, "CHILDREN_IDS");
    if (childNodeIdsValue == Value::null)
    {
        delete node;
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractPlanNode::fromJSONObject:"
                                      " CHILDREN_IDS value is null");
    }

    Array childNodeIdsArray = childNodeIdsValue.get_array();
    for (int ii = 0; ii < childNodeIdsArray.size(); ii++)
    {
        int32_t childNodeId = (int32_t) childNodeIdsArray[ii].get_int();
        node->m_childIds.push_back(childNodeId);
    }

    Value outputSchemaValue = find_value(obj, "OUTPUT_SCHEMA");
    if (outputSchemaValue == Value::null)
    {
        delete node;
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractPlanNode::loadFromJSONObject:"
                                      " Can't find OUTPUT_SCHEMA value");
    }
    Array outputSchemaArray = outputSchemaValue.get_array();

    for (int ii = 0; ii < outputSchemaArray.size(); ii++)
    {
        Value outputColumnValue = outputSchemaArray[ii];
        SchemaColumn* outputColumn =
            new SchemaColumn(outputColumnValue.get_obj());
        node->m_outputSchema.push_back(outputColumn);
    }

    try {
        node->loadFromJSONObject(obj);
    }
    catch (SerializableEEException &ex) {
        delete node;
        throw;
    }
    return node;
}

// ------------------------------------------------------------------
// UTILITY METHODS
// ------------------------------------------------------------------
string
AbstractPlanNode::debug() const
{
    ostringstream buffer;
    buffer << plannodeutil::getTypeName(this->getPlanNodeType())
           << "[" << this->getPlanNodeId() << "]";
    return buffer.str();
}

string
AbstractPlanNode::debug(bool traverse) const
{
    return (traverse ? this->debug(string("")) : this->debug());
}

string
AbstractPlanNode::debug(const string& spacer) const
{
    ostringstream buffer;
    buffer << spacer << "* " << this->debug() << "\n";
    string info_spacer = spacer + "  |";
    buffer << this->debugInfo(info_spacer);
    //
    // Inline PlanNodes
    //
    if (!m_inlineNodes.empty())
    {
        buffer << info_spacer << "Inline Plannodes: "
               << m_inlineNodes.size() << "\n";
        string internal_spacer = info_spacer + "  ";
        map<PlanNodeType, AbstractPlanNode*>::const_iterator it;
        for (it = m_inlineNodes.begin(); it != m_inlineNodes.end(); it++)
        {
            buffer << info_spacer << "Inline "
                   << plannodeutil::getTypeName(it->second->getPlanNodeType())
                   << ":\n";
            buffer << it->second->debugInfo(internal_spacer);
        }
    }
    //
    // Traverse the tree
    //
    string child_spacer = spacer + "  ";
    for (int ctr = 0, cnt = static_cast<int>(m_children.size());
         ctr < cnt; ctr++)
    {
        buffer << child_spacer << m_children[ctr]->getPlanNodeType() << "\n";
        buffer << m_children[ctr]->debug(child_spacer);
    }
    return (buffer.str());
}
