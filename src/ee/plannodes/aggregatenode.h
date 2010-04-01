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

#ifndef HSTOREAGGREGATENODE_H
#define HSTOREAGGREGATENODE_H

#include "plannodes/abstractplannode.h"

namespace voltdb
{

class AggregatePlanNode : public AbstractPlanNode
{
public:
    AggregatePlanNode(CatalogId id);
    AggregatePlanNode(PlanNodeType type);
    ~AggregatePlanNode();

    virtual PlanNodeType getPlanNodeType() const;

    std::vector<std::string>& getOutputColumnNames();
    const std::vector<std::string>& getOutputColumnNames() const;

    std::vector<ValueType>& getOutputColumnTypes();
    const std::vector<ValueType>& getOutputColumnTypes() const;

    std::vector<int32_t>& getOutputColumnSizes();
    const std::vector<int32_t>& getOutputColumnSizes() const;

    std::vector<int>& getOutputColumnInputGuids();
    const std::vector<int>& getOutputColumnInputGuids() const;

    // abstractplannode virtual method
    virtual int getColumnIndexFromGuid(int guid, const catalog::Database*) const;

    std::vector<ExpressionType> getAggregates();
    const std::vector<ExpressionType> getAggregates() const;

    void setAggregateColumns(std::vector<int> columns);

    /*
     * Value returned by getAggregateColumn is uninitialized after
     * deserialization. Use getAggregateColumnName to determine the
     * proper value for the context the node is being used in.
     */
    std::vector<int> getAggregateColumns() const;

    /*
     * Returns a list of output column indices that map from each
     * aggregation to an output column. These are serialized as
     * integers and not by name as is usually done to make the plan
     * node format more human readable.
     */
    inline std::vector<int>& getAggregateOutputColumns()
    {
        return m_aggregateOutputColumns;
    }

    std::vector<std::string> getAggregateColumnNames() const;
    std::vector<int> getAggregateColumnGuids() const;

    void setGroupByColumns(std::vector<int> &columns);

    std::vector<int>& getGroupByColumns();
    const std::vector<int>& getGroupByColumns() const;

    std::vector<int>& getGroupByColumnGuids();
    const std::vector<int>& getGroupByColumnGuids() const;

    std::vector<std::string>& getGroupByColumnNames();
    const std::vector<std::string>& getGroupByColumnNames() const;

    std::string debugInfo(const std::string &spacer) const;

    //
    // Public methods used only for tests
    //
    void setOutputColumnNames(std::vector<std::string>& names);
    void setOutputColumnTypes(std::vector<ValueType>& types);
    void setOutputColumnSizes(std::vector<int32_t>& sizes);
    void setAggregates(std::vector<ExpressionType> &aggregates);
    void setAggregateOutputColumns(std::vector<int> outputColumns);
    void setAggregateColumnNames(std::vector<std::string> column_names);

protected:
    friend AbstractPlanNode*
    AbstractPlanNode::fromJSONObject(json_spirit::Object& obj,
                                     const catalog::Database* catalog_db);

    virtual void loadFromJSONObject(json_spirit::Object& obj,
                                    const catalog::Database* catalog_db);

    //
    // The node must define what the columns in the output table are
    // going to look like
    //
    std::vector<int> m_outputColumnGuids;
    std::vector<std::string> m_outputColumnNames;
    std::vector<ValueType> m_outputColumnTypes;
    std::vector<int32_t> m_outputColumnSizes;

    //
    // HACK: We use a simple type to keep track of function we are
    // going to execute.  For TPC-C, there will only be output column
    // produced...
    //
    std::vector<ExpressionType> m_aggregates;
    std::vector<int> m_aggregateColumns;
    std::vector<std::string> m_aggregateColumnNames;
    std::vector<int> m_aggregateColumnGuids;
    std::vector<int> m_aggregateOutputColumns;

    //
    // What columns to group by on
    //
    std::vector<int> m_groupByColumns;
    std::vector<int> m_groupByColumnGuids;
    std::vector<std::string> m_groupByColumnNames;

    PlanNodeType m_type; //AGGREGATE OR HASHAGGREGATE
};

}

#endif
