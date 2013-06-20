/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#include <sstream>
#include "materializedscanplannode.h"
#include "common/common.h"
#include "expressions/abstractexpression.h"
#include "storage/table.h"

namespace voltdb {

    MaterializedScanPlanNode::~MaterializedScanPlanNode() {
        delete getOutputTable();
        setOutputTable(NULL);

        if (m_tableRowsExpression) {
            delete m_tableRowsExpression;
        }
    }

    std::string MaterializedScanPlanNode::debugInfo(const std::string &spacer) const {
        std::ostringstream buffer;
        buffer << spacer << "MATERERIALIZED SCAN Expression: <NULL>";
        return (buffer.str());
    }

    void MaterializedScanPlanNode::loadFromJSONObject(PlannerDomValue obj) {
        PlannerDomValue rowExpressionObj = obj.valueForKey("TABLE_DATA");
        assert(!m_tableRowsExpression);
        m_tableRowsExpression = AbstractExpression::buildExpressionTree(rowExpressionObj);
    }

}
