/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

#include <sstream>
#include "tablecountnode.h"
#include "common/common.h"
#include "expressions/abstractexpression.h"
#include "storage/table.h"

namespace voltdb {

TableCountPlanNode::~TableCountPlanNode() {
    delete getOutputTable();
    setOutputTable(NULL);
}

std::string TableCountPlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << this->AbstractScanPlanNode::debugInfo(spacer);
    assert(m_predicate == NULL);
    buffer << spacer << "TABLE COUNT Expression: <NULL>";
    return (buffer.str());
}

}
