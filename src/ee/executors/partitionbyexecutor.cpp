/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
/*
 * partitionbyexecutor.cpp
 */

#include "executors/partitionbyexecutor.h"
#include "plannodes/partitionbynode.h"

namespace voltdb {

PartitionByExecutor::~PartitionByExecutor() {
}

// The PartitionByExecutor wants one row for each input row.
bool PartitionByExecutor::outputForEachInputRow() const {
    return true;
}

bool PartitionByExecutor::p_init(AbstractPlanNode *node, TempTableLimits *limits) {
    if (!AggregateSerialExecutor::p_init(node, limits)) {
        return false;
    }
    return true;
}

} /* namespace voltdb */
