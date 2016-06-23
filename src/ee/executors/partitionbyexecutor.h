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
#ifndef SRC_EE_EXECUTORS_PARTITIONBYEXECUTOR_H_
#define SRC_EE_EXECUTORS_PARTITIONBYEXECUTOR_H_

#include "aggregateexecutor.h"

namespace voltdb {

/*
 *
 */
class PartitionByExecutor: public AggregateSerialExecutor {
public:
    PartitionByExecutor(VoltDBEngine* engine, AbstractPlanNode* abstract_node)
      : AggregateSerialExecutor(engine, abstract_node) {
    }
    virtual ~PartitionByExecutor();
    bool outputForEachInputRow() const;
};

} /* namespace voltdb */

#endif /* SRC_EE_EXECUTORS_PARTITIONBYEXECUTOR_H_ */
