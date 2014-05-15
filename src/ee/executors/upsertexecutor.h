/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#ifndef HSTOREUPSERTEXECUTOR_H
#define HSTOREUPSERTEXECUTOR_H

#include "common/common.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "executors/abstractexecutor.h"

namespace voltdb {

class UpsertPlanNode;
class TempTable;

/**
 *
 */
class UpsertExecutor : public AbstractExecutor
{
public:
    UpsertExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
        : AbstractExecutor(engine, abstract_node)
    {
        m_inputTable = NULL;
        m_node = NULL;
        m_engine = engine;
        m_partitionColumn = -1;
        m_multiPartition = false;
    }

    protected:
        bool p_init(AbstractPlanNode*,
                    TempTableLimits* limits);
        bool p_execute(const NValueArray &params);

        UpsertPlanNode* m_node;
        TempTable* m_inputTable;

        int m_partitionColumn;
        bool m_partitionColumnIsString;
        bool m_multiPartition;

        /** reference to the engine/context to store the number of modified tuples */
        VoltDBEngine* m_engine;
};

}

#endif
