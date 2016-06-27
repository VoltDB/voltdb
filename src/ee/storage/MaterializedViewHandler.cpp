/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "MaterializedViewHandler.h"

#include "catalog/statement.h"
#include "catalog/table.h"
#include "catalog/tableref.h"
#include "common/executorcontext.hpp"

ENABLE_BOOST_FOREACH_ON_CONST_MAP(TableRef);
typedef std::pair<std::string, catalog::TableRef*> LabeledTableRef;

namespace voltdb {

    MaterializedViewHandler::MaterializedViewHandler(PersistentTable *targetTable,
                                                     catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                                                     std::map<CatalogId, Table*> &tables) :
        m_targetTable(targetTable)
    {
        delete m_targetTable->m_mvHandler;
        m_targetTable->m_mvHandler = this;
        BOOST_FOREACH (LabeledTableRef labeledTableRef, mvHandlerInfo->sourceTables()) {
            catalog::TableRef *sourceTableRef = labeledTableRef.second;
            int32_t catalogIndex = sourceTableRef->table()->relativeIndex();
            PersistentTable *sourceTable = static_cast<PersistentTable*>(tables[catalogIndex]);
            assert(sourceTable);
            addSourceTable(sourceTable);
        }
        // Handle the query plans for the create query and the query for min/max recalc.
        catalog::Statement *createQueryStatement = mvHandlerInfo->createQuery().get("createQuery");
// #ifdef VOLT_TRACE_ENABLED
        if (ExecutorContext::getExecutorContext()->m_siteId == 0) {
            const std::string& hexString = createQueryStatement->explainplan();
            assert(hexString.length() % 2 == 0);
            int bufferLength = (int)hexString.size() / 2 + 1;
            char* explanation = new char[bufferLength];
            boost::shared_array<char> memoryGuard(explanation);
            catalog::Catalog::hexDecodeString(hexString, explanation);
            cout << explanation << endl;
        }
// #endif
    }

    MaterializedViewHandler::~MaterializedViewHandler() {
        for (int i=m_sourceTables.size()-1; i>=0; i--) {
            dropSourceTable(m_sourceTables[i]);
        }
    }

    void MaterializedViewHandler::addSourceTable(PersistentTable *sourceTable) {
        sourceTable->addViewToTrigger(this);
        m_sourceTables.push_back(sourceTable);
    }

    void MaterializedViewHandler::dropSourceTable(PersistentTable *sourceTable) {
        assert( ! m_sourceTables.empty());
        sourceTable->dropViewToTrigger(this);
        PersistentTable* lastTable = m_sourceTables.back();
        if (sourceTable != lastTable) {
            // iterator to vector element:
            std::vector<PersistentTable*>::iterator it = find(m_sourceTables.begin(),
                                                              m_sourceTables.end(),
                                                              sourceTable);
            assert(it != m_sourceTables.end());
            // Use the last view to patch the potential hole.
            *it = lastTable;
        }
        // The last element is now excess.
        m_sourceTables.pop_back();
    }

} // namespace voltdb
