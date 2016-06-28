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

#include "MaterializedViewHandler.h"
#include "catalog/statement.h"
#include "catalog/table.h"
#include "catalog/tableref.h"
#include "common/executorcontext.hpp"
#include "TableCatalogDelegate.hpp"


ENABLE_BOOST_FOREACH_ON_CONST_MAP(TableRef);
ENABLE_BOOST_FOREACH_ON_CONST_MAP(Statement);
typedef std::pair<std::string, catalog::TableRef*> LabeledTableRef;
typedef std::pair<std::string, catalog::Statement*> LabeledStatement;

namespace voltdb {

    MaterializedViewHandler::MaterializedViewHandler(PersistentTable *destTable,
                                                     catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                                                     VoltDBEngine *engine) {
        if (ExecutorContext::getExecutorContext()->m_siteId == 0) { cout << "View " << destTable->name() << ":\n"; }
        install(destTable, mvHandlerInfo, engine);
        setUpCreateQuery(mvHandlerInfo, engine);
        setUpMinMaxQueries(mvHandlerInfo, engine);
        m_dirty = false;
    }

    MaterializedViewHandler::~MaterializedViewHandler() {
        if (ExecutorContext::getExecutorContext()->m_siteId == 0) { cout << "MaterializedViewHandler::~MaterializedViewHandler() " << endl; }
        for (int i=m_sourceTables.size()-1; i>=0; i--) {
            dropSourceTable(m_sourceTables[i]);
        }
    }

    void MaterializedViewHandler::addSourceTable(PersistentTable *sourceTable) {
        if (ExecutorContext::getExecutorContext()->m_siteId == 0) { cout << "MaterializedViewHandler::addSourceTable() " << sourceTable->name()  << endl; }
        sourceTable->addViewToTrigger(this);
        m_sourceTables.push_back(sourceTable);
        m_dirty = true;
    }

    void MaterializedViewHandler::dropSourceTable(PersistentTable *sourceTable) {
        if (ExecutorContext::getExecutorContext()->m_siteId == 0) { cout << "MaterializedViewHandler::dropSourceTable() " << sourceTable->name()  << endl; }
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
        m_dirty = true;
    }

    void MaterializedViewHandler::install(PersistentTable *destTable,
                                          catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                                          VoltDBEngine *engine) {
        m_destTable = destTable;
        // Delete the existing handler if exists. When the existing handler is destructed,
        // it will automatically removes itself from all the viewsToTrigger lists of its source tables.
        delete m_destTable->m_mvHandler;
        // The handler will not only be installed on the view table, but also the source tables.
        m_destTable->m_mvHandler = this;
        BOOST_FOREACH (LabeledTableRef labeledTableRef, mvHandlerInfo->sourceTables()) {
            catalog::TableRef *sourceTableRef = labeledTableRef.second;
            TableCatalogDelegate *sourceTcd =  engine->getTableDelegate(sourceTableRef->table()->name());
            PersistentTable *sourceTable = sourceTcd->getPersistentTable();
            assert(sourceTable);
            addSourceTable(sourceTable);
        }
    }

    void MaterializedViewHandler::setUpCreateQuery(catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                                                      VoltDBEngine *engine) {
        catalog::Statement *createQueryStatement = mvHandlerInfo->createQuery().get("createQuery");
        m_createQueryExecutorVector = ExecutorVector::fromCatalogStatement(engine, createQueryStatement);
        m_createQueryExecutorVector->getRidOfSendExecutor();
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

    void MaterializedViewHandler::setUpMinMaxQueries(catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                                                 VoltDBEngine *engine) {
        m_minMaxExecutorVectors.clear();
        BOOST_FOREACH (LabeledStatement labeledStatement, mvHandlerInfo->fallbackQueryStmts()) {
            catalog::Statement *stmt = labeledStatement.second;
            boost::shared_ptr<ExecutorVector> execVec = ExecutorVector::fromCatalogStatement(engine, stmt);
            execVec->getRidOfSendExecutor();
            m_minMaxExecutorVectors.push_back(execVec);
        }
    }

} // namespace voltdb
