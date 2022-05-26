/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#include <iostream>
#include "indexes/tableindex.h"
#include "common/StackTrace.h"
#include "expressions/expressionutil.h"
#include "expressions/conjunctionexpression.h"
#include "storage/TableCatalogDelegate.hpp"

using namespace voltdb;

TableIndexScheme::TableIndexScheme(
    const std::string &a_name,
    TableIndexType a_type,
    const std::vector<int32_t>& a_columnIndices,
    const std::vector<AbstractExpression*>& a_indexedExpressions,
    AbstractExpression* a_predicate,
    bool a_unique,
    bool a_countable,
    bool migrating,
    const std::string& a_expressionsAsText,
    const std::string& a_predicateAsText,
    const TupleSchema *a_tupleSchema) :
      name(a_name),
      type(a_type),
      columnIndices(a_columnIndices),
      indexedExpressions(a_indexedExpressions),
      predicate(a_predicate),
      allColumnIndices(a_columnIndices),
      unique(a_unique),
      countable(a_countable),
      migrating(migrating),
      expressionsAsText(a_expressionsAsText),
      predicateAsText(a_predicateAsText),
      tupleSchema(a_tupleSchema) {
        if (predicate != NULL) {
            // Collect predicate column indicies
            ExpressionUtil::extractTupleValuesColumnIdx(a_predicate, allColumnIndices);
        }
        // Deprecating "CREATE MIGRATING INDEX ..." syntax, but
        // retain the catalog flag. Do not modify the index.
    }

void TableIndexScheme::setMigrate() {
   size_t const hiddenColumnIndex = tupleSchema->totalColumnCount() - 1;
   auto* hiddenColumnExpr = ExpressionUtil::columnIsNull(0, hiddenColumnIndex);
   if (predicate == nullptr) {
      predicate = hiddenColumnExpr;
   } else {
      predicate = ExpressionUtil::conjunctionFactory(ExpressionType::EXPRESSION_TYPE_CONJUNCTION_AND,
            hiddenColumnExpr, predicate);
   }
   // NOTE: we are not updating JSON expressions for the predicate, which
   // involves work on rapidjson (that we may deprecate soon), and serialization
   // of expression.
   if (allColumnIndices.empty()) {
      // if no explicit columns are used, index on the
      // hidden column (i.e. the transaction id as of
      // INTEGER type).
      allColumnIndices.emplace_back(hiddenColumnIndex);
   }

}

TableIndex::TableIndex(const TupleSchema *keySchema, const TableIndexScheme &scheme) :
    m_scheme(scheme),
    m_keySchema(keySchema),
    m_id(TableCatalogDelegate::getIndexIdString(scheme)),

    // initialize all the counters to zero
    m_inserts(0),
    m_deletes(0),
    m_updates(0),

    m_stats(this)
{}

TableIndex::~TableIndex()
{
    TupleSchema::freeTupleSchema(const_cast<TupleSchema*>(m_keySchema));
    const std::vector<AbstractExpression*> &indexed_expressions = getIndexedExpressions();
    for (int ii = 0; ii < indexed_expressions.size(); ++ii) {
        delete indexed_expressions[ii];
    }
    delete getPredicate();
}

std::string TableIndex::debug() const
{
    std::ostringstream buffer;
    buffer << getTypeName() << "(" << getName() << ")";
    buffer << (isUniqueIndex() ? " UNIQUE " : " NON-UNIQUE ");
    buffer << (isMigratingIndex() ? " MIGRATING " : " NON-MIGRATING ");
    //
    // Columns
    //
    const std::vector<int> &column_indices_vector = getColumnIndices();
    const std::vector<AbstractExpression*> &indexed_expressions = getIndexedExpressions();

    std::string add = "";
    if (indexed_expressions.size() > 0) {
        buffer << " -> " << indexed_expressions.size() << " expressions[";
        for (int ctr = 0; ctr < indexed_expressions.size(); ctr++) {
            buffer << add << ctr << "th entry=" << indexed_expressions[ctr]->debug()
                   << " type=(" << voltdb::valueToString(m_keySchema->columnType(ctr)) << ")";
            add = ", ";
        }
        buffer << "] -> Base Columns[";
    } else {
        buffer << " -> Columns[";
    }
    add = "";
    for (int ctr = 0; ctr < column_indices_vector.size(); ctr++) {
        buffer << add << ctr << "th entry=" << column_indices_vector[ctr]
               << "th (" << voltdb::valueToString(m_keySchema->columnType(ctr))
               << ") column in parent table";
        add = ", ";
    }
    buffer << "] --- size: " << getSize();
    // Predicate
    if (isPartialIndex())
    {
        buffer << " -> Predicate[" << getPredicate()->debug() << "]";
    }

    std::string ret(buffer.str());
    return (ret);
}

IndexStats* TableIndex::getIndexStats() {
    return &m_stats;
}

void TableIndex::printReport()
{
    std::cout << m_scheme.name << ",";
    std::cout << getTypeName() << ",";
    std::cout << m_inserts << ",";
    std::cout << m_deletes << ",";
    std::cout << m_updates << std::endl;
}

bool TableIndex::equals(const TableIndex *other) const {
    //TODO Do something useful here!
    return true;
}

void TableIndex::addEntry(const TableTuple *tuple, TableTuple *conflictTuple) {
    if (isPartialIndex() && !getPredicate()->eval(tuple, NULL).isTrue()) {
        // Tuple fails the predicate. Do not add it.
        return;
    }
    for(auto const* expr : getIndexedExpressions()) {
       expr->eval(tuple, nullptr);
    }
    addEntryDo(tuple, conflictTuple);
}

bool TableIndex::deleteEntry(const TableTuple *tuple) {
    if (isPartialIndex() && !getPredicate()->eval(tuple, NULL).isTrue()) {
        // Tuple fails the predicate. Nothing to delete
        return true;
    }
    return deleteEntryDo(tuple);
}

bool TableIndex::replaceEntryNoKeyChange(const TableTuple &destinationTuple,
        const TableTuple &originalTuple) {
    vassert(originalTuple.address() != destinationTuple.address());

    if (isPartialIndex()) {
        const AbstractExpression* predicate = getPredicate();
        bool destinationTupleMatch = predicate->eval(&destinationTuple, NULL).isTrue();
        bool originalTupleMatch = predicate->eval(&originalTuple, NULL).isTrue();

        if (!destinationTupleMatch && !originalTupleMatch) {
            // both tuples fail the predicate. Nothing to do. Return TRUE
            return true;
        } else if (destinationTupleMatch && !originalTupleMatch) {
            // The original tuple fails the predicate meaning the tuple is not indexed.
            // Simply add the new tuple
            TableTuple conflict(destinationTuple.getSchema());
            addEntryDo(&destinationTuple, &conflict);
            return conflict.isNullTuple();
        } else if (!destinationTupleMatch && originalTupleMatch) {
            // The destination tuple fails the predicate. Simply delete the original tuple
            return deleteEntryDo(&originalTuple);
        }
        // both tuples pass the predicate.
        vassert(predicate->eval(&destinationTuple, NULL).isTrue() && predicate->eval(&originalTuple, NULL).isTrue());
    }
    return replaceEntryNoKeyChangeDo(destinationTuple, originalTuple);
}

bool TableIndex::exists(const TableTuple *persistentTuple) const {
    if (isPartialIndex() && !getPredicate()->eval(persistentTuple, NULL).isTrue()) {
        // Tuple fails the predicate.
        return false;
    }
    return existsDo(persistentTuple);
}

bool TableIndex::checkForIndexChange(const TableTuple *lhs, const TableTuple *rhs) const {
    if (isPartialIndex()) {
        const AbstractExpression* predicate = getPredicate();
        bool lhsMatch = predicate->eval(lhs, NULL).isTrue();
        bool rhsMatch = predicate->eval(rhs, NULL).isTrue();

        if (lhsMatch != rhsMatch) {
            // only one tuple passes the predicate. Index is affected -
            // either existing tuple needs to be deleted or the new one added from/to the index
            return true;
        } else  if (!lhsMatch) {
            // both tuples fail the predicate. Index is unaffected. Return FALSE
            return false;
        }
        vassert(predicate->eval(lhs, NULL).isTrue() && predicate->eval(rhs, NULL).isTrue());
    }
    return checkForIndexChangeDo(lhs, rhs);
}

