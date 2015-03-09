/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
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

#include <boost/shared_ptr.hpp>
#include "harness.h"
#include "common/common.h"
#include "common/valuevector.h"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "expressions/expressions.h"
#include "expressions/expressionutil.h"
#include "expressions/functionexpression.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"

#define TUPLES 1000

using namespace voltdb;

class FilterTest : public Test {
public:
    FilterTest() {
        if (table_static) {
            table = table_static;
            return; // to prevent Table from being contructed more than once
        }
        CatalogId database_id = 1000;

        std::vector<std::string> columnNames(5);
        std::vector<voltdb::ValueType> columnTypes;
        std::vector<int32_t> columnLengths;
        std::vector<bool> columnAllowNull;
        for (int ctr = 0; ctr < 5; ctr++) {
            char name[16];
            if (ctr == 0) ::snprintf(name, 16, "id");
            else ::snprintf(name, 16, "val%02d", ctr);
            columnNames[ctr] = name;
            columnTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
            columnLengths.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
            columnAllowNull.push_back(false);
        }
        TupleSchema *schema = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);

        table = TableFactory::getTempTable(database_id, "test_table", schema, columnNames, NULL);

        //::printf("making a test table...\n");
        for (int64_t i = 1; i <= TUPLES; ++i) {
            TableTuple &tuple = table->tempTuple();
            tuple.setNValue(0, ValueFactory::getBigIntValue(i));
            tuple.setNValue(1, ValueFactory::getBigIntValue(i % 2));
            tuple.setNValue(2, ValueFactory::getBigIntValue(i % 3));
            tuple.setNValue(3, ValueFactory::getBigIntValue(i % 5));
            tuple.setNValue(4, ValueFactory::getBigIntValue(i % 7));
            table->insertTuple(tuple);
        }
        //::printf("made.\n");
        //::printf("%s", table->debug().c_str());
        table_static = table;
    };
    static void releaseAll() {
        delete table_static;
    };

    static Table* table_static;
    Table* table;
};
Table* FilterTest::table_static = NULL;

TEST_F(FilterTest, SimpleFilter) {
    // WHERE id = 20

    // ComparisonExpression equal(EXPRESSION_TYPE_COMPARE_EQUAL,
    //                           TupleValueExpression::getInstance(0),
    //                           ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(20)));

    TupleValueExpression *tup_val_exp = new TupleValueExpression(0, 0);
    ConstantValueExpression *const_val_exp = new ConstantValueExpression(ValueFactory::getBigIntValue(20));
    ComparisonExpression<CmpEq> *equal = new ComparisonExpression<CmpEq>(EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, const_val_exp);

    // ::printf("\nFilter:%s\n", equal->debug().c_str());

    int count = 0;
    TableIterator iter = table->iterator();
    TableTuple match(table->schema());
    while (iter.next(match)) {
        if (equal->eval(&match, NULL).isTrue()) {
            //::printf("  match:%s", match->debug(table).c_str());
            ++count;
        }
    }
    ASSERT_EQ(1, count);

    // delete the root to destroy the full tree.
    delete equal;
}

TEST_F(FilterTest, FunctionAbs1Filter) {
    // WHERE abs(id) = 20

    // ComparisonExpression equal(EXPRESSION_TYPE_COMPARE_EQUAL,
    //                           TupleValueExpression::getInstance(0),
    //                           UnaryFunctionExpression(FUNC_ABS,
    //                                                   ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(20))));

    TupleValueExpression *tup_val_exp = new TupleValueExpression(0, 0);
    ConstantValueExpression *const_val_exp = new ConstantValueExpression(ValueFactory::getBigIntValue(20));
    std::vector<AbstractExpression*>* argument = new std::vector<AbstractExpression*>();
    argument->push_back(const_val_exp);
    AbstractExpression* abs_exp = ExpressionUtil::functionFactory(FUNC_ABS, argument);
    ComparisonExpression<CmpEq> *equal = new ComparisonExpression<CmpEq>(EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, abs_exp);

    // ::printf("\nFilter:%s\n", equal->debug().c_str());

    int count = 0;
    TableIterator iter = table->iterator();
    TableTuple match(table->schema());
    while (iter.next(match)) {
        if (equal->eval(&match, NULL).isTrue()) {
            //::printf("  match:%s", match->debug(table).c_str());
            ++count;
        }
    }
    ASSERT_EQ(1, count);

    // delete the root to destroy the full tree.
    delete equal;
}

TEST_F(FilterTest, FunctionAbs2Filter) {
    // WHERE abs(0 - id) = 20

    // ComparisonExpression equal(EXPRESSION_TYPE_COMPARE_EQUAL,
    //                           0 - TupleValueExpression::getInstance(0),
    //                           UnaryFunctionExpression(EXPRESSION_TYPE_FUNCTION_ABS,
    //                                                   ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(20))));

    ConstantValueExpression *zero_val_exp = new ConstantValueExpression(ValueFactory::getBigIntValue(0));
    TupleValueExpression *tup_val_exp = new TupleValueExpression(0, 0);
    AbstractExpression* minus_exp = new OperatorExpression<OpMinus>(EXPRESSION_TYPE_OPERATOR_MINUS, zero_val_exp, tup_val_exp);
    std::vector<AbstractExpression*>* argument = new std::vector<AbstractExpression*>();
    argument->push_back(minus_exp);
    AbstractExpression* abs_exp = ExpressionUtil::functionFactory(FUNC_ABS, argument);
    ConstantValueExpression *const_val_exp = new ConstantValueExpression(ValueFactory::getBigIntValue(20));
    ComparisonExpression<CmpEq> *equal = new ComparisonExpression<CmpEq>(EXPRESSION_TYPE_COMPARE_EQUAL, abs_exp, const_val_exp);

    // ::printf("\nFilter:%s\n", equal->debug().c_str());

    int count = 0;
    TableIterator iter = table->iterator();
    TableTuple match(table->schema());
    while (iter.next(match)) {
        if (equal->eval(&match, NULL).isTrue()) {
            // ::printf("  match:%s\n", match.debug(std::string("tablename")).c_str());
            ++count;
        }
    }
    ASSERT_EQ(1, count);

    // delete the root to destroy the full tree.
    delete equal;
}

TEST_F(FilterTest, OrFilter) {
    // WHERE id = 20 OR id=30

    // shared_ptr<AbstractExpression> equal1
    //     = ComparisonExpression::getInstance(EXPRESSION_TYPE_COMPARE_EQUAL, TupleValueExpression::getInstance(0), ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(20)));
    // shared_ptr<AbstractExpression> equal2
    //     = ComparisonExpression::getInstance(EXPRESSION_TYPE_COMPARE_EQUAL, TupleValueExpression::getInstance(0), ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(30)));

    // ConjunctionExpression predicate(EXPRESSION_TYPE_CONJUNCTION_OR, equal1, equal2);

    TupleValueExpression *tup_val_a = new TupleValueExpression(0, 0);
    ConstantValueExpression *const_val_a = new ConstantValueExpression(ValueFactory::getBigIntValue(20));
    ComparisonExpression<CmpEq> *comp_a = new ComparisonExpression<CmpEq>(EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_a, const_val_a);

    TupleValueExpression *tup_val_b = new TupleValueExpression(0, 0);
    ConstantValueExpression *const_val_b = new ConstantValueExpression(ValueFactory::getBigIntValue(30));
    ComparisonExpression<CmpEq> *comp_b = new ComparisonExpression<CmpEq>(EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_b, const_val_b);

    ConjunctionExpression<ConjunctionOr> *predicate = new ConjunctionExpression<ConjunctionOr>(EXPRESSION_TYPE_CONJUNCTION_OR, comp_a, comp_b);

    // ::printf("\nFilter:%s\n", predicate->debug().c_str());

    int count = 0;
    TableIterator iter = table->iterator();
    TableTuple match(table->schema());
    while (iter.next(match)) {
        if (predicate->eval(&match, NULL).isTrue()) {
            //::printf("  match:%s\n", match->debug(table).c_str());
            ++count;
        }
    }
    ASSERT_EQ(2, count);

    // delete the root to cleanup the full tree
    delete predicate;
}
TEST_F(FilterTest, AndFilter) {
    // WHERE id <= 20 AND val1=0

    // shared_ptr<AbstractExpression> equal1
    //     = ComparisonExpression::getInstance(EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO, TupleValueExpression::getInstance(0), ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(20)));
    //
    // shared_ptr<AbstractExpression> equal2
    //     = ComparisonExpression::getInstance(EXPRESSION_TYPE_COMPARE_EQUAL, TupleValueExpression::getInstance(1), ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(0)));
    //
    // ConjunctionExpression predicate(EXPRESSION_TYPE_CONJUNCTION_AND, equal1, equal2);

    TupleValueExpression *tup_val_a = new TupleValueExpression(0, 0);
    ConstantValueExpression *const_val_a = new ConstantValueExpression(ValueFactory::getBigIntValue(20));
    AbstractExpression *comp_a = ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO, tup_val_a, const_val_a);

    TupleValueExpression *tup_val_b = new TupleValueExpression(0, 1);
    ConstantValueExpression *const_val_b = new ConstantValueExpression(ValueFactory::getBigIntValue(0));
    AbstractExpression *comp_b = ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_b, const_val_b);

    ConjunctionExpression<ConjunctionAnd> *predicate = new ConjunctionExpression<ConjunctionAnd>
                                                               (EXPRESSION_TYPE_CONJUNCTION_AND,
                                                                comp_a,
                                                                comp_b);

    // ::printf("\nFilter:%s\n", predicate->debug().c_str());

    int count = 0;
    TableIterator iter = table->iterator();
    TableTuple match(table->schema());
    while (iter.next(match)) {
        if (predicate->eval(&match, NULL).isTrue()) {
            //::printf("  match:%s\n", match->debug(table).c_str());
            ++count;
        }
    }
    ASSERT_EQ(10, count);

    // delete root to cleanup the full tree
    delete predicate;
}
TEST_F(FilterTest, ComplexFilter) {

    // WHERE val1=1 AND val2=2 AND val3=3 AND val4=4

    // shared_ptr<AbstractExpression> equal1
    //     = ComparisonExpression::getInstance(EXPRESSION_TYPE_COMPARE_EQUAL, TupleValueExpression::getInstance(1), ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(1)));
    // shared_ptr<AbstractExpression> equal2
    //     = ComparisonExpression::getInstance(EXPRESSION_TYPE_COMPARE_EQUAL, TupleValueExpression::getInstance(2), ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(2)));
    // shared_ptr<AbstractExpression> equal3
    //     = ComparisonExpression::getInstance(EXPRESSION_TYPE_COMPARE_EQUAL, TupleValueExpression::getInstance(3), ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(3)));
    // shared_ptr<AbstractExpression> equal4
    //     = ComparisonExpression::getInstance(EXPRESSION_TYPE_COMPARE_EQUAL, TupleValueExpression::getInstance(4), ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(4)));
    //
    // shared_ptr<AbstractExpression> predicate3
    //     = ConjunctionExpression::getInstance(EXPRESSION_TYPE_CONJUNCTION_AND, equal3, equal4);
    // shared_ptr<AbstractExpression> predicate2
    //     = ConjunctionExpression::getInstance(EXPRESSION_TYPE_CONJUNCTION_AND, equal2, predicate3);
    //
    // ConjunctionExpression predicate(EXPRESSION_TYPE_CONJUNCTION_AND, equal1, predicate2);

    AbstractExpression *equal1 = ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_EQUAL,
                                                   new TupleValueExpression(0, 1),
                                                   new ConstantValueExpression(ValueFactory::getBigIntValue(1)));

    AbstractExpression *equal2 = ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_EQUAL,
                                                   new TupleValueExpression(0, 2),
                                                   new ConstantValueExpression(ValueFactory::getBigIntValue(2)));

    AbstractExpression *equal3 = ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_EQUAL,
                                                   new TupleValueExpression(0, 3),
                                                   new ConstantValueExpression(ValueFactory::getBigIntValue(3)));

    AbstractExpression *equal4 = ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_EQUAL,
                                                   new TupleValueExpression(0, 4),
                                                   new ConstantValueExpression(ValueFactory::getBigIntValue(4)));

    AbstractExpression *predicate3 = ExpressionUtil::conjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND, equal3, equal4);
    AbstractExpression *predicate2 = ExpressionUtil::conjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND, equal2, predicate3);
    AbstractExpression *predicate = ExpressionUtil::conjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND, equal1, predicate2);


    // ::printf("\nFilter:%s\n", predicate->debug().c_str());

    int count = 0;
    TableIterator iter = table->iterator();
    TableTuple match(table->schema());
    while (iter.next(match)) {
        if (predicate->eval(&match, NULL).isTrue()) {
            //::printf("  match:%s\n", match->debug(table).c_str());
            ++count;
        }
    }
    ASSERT_EQ(5, count);

    delete predicate;
}
TEST_F(FilterTest, SubstituteFilter) {

    // WHERE id <= 20 AND val4=$1

    // shared_ptr<AbstractExpression> equal1
    //     = ComparisonExpression::getInstance(EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO, TupleValueExpression::getInstance(0), ConstantValueExpression::getInstance(voltdb::Value::newBigIntValue(20)));
    //
    // shared_ptr<AbstractExpression> equal2
    //     = ComparisonExpression::getInstance(EXPRESSION_TYPE_COMPARE_EQUAL, TupleValueExpression::getInstance(4), ParameterValueExpression::getInstance(0));
    //
    // ConjunctionExpression predicate(EXPRESSION_TYPE_CONJUNCTION_AND, equal1, equal2);

    NValueArray params(1);
    AbstractExpression *tv1 = new TupleValueExpression(0, 0);
    AbstractExpression *cv1 = new ConstantValueExpression(ValueFactory::getBigIntValue(20));
    AbstractExpression *equal1 = ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO, tv1, cv1);

    AbstractExpression *tv2 = new TupleValueExpression(0, 4);
    AbstractExpression *pv2 = new ParameterValueExpression(0, &params[0]);
    AbstractExpression *equal2 = ExpressionUtil::comparisonFactory(EXPRESSION_TYPE_COMPARE_EQUAL, tv2, pv2);

    AbstractExpression *predicate = ExpressionUtil::conjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND, equal1, equal2);

    // ::printf("\nFilter:%s\n", predicate->debug().c_str());

    for (int64_t implantedValue = 1; implantedValue < 5; ++implantedValue) {
        params[0] = ValueFactory::getBigIntValue(implantedValue);
        // ::printf("\nSubstituted Filter:%s\n", predicate->debug().c_str());
        // ::printf("\tLEFT:  %s\n", predicate->getLeft()->debug().c_str());
        // ::printf("\tRIGHT: %s\n", predicate->getRight()->debug().c_str());

        int count = 0;
        TableIterator iter = table->iterator();
        TableTuple match(table->schema());
        while (iter.next(match)) {
            if (predicate->eval(&match, NULL).isTrue()) {
                ++count;
            }
        }
        ASSERT_EQ(3, count);
    }

    delete predicate;
}

int main() {
    int ret = TestSuite::globalInstance()->runAll();
    FilterTest::releaseAll();// will be eventually done as its smart pointer, but safer is better.
    return ret;
}
