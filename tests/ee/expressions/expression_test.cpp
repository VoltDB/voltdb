/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <time.h>
#include <queue>
#include <boost/scoped_array.hpp>

#include "harness.h"
#include "json/json.h"

#include "expressions/abstractexpression.h"
#include "expressions/expressions.h"
#include "common/types.h"
#include "common/ValuePeeker.hpp"
#include "common/PlannerDomValue.h"


using namespace std;
using namespace voltdb;
//using namespace boost;


/*
   Description of test:

   1. This test defines a data structure for each expression type with
   unique fields.

   2. The test includes a helper to convert a std::queue of these structures
   into a tree of AbstractExpressions, using the expression factory via a
   json serialization.

   3. Using this utilities, the test defines several expressions (in
   std::queue) formats and asserts on the expected result.

   TODO: Unfortunately, the json_spirit serialization asserts when trying
   to read an int field from a value serialized as a string; need to figure
   out what's going on here .. and what the proper serialization method is.
*/


/*
 *  abstract expression mock object
 */
class AE {
  public:
    AE(ExpressionType et, ValueType vt, int vs) :
        m_type(et), m_valueType(vt), m_valueSize(vs) , left(NULL), right(NULL) {}

    virtual ~AE() {
        delete left;
        delete right;
    }

    virtual Json::Value serializeValue() {
        Json::Value json;
        serialize(json);
        return json;
    }

    /* this is how java serializes.. note derived class data follows
       the serialization of children */
    virtual void serialize(Json::Value &json) {
        json["TYPE"] = static_cast<int>(m_type);
        json["VALUE_TYPE"] = static_cast<int>(m_valueType);
        json["VALUE_SIZE"] = m_valueSize;

        if (left)
            json["LEFT"] = left->serializeValue();
        if (right)
            json["RIGHT"] = right->serializeValue();
    }

    ExpressionType m_type;  // TYPE
    ValueType m_valueType;  // VALUE_TYPE
    int m_valueSize;        // VALUE_SIZE

    // to build a tree
    AE *left;
    AE *right;
};

/*
 * constant value expression mock object
 */
class CV : public AE {
  public:
    CV(ExpressionType et, ValueType vt, int vs, int64_t v) :
        AE(et, vt, vs), m_jsontype (1), m_intValue(v)
    {
        m_stringValue = NULL;
        m_doubleValue = 0.0;
    }

    CV(ExpressionType et, ValueType vt, int vs, char * v) :
        AE(et, vt, vs), m_jsontype (1), m_stringValue(strdup(v))
    {
        m_intValue = 0;
        m_doubleValue = 0.0;
    }

    CV(ExpressionType et, ValueType vt, int vs, double v) :
        AE(et, vt, vs), m_jsontype (1), m_doubleValue(v)
    {
        m_stringValue = NULL;
        m_intValue = 0;
    }

    ~CV() {
    }

    virtual void serialize(Json::Value &json) {
        AE::serialize(json);
        if (m_jsontype == 0)
            json["VALUE"] = m_stringValue;
        else if (m_jsontype == 1)
            json["VALUE"] = static_cast<Json::Int64>(m_intValue);
        else if (m_jsontype == 2)
            json["VALUE"] = m_doubleValue;
        json["ISNULL"] = false;
    }

    int m_jsontype;  // 0 = string, 1 = int64_t, 2 = double

    int64_t  m_intValue;     // VALUE
    char    *m_stringValue;  // VALUE
    double   m_doubleValue;  // VALUE
};

/*
 * parameter value expression mock object
 */
class PV : public AE {
  public:
    PV(ExpressionType et, ValueType vt, int vs, int pi) :
        AE(et, vt, vs), m_paramIdx(pi) {}

    virtual void serialize(Json::Value &json) {
        AE::serialize(json);
        json["PARAM_IDX"] = m_paramIdx;
    }

    int m_paramIdx;  // PARAM_IDX
};

/*
 * tuple value expression mock object
 */
class TV  : public AE {
  public:
    TV(ExpressionType et, ValueType vt, int vs, int ci,
       const char *tn, const char *cn, const char *ca) :
        AE(et, vt, vs), m_columnIdx(ci), m_tableName(strdup(tn)),
        m_colName(strdup(cn)), m_colAlias(strdup(ca)) {}

    ~TV() {
        delete m_tableName;
        delete m_colName;
        delete m_colAlias;
    }

    virtual void serialize(Json::Value &json) {
        AE::serialize(json);
        json["COLUMN_IDX"] = m_columnIdx;
        json["TABLE_NAME"] = m_tableName;
        json["COLUMN_NAME"] = m_colName;
        json["COLUMN_ALIAS"] = m_colAlias;
    }

    int m_columnIdx;      // COLUMN_IDX
    char * m_tableName;   // TABLE_NAME
    char * m_colName;     // COLUMN_NAME
    char * m_colAlias;    // COLUMN_ALIAS
};

/*
 * Hash range expression mock object
 */
class HR : public AE {
public:
    HR(int hashColumn, int32_t ranges[][2], int numRanges) :
        AE(EXPRESSION_TYPE_HASH_RANGE, ValueType::tBIGINT, 8),
        m_hashColumn(hashColumn),
        m_ranges(ranges),
        m_numRanges(numRanges) {

    }

    virtual void serialize(Json::Value &json) {
        AE::serialize(json);
        json["HASH_COLUMN"] = m_hashColumn;
        Json::Value array;
        for (int ii = 0; ii < m_numRanges; ii++) {
            Json::Value range;
            range["RANGE_START"] = static_cast<Json::Int64>(m_ranges[ii][0]);
            range["RANGE_END"] = static_cast<Json::Int64>(m_ranges[ii][1]);
            array.append(range);
        }
        json["RANGES"] = array;
    }

    const int m_hashColumn;
    const int32_t (*m_ranges)[2];
    const int m_numRanges;
};

/*
   helpers to build trivial left-associative trees
   that is (a, *, b, +, c) returns (a * b) + c
   and (a, +, b, * c) returns (a + b) * c
*/

AE * join(AE *op, AE *left, AE *right) {
    op->left = left;
    op->right = right;
    return op;
}

AE * makeTree(AE *tree, queue<AE*> &q) {
    if (!q.empty()) {
        AE *left, *right, *op;
        if (tree) {
            left = tree;
        }
        else {
            left = q.front();
            q.pop();
        }

        op = q.front();
        q.pop();
        right = q.front();
        q.pop();

        tree = makeTree(join(op, left, right), q);
    }
    return tree;
}

/* boilerplate to turn the queue into a real AbstractExpression tree;
   return the generated AE tree by reference to allow deletion (the queue
   is emptied by the tree building process) */
AbstractExpression * convertToExpression(queue<AE*> &e) {
    AE *tree = makeTree(NULL, e);
    Json::Value json = tree->serializeValue();
    Json::FastWriter writer;
    std::string jsonText = writer.write(json);

    PlannerDomRoot domRoot(jsonText.c_str());

    AbstractExpression * exp = AbstractExpression::buildExpressionTree(domRoot());
    delete tree;
    return exp;
}


class ExpressionTest : public Test {
    public:
        ExpressionTest() {
        }
};

/*
 * Show that simple addition works with the framework
 */
TEST_F(ExpressionTest, SimpleAddition) {
    queue<AE*> e;
    TableTuple junk;

    // 1 + 4
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)1));
    e.push(new AE(EXPRESSION_TYPE_OPERATOR_PLUS, ValueType::tTINYINT, 1));
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)4));
    boost::scoped_ptr<AbstractExpression> testexp(convertToExpression(e));

    NValue result = testexp->eval(&junk,NULL);
    ASSERT_EQ(ValuePeeker::peekAsBigInt(result), 5LL);
}

/*
 * Show that unary minus works with the framework
 */
TEST_F(ExpressionTest, SimpleUnaryMinus) {
    queue<AE*> e;
    TableTuple junk;

    // -5
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)5));
    e.push(new AE(EXPRESSION_TYPE_OPERATOR_UNARY_MINUS, ValueType::tTINYINT, 1));
    // dummy right expression to prevent segmentation fault
    // since unary minus is the only arithmetic operator with one operand
    e.push(NULL);

    boost::scoped_ptr<AbstractExpression> testexp1(convertToExpression(e));
    NValue r1 = testexp1->eval(&junk,NULL);
    ASSERT_EQ(ValuePeeker::peekAsBigInt(r1), -5LL);

    // -(-3)
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)3));
    e.push(new AE(EXPRESSION_TYPE_OPERATOR_UNARY_MINUS, ValueType::tTINYINT, 1));
    // dummy right expression to prevent segmentation fault
    e.push(NULL);
    e.push(new AE(EXPRESSION_TYPE_OPERATOR_UNARY_MINUS, ValueType::tTINYINT, 1));
    // dummy right expression to prevent segmentation fault
    e.push(NULL);

    boost::scoped_ptr<AbstractExpression> testexp2(convertToExpression(e));
    NValue r2 = testexp2->eval(&junk,NULL);
    ASSERT_EQ(ValuePeeker::peekAsBigInt(r2), 3LL);
}

/*
 * Show that the associative property is as expected
 */
TEST_F(ExpressionTest, SimpleMultiplication) {
    queue<AE*> e;
    TableTuple junk;

    // (1 + 4) * 5
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)1));
    e.push(new AE(EXPRESSION_TYPE_OPERATOR_PLUS, ValueType::tTINYINT, 1));
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)4));
    e.push(new AE(EXPRESSION_TYPE_OPERATOR_MULTIPLY, ValueType::tTINYINT, 1));
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)5));

    boost::scoped_ptr<AbstractExpression> e1(convertToExpression(e));
    NValue r1 = e1->eval(&junk,NULL);
    ASSERT_EQ(ValuePeeker::peekAsBigInt(r1), 25LL);

    // (2 * 5) + 3
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)2));
    e.push(new AE(EXPRESSION_TYPE_OPERATOR_MULTIPLY, ValueType::tTINYINT, 1));
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)5));
    e.push(new AE(EXPRESSION_TYPE_OPERATOR_PLUS, ValueType::tTINYINT, 1));
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)3));

    boost::scoped_ptr<AbstractExpression> e2(convertToExpression(e));
    NValue r2 = e2->eval(&junk,NULL);
    ASSERT_EQ(ValuePeeker::peekAsBigInt(r2), 13LL);

    // -(1 + 4) * 5
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)1));
    e.push(new AE(EXPRESSION_TYPE_OPERATOR_PLUS, ValueType::tTINYINT, 1));
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)4));
    e.push(new AE(EXPRESSION_TYPE_OPERATOR_UNARY_MINUS, ValueType::tTINYINT, 1));
    // dummy right expression to prevent segmentation fault
    e.push(NULL);
    e.push(new AE(EXPRESSION_TYPE_OPERATOR_MULTIPLY, ValueType::tTINYINT, 1));
    e.push(new CV(EXPRESSION_TYPE_VALUE_CONSTANT, ValueType::tTINYINT, 1, (int64_t)5));

    boost::scoped_ptr<AbstractExpression> testexp3(convertToExpression(e));
    NValue r3 = testexp3->eval(&junk,NULL);
    ASSERT_EQ(ValuePeeker::peekAsBigInt(r3), -25LL);
}

/*
 * Show that the hash range expression correctly selects (or doesn't) rows in ranges
 */
TEST_F(ExpressionTest, HashRange) {
    queue<AE*> e;

    const int32_t range1Max = -(numeric_limits<int32_t>::max() / 2);
    const int32_t range1Min = numeric_limits<int32_t>::min() - (range1Max / 2);
    const int32_t range2Min = 0;
    const int32_t range2Max = numeric_limits<int32_t>::max() / 2;
    const int32_t range3Min = range2Max + (range2Max / 2);
    const int32_t range3Max = numeric_limits<int32_t>::max();

    int32_t ranges[][2] = {
            { range1Min, range1Max},
            { range2Min, range2Max},
            { range3Min, range3Max}
    };

    boost::scoped_ptr<AE> ae(new HR(1, ranges, 3));
    Json::Value json = ae->serializeValue();
    Json::FastWriter writer;
    std::string jsonText = writer.write(json);
    PlannerDomRoot domRoot(jsonText.c_str());
    boost::scoped_ptr<AbstractExpression> e1(AbstractExpression::buildExpressionTree(domRoot()));

    vector<std::string> columnNames;
    columnNames.push_back("foo");
    columnNames.push_back("bar");

    vector<int32_t> columnSizes;
    columnSizes.push_back(8);
    columnSizes.push_back(4);

    vector<bool> allowNull;
    allowNull.push_back(true);
    allowNull.push_back(false);

    vector<voltdb::ValueType> types;
    types.push_back(voltdb::ValueType::tBIGINT);
    types.push_back(voltdb::ValueType::tINTEGER);

    TupleSchema *schema = TupleSchema::createTupleSchemaForTest(types,columnSizes,allowNull);

    boost::scoped_array<char> tupleStorage(new char[schema->tupleLength() + TUPLE_HEADER_SIZE]);

    TableTuple t(tupleStorage.get(), schema);
    const time_t seed = time(NULL);
    std::cout << "Seed " << seed << std::endl;
    srand(static_cast<unsigned int>(seed));

    for (int ii = 0; ii < 100000; ii++) {
        NValue val = ValueFactory::getIntegerValue(rand());
        const int32_t hash = val.murmurHash3();
        t.setNValue(1, val);
        NValue inrange = e1->eval( &t );
        if ((hash >= range1Min && hash <= range1Max) ||
             (hash >= range2Min && hash <= range2Max) ||
             (hash >= range3Min && hash <= range3Max)) {
             //We no longer allow wrapping so this condition isn't true
             //(hash >= range3Min || hash < range3Max)) {
            ASSERT_TRUE(inrange.isTrue());
        } else {
            ASSERT_FALSE(inrange.isTrue());
        }
    }
    TupleSchema::freeTupleSchema(schema);
}

TEST_F(ExpressionTest, Timestamp) {
    int64_t epoch_micros = -8881540068000000; // timestamp from "1688-07-21 09:32:12"
    boost::posix_time::ptime input_ptime = EPOCH + boost::posix_time::microseconds(epoch_micros);
    boost::gregorian::date as_date = input_ptime.date();
    ASSERT_EQ(as_date.year(), 1688);
    ASSERT_EQ(as_date.month(), 7);
    ASSERT_EQ(as_date.day(), 21);
    boost::posix_time::time_duration as_time = input_ptime.time_of_day();
    ASSERT_EQ(as_time.hours(), 9);
    ASSERT_EQ(as_time.minutes(), 32);
    ASSERT_EQ(as_time.seconds(), 12);

    // test loweset timestamp to support
    epoch_micros = GREGORIAN_EPOCH;
    input_ptime = EPOCH + boost::posix_time::microseconds(epoch_micros);
    as_date = input_ptime.date();
    ASSERT_EQ(as_date.year(), 1583);
    ASSERT_EQ(as_date.month(), 1);
    ASSERT_EQ(as_date.day(), 1);
    as_time = input_ptime.time_of_day();
    ASSERT_EQ(as_time.hours(), 0);
    ASSERT_EQ(as_time.minutes(), 0);
    ASSERT_EQ(as_time.seconds(), 0);

}

int main() {
     return TestSuite::globalInstance()->runAll();
}
