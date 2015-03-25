/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include <getopt.h>

#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <set>

#include "boost/timer.hpp"
#include "boost/format.hpp"
#include "boost/lexical_cast.hpp"

#include "harness.h"

#include "common/ThreadLocalPool.h"
#include "common/TupleSchema.h"
#include "common/UndoQuantum.h"
#include "common/ValueFactory.hpp"
#include "common/executorcontext.hpp"
#include "common/tabletuple.h"
#include "executors/OptimizedProjector.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/tuplevalueexpression.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"

using namespace voltdb;
using namespace std;


static int64_t NUM_ROWS = 10000;
static int64_t NUM_COLS = 32;


class OptimizedProjectorTest : public Test
{
public:

    static CatalogId databaseId() {
        return DATABASE_ID;
    }

    static void init() {
        if (ExecutorContext::getExecutorContext() == NULL) {
            Pool* testPool = new Pool();
            UndoQuantum* wantNoQuantum = NULL;
            Topend* topless = NULL;
            (void)new ExecutorContext(0, 0, wantNoQuantum, topless, testPool, NULL, "", 0, NULL, NULL);
        }
    }

    enum TypeAndSize {
        TINYINT,
        SMALLINT,
        INTEGER,
        BIGINT,
        DOUBLE,
        VARCHAR_INLINE,
        VARCHAR_OUTLINE,
        TIMESTAMP,
        DECIMAL,
        VARBINARY_INLINE,
        VARBINARY_OUTLINE
    };

    static ValueType toValueType(TypeAndSize tas) {
        switch (tas) {
        case TINYINT: return VALUE_TYPE_TINYINT;
        case SMALLINT: return VALUE_TYPE_SMALLINT;
        case INTEGER: return VALUE_TYPE_INTEGER;
        case BIGINT: return VALUE_TYPE_BIGINT;
        case VARCHAR_INLINE: return VALUE_TYPE_VARCHAR;
        case VARCHAR_OUTLINE: return VALUE_TYPE_VARCHAR;
        case VARBINARY_INLINE: return VALUE_TYPE_VARBINARY;
        case VARBINARY_OUTLINE: return VALUE_TYPE_VARBINARY;
        case TIMESTAMP: return VALUE_TYPE_TIMESTAMP;
        case DECIMAL: return VALUE_TYPE_DECIMAL;
        default:
            break;
        }

        return VALUE_TYPE_INVALID;
    }

    static vector<ValueType> toValueType(const vector<TypeAndSize>  tasVec) {
        vector<ValueType> valTypes;
        BOOST_FOREACH(TypeAndSize tas, tasVec) {
            valTypes.push_back(toValueType(tas));
        }

        return valTypes;
    }

    static TupleSchema* createSchemaEz(const std::vector<TypeAndSize> &types) {
        std::vector<int32_t> sizes;
        BOOST_FOREACH(TypeAndSize tas, types) {
            switch (tas) {
            case VARCHAR_INLINE:
            case VARBINARY_INLINE:
                // arbitrarily choose the size here.
                sizes.push_back(8);
                break;
            case VARCHAR_OUTLINE:
            case VARBINARY_OUTLINE:
                // arbitrarily choose the size here.
                sizes.push_back(256);
                break;
            default:
                sizes.push_back(NValue::getTupleStorageSize(toValueType(tas)));
            }
        }

        TupleSchema *schema = TupleSchema::createTupleSchemaForTest(toValueType(types), sizes, std::vector<bool>(sizes.size()));
        return schema;
    }

    enum TableType { TEMP, PERSISTENT };

    static voltdb::Table* createTableEz(TableType tableType, const std::vector<TypeAndSize> &types) {

        const std::string tableName = "a_table";

        TupleSchema *schema = createSchemaEz(types);
        std::vector<std::string> names;
        int i = 0;
        BOOST_FOREACH(TypeAndSize tas, types) {
            (void)tas;
            std::string name = "C";
            name += static_cast<char>(i);
            names.push_back(name);
            ++i;
        }

        voltdb::Table* tbl = NULL;
        if (tableType == PERSISTENT) {
            char signature[20];
            tbl = voltdb::TableFactory::getPersistentTable(databaseId(),
                                                           tableName,
                                                           schema,
                                                           names,
                                                           signature);
        }
        else {
            tbl = voltdb::TableFactory::getTempTable(databaseId(),
                                                     tableName,
                                                     schema,
                                                     names,
                                                     NULL);
        }

        return tbl;
    }

    std::string randomString(int maxLen) {
        static const std::string chars =
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "0123456789~`!@#$%^&*()-=_+,./<>?;':\"[]\{}|";

        std::ostringstream oss;
        int len = rand() % maxLen;
        for (int i = 0; i < len; ++i) {
            oss << chars[rand() % chars.length()];
        }

        return oss.str();
    }

    void fillTable(Table* tbl, int64_t numRows) {
        const TupleSchema* schema = tbl->schema();
        StandAloneTupleStorage storage(schema);
        TableTuple &srcTuple = const_cast<TableTuple&>(storage.tuple());

        for (int64_t i = 0; i < numRows; ++i) {
            for (int j = 0; j < schema->columnCount(); ++j) {
                uint32_t length = schema->getColumnInfo(j)->length;
                ValueType vt = schema->columnType(j);
                NValue nval;
                switch (vt) {
                case VALUE_TYPE_BIGINT: {
                    nval = ValueFactory::getBigIntValue(i * 10000 + j);
                    break;
                }
                case VALUE_TYPE_VARCHAR:
                    nval = ValueFactory::getStringValue(randomString(length));
                    break;
                case VALUE_TYPE_VARBINARY:
                    nval = ValueFactory::getBinaryValue(randomString(length));
                    break;
                default:
                    assert(false);
                }

                srcTuple.setNValue(j, nval);
            }
            tbl->insertTuple(srcTuple);
        }
    }

    void projectFields(Table* srcTable, Table* dstTable,
                       const OptimizedProjector& projector) {

        TableTuple srcTuple(srcTable->schema());
        StandAloneTupleStorage dstStorage(dstTable->schema());
        TableTuple& dstTuple = const_cast<TableTuple&>(dstStorage.tuple());
        TableIterator iterator = srcTable->iteratorDeletingAsWeGo();
        while (iterator.next(srcTuple)) {

            projector.exec(dstTuple, srcTuple);

            dstTable->insertTuple(dstTuple);
        }
    }

    bool assertProjection(Table* srcTable, Table* dstTable, const OptimizedProjector& baselineProjector) {
        TableTuple srcTuple(srcTable->schema());
        TableTuple dstTuple(dstTable->schema());
        TableIterator srcIterator = srcTable->iteratorDeletingAsWeGo();
        TableIterator dstIterator = dstTable->iteratorDeletingAsWeGo();
        while (srcIterator.next(srcTuple)) {
            if (! dstIterator.next(dstTuple)) {
                cout << "Too few rows in dst table\n";
                return false;
            }

            std::vector<AbstractExpression*> exprs = baselineProjector.exprs();
            int dstIdx = 0;
            BOOST_FOREACH(AbstractExpression* expr, exprs) {
                NValue expectedVal = expr->eval(&srcTuple, NULL);
                NValue actualVal = dstTuple.getNValue(dstIdx);

                bool b = expectedVal.op_equals(actualVal).isTrue();
                if (!b) {
                    cout << "\nFields failed to compare as equal.  "
                         << "Dst: " << dstIdx << "\n";
                    cout << "  " << srcTuple.debug("src") << "\n";
                    cout << "  " << dstTuple.debug("dst") << "\n\n";
                    return false;
                }

                ++dstIdx;
            }
        }

        if (dstIterator.next(dstTuple)) {
            cout << "Too many rows in dst table\n";
            return false;
        }

        return true;
    }

    pair<bool, double> runSteps(const std::string &name,
                                const std::vector<TypeAndSize>& dstTableTypes,
                                Table& srcTbl,
                                const OptimizedProjector& projector,
                                const OptimizedProjector& baselineProjector,
                                double baselineRate) {

        boost::scoped_ptr<voltdb::Table> dstTable(createTableEz(TEMP, dstTableTypes));
        boost::timer t;

        t.restart();
        projectFields(&srcTbl, dstTable.get(), projector);
        double rowsPerSecond = static_cast<double>(NUM_ROWS) / t.elapsed();
        cout << "            Projected " << boost::format("%10.0f") % rowsPerSecond
             << " rows per second.  (" << name << ")\n";

        // Make sure we get the same answer as normal evaluation.
        bool success = assertProjection(&srcTbl, dstTable.get(), baselineProjector);

        if (baselineRate > 0.0) {
            double percentChange = (rowsPerSecond - baselineRate) / baselineRate * 100;
            cout << "              Percent change: " << boost::format("%3.3f%%") % (percentChange) << "\n";
        }

        return make_pair(success, rowsPerSecond);
    }

    template <typename T>
    T log2(T n) {
        return static_cast<T>(log(static_cast<double>(n)) / log(2.0));
    }

    void runProjectionTest(const std::vector<TypeAndSize>& srcTableTypes,
                           const std::vector<TypeAndSize>& dstTableTypes,
                           const OptimizedProjector& baselineProjector) {

        TupleSchema* dstSchema = createSchemaEz(dstTableTypes);

        boost::scoped_ptr<voltdb::Table> srcTable(createTableEz(PERSISTENT, srcTableTypes));
        fillTable(srcTable.get(), NUM_ROWS);

        cout << "\n";
        int numBits = static_cast<int>(log2(NUM_COLS));
        for (int i = 0; i <= numBits; ++i) {

            std::string prefix;
            if (i == numBits) {
                prefix = "no permutation ";
            }
            else {
                prefix = "permute index bit " + boost::lexical_cast<std::string>(i) + " ";
            }

            OptimizedProjector permutedProjector = OptimizedProjector(baselineProjector);
            permutedProjector.permuteOnIndexBit(numBits, i);

            OptimizedProjector optimizedProjector = OptimizedProjector(permutedProjector);
            optimizedProjector.optimize(dstSchema, srcTable->schema());

            // Depending on how we're permuting, we can figure out how many optimized steps
            // there should be:
            //
            //   flipping index bit 0              -->    NUM_COLS / 1 steps (swapping odd even pairs)
            //                      1              -->    NUM_COLS / 2 steps
            //                      2              -->    NUM_COLS / 4 steps
            //                      ...
            //                 log2(NUM_COLS)      -->    1 step (projection just moves contiguous data)
            //
            // I.e., if we're flipping bit i, optimized steps should contain NUM_COLS / 2^i things.
            size_t expectedNumberOfSteps = NUM_COLS >> i;
            ASSERT_EQ(optimizedProjector.numSteps(), expectedNumberOfSteps);

            std::pair<bool, double> successAndRate;
            successAndRate = runSteps(prefix + "baseline",
                                                  dstTableTypes, *srcTable,
                                                  permutedProjector,
                                                  permutedProjector, 0.0);
            ASSERT_TRUE_MSG(successAndRate.first, "Baseline src and dst failed to verify");

            successAndRate = runSteps(prefix + "optimized",
                                      dstTableTypes, *srcTable,
                                      optimizedProjector,
                                      permutedProjector, successAndRate.second);
            ASSERT_TRUE_MSG(successAndRate.first, "Memcpy src and dst failed to verify");
        }

        TupleSchema::freeTupleSchema(dstSchema);
        cout << "            ";
    }

private:
    static const CatalogId DATABASE_ID = 100;
};

TEST_F(OptimizedProjectorTest, ProjectTupleValueExpressions)
{
    std::vector<TypeAndSize> bigIntColumns;

    for (int i = 0; i < NUM_COLS; ++i) {
        bigIntColumns.push_back(BIGINT);
    }

    // Describe a way to move fields from one tuple to another
    vector<TupleValueExpression*> exprs;
    for (int i = 0; i < NUM_COLS; ++i) {
        exprs.push_back(new TupleValueExpression(0, i));
    }

    OptimizedProjector projector;
    BOOST_FOREACH(TupleValueExpression* e, exprs) {
        projector.insertStep(e, e->getColumnId());
    }

    cout << "\n\n          " << "BIGINT columns:\n";
    runProjectionTest(bigIntColumns, bigIntColumns, projector);

    std::vector<TypeAndSize> varcharColumns;
    for (int i = 0; i < NUM_COLS; ++i) {
        varcharColumns.push_back(VARCHAR_INLINE);
    }

    cout << "\n          " << "VARCHAR columns (inlined):\n";
    runProjectionTest(varcharColumns, varcharColumns, projector);

    std::vector<TypeAndSize> outlinedVarcharColumns;
    for (int i = 0; i < NUM_COLS; ++i) {
        outlinedVarcharColumns.push_back(VARCHAR_OUTLINE);
    }

    cout << "\n          " << "VARCHAR columns (outlined):\n";
    runProjectionTest(outlinedVarcharColumns, outlinedVarcharColumns, projector);

    BOOST_FOREACH(TupleValueExpression* e, exprs) {
        delete e;
    }

    // Still to test:
    //   Different data types
    //   Different expressions
    //   Implicit casts between types
    //   Combinations of above
}

int main(int argc, char* argv[]) {
    int opt;
    while ((opt = getopt(argc, argv, "r:c:")) != -1) {
        switch (opt) {
        case 'r':
            NUM_ROWS = boost::lexical_cast<int64_t>(optarg);
            break;
        case 'c': {
            unsigned long val = boost::lexical_cast<unsigned long>(optarg);

            assert (std::bitset<64>(val).count() == 1);
            NUM_COLS = static_cast<int64_t>(val);
            break;
        }
        default:
            cerr << "Usage: " << argv[0] << " [-r <num_rows> ] [-c <num_cols>]\n";
            exit(1);
        }
    }

    OptimizedProjectorTest::init();
    return TestSuite::globalInstance()->runAll();
}
