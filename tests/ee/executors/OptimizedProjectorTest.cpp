/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <set>

#include "boost/format.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/make_shared.hpp"
#include "boost/shared_ptr.hpp"
#include "boost/timer.hpp"

#include "harness.h"

#include "common/ThreadLocalPool.h"
#include "common/TupleSchema.h"
#include "common/UndoQuantum.h"
#include "common/ValueFactory.hpp"
#include "common/executorcontext.hpp"
#include "common/tabletuple.h"
#include "executors/OptimizedProjector.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/operatorexpression.h"
#include "expressions/tuplevalueexpression.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"


static int64_t NUM_ROWS = 100;
static int64_t NUM_COLS = 32;

namespace voltdb {
static const CatalogId DATABASE_ID = 100;

// Declarations in this namespace should someday become
// more widely visible for use in other tests.
namespace eetest {

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
        case TINYINT: return ValueType::tTINYINT;
        case SMALLINT: return ValueType::tSMALLINT;
        case INTEGER: return ValueType::tINTEGER;
        case BIGINT: return ValueType::tBIGINT;
        case VARCHAR_INLINE: return ValueType::tVARCHAR;
        case VARCHAR_OUTLINE: return ValueType::tVARCHAR;
        case VARBINARY_INLINE: return ValueType::tVARBINARY;
        case VARBINARY_OUTLINE: return ValueType::tVARBINARY;
        case TIMESTAMP: return ValueType::tTIMESTAMP;
        case DECIMAL: return ValueType::tDECIMAL;
        default:
                      break;
    }

    return ValueType::tINVALID;
}

static std::vector<ValueType> toValueType(const std::vector<TypeAndSize> &tasVec) {
    std::vector<ValueType> valTypes;
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
        tbl = voltdb::TableFactory::getPersistentTable(DATABASE_ID, tableName.c_str(), schema, names, signature);
    }
    else {
        tbl = voltdb::TableFactory::buildTempTable(tableName, schema, names, NULL);
    }

    return tbl;
}

static std::string randomString(int maxLen) {
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

static void fillTable(Table* tbl, int64_t numRows) {
    const TupleSchema* schema = tbl->schema();
    StandAloneTupleStorage storage(schema);
    TableTuple srcTuple = storage.tuple();

    for (int64_t i = 0; i < numRows; ++i) {
        int numCols = schema->columnCount();
        for (int j = 0; j < numCols; ++j) {
            uint32_t length = schema->getColumnInfo(j)->length;
            ValueType vt = schema->columnType(j);
            NValue nval;
            switch (vt) {
                case ValueType::tBIGINT: {
                nval = ValueFactory::getBigIntValue(i * 10000 + j);
                break;
            }
                case ValueType::tVARCHAR:
                nval = ValueFactory::getTempStringValue(randomString(length));
                break;
                case ValueType::tVARBINARY:
                nval = ValueFactory::getTempBinaryValue(randomString(length));
                break;
            default:
                assert(false);
            }

            srcTuple.setNValue(j, nval);
        }

        tbl->insertTuple(srcTuple);
    }
}

} // end namespace eetest

class OptimizedProjectorTest : public Test
{
public:

    void projectFields(Table* srcTable, Table* dstTable,
                       const OptimizedProjector& projector) {

        TableTuple srcTuple(srcTable->schema());
        StandAloneTupleStorage dstStorage(dstTable->schema());
        TableTuple dstTuple = dstStorage.tuple();
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
                std::cout << "Too few rows in dst table\n";
                return false;
            }

            std::vector<AbstractExpression*> exprs = baselineProjector.exprsForTest();
            int dstIdx = 0;
            BOOST_FOREACH(AbstractExpression* expr, exprs) {
                NValue expectedVal = expr->eval(&srcTuple, NULL);
                NValue actualVal = dstTuple.getNValue(dstIdx);

                bool b = expectedVal.op_equals(actualVal).isTrue();
                if (!b) {
                    std::cout << "\nFields failed to compare as equal.  "
                              << "Dst: " << dstIdx << "\n";
                    std::cout << "  " << srcTuple.debug("src") << "\n";
                    std::cout << "  " << dstTuple.debug("dst") << "\n\n";
                    return false;
                }

                ++dstIdx;
            }
        }

        if (dstIterator.next(dstTuple)) {
            std::cout << "Too many rows in dst table\n";
            return false;
        }
        return true;
    }

    std::pair<bool, double> runSteps(const std::string &name,
                                     const std::vector<eetest::TypeAndSize>& dstTableTypes,
                                     Table& srcTbl,
                                     const OptimizedProjector& projector,
                                     const OptimizedProjector& baselineProjector,
                                     double baselineRate) {

        boost::scoped_ptr<voltdb::Table> dstTable(createTableEz(eetest::TEMP, dstTableTypes));
        boost::timer t;

        t.restart();
        projectFields(&srcTbl, dstTable.get(), projector);
        double rowsPerSecond = static_cast<double>(NUM_ROWS) / t.elapsed();
        std::cout << "            Projected " << boost::format("%10.0f") % rowsPerSecond
                  << " rows per second.  (" << name << ")\n";

        // Make sure we get the same answer as normal evaluation.
        bool success = assertProjection(&srcTbl, dstTable.get(), baselineProjector);

        if (baselineRate > 0.0) {
            double percentChange = (rowsPerSecond - baselineRate) / baselineRate * 100;
            std::cout << "              Percent change: " << boost::format("%3.3f%%") % (percentChange) << "\n";
        }

        return std::make_pair(success, rowsPerSecond);
    }

    template <typename T>
    T log2(T n) {
        return static_cast<T>(log(static_cast<double>(n)) / log(2.0));
    }

    void runProjectionTest(const std::vector<eetest::TypeAndSize>& tableTypes,
                           const OptimizedProjector& baselineProjector) {

        TupleSchema* dstSchema = createSchemaEz(tableTypes);

        boost::scoped_ptr<voltdb::Table> srcTable(createTableEz(eetest::PERSISTENT, tableTypes));
        eetest::fillTable(srcTable.get(), NUM_ROWS);

        std::cout << "\n";
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
            permutedProjector.permuteOnIndexBitForTest(numBits, i);

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
                                      tableTypes, *srcTable,
                                      permutedProjector,
                                      permutedProjector, 0.0);
            ASSERT_TRUE_WITH_MESSAGE(successAndRate.first, "Baseline src and dst failed to verify");

            successAndRate = runSteps(prefix + "optimized",
                                      tableTypes, *srcTable,
                                      optimizedProjector,
                                      permutedProjector, successAndRate.second);
            ASSERT_TRUE_WITH_MESSAGE(successAndRate.first, "Memcpy src and dst failed to verify");
        }

        TupleSchema::freeTupleSchema(dstSchema);
        std::cout << "            ";
    }
};

template<class T>
std::vector<T*> toRawPtrVector(const std::vector<boost::shared_ptr<T> > &vec) {
    std::vector<T*> retVec;
    BOOST_FOREACH(const boost::shared_ptr<T> &elem, vec) {
        retVec.push_back(elem.get());
    }

    return retVec;
}

TEST_F(OptimizedProjectorTest, ProjectTupleValueExpressions)
{
    std::vector<eetest::TypeAndSize> bigIntColumns;
    for (int i = 0; i < NUM_COLS; ++i) {
        bigIntColumns.push_back(eetest::BIGINT);
    }

    // Describe a way to move fields from one tuple to another
    std::vector<boost::shared_ptr<TupleValueExpression> > exprs(NUM_COLS);
    for (int i = 0; i < NUM_COLS; ++i) {
        exprs[i].reset(new TupleValueExpression(0, i));
    }

    OptimizedProjector projector;
    BOOST_FOREACH(const boost::shared_ptr<TupleValueExpression> &e, exprs) {
        projector.insertStep(e.get(), e->getColumnId());
    }

    std::cout << "\n\n          " << "BIGINT columns:\n";
    runProjectionTest(bigIntColumns, projector);

    std::vector<eetest::TypeAndSize> varcharColumns;
    for (int i = 0; i < NUM_COLS; ++i) {
        varcharColumns.push_back(eetest::VARCHAR_INLINE);
    }

    std::cout << "\n          " << "VARCHAR columns (inlined):\n";
    runProjectionTest(varcharColumns, projector);

    std::vector<eetest::TypeAndSize> outlinedVarcharColumns;
    for (int i = 0; i < NUM_COLS; ++i) {
        outlinedVarcharColumns.push_back(eetest::VARCHAR_OUTLINE);
    }

    std::cout << "\n          " << "VARCHAR columns (outlined):\n";
    runProjectionTest(outlinedVarcharColumns, projector);
}

TEST_F(OptimizedProjectorTest, ProjectNonTVE)
{
    std::vector<boost::shared_ptr<AbstractExpression> > exprs(NUM_COLS);

    // Create a expr vector for a projection like
    // (for NUM_COLS == 4)
    //
    //   TVE       TVE        ADD   TVE
    //
    for (int i = 0; i < NUM_COLS; ++i) {
        if (i == NUM_COLS / 2) {
            TupleValueExpression* lhs = new TupleValueExpression(0, i - 1);
            TupleValueExpression* rhs = new TupleValueExpression(0, i);
            boost::shared_ptr<AbstractExpression> plus(new OperatorExpression<OpPlus>(EXPRESSION_TYPE_OPERATOR_PLUS, lhs, rhs));
            exprs[i] = plus;
        }
        else {
            boost::shared_ptr<AbstractExpression> tve(new TupleValueExpression(0, i));
            exprs[i] = tve;
        }
    }

    std::vector<eetest::TypeAndSize> types(NUM_COLS, eetest::BIGINT);
    TupleSchema* schema = createSchemaEz(types);

    OptimizedProjector projector(toRawPtrVector(exprs));
    projector.optimize(schema, schema);

    // There should be at most 3 steps. The plus operator in the
    // middle of the tuple will break up the memcpy steps.  The steps
    // in the optimized projection will look like this:
    //
    //   [memcpy 2 fields]    ADD   [memcpy 1 field]

    size_t expectedNumSteps = std::min(::int64_t(3), NUM_COLS);
    ASSERT_EQ(expectedNumSteps, projector.numSteps());

    TupleSchema::freeTupleSchema(schema);
}

TEST_F(OptimizedProjectorTest, ProjectTypeMismatch)
{
    // If destination table has different types than source, a TVE may
    // be an implicit cast.  We shouldn't create a memcpy step for
    // this case---it should be treated like a non-TVE.

    std::vector<eetest::TypeAndSize> colTypes(NUM_COLS, eetest::INTEGER);
    TupleSchema* srcSchema = createSchemaEz(colTypes);
    colTypes[NUM_COLS / 2] = eetest::BIGINT;
    TupleSchema* dstSchema = createSchemaEz(colTypes);

    std::vector<boost::shared_ptr<AbstractExpression> > exprs(NUM_COLS);
    for (int i = 0; i < NUM_COLS; ++i) {
        boost::shared_ptr<AbstractExpression> tve(new TupleValueExpression(0, i));
        exprs[i] = tve;
    }

    OptimizedProjector projector(toRawPtrVector(exprs));
    projector.optimize(dstSchema, srcSchema);

    // Should be at most 3 steps, because implicit cast is a
    // combo-breaker.
    size_t expectedNumSteps = std::min(::int64_t(3), NUM_COLS);
    ASSERT_EQ(expectedNumSteps, projector.numSteps());

    TupleSchema::freeTupleSchema(dstSchema);
    TupleSchema::freeTupleSchema(srcSchema);
}

} // end namespace voltdb


void printUsageAndExit(const std::string& progName) {

    std::cerr << "Usage: " << progName << " [-r <num_rows>] [-c <num_cols>]\n";
    std::cerr << "  Note that <num_cols> must be equal to a power of two "
              << "(for easy permuations).\n";
    exit(1);

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

            // number of column must be a power of two.
            if (std::bitset<64>(val).count() != 1) {
                printUsageAndExit(argv[0]);
            }

            NUM_COLS = static_cast<int64_t>(val);
            break;
        }
        default:
            printUsageAndExit(argv[0]);
        }
    }

    assert (voltdb::ExecutorContext::getExecutorContext() == NULL);

    boost::scoped_ptr<voltdb::Pool> testPool(new voltdb::Pool());
    voltdb::UndoQuantum* wantNoQuantum = NULL;
    voltdb::Topend* topless = NULL;
    boost::scoped_ptr<voltdb::AbstractDRTupleStream> drStream(new voltdb::DRTupleStream(0, 1024));
    boost::scoped_ptr<voltdb::ExecutorContext>
        executorContext(new voltdb::ExecutorContext(0,              // siteId
                                                    0,              // partitionId
                                                    wantNoQuantum,  // undoQuantum
                                                    topless,        // topend
                                                    testPool.get(), // tempStringPool
                                                    NULL,           // engine
                                                    "",             // hostname
                                                    0,              // hostId
                                                    drStream.get(), // drTupleStream
                                                    NULL,           // drReplicatedStream
                                                    0));            // drClusterId

    return TestSuite::globalInstance()->runAll();
}
