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

#include <vector>
#include <map>
#include <string>
#include <istream>
#include <iostream>
#include <fstream>
#include <cassert>
#include <sys/time.h>
#include <boost/foreach.hpp>
#include <boost/scoped_ptr.hpp>
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "common/types.h"
#include "common/valuevector.h"

using namespace std;
using namespace voltdb;

const int kMaxInputLineSize = 2048;
const char *kScriptFileName = "index_script.txt";

const char *kBeginCommand = "begin";
const char *kExecCommand = "exec";
const char *kDoneCommand = "done";

const char *kBigIntTypecode = "bint";
const char *kIntegerTypecode = "int";
const char *kSmallIntTypecode = "sint";
const char *kTinyIntTypecode = "tint";
const char *kFloatTypecode = "float";
const char *kDecimalTypecode = "dec";
const char *kStringTypecode4 = "str4";         // VARCHAR(4)
const char *kStringTypecode128 = "str128";       // VARCHAR(128)

const char *kInsertSuccess = "is";
const char *kInsertFailure = "if";
const char *kLookupSuccess = "ls";
const char *kLookupFailure = "lf";
const char *kDeleteSuccess = "ds";
const char *kDeleteFailure = "df";

const char *kMultiIntsHash = "MultiIntsHash";
const char *kMultiIntsTree = "MultiIntsTree";
const char *kMultiGenericHash = "MultiGenericHash";
const char *kMultiGenericTree = "MultiGenericTree";
const char *kUniqueIntsHash = "UniqueIntsHash";
const char *kUniqueIntsTree = "UniqueIntsTree";
const char *kUniqueGenericHash = "UniqueGenericHash";
const char *kUniqueGenericTree = "UniqueGenericTree";


struct Command {
    const char *op;
    voltdb::TableTuple* key;
    voltdb::TableTuple* key2;
};

vector<voltdb::TableIndex*> currentIndexes;
voltdb::TableIndex *currentIndex;

vector<voltdb::ValueType> currentColumnTypes;
vector<int32_t> currentColumnLengths;
vector<bool> currentColumnAllowNull;
map<string, voltdb::TableTuple*> tuples;
vector<char*> pool;
vector<TupleSchema *> schemaCache;
vector<TableTuple *> tupleCache;
int globalFailures = 0;

vector<Command> currentCommands;
int line = 0;

bool commandIS(voltdb::TableTuple &key)
{
    //cout << "running is" << endl;
    //cout << " candidate key : " << key.tupleLength() << " - " << key.debug("") << endl;
    TableTuple conflict(key.getSchema());
    currentIndex->addEntry(&key, &conflict);
    return conflict.isNullTuple();
}

bool commandIF(voltdb::TableTuple &key)
{
    //cout << "running if" << endl;
    //cout << " candidate key : " << key.tupleLength() << " - " << key.debug("") << endl;
    return !commandIS(key);
}

bool commandLS(voltdb::TableTuple &key)
{
    //cout << "running ls" << endl;
    //cout << " candidate key : " << key.tupleLength() << " - " << key.debug("") << endl;
    IndexCursor indexCursor(currentIndex->getTupleSchema());

    bool result = currentIndex->moveToKey(&key, indexCursor);
    if (!result) {
        cout << "ls FAIL(moveToKey()) key length: " << key.tupleLength() << endl << key.debug("") << endl;
        return false;
    }
    voltdb::TableTuple value = currentIndex->nextValueAtKey(indexCursor);
    if (value.isNullTuple()) {
        cout << "ls FAIL(isNullTuple()) key length: " << key.tupleLength() << endl << key.debug("") << endl;
        return false;
    }
    if (!value.equals(key)) {
        cout << "ls FAIL(!equals()) key length: " << key.tupleLength() << key.debug("")
            << endl << " value length: " << value.tupleLength() << endl << value.debug("") << endl;
        return false;
    }
    return true;
}

bool commandLF(voltdb::TableTuple &key)
{
    //cout << "running lf" << endl;
    //cout << " candidate key : " << key.tupleLength() << " - " << key.debug("") << endl;

    // Don't just call !commandLS(key) here. That does an equality check.
    // Here, the valid test is for existence, not equality.
    IndexCursor indexCursor(currentIndex->getTupleSchema());
    return !(currentIndex->moveToKey(&key, indexCursor));
}

bool commandDS(voltdb::TableTuple &key)
{
    //cout << "running ds" << endl;
    //cout << " candidate key : " << key.tupleLength() << " - " << key.debug("") << endl;
    return currentIndex->deleteEntry(&key);
}

bool commandDF(voltdb::TableTuple &key)
{
    //cout << "running df" << endl;
    //cout << " candidate key : " << key.tupleLength() << " - " << key.debug("") << endl;
    return !currentIndex->deleteEntry(&key);
}

void cleanUp()
{
    assert(currentIndexes.size() == 0);

    currentColumnTypes.clear();
    currentColumnLengths.clear();
    currentColumnAllowNull.clear();
    currentCommands.clear();

    for (int i = 0; i < tupleCache.size(); ++i) {
        tupleCache[i]->freeObjectColumns();
        delete tupleCache[i];
    }
    tupleCache.clear();

    for (int i = 0; i < pool.size(); ++i)
        delete[] pool[i];
    pool.clear();

    for (int i = 0; i < schemaCache.size(); ++i)
        TupleSchema::freeTupleSchema(schemaCache[i]);
    schemaCache.clear();

    tuples.clear();
}

void setNewCurrent(const char *testName,
                   vector<const char*> indexNames,
                   vector<voltdb::ValueType> columnTypes,
                   vector<int32_t> columnLengths,
                   vector<bool> columnAllowNull)
{
    cleanUp();

    currentColumnTypes = columnTypes;
    currentColumnLengths = columnLengths;
    currentColumnAllowNull = columnAllowNull;

    voltdb::TupleSchema *schema = voltdb::TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);
    schemaCache.push_back(schema);
    // just pack the indices tightly
    vector<int> columnIndices;
    for (int i = 0; i < (int)columnTypes.size(); i++) {
        columnIndices.push_back(i);
    }

    BOOST_FOREACH(const char* indexName, indexNames) {
        voltdb::TableIndex *index;

        if (strcmp(indexName, kMultiIntsHash) == 0) {
            voltdb::TableIndexScheme scheme(indexName, voltdb::HASH_TABLE_INDEX,
                                            columnIndices, TableIndex::simplyIndexColumns(),
                                            false, false, false, schema);
            index = voltdb::TableIndexFactory::getInstance(scheme);
        }
        else if (strcmp(indexName, kMultiIntsTree) == 0) {
            voltdb::TableIndexScheme scheme(indexName, voltdb::BALANCED_TREE_INDEX,
                                            columnIndices, TableIndex::simplyIndexColumns(),
                                            false, true, false, schema);
            index = voltdb::TableIndexFactory::getInstance(scheme);
        }
        else if (strcmp(indexName, kMultiGenericHash) == 0) {
            voltdb::TableIndexScheme scheme(indexName, voltdb::HASH_TABLE_INDEX,
                                            columnIndices, TableIndex::simplyIndexColumns(),
                                            false, false, false, schema);
            index = voltdb::TableIndexFactory::getInstance(scheme);
        }
        else if (strcmp(indexName, kMultiGenericTree) == 0) {
            voltdb::TableIndexScheme scheme(indexName, voltdb::BALANCED_TREE_INDEX,
                                            columnIndices, TableIndex::simplyIndexColumns(),
                                            false, true, false, schema);
            index = voltdb::TableIndexFactory::getInstance(scheme);
        }
        else if (strcmp(indexName, kUniqueIntsHash) == 0) {
            voltdb::TableIndexScheme scheme(indexName, voltdb::HASH_TABLE_INDEX,
                                            columnIndices, TableIndex::simplyIndexColumns(),
                                            true, false, false, schema);
            index = voltdb::TableIndexFactory::getInstance(scheme);
        }
        else if (strcmp(indexName, kUniqueIntsTree) == 0) {
            voltdb::TableIndexScheme scheme(indexName, voltdb::BALANCED_TREE_INDEX,
                                            columnIndices, TableIndex::simplyIndexColumns(),
                                            true, true, false, schema);
            index = voltdb::TableIndexFactory::getInstance(scheme);
        }
        else if (strcmp(indexName, kUniqueGenericHash) == 0) {
            voltdb::TableIndexScheme scheme(indexName, voltdb::HASH_TABLE_INDEX,
                                            columnIndices, TableIndex::simplyIndexColumns(),
                                            true, false, false, schema);
            index = voltdb::TableIndexFactory::getInstance(scheme);
        }
        else if (strcmp(indexName, kUniqueGenericTree) == 0) {
            voltdb::TableIndexScheme scheme(indexName, voltdb::BALANCED_TREE_INDEX,
                                            columnIndices, TableIndex::simplyIndexColumns(),
                                            true, true, false, schema);
            index = voltdb::TableIndexFactory::getInstance(scheme);
        }
        else {
            cerr << "Unable to load index named: " << indexName << " on line: " << line << endl;
            exit(-1);
        }
        currentIndexes.push_back(index);
    }
}

void runTest()
{
    timeval tStart, tEnd;

    while (currentIndexes.size() > 0) {

        int successes = 0;
        int failures = 0;

        currentIndex = currentIndexes.back();
        currentIndexes.pop_back();

        gettimeofday(&tStart, NULL);

        size_t commandCount = currentCommands.size();
        for (size_t i = 0; i < commandCount; ++i) {
            Command &command = currentCommands[i];
            bool result;
            if (command.op == kInsertSuccess)
                result = commandIS(*command.key);
            else if (command.op == kInsertFailure)
                result = commandIF(*command.key);
            else if (command.op == kLookupSuccess)
                result = commandLS(*command.key);
            else if (command.op == kLookupFailure)
                result = commandLF(*command.key);
            else if (command.op == kDeleteSuccess)
                result = commandDS(*command.key);
            else if (command.op == kDeleteFailure)
                result = commandDF(*command.key);
            else {
                cerr << "Unexpected command when running test." << endl;
                exit(-1);
            }
            if (result) ++successes;
            else {
                cout << "(" << successes << "/" << failures << ") new FAILURE: " << command.op << endl;
                ++failures;
            }
        }

        gettimeofday(&tEnd, NULL);

        int64_t us = (tEnd.tv_sec - tStart.tv_sec) * 1000000;
        us += tEnd.tv_usec - tStart.tv_usec;

        cout << "successes/failures: " << successes << "/" << failures;
        cout << " in " << us << "us";
        cout << " on " << currentIndex->getName() << "/" << currentIndex->getTypeName() << endl;
        globalFailures += failures;

        delete currentIndex;
        currentIndex = NULL;
    }

    cleanUp();
}

voltdb::TableTuple *tupleFromString(char *tupleStr, voltdb::TupleSchema *tupleSchema) {
    string key(tupleStr);

    map<string, voltdb::TableTuple*>::iterator iter;
    iter = tuples.find(key);
    if (iter != tuples.end())
        return iter->second;

    voltdb::TableTuple *tuple = new TableTuple(tupleSchema);
    tupleCache.push_back(tuple);
    char *data = new char[tuple->tupleLength()];
    pool.push_back(data);
    memset(data, 0, tuple->tupleLength());
    tuple->move(data);
    tuples[key] = tuple;

    char *value = strtok(tupleStr, ",");
    for (int i = 0; i < currentColumnTypes.size(); i++) {
        voltdb::ValueType type = currentColumnTypes[i];
        int64_t bi_value;
        double d_value;
        switch (type) {
            case voltdb::ValueType::tTINYINT:
                bi_value = static_cast<int64_t>(atoll(value));
                tuple->setNValue(i, ValueFactory::getTinyIntValue(static_cast<int8_t>(bi_value)));
                break;
            case voltdb::ValueType::tSMALLINT:
                bi_value = static_cast<int64_t>(atoll(value));
                tuple->setNValue(i, ValueFactory::getSmallIntValue(static_cast<int16_t>(bi_value)));
                break;
            case voltdb::ValueType::tINTEGER:
                bi_value = static_cast<int64_t>(atoll(value));
                tuple->setNValue(i, ValueFactory::getIntegerValue(static_cast<int32_t>(bi_value)));
                break;
            case voltdb::ValueType::tBIGINT:
                bi_value = static_cast<int64_t>(atoll(value));
                tuple->setNValue(i, ValueFactory::getBigIntValue(bi_value));
                break;
            case voltdb::ValueType::tDOUBLE:
                d_value = atof(value);
                tuple->setNValue(i, ValueFactory::getDoubleValue(d_value));
                break;
            case voltdb::ValueType::tDECIMAL:
                tuple->setNValue(i, ValueFactory::getDecimalValueFromString(value));
                break;
            case voltdb::ValueType::tVARCHAR: {
                NValue nv = ValueFactory::getStringValue(value);
                tuple->setNValueAllocateForObjectCopies(i, nv);
                nv.free();
                break;
            }
            default:
                cerr << "Type not supported. Error from line: " << line << endl;
                exit(-1);
                break;
        }
        value = strtok(NULL, ",");
    }

    return tuple;
}

int main(int argc, char **argv)
{
    // default input is stdin
    istream *input = &cin;

    // alternate input
    if (argc > 1) {
        input = new ifstream(argv[1]);
        if (!input->good()) {
            cerr << "Couln't open file specified." << endl;
            exit(-1);
        }
    }
    char buf[kMaxInputLineSize];

    bool done = false;
    while (!done) {
        input->getline(buf, kMaxInputLineSize);
        if (buf[0] == '#') continue;

        char *command = strtok(buf, " ");
        if (!command)
            continue;

        // parse the begin command
        if (strcmp(command, kBeginCommand) == 0) {
            char *testName = strtok(NULL, " ");
            char *indexNames = strtok(NULL, " ");
            char *schema = strtok(NULL, " ");

            // read all the types
            vector<voltdb::ValueType> columnTypes;
            vector<int32_t> columnLengths;
            vector<bool> columnAllowNull;

            char *typecode = strtok(schema, ",");
            while (typecode) {
                if (strcmp(typecode, kBigIntTypecode) == 0) {
                    columnTypes.push_back(ValueType::tBIGINT);
                    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tBIGINT));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kIntegerTypecode) == 0) {
                    columnTypes.push_back(ValueType::tINTEGER);
                    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tINTEGER));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kSmallIntTypecode) == 0) {
                    columnTypes.push_back(ValueType::tSMALLINT);
                    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tSMALLINT));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kTinyIntTypecode) == 0) {
                    columnTypes.push_back(ValueType::tTINYINT);
                    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tTINYINT));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kFloatTypecode) == 0) {
                    columnTypes.push_back(ValueType::tDOUBLE);
                    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tDOUBLE));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kDecimalTypecode) == 0) {
                    columnTypes.push_back(ValueType::tDECIMAL);
                    columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tDECIMAL));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kStringTypecode4) == 0) {
                    columnTypes.push_back(ValueType::tVARCHAR);
                    columnLengths.push_back(4);
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kStringTypecode128) == 0) {
                    columnTypes.push_back(ValueType::tVARCHAR);
                    columnLengths.push_back(128);
                    columnAllowNull.push_back(false);
                }
                else {
                    cerr << "Typecode parse error on line: " << line << endl;
                    exit(-1);
                }

                typecode = strtok(NULL, ",");
            }

            // now go back and get the index names (out of order due to strtok)
            vector<const char*> indexNameVec;
            //cout << "Full set of indexnames is: " << indexNames << endl;
            char *indexName = strtok(indexNames, ",");
            //cout << "Adding index of type: " << indexName << endl;
            indexNameVec.push_back(indexName);
            while ((indexName = strtok(NULL, ","))) {
                //cout << "Adding index of type: " << indexName << endl;
                indexNameVec.push_back(indexName);
            }

            // initialize the index
            setNewCurrent(testName, indexNameVec, columnTypes, columnLengths, columnAllowNull);
        }

        // parse the exec command and run the loaded test
        else if (strcmp(command, kExecCommand) == 0) {
            runTest();
            // run until an error occurs. if this is undesired, maybe
            // introduce an exec-continue command that runs past errors?
            if (globalFailures > 0) {
                done = true;
            }
        }

        // parse the done command and quit the test
        else if (strcmp(command, kDoneCommand) == 0) {
            done = true;
        }

        // read a regular command
        else {
            char *tuple1 = strtok(NULL, " ");
            char *tuple2 = strtok(NULL, " ");

            // read the command opcode
            Command cmd;
            if (strcmp(command, kInsertSuccess) == 0) cmd.op = kInsertSuccess;
            else if (strcmp(command, kInsertFailure) == 0) cmd.op = kInsertFailure;
            else if (strcmp(command, kLookupSuccess) == 0) cmd.op = kLookupSuccess;
            else if (strcmp(command, kLookupFailure) == 0) cmd.op = kLookupFailure;
            else if (strcmp(command, kDeleteSuccess) == 0) cmd.op = kDeleteSuccess;
            else if (strcmp(command, kDeleteFailure) == 0) cmd.op = kDeleteFailure;
            else {
                cerr << "Operation code parse error on line: " << line << endl;
                exit(-1);
            }
            voltdb::TupleSchema *tupleSchema = voltdb::TupleSchema::createTupleSchemaForTest(currentColumnTypes,
                                                                                      currentColumnLengths,
                                                                                      currentColumnAllowNull);
            schemaCache.push_back(tupleSchema);
            cmd.key = tupleFromString(tuple1, tupleSchema);
            if (tuple2) cmd.key2 = tupleFromString(tuple2, tupleSchema);
            currentCommands.push_back(cmd);
        }

        line++;
    }
    if (input != &cin) delete input;
    return globalFailures;
}
