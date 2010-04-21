/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
#include <string>
#include <istream>
#include <iostream>
#include <fstream>
#include <cassert>
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
const char *kStringTypecode = "str";

const char *kInsertSuccess = "is";
const char *kInsertFailure = "if";
const char *kLookupSuccess = "ls";
const char *kLookupFailure = "lf";

const char *kIntsMultiMapIndex = "IntsMultiMapIndex";
const char *kGenericMultiMapIndex = "GenericMultiMapIndex";

struct Command {
    const char *op;
    voltdb::TableTuple key;
};

vector<voltdb::ValueType> currentColumnTypes;
vector<int32_t> currentColumnLengths;
vector<bool> currentColumnAllowNull;

voltdb::TableIndex *currentIndex = NULL;
vector<Command> currentCommands;
int line = 0;

bool
commandIS(voltdb::TableTuple &key)
{
    cout << "running is" << endl;
    cout << "candidate key : " << key.tupleLength() << " - " << key.debug("") << endl;
    return currentIndex->addEntry(&key);
}

bool
commandIF(voltdb::TableTuple &key)
{
    cout << "running if" << endl;
    return !commandIS(key);
}

bool
commandLS(voltdb::TableTuple &key)
{
    cout << "running ls" << endl;
    cout << "candidate key : " << key.tupleLength() << " - " << key.debug("") << endl;
    bool result = currentIndex->moveToKey(&key);
    if (!result) return false;
    voltdb::TableTuple value = currentIndex->nextValueAtKey();
    if (value.isNullTuple()) return false;
    if (!value.equals(key)) return false;
    return true;
}

bool
commandLF(voltdb::TableTuple &key)
{
    cout << "running lf" << endl;
    return !commandLS(key);
}

void
cleanUp()
{
    if (currentIndex) delete currentIndex;
    currentIndex = NULL;
    currentColumnTypes.clear();
    currentColumnLengths.clear();
    currentColumnAllowNull.clear();
    currentCommands.clear();
}

void
setNewCurrent(const char *testName, vector<const char*> indexNames, vector<voltdb::ValueType> columnTypes, vector<int32_t> columnLengths, vector<bool> columnAllowNull)
{
    cleanUp();

    currentColumnTypes = columnTypes;
    currentColumnLengths = columnLengths;
    currentColumnAllowNull = columnAllowNull;

    voltdb::TupleSchema *schema = voltdb::TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, true);
    // just pack the indices tightly
    vector<int> columnIndices;
    for (int i = 0; i < (int)columnTypes.size(); i++) {
        columnIndices.push_back(i);
    }
    const char *indexName = indexNames[0];

    if (strcmp(indexName, kIntsMultiMapIndex) == 0) {
        voltdb::TableIndexScheme scheme(kIntsMultiMapIndex, voltdb::BALANCED_TREE_INDEX, columnIndices, columnTypes, false, true, schema);
        currentIndex = voltdb::TableIndexFactory::getInstance(scheme);
        cout << "Created kIntsMultiMapIndex" << endl;
    }
    else if (strcmp(indexName, kGenericMultiMapIndex) == 0) {
        voltdb::TableIndexScheme scheme(kGenericMultiMapIndex, voltdb::BALANCED_TREE_INDEX, columnIndices, columnTypes, false, false, schema);
        currentIndex = voltdb::TableIndexFactory::getInstance(scheme);
        cout << "Created kGenericMultiMapIndex" << endl;
    }
    else {
        cerr << "Unable to load index named: " << indexName << " on line: " << line << endl;
        exit(-1);
    }
}

void
runTest()
{
    int successes = 0;
    int failures = 0;

    size_t commandCount = currentCommands.size();
    for (size_t i = 0; i < commandCount; ++i) {
        Command &command = currentCommands[i];
        bool result;
        if (command.op == kInsertSuccess)
            result = commandIS(command.key);
        else if (command.op == kInsertFailure)
            result = commandIF(command.key);
        else if (command.op == kLookupSuccess)
            result = commandLS(command.key);
        else if (command.op == kLookupFailure)
            result = commandLS(command.key);
        else {
            cerr << "Unexpected command when running test." << endl;
            exit(-1);
        }
        if (result) ++successes;
        else ++failures;
    }

    cout << "successes/failures: " << successes << "/" << failures << endl;
    cleanUp();
}

int
main(int argc, char **argv)
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

            // read all the types
            vector<voltdb::ValueType> columnTypes;
            vector<int32_t> columnLengths;
            vector<bool> columnAllowNull;
            while (char *typecode = strtok(NULL, " ")) {
                if (strcmp(typecode, kBigIntTypecode) == 0) {
                    columnTypes.push_back(VALUE_TYPE_BIGINT);
                    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kIntegerTypecode) == 0) {
                    columnTypes.push_back(VALUE_TYPE_INTEGER);
                    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kSmallIntTypecode) == 0) {
                    columnTypes.push_back(VALUE_TYPE_SMALLINT);
                    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_SMALLINT));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kTinyIntTypecode) == 0) {
                    columnTypes.push_back(VALUE_TYPE_TINYINT);
                    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kFloatTypecode) == 0) {
                    columnTypes.push_back(VALUE_TYPE_DOUBLE);
                    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kDecimalTypecode) == 0) {
                    columnTypes.push_back(VALUE_TYPE_DECIMAL);
                    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DECIMAL));
                    columnAllowNull.push_back(false);
                }
                else if (strcmp(typecode, kStringTypecode) == 0) {
                    columnTypes.push_back(VALUE_TYPE_VARCHAR);
                    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_VARCHAR));
                    columnAllowNull.push_back(false);
                }
                else {
                    cerr << "Typecode parse error on line: " << line << endl;
                    exit(-1);
                }
            }

            // now go back and get the index names (out of order due to strtok)
            vector<const char*> indexNameVec;
            char *indexName = strtok(indexNames, "/ ");
            indexNameVec.push_back(indexName);
            while (indexName == strtok(NULL, "/ "))
                 indexNameVec.push_back(indexName);

            // initialize the index
            setNewCurrent(testName, indexNameVec, columnTypes, columnLengths, columnAllowNull);
        }

        // parse the exec command and run the loaded test
        else if (strcmp(command, kExecCommand) == 0) {
            runTest();
        }

        // parse the done command and quit the test
        else if (strcmp(command, kDoneCommand) == 0) {
            done = true;
        }

        // read a regular command
        else {
            // read the command opcode
            Command cmd;
            if (strcmp(command, kInsertSuccess) == 0) cmd.op = kInsertSuccess;
            else if (strcmp(command, kInsertFailure) == 0) cmd.op = kInsertFailure;
            else if (strcmp(command, kLookupSuccess) == 0) cmd.op = kLookupSuccess;
            else if (strcmp(command, kLookupFailure) == 0) cmd.op = kLookupFailure;
            else {
                cerr << "Operation code parse error on line: " << line << endl;
                exit(-1);
            }
            voltdb::TupleSchema *tupleSchema = voltdb::TupleSchema::createTupleSchema(currentColumnTypes, currentColumnLengths, currentColumnAllowNull, true);
            cmd.key = voltdb::TableTuple(tupleSchema);
            cmd.key.move(new char[cmd.key.tupleLength()]);

            for (int i = 0; i < currentColumnTypes.size(); i++) {
                voltdb::ValueType type = currentColumnTypes[i];
                char *value = strtok(NULL, " ");
                int64_t bi_value;
                switch (type) {
                    case voltdb::VALUE_TYPE_BIGINT:
                        bi_value = atol(value);
                        cmd.key.setNValue(i, ValueFactory::getBigIntValue(bi_value));
                        break;
                    case voltdb::VALUE_TYPE_VARCHAR: {
                        cmd.key.setNValue(i, ValueFactory::getStringValue(value));
                        break;
                    }
                    default:
                        cerr << "Type not supported. Error from line: " << line << endl;
                        exit(-1);
                        break;
                }
            }
            currentCommands.push_back(cmd);
        }

        line++;
    }

    return 0;
}
