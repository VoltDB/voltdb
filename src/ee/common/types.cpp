/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include "types.h"
#include "common/debuglog.h"
#include "common/ValueFactory.hpp"
#include "common/FatalException.hpp"
#include <string>

namespace voltdb {
using namespace std;

/** Testing utility */
bool isNumeric(ValueType type) {
    switch (type) {
      case (VALUE_TYPE_TINYINT):
      case (VALUE_TYPE_SMALLINT):
      case (VALUE_TYPE_INTEGER):
      case (VALUE_TYPE_BIGINT):
      case (VALUE_TYPE_DOUBLE):
        return true;
      break;
      case (VALUE_TYPE_VARCHAR):
      case (VALUE_TYPE_VARBINARY):
      case (VALUE_TYPE_TIMESTAMP):
      case (VALUE_TYPE_NULL):
      case (VALUE_TYPE_DECIMAL):   // test assumes castAsBigInt() makes sense if isNumeric()
      case (VALUE_TYPE_INVALID):
      case (VALUE_TYPE_ARRAY):
        return false;
      default:
          throw exception();
    }
    throw exception();
}

/** Used in index optimization **/
bool isIntegralType(ValueType type) {
    switch (type) {
      case (VALUE_TYPE_TINYINT):
      case (VALUE_TYPE_SMALLINT):
      case (VALUE_TYPE_INTEGER):
      case (VALUE_TYPE_BIGINT):
        return true;
      break;
      case (VALUE_TYPE_DOUBLE):
      case (VALUE_TYPE_VARCHAR):
      case (VALUE_TYPE_VARBINARY):
      case (VALUE_TYPE_TIMESTAMP):
      case (VALUE_TYPE_NULL):
      case (VALUE_TYPE_DECIMAL):
      case (VALUE_TYPE_ARRAY):
        return false;
      default:
          throw exception();
    }
    throw exception();
}

NValue getRandomValue(ValueType type) {
    switch (type) {
        case VALUE_TYPE_TIMESTAMP:
            return ValueFactory::getTimestampValue(static_cast<int64_t>(time(NULL)));
        case VALUE_TYPE_TINYINT:
            return ValueFactory::getTinyIntValue(static_cast<int8_t>(rand() % 128));
        case VALUE_TYPE_SMALLINT:
            return ValueFactory::getSmallIntValue(static_cast<int16_t>(rand() % 32768));
        case VALUE_TYPE_INTEGER:
            return ValueFactory::getIntegerValue(rand() % (1 << 31));
        case VALUE_TYPE_BIGINT:
            return ValueFactory::getBigIntValue(rand());
        case VALUE_TYPE_DOUBLE:
            return ValueFactory::getDoubleValue((rand() % 10000) / (double)(rand() % 10000));
        case VALUE_TYPE_VARCHAR: {
            int length = (rand() % 10);
            char characters[11];
            for (int ii = 0; ii < length; ii++) {
                characters[ii] = (char)(32 + (rand() % 94)); //printable characters
            }
            characters[length] = '\0';
            //printf("Characters are \"%s\"\n", characters);
            return ValueFactory::getStringValue(string(characters));
        }
        case VALUE_TYPE_VARBINARY: {
            int length = (rand() % 16);
            unsigned char bytes[17];
            for (int ii = 0; ii < length; ii++) {
                bytes[ii] = (unsigned char)rand() % 256; //printable characters
            }
            bytes[length] = '\0';
            //printf("Characters are \"%s\"\n", characters);
            return ValueFactory::getBinaryValue(bytes, length);
        }
            break;
        case VALUE_TYPE_ARRAY:
        default: {
            throwFatalException("Attempted to get a random value of unsupported value type %d", type);
        }
    }
    throw exception();
}

string getTypeName(ValueType type) {
    string ret;
    switch (type) {
        case (VALUE_TYPE_TINYINT):
            ret = "tinyint";
            break;
        case (VALUE_TYPE_SMALLINT):
            ret = "smallint";
            break;
        case (VALUE_TYPE_INTEGER):
            ret = "integer";
            break;
        case (VALUE_TYPE_BIGINT):
            ret = "bigint";
            break;
        case (VALUE_TYPE_DOUBLE):
            ret = "double";
            break;
        case (VALUE_TYPE_VARCHAR):
            ret = "varchar";
            break;
        case (VALUE_TYPE_VARBINARY):
            ret = "varbinary";
            break;
        case (VALUE_TYPE_TIMESTAMP):
            ret = "timestamp";
            break;
        case (VALUE_TYPE_DECIMAL):
            ret = "decimal";
            break;
        case (VALUE_TYPE_BOOLEAN):
            ret = "boolean";
            break;
        case (VALUE_TYPE_ADDRESS):
            ret = "address";
            break;
        case (VALUE_TYPE_INVALID):
            ret = "INVALID";
            break;
        case (VALUE_TYPE_NULL):
            ret = "NULL";
            break;
        case (VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC):
            ret = "numeric";
            break;
        case (VALUE_TYPE_ARRAY):
            ret = "array";
            break;
        default: {
            char buffer[32];
            snprintf(buffer, 32, "UNKNOWN[%d]", type);
            ret = buffer;
        }
    }
    return (ret);
}

std::string tableStreamTypeToString(TableStreamType type) {
    switch (type) {
      case TABLE_STREAM_SNAPSHOT: {
          return "TABLE_STREAM_SNAPSHOT";
      }
      case TABLE_STREAM_ELASTIC_INDEX: {
          return "TABLE_STREAM_ELASTIC_INDEX";
      }
      case TABLE_STREAM_ELASTIC_INDEX_READ: {
          return "TABLE_STREAM_ELASTIC_INDEX_READ";
      }
      case TABLE_STREAM_ELASTIC_INDEX_CLEAR: {
          return "TABLE_STREAM_ELASTIC_INDEX_CLEAR";
      }
      case TABLE_STREAM_RECOVERY: {
          return "TABLE_STREAM_RECOVERY";
      }
      case TABLE_STREAM_NONE: {
          return "TABLE_STREAM_NONE";
      }
      default:
          return "INVALID";
    }
}


string valueToString(ValueType type)
{
    switch (type) {
      case VALUE_TYPE_INVALID: {
          return "INVALID";
      }
      case VALUE_TYPE_NULL: {
          return "NULL";
      }
      case VALUE_TYPE_TINYINT: {
          return "TINYINT";
      }
      case VALUE_TYPE_SMALLINT: {
          return "SMALLINT";
          break;
      }
      case VALUE_TYPE_INTEGER: {
          return "INTEGER";
      }
      case VALUE_TYPE_BIGINT: {
          return "BIGINT";
      }
      case VALUE_TYPE_DOUBLE: {
          return "FLOAT";
      }
      case VALUE_TYPE_VARCHAR: {
          return "VARCHAR";
      }
      case VALUE_TYPE_VARBINARY: {
          return "VARBINARY";
      }
      case VALUE_TYPE_TIMESTAMP: {
          return "TIMESTAMP";
      }
      case VALUE_TYPE_DECIMAL: {
          return "DECIMAL";
      }
      case VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC: {
          return "NUMERIC";
      }
      case VALUE_TYPE_ARRAY: {
          return "ARRAY";
      }
      default:
          return "INVALID";
    }
}

ValueType stringToValue(string str )
{
    if (str == "INVALID") {
        return VALUE_TYPE_INVALID;
    } else if (str == "NULL") {
        return VALUE_TYPE_NULL;
    } else if (str == "TINYINT") {
        return VALUE_TYPE_TINYINT;
    } else if (str == "SMALLINT") {
        return VALUE_TYPE_SMALLINT;
    } else if (str == "INTEGER") {
        return VALUE_TYPE_INTEGER;
    } else if (str == "BIGINT") {
        return VALUE_TYPE_BIGINT;
    } else if (str == "FLOAT") {
        return VALUE_TYPE_DOUBLE;
    } else if (str == "STRING") {
        return VALUE_TYPE_VARCHAR;
    } else if (str == "VARBINARY") {
        return VALUE_TYPE_VARBINARY;
    } else if (str == "TIMESTAMP") {
        return VALUE_TYPE_TIMESTAMP;
    } else if (str == "DECIMAL") {
        return VALUE_TYPE_DECIMAL;
    } else if (str == "ARRAY") {
        return VALUE_TYPE_ARRAY;
    }
    else {
        throwFatalException( "No conversion from string %s.", str.c_str());
    }
    return VALUE_TYPE_INVALID;
}

string joinToString(JoinType type)
{
    switch (type) {
    case JOIN_TYPE_INVALID: {
        return "INVALID";
    }
    case JOIN_TYPE_INNER: {
        return "INNER";
    }
    case JOIN_TYPE_LEFT: {
        return "LEFT";
    }
    case JOIN_TYPE_FULL: {
        return "FULL";
    }
    case JOIN_TYPE_RIGHT: {
        return "RIGHT";
    }
    }
    return "INVALID";
}

JoinType stringToJoin(string str )
{
    if (str == "INVALID") {
        return JOIN_TYPE_INVALID;
    } else if (str == "INNER") {
        return JOIN_TYPE_INNER;
    } else if (str == "LEFT") {
        return JOIN_TYPE_LEFT;
    } else if (str == "FULL") {
        return JOIN_TYPE_FULL;
    } else if (str == "RIGHT") {
        return JOIN_TYPE_RIGHT;
    }
    return JOIN_TYPE_INVALID;
}

string sortDirectionToString(SortDirectionType type)
{
    switch (type) {
    case SORT_DIRECTION_TYPE_INVALID: {
        return "INVALID";
    }
    case SORT_DIRECTION_TYPE_ASC: {
        return "ASC";
    }
    case SORT_DIRECTION_TYPE_DESC: {
        return "DESC";
    }
    }
    return "INVALID";
}

SortDirectionType stringToSortDirection(string str )
{
    if (str == "INVALID") {
        return SORT_DIRECTION_TYPE_INVALID;
    } else if (str == "ASC") {
        return SORT_DIRECTION_TYPE_ASC;
    } else if (str == "DESC") {
        return SORT_DIRECTION_TYPE_DESC;
    }
    return SORT_DIRECTION_TYPE_INVALID;
}

string planNodeToString(PlanNodeType type)
{
    switch (type) {
    case PLAN_NODE_TYPE_INVALID: {
        return "INVALID";
    }
    case PLAN_NODE_TYPE_SEQSCAN: {
        return "SEQSCAN";
    }
    case PLAN_NODE_TYPE_INDEXSCAN: {
        return "INDEXSCAN";
    }
    case PLAN_NODE_TYPE_INDEXCOUNT: {
        return "INDEXCOUNT";
    }
    case PLAN_NODE_TYPE_TABLECOUNT: {
        return "TABLECOUNT";
    }
    case PLAN_NODE_TYPE_NESTLOOP: {
        return "NESTLOOP";
    }
    case PLAN_NODE_TYPE_NESTLOOPINDEX: {
        return "NESTLOOPINDEX";
    }
    case PLAN_NODE_TYPE_UPDATE: {
        return "UPDATE";
    }
    case PLAN_NODE_TYPE_INSERT: {
        return "INSERT";
    }
    case PLAN_NODE_TYPE_DELETE: {
        return "DELETE";
    }
    case PLAN_NODE_TYPE_SEND: {
        return "SEND";
    }
    case PLAN_NODE_TYPE_RECEIVE: {
        return "RECEIVE";
    }
    case PLAN_NODE_TYPE_AGGREGATE: {
        return "AGGREGATE";
    }
    case PLAN_NODE_TYPE_HASHAGGREGATE: {
        return "HASHAGGREGATE";
    }
    case PLAN_NODE_TYPE_PARTIALAGGREGATE: {
        return "PARTIALAGGREGATE";
    }
    case PLAN_NODE_TYPE_UNION: {
        return "UNION";
    }
    case PLAN_NODE_TYPE_ORDERBY: {
        return "ORDERBY";
    }
    case PLAN_NODE_TYPE_PROJECTION: {
        return "PROJECTION";
    }
    case PLAN_NODE_TYPE_MATERIALIZE: {
        return "MATERIALIZE";
    }
    case PLAN_NODE_TYPE_LIMIT: {
        return "LIMIT";
    }
    case PLAN_NODE_TYPE_MATERIALIZEDSCAN: {
        return "MATERIALIZEDSCAN";
    }
    }
    return "UNDEFINED";
}

PlanNodeType stringToPlanNode(string str )
{
    if (str == "INVALID") {
        return PLAN_NODE_TYPE_INVALID;
    } else if (str == "SEQSCAN") {
        return PLAN_NODE_TYPE_SEQSCAN;
    } else if (str == "INDEXSCAN") {
        return PLAN_NODE_TYPE_INDEXSCAN;
    } else if (str == "INDEXCOUNT") {
        return PLAN_NODE_TYPE_INDEXCOUNT;
    } else if (str == "TABLECOUNT") {
        return PLAN_NODE_TYPE_TABLECOUNT;
    } else if (str == "NESTLOOP") {
        return PLAN_NODE_TYPE_NESTLOOP;
    } else if (str == "NESTLOOPINDEX") {
        return PLAN_NODE_TYPE_NESTLOOPINDEX;
    } else if (str == "UPDATE") {
        return PLAN_NODE_TYPE_UPDATE;
    } else if (str == "INSERT") {
        return PLAN_NODE_TYPE_INSERT;
    } else if (str == "DELETE") {
        return PLAN_NODE_TYPE_DELETE;
    } else if (str == "SEND") {
        return PLAN_NODE_TYPE_SEND;
    } else if (str == "RECEIVE") {
        return PLAN_NODE_TYPE_RECEIVE;
    } else if (str == "AGGREGATE") {
        return PLAN_NODE_TYPE_AGGREGATE;
    } else if (str == "HASHAGGREGATE") {
        return PLAN_NODE_TYPE_HASHAGGREGATE;
    } else if (str == "PARTIALAGGREGATE") {
        return PLAN_NODE_TYPE_PARTIALAGGREGATE;
    } else if (str == "UNION") {
        return PLAN_NODE_TYPE_UNION;
    } else if (str == "ORDERBY") {
        return PLAN_NODE_TYPE_ORDERBY;
    } else if (str == "PROJECTION") {
        return PLAN_NODE_TYPE_PROJECTION;
    } else if (str == "MATERIALIZE") {
        return PLAN_NODE_TYPE_MATERIALIZE;
    } else if (str == "LIMIT") {
        return PLAN_NODE_TYPE_LIMIT;
    } else if (str == "MATERIALIZEDSCAN") {
        return PLAN_NODE_TYPE_MATERIALIZEDSCAN;
    }
    return PLAN_NODE_TYPE_INVALID;
}

string expressionToString(ExpressionType type)
{
    switch (type) {
    case EXPRESSION_TYPE_INVALID: {
        return "INVALID";
    }
    case EXPRESSION_TYPE_OPERATOR_PLUS: {
        return "OPERATOR_PLUS";
    }
    case EXPRESSION_TYPE_OPERATOR_MINUS: {
        return "OPERATOR_MINUS";
    }
    case EXPRESSION_TYPE_OPERATOR_MULTIPLY: {
        return "OPERATOR_MULTIPLY";
    }
    case EXPRESSION_TYPE_OPERATOR_DIVIDE: {
        return "OPERATOR_DIVIDE";
    }
    case EXPRESSION_TYPE_OPERATOR_CONCAT: {
        return "OPERATOR_CONCAT";
    }
    case EXPRESSION_TYPE_OPERATOR_MOD: {
        return "OPERATOR_MOD";
    }
    case EXPRESSION_TYPE_OPERATOR_CAST: {
        return "OPERATOR_CAST";
    }
    case EXPRESSION_TYPE_OPERATOR_NOT: {
        return "OPERATOR_NOT";
    }
    case EXPRESSION_TYPE_OPERATOR_IS_NULL: {
        return "OPERATOR_IS_NULL";
    }
    case EXPRESSION_TYPE_COMPARE_EQUAL: {
        return "COMPARE_EQUAL";
    }
    case EXPRESSION_TYPE_COMPARE_NOTEQUAL: {
        return "COMPARE_NOT_EQUAL";
    }
    case EXPRESSION_TYPE_COMPARE_LESSTHAN: {
        return "COMPARE_LESSTHAN";
    }
    case EXPRESSION_TYPE_COMPARE_GREATERTHAN: {
        return "COMPARE_GREATERTHAN";
    }
    case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO: {
        return "COMPARE_LESSTHANOREQUALTO";
    }
    case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO: {
        return "COMPARE_GREATERTHANOREQUALTO";
    }
    case EXPRESSION_TYPE_COMPARE_LIKE: {
        return "COMPARE_LIKE";
    }
    case EXPRESSION_TYPE_COMPARE_IN: {
        return "COMPARE_IN";
    }
    case EXPRESSION_TYPE_CONJUNCTION_AND: {
        return "CONJUNCTION_AND";
    }
    case EXPRESSION_TYPE_CONJUNCTION_OR: {
        return "CONJUNCTION_OR";
    }
    case EXPRESSION_TYPE_VALUE_CONSTANT: {
        return "VALUE_CONSTANT";
    }
    case EXPRESSION_TYPE_VALUE_PARAMETER: {
        return "VALUE_PARAMETER";
    }
    case EXPRESSION_TYPE_VALUE_TUPLE: {
        return "VALUE_TUPLE";
    }
    case EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS: {
        return "VALUE_TUPLE_ADDRESS";
    }
    case EXPRESSION_TYPE_VALUE_NULL: {
        return "VALUE_NULL";
    }
    case EXPRESSION_TYPE_AGGREGATE_COUNT: {
        return "AGGREGATE_COUNT";
    }
    case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR: {
        return "AGGREGATE_COUNT_STAR";
    }
    case EXPRESSION_TYPE_AGGREGATE_SUM: {
        return "AGGREGATE_SUM";
    }
    case EXPRESSION_TYPE_AGGREGATE_MIN: {
        return "AGGREGATE_MIN";
    }
    case EXPRESSION_TYPE_AGGREGATE_MAX: {
        return "AGGREGATE_MAX";
    }
    case EXPRESSION_TYPE_AGGREGATE_AVG: {
        return "AGGREGATE_AVG";
    }
    case EXPRESSION_TYPE_FUNCTION: {
        return "FUNCTION";
    }
    case EXPRESSION_TYPE_VALUE_VECTOR: {
        return "VALUE_VECTOR";
    }
    case EXPRESSION_TYPE_HASH_RANGE: {
        return "HASH_RANGE";
    }
    case EXPRESSION_TYPE_OPERATOR_CASE_WHEN: {
        return "OPERATOR_CASE_WHEN";
    }
    case EXPRESSION_TYPE_OPERATOR_ALTERNATIVE: {
        return "OPERATOR_ALTERNATIVE";
    }
    }
    return "INVALID";
}

ExpressionType stringToExpression(string str )
{
    if (str == "INVALID") {
        return EXPRESSION_TYPE_INVALID;
    } else if (str == "OPERATOR_PLUS") {
        return EXPRESSION_TYPE_OPERATOR_PLUS;
    } else if (str == "OPERATOR_MINUS") {
        return EXPRESSION_TYPE_OPERATOR_MINUS;
    } else if (str == "OPERATOR_MULTIPLY") {
        return EXPRESSION_TYPE_OPERATOR_MULTIPLY;
    } else if (str == "OPERATOR_DIVIDE") {
        return EXPRESSION_TYPE_OPERATOR_DIVIDE;
    } else if (str == "OPERATOR_CONCAT") {
        return EXPRESSION_TYPE_OPERATOR_CONCAT;
    } else if (str == "OPERATOR_MOD") {
        return EXPRESSION_TYPE_OPERATOR_MOD;
    } else if (str == "OPERATOR_CAST") {
        return EXPRESSION_TYPE_OPERATOR_CAST;
    } else if (str == "OPERATOR_NOT") {
        return EXPRESSION_TYPE_OPERATOR_NOT;
    } else if (str == "OPERATOR_IS_NULL") {
        return EXPRESSION_TYPE_OPERATOR_IS_NULL;
    } else if (str == "COMPARE_EQUAL") {
        return EXPRESSION_TYPE_COMPARE_EQUAL;
    } else if (str == "COMPARE_NOTEQUAL") {
        return EXPRESSION_TYPE_COMPARE_NOTEQUAL;
    } else if (str == "COMPARE_LESSTHAN") {
        return EXPRESSION_TYPE_COMPARE_LESSTHAN;
    } else if (str == "COMPARE_GREATERTHAN") {
        return EXPRESSION_TYPE_COMPARE_GREATERTHAN;
    } else if (str == "COMPARE_LESSTHANOREQUALTO") {
        return EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO;
    } else if (str == "COMPARE_GREATERTHANOREQUALTO") {
        return EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO;
    } else if (str == "COMPARE_LIKE") {
        return EXPRESSION_TYPE_COMPARE_LIKE;
    } else if (str == "COMPARE_IN") {
        return EXPRESSION_TYPE_COMPARE_IN;
    } else if (str == "CONJUNCTION_AND") {
        return EXPRESSION_TYPE_CONJUNCTION_AND;
    } else if (str == "CONJUNCTION_OR") {
        return EXPRESSION_TYPE_CONJUNCTION_OR;
    } else if (str == "VALUE_CONSTANT") {
        return EXPRESSION_TYPE_VALUE_CONSTANT;
    } else if (str == "VALUE_PARAMETER") {
        return EXPRESSION_TYPE_VALUE_PARAMETER;
    } else if (str == "VALUE_TUPLE") {
        return EXPRESSION_TYPE_VALUE_TUPLE;
    } else if (str == "VALUE_TUPLE_ADDRESS") {
        return EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS;
    } else if (str == "VALUE_NULL") {
        return EXPRESSION_TYPE_VALUE_NULL;
    } else if (str == "AGGREGATE_COUNT") {
        return EXPRESSION_TYPE_AGGREGATE_COUNT;
    } else if (str == "AGGREGATE_COUNT_STAR") {
        return EXPRESSION_TYPE_AGGREGATE_COUNT_STAR;
    } else if (str == "AGGREGATE_SUM") {
        return EXPRESSION_TYPE_AGGREGATE_SUM;
    } else if (str == "AGGREGATE_MIN") {
        return EXPRESSION_TYPE_AGGREGATE_MIN;
    } else if (str == "AGGREGATE_MAX") {
        return EXPRESSION_TYPE_AGGREGATE_MAX;
    } else if (str == "AGGREGATE_AVG") {
        return EXPRESSION_TYPE_AGGREGATE_AVG;
    } else if (str == "FUNCTION") {
        return EXPRESSION_TYPE_FUNCTION;
    } else if (str == "VALUE_VECTOR") {
        return EXPRESSION_TYPE_VALUE_VECTOR;
    } else if (str == "HASH_RANGE") {
        return EXPRESSION_TYPE_HASH_RANGE;
    } else if (str == "OPERATOR_CASE_WHEN") {
        return EXPRESSION_TYPE_OPERATOR_CASE_WHEN;
    } else if (str == "OPERATOR_ALTERNATIVE") {
        return EXPRESSION_TYPE_OPERATOR_ALTERNATIVE;
    }

    return EXPRESSION_TYPE_INVALID;
}

string indexLookupToString(IndexLookupType type)
{
    switch (type) {
    case INDEX_LOOKUP_TYPE_INVALID: {
        return "INVALID";
    }
    case INDEX_LOOKUP_TYPE_EQ: {
        return "EQ";
    }
    case INDEX_LOOKUP_TYPE_GT: {
        return "GT";
    }
    case INDEX_LOOKUP_TYPE_GTE: {
        return "GTE";
    }
    case INDEX_LOOKUP_TYPE_LT: {
        return "LT";
    }
    case INDEX_LOOKUP_TYPE_LTE: {
        return "LTE";
    }
    case INDEX_LOOKUP_TYPE_GT_LT: {
        return "GT_LT";
    }
    case INDEX_LOOKUP_TYPE_GTE_LT: {
        return "GTE_LT";
    }
    case INDEX_LOOKUP_TYPE_GTL_TE: {
        return "GTL_TE";
    }
    case INDEX_LOOKUP_TYPE_GTE_LTE: {
        return "GTE_LTE";
    }
    }
    return "INVALID";
}

IndexLookupType stringToIndexLookup(string str )
{
    if (str == "INVALID") {
        return INDEX_LOOKUP_TYPE_INVALID;
    } else if (str == "EQ") {
        return INDEX_LOOKUP_TYPE_EQ;
    } else if (str == "GT") {
        return INDEX_LOOKUP_TYPE_GT;
    }  else if (str == "GTE") {
        return INDEX_LOOKUP_TYPE_GTE;
    }  else if (str == "LT") {
        return INDEX_LOOKUP_TYPE_LT;
    }  else if (str == "LTE") {
        return INDEX_LOOKUP_TYPE_LTE;
    } else if (str == "GT_LT") {
        return INDEX_LOOKUP_TYPE_GT_LT;
    }  else if (str == "GTE_LT") {
        return INDEX_LOOKUP_TYPE_GTE_LT;
    }  else if (str == "GTL_TE") {
        return INDEX_LOOKUP_TYPE_GTL_TE;
    }  else if (str == "GTE_LTE") {
        return INDEX_LOOKUP_TYPE_GTE_LTE;
    }
    return INDEX_LOOKUP_TYPE_INVALID;
}

/** takes in 0-F, returns 0-15 */
int32_t hexCharToInt(char c) {
    c = static_cast<char>(toupper(c));
    if ((c < '0' || c > '9') && (c < 'A' || c > 'F')) {
        return -1;
    }
    int32_t retval;
    if (c >= 'A')
        retval = c - 'A' + 10;
    else
        retval = c - '0';
    assert(retval >=0 && retval < 16);
    return retval;
}

int64_t getMaxTypeValue (ValueType type) {
    switch(type) {
    case VALUE_TYPE_TINYINT:
        return static_cast<int64_t>(INT8_MAX);
    case VALUE_TYPE_SMALLINT:
        return static_cast<int64_t>(INT16_MAX);
    case VALUE_TYPE_INTEGER:
        return static_cast<int64_t>(INT32_MAX);
    case VALUE_TYPE_BIGINT:
        return static_cast<int64_t>(INT64_MAX);
    default:
        return static_cast<int64_t>(-1);
    }
}

bool hexDecodeToBinary(unsigned char *bufferdst, const char *hexString) {
    assert (hexString);
    size_t len = strlen(hexString);
    if ((len % 2) != 0)
        return false;

    int32_t i;
    for (i = 0; i < len / 2; i++) {
        int32_t high = hexCharToInt(hexString[i * 2]);
        int32_t low = hexCharToInt(hexString[i * 2 + 1]);
        if ((high == -1) || (low == -1))
            return false;

        int32_t result = high * 16 + low;
        assert (result >= 0 && result < 256);
        bufferdst[i] = static_cast<unsigned char>(result);
    }
    return true;
}

// namespace voltdb
}
