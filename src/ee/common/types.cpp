/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include <string>
#include <map>

#include "types.h"
#include "common/FatalException.hpp"
#include "common/Pool.hpp"
#include "common/ValueFactory.hpp"

namespace voltdb {
using namespace std;

template<typename K, typename V>
inline V lookup(map<K, V> const& dictionary, K const& key, V const& defaultValue) {
   auto const iter = dictionary.find(key);
   return iter == dictionary.cend() ? defaultValue : iter->second;
}

template<typename K> // partial instantiatin
inline string lookup(map<K, string> const& dictionary, K const& key, char const* defaultValue) {
   auto const iter = dictionary.find(key);
   return iter == dictionary.cend() ? defaultValue : iter->second;
}

// TODO: when upgrade boost library, use boost::bimap
template<typename K, typename V>
map<V, K> revert(map<K, V>const& original) {
   map<V, K> reverted;
   for(auto const& kv : original) {
      reverted.emplace(make_pair(kv.second, kv.first));
   }
   return reverted;
}

map<ValueType, string> const mapOfTypeName {
   {ValueType::tTINYINT, "TINYINT"},
   {ValueType::tSMALLINT, "SMALLINT"},
   {ValueType::tINTEGER, "INTEGER"},
   {ValueType::tBIGINT, "BIGINT"},
   {ValueType::tDOUBLE, "FLOAT"},
   {ValueType::tVARCHAR, "VARCHAR"},
   {ValueType::tVARBINARY, "VARBINARY"},
   {ValueType::tTIMESTAMP, "TIMESTAMP"},
   {ValueType::tDECIMAL, "DECIMAL"},
   {ValueType::tBOOLEAN, "BOOLEAN"},
   {ValueType::tPOINT, "POINT"},
   {ValueType::tGEOGRAPHY, "GEOGRAPHY"},
   {ValueType::tADDRESS, "ADDRESS"},
   {ValueType::tINVALID, "INVALID"},
   {ValueType::tNULL, "NULL"},
   {ValueType::NumericDiagnostics, "NUMERIC"},
   {ValueType::tARRAY, "ARRAY"}
};

map<TableStreamType, string> const mapOfStreamTypeName {
   {TABLE_STREAM_SNAPSHOT, "TABLE_STREAM_SNAPSHOT"},
   {TABLE_STREAM_ELASTIC_INDEX, "TABLE_STREAM_ELASTIC_INDEX"},
   {TABLE_STREAM_ELASTIC_INDEX_READ, "TABLE_STREAM_ELASTIC_INDEX_READ"},
   {TABLE_STREAM_ELASTIC_INDEX_CLEAR, "TABLE_STREAM_ELASTIC_INDEX_CLEAR"},
   {TABLE_STREAM_NONE, "TABLE_STREAM_NONE"}
};

map<string, ValueType> const mapToValueType = revert(mapOfTypeName);

map<JoinType, string> const mapOfJoinType {
   {JOIN_TYPE_INVALID, "INVALID"},
   {JOIN_TYPE_INNER, "INNER"},
   {JOIN_TYPE_LEFT, "LEFT"},
   {JOIN_TYPE_FULL, "FULL"},
   {JOIN_TYPE_RIGHT, "RIGHT"}
};

map<string, JoinType> const mapToJoinType = revert(mapOfJoinType);

map<SortDirectionType, string> const mapOfSortDirectionType {
   {SORT_DIRECTION_TYPE_INVALID, "INVALID"},
   {SORT_DIRECTION_TYPE_ASC, "ASC"},
   {SORT_DIRECTION_TYPE_DESC, "DESC"}
};

map<string, SortDirectionType> const mapToSortDirectionType = revert(mapOfSortDirectionType);

map<PlanNodeType, string> const mapOfPlanNodeType {
   {PlanNodeType::Invalid, "INVALID"},
   {PlanNodeType::SeqScan, "SEQSCAN"},
   {PlanNodeType::IndexScan, "INDEXSCAN"},
   {PlanNodeType::IndexCount, "INDEXCOUNT"},
   {PlanNodeType::TableCount, "TABLECOUNT"},
   {PlanNodeType::Nestloop, "NESTLOOP"},
   {PlanNodeType::NestloopIndex, "NESTLOOPINDEX"},
   {PlanNodeType::MergeJoin, "MERGEJOIN"},
   {PlanNodeType::Update, "UPDATE"},
   {PlanNodeType::Insert, "INSERT"},
   {PlanNodeType::Delete, "DELETE"},
   {PlanNodeType::SwapTables, "SWAPTABLES"},
   {PlanNodeType::Migrate, "MIGRATE"},
   {PlanNodeType::Send, "SEND"},
   {PlanNodeType::Receive, "RECEIVE"},
   {PlanNodeType::MergeReceive, "MERGERECEIVE"},
   {PlanNodeType::Aggregate, "AGGREGATE"},
   {PlanNodeType::HashAggregate, "HASHAGGREGATE"},
   {PlanNodeType::PartialAggregate, "PARTIALAGGREGATE"},
   {PlanNodeType::Union, "UNION"},
   {PlanNodeType::OrderBy, "ORDERBY"},
   {PlanNodeType::Projection, "PROJECTION"},
   {PlanNodeType::Materialize, "MATERIALIZE"},
   {PlanNodeType::Limit, "LIMIT"},
   {PlanNodeType::MaterializedScan, "MATERIALIZEDSCAN"},
   {PlanNodeType::TupleScan, "TUPLESCAN"},
   {PlanNodeType::WindowFunction, "WINDOWFUNCTION"},
   {PlanNodeType::CommonTable, "COMMONTABLE"}
};

map<string, PlanNodeType> const mapToPlanNodeType = revert(mapOfPlanNodeType);

map<ExpressionType, string> const mapOfExpressionType {
   {EXPRESSION_TYPE_INVALID, "INVALID"},
   {EXPRESSION_TYPE_OPERATOR_PLUS, "OPERATOR_PLUS"},
   {EXPRESSION_TYPE_OPERATOR_MINUS, "OPERATOR_MINUS"},
   {EXPRESSION_TYPE_OPERATOR_MULTIPLY, "OPERATOR_MULTIPLY"},
   {EXPRESSION_TYPE_OPERATOR_DIVIDE, "OPERATOR_DIVIDE"},
   {EXPRESSION_TYPE_OPERATOR_CONCAT, "OPERATOR_CONCAT"},
   {EXPRESSION_TYPE_OPERATOR_MOD, "OPERATOR_MOD"},
   {EXPRESSION_TYPE_OPERATOR_CAST, "OPERATOR_CAST"},
   {EXPRESSION_TYPE_OPERATOR_NOT, "OPERATOR_NOT"},
   {EXPRESSION_TYPE_OPERATOR_IS_NULL, "OPERATOR_IS_NULL"},
   {EXPRESSION_TYPE_OPERATOR_EXISTS, "OPERATOR_EXISTS"},
   {EXPRESSION_TYPE_OPERATOR_UNARY_MINUS, "OPERATOR_UNARY_MINUS"},
   {EXPRESSION_TYPE_COMPARE_EQUAL, "COMPARE_EQUAL"},
   {EXPRESSION_TYPE_COMPARE_NOTEQUAL, "COMPARE_NOT_EQUAL"},
   {EXPRESSION_TYPE_COMPARE_LESSTHAN, "COMPARE_LESSTHAN"},
   {EXPRESSION_TYPE_COMPARE_GREATERTHAN, "COMPARE_GREATERTHAN"},
   {EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO, "COMPARE_LESSTHANOREQUALTO"},
   {EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, "COMPARE_GREATERTHANOREQUALTO"},
   {EXPRESSION_TYPE_COMPARE_LIKE, "COMPARE_LIKE"},
   {EXPRESSION_TYPE_COMPARE_STARTSWITH, "COMPARE_STARTSWITH"},
   {EXPRESSION_TYPE_COMPARE_IN, "COMPARE_IN"},
   {EXPRESSION_TYPE_COMPARE_NOTDISTINCT, "COMPARE_NOTDISTINCT"},
   {EXPRESSION_TYPE_CONJUNCTION_AND, "CONJUNCTION_AND"},
   {EXPRESSION_TYPE_CONJUNCTION_OR, "CONJUNCTION_OR"},
   {EXPRESSION_TYPE_VALUE_CONSTANT, "VALUE_CONSTANT"},
   {EXPRESSION_TYPE_VALUE_PARAMETER, "VALUE_PARAMETER"},
   {EXPRESSION_TYPE_VALUE_TUPLE, "VALUE_TUPLE"},
   {EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS, "VALUE_TUPLE_ADDRESS"},
   {EXPRESSION_TYPE_VALUE_SCALAR, "VALUE_SCALAR"},
   {EXPRESSION_TYPE_VALUE_NULL, "VALUE_NULL"},
   {EXPRESSION_TYPE_AGGREGATE_COUNT, "AGGREGATE_COUNT"},
   {EXPRESSION_TYPE_AGGREGATE_COUNT_STAR, "AGGREGATE_COUNT_STAR"},
   {EXPRESSION_TYPE_AGGREGATE_APPROX_COUNT_DISTINCT, "AGGREGATE_APPROX_COUNT_DISTINCT"},
   {EXPRESSION_TYPE_AGGREGATE_VALS_TO_HYPERLOGLOG, "AGGREGATE_VALS_TO_HYPERLOGLOG"},
   {EXPRESSION_TYPE_AGGREGATE_HYPERLOGLOGS_TO_CARD, "AGGREGATE_HYPERLOGLOGS_TO_CARD"},
   {EXPRESSION_TYPE_AGGREGATE_WINDOWED_RANK, "AGGREGATE_WINDOWED_RANK"},
   {EXPRESSION_TYPE_AGGREGATE_WINDOWED_DENSE_RANK, "AGGREGATE_WINDOWED_DENSE_RANK"},
   {EXPRESSION_TYPE_AGGREGATE_WINDOWED_ROW_NUMBER, "AGGREGATE_WINDOWED_ROW_NUMBER"},
   {EXPRESSION_TYPE_AGGREGATE_WINDOWED_COUNT, "AGGREGATE_WINDOWED_COUNT"},
   {EXPRESSION_TYPE_AGGREGATE_WINDOWED_MAX, "AGGREGATE_WINDOWED_MAX"},
   {EXPRESSION_TYPE_AGGREGATE_WINDOWED_MIN, "AGGREGATE_WINDOWED_MIN"},
   {EXPRESSION_TYPE_AGGREGATE_WINDOWED_SUM, "AGGREGATE_WINDOWED_SUM"},
   {EXPRESSION_TYPE_AGGREGATE_SUM, "AGGREGATE_SUM"},
   {EXPRESSION_TYPE_AGGREGATE_MIN, "AGGREGATE_MIN"},
   {EXPRESSION_TYPE_AGGREGATE_MAX, "AGGREGATE_MAX"},
   {EXPRESSION_TYPE_AGGREGATE_AVG, "AGGREGATE_AVG"},
   {EXPRESSION_TYPE_USER_DEFINED_AGGREGATE, "USER_DEFINED_AGGREGATE"},
   {EXPRESSION_TYPE_FUNCTION, "FUNCTION"},
   {EXPRESSION_TYPE_VALUE_VECTOR, "VALUE_VECTOR"},
   {EXPRESSION_TYPE_HASH_RANGE, "HASH_RANGE"},
   {EXPRESSION_TYPE_OPERATOR_CASE_WHEN, "OPERATOR_CASE_WHEN"},
   {EXPRESSION_TYPE_OPERATOR_ALTERNATIVE, "OPERATOR_ALTERNATIVE"},
   {EXPRESSION_TYPE_ROW_SUBQUERY, "ROW_SUBQUERY"},
   {EXPRESSION_TYPE_SELECT_SUBQUERY, "SELECT_SUBQUERY"}
};

map<string, ExpressionType> const mapToExpressionType = revert(mapOfExpressionType);

map<QuantifierType, string> const mapOfQuantifierType {
   {QUANTIFIER_TYPE_NONE, "NONE"},
   {QUANTIFIER_TYPE_ANY, "ANY"},
   {QUANTIFIER_TYPE_ALL, "ALL"}
};

map<string, QuantifierType> const mapToQuantifierType = revert(mapOfQuantifierType);

map<IndexLookupType, string> const mapOfIndexLookupType {
   {IndexLookupType::Invalid, "INVALID"},
   {IndexLookupType::Equal, "EQ"},
   {IndexLookupType::Greater, "GT"},
   {IndexLookupType::GreaterEqual, "GTE"},
   {IndexLookupType::Less, "LT"},
   {IndexLookupType::LessEqual, "LTE"},
   {IndexLookupType::GeoContains, "GEO_CONTAINS"}
};

map<string, IndexLookupType> const mapToIndexLookupType = revert(mapOfIndexLookupType);

/** Testing utility */
bool isNumeric(ValueType type) {
    switch (type) {
        case ValueType::tTINYINT:
        case ValueType::tSMALLINT:
        case ValueType::tINTEGER:
        case ValueType::tBIGINT:
        case ValueType::tDECIMAL:
        case ValueType::tDOUBLE:
            return true;
            break;
        case ValueType::tVARCHAR:
        case ValueType::tVARBINARY:
        case ValueType::tTIMESTAMP:
        case ValueType::tPOINT:
        case ValueType::tGEOGRAPHY:
        case ValueType::tNULL:
        case ValueType::tINVALID:
        case ValueType::tARRAY:
            return false;
        default:
            throw exception();
    }
    throw exception();
}

/** Used in index optimization **/
bool isIntegralType(ValueType type) {
    switch (type) {
        case ValueType::tTINYINT:
        case ValueType::tSMALLINT:
        case ValueType::tINTEGER:
        case ValueType::tBIGINT:
            return true;
            break;
        case ValueType::tDOUBLE:
        case ValueType::tVARCHAR:
        case ValueType::tVARBINARY:
        case ValueType::tTIMESTAMP:
        case ValueType::tPOINT:
        case ValueType::tGEOGRAPHY:
        case ValueType::tNULL:
        case ValueType::tDECIMAL:
        case ValueType::tARRAY:
            return false;
        default:
            throw exception();
    }
    throw exception();
}

bool isVariableLengthType(ValueType type) {
    switch (type) {
        case ValueType::tVARCHAR:
    case ValueType::tVARBINARY:
    case ValueType::tGEOGRAPHY:
        return true;
    default:
        return false;
    }
}

string getTypeName(ValueType type) {
  return lookup(mapOfTypeName, type,
         string("UNKNOWN[").append(std::to_string(static_cast<int>(type))).append("]"));
}

string tableStreamTypeToString(TableStreamType type) {
   return lookup(mapOfStreamTypeName, type, "INVALID");
}


string valueToString(ValueType type) {
   return getTypeName(type);
}

ValueType stringToValue(string const& nam) {
   return lookup(mapToValueType, nam, ValueType::tINVALID);
}

string joinToString(JoinType type) {
   return lookup(mapOfJoinType, type, "INVALID");
}

JoinType stringToJoin(string const& nam) {
   return lookup(mapToJoinType, nam, JOIN_TYPE_INVALID);
}

string sortDirectionToString(SortDirectionType type) {
   return lookup(mapOfSortDirectionType, type, "INVALID");
}

SortDirectionType stringToSortDirection(string const& nam) {
   return lookup(mapToSortDirectionType, nam, SORT_DIRECTION_TYPE_INVALID);
}

string planNodeToString(PlanNodeType type) {
   return lookup(mapOfPlanNodeType, type, "UNDEFINED");
}

PlanNodeType stringToPlanNode(string const& nam) {
   return lookup(mapToPlanNodeType, nam, PlanNodeType::Invalid);
}

string expressionToString(ExpressionType type) {
   return lookup(mapOfExpressionType, type, "INVALID");
}

ExpressionType stringToExpression(string const& str) {
   return lookup(mapToExpressionType, str, EXPRESSION_TYPE_INVALID);
}

string quantifierToString(QuantifierType type) {
   return lookup(mapOfQuantifierType, type, "INVALID");
}

QuantifierType stringToQuantifier(string const& nam) {
   return lookup(mapToQuantifierType, nam, QUANTIFIER_TYPE_NONE);
}

string indexLookupToString(IndexLookupType type) {
   return lookup(mapOfIndexLookupType, type, "INVALID");
}

IndexLookupType stringToIndexLookup(string const& nam) {
   return lookup(mapToIndexLookupType, nam, IndexLookupType::Invalid);
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
    vassert(retval >=0 && retval < 16);
    return retval;
}

int64_t getMaxTypeValue (ValueType type) {
    switch(type) {
        case ValueType::tTINYINT:
            return static_cast<int64_t>(INT8_MAX);
        case ValueType::tSMALLINT:
            return static_cast<int64_t>(INT16_MAX);
        case ValueType::tINTEGER:
            return static_cast<int64_t>(INT32_MAX);
        case ValueType::tBIGINT:
            return static_cast<int64_t>(INT64_MAX);
        default:
            return static_cast<int64_t>(-1);
    }
}

bool hexDecodeToBinary(unsigned char *bufferdst, const char *hexString) {
    vassert(hexString);
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
        vassert(result >= 0 && result < 256);
        bufferdst[i] = static_cast<unsigned char>(result);
    }
    return true;
}

// namespace voltdb
}
