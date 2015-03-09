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

package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 *
 */
public enum QueryType {
    INVALID     (0), // used for parsing
    NOOP        (1),
    SELECT      (2),
    INSERT      (3),
    UPDATE      (4),
    DELETE      (5),
    UPSERT      (6);

    QueryType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, QueryType> idx_lookup = new HashMap<Integer, QueryType>();
    protected static final Map<String, QueryType> name_lookup = new HashMap<String, QueryType>();
    static {
        for (QueryType vt : EnumSet.allOf(QueryType.class)) {
            QueryType.idx_lookup.put(vt.ordinal(), vt);
            QueryType.name_lookup.put(vt.name().intern(), vt);
        }
    }

    public static Map<Integer, QueryType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, QueryType> getNameMap() {
        return name_lookup;
    }

    public static QueryType get(Integer idx) {
        QueryType ret = QueryType.idx_lookup.get(idx);
        return (ret == null ? QueryType.INVALID : ret);
    }

    public static QueryType get(String name) {
        QueryType ret = QueryType.name_lookup.get(name.intern());
        return (ret == null ? QueryType.INVALID : ret);
    }

    /**
     * Determine what kind of SQL statement the given text represents.
     * Don't use rocket science or anything, use the first word mostly.
     * Code moved from deeper in the planner to here to be more general.
     * @param stmt String of SQL
     * @return Type of query
     */
    public static QueryType getFromSQL(String stmt) {
        // trim the front whitespace, substring to the first word and normalize
        stmt = StringUtils.stripStart(stmt, null).substring(0, 6).toLowerCase();

        // determine the type of the query
        if (stmt.startsWith("insert")) {
            return QueryType.INSERT;
        }
        else if (stmt.startsWith("update")) {
            return QueryType.UPDATE;
        }
        else if (stmt.startsWith("delete") || stmt.startsWith("trunca")) {
            return QueryType.DELETE;
        }
        else if (stmt.startsWith("select")) {
            // This covers simple select statements as well as UNIONs and other set operations that are being used with default precedence
            // as in "select ... from ... UNION select ... from ...;"
            // Even if set operations are not currently supported, let them pass as "select" statements to let the parser sort them out.
            return QueryType.SELECT;
        }
        else if (stmt.startsWith("upsert")) {
            return QueryType.UPSERT;
        }
        else if (stmt.startsWith("(")) {
            // There does not seem to be a need to support parenthesized DML statements, so assume a read-only statement.
            // If that assumption is wrong, then it has probably gotten to the point that we want to drop this up-front
            // logic in favor of relying on the full parser/planner to determine the cataloged query type and read-only-ness.
            // Parenthesized query statements are typically complex set operations (UNIONS, etc.)
            // requiring parenthesis to explicitly determine precedence,
            // but they MAY be as simple as a needlessly parenthesized single select statement:
            // "( select * from table );" is valid SQL.
            // So, assume QueryType.SELECT.
            // If set operations require their own QueryType in the future, that's probably another case
            // motivating diving right in to the full parser/planner without this pre-check.
            // We don't want to be re-implementing the parser here -- this has already gone far enough.
            return QueryType.SELECT;
        }
        // else:
        // All the known statements are handled above, so default to cataloging an invalid read-only statement
        // and leave it to the parser/planner to more intelligently reject the statement as unsupported.
        return QueryType.INVALID;
    }

    public boolean isReadOnly() {
        return (this == SELECT) || (this == NOOP) || (this == INVALID);
    }
}
