/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef MATERIALIZEDVIEWUTIL_H
#define MATERIALIZEDVIEWUTIL_H

namespace voltdb {

struct MaterializedViewUtil {
    static std::string sourceTableDidact(std::string const& statementSQL,
            std::string const& tableName) {
        std::string result = statementSQL;
        // This dummy head start string should be no longer than the minimum
        // allowed from the SELECT to the FROM in materialized view definitions.
        // Aside from a small performance boost, starting after 0 also makes it
        // safe to do a one-character look-behind to check that we are not
        // matching the table name to the middle of some other symbol.
        static const size_t HEAD_START = strlen("select count(*) from");
        static const string TRACER("@");
        static const string SYMBOL_CHARS(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz");
        size_t length = tableName.size();
        size_t pos = HEAD_START;
        while ((pos = result.find(tableName, pos+1)) != std::string::npos) {
            if (result.find_first_of(SYMBOL_CHARS, pos-1) == 0) {
                // False positive -- the table name symbol was not preceded
                // by a proper symbol separator in the statement -- it matched
                // only a piece of a longer symbol.
                // E.g. for "SELECT ... FROM DEPT ... GROUP BY DEPT.HEADOFDEPT"
                // we want "SELECT ... FROM @ ... GROUP BY @.HEADOFDEPT"
                // and not "SELECT ... FROM @ ... GROUP BY @.HEADOF@"
                continue;
            }
            if (result.find_first_of(SYMBOL_CHARS, pos + length) == pos + length) {
                // False positive -- the table name symbol was not terminated
                // in the statement -- it matched a prefix of another symbol.
                // E.g. for "SELECT ... FROM DEPT ... GROUP BY DEPT_ID".
                // we want "SELECT ... FROM @ ... GROUP BY DEPT_ID"
                // and not "SELECT ... FROM @ ... GROUP BY @_ID"
                continue;
            }
            result = result.replace(pos, length, TRACER);
        }
        return result;
    }
};

}// namespace voltdb

#endif // MATERIALIZEDVIEWUTIL_H
