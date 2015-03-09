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

#include <string>
#include <limits>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "StreamPredicateList.h"
#include "expressions/abstractexpression.h"
#include "common/PlannerDomValue.h"

namespace voltdb
{

/*
 * Produce a list of StreamPredicateHashRange objects by parsing predicate range strings.
 * Add error messages to errmsg.
 * Return true on success.
 */
bool StreamPredicateList::parseStrings(
        const std::vector<std::string> &predicateStrings,
        std::ostringstream& errmsg,
        std::vector<bool> &predicateDeletes)
{
    bool failed = false;
    for (std::vector<std::string>::const_iterator iter = predicateStrings.begin();
         iter != predicateStrings.end(); ++iter) {
        bool predFailed = false;
        std::string predicateString = *iter;
        if (!predicateString.empty()) {
            try {
                PlannerDomRoot domRoot((*iter).c_str());
                if (!domRoot.isNull()) {
                    PlannerDomValue predicateObject = domRoot.rootObject();

                    predicateDeletes.push_back(predicateObject.valueForKey("triggersDelete").asBool());

                    AbstractExpression *expr = NULL;
                    if (predicateObject.hasKey("predicateExpression")) {
                        expr = AbstractExpression::buildExpressionTree(
                                    predicateObject.valueForKey("predicateExpression"));
                        if (expr != NULL) {
                            // Got ourselves a predicate expression tree!
                            push_back(expr);
                        }
                        else {
                            errmsg << "Predicate JSON generated a NULL expression tree";
                            predFailed = true;
                        }
                    }
                    else {
                        // NULL represents an empty predicate object that should not be evaluated.
                        push_back(NULL);
                    }
                }
                else {
                    errmsg << "Stream predicate JSON document is NULL";
                    predFailed = true;
                }
            }
            catch(std::exception &exc) {
                errmsg << "Exception occurred while parsing stream predicate: " << exc.what();
                predFailed = true;
            }
            if (predFailed) {
                errmsg << std::endl << (*iter) << std::endl;
                failed = true;
            }
        }
        else {
            // NULL predicates are okay.
            push_back(NULL);
        }
    }
    return !failed;
}

} // namespace voltdb
